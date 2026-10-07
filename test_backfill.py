"""历史事件补投（backfill）专项验证（非 Docker，便携 Postgres + 本地文件载荷后端）。

Part A：仓储层单测（直接调用 repository.backfill_subscription）：筛选条件
        （source/闭区间/已提交/未撤销/尚无初始投递）、按主键升序建 pending、
        幂等（重复与重叠区间）、暂停可补、停用 409、未知订阅 404、版本固定、
        队列 seq 接在既有队列之后。
Part B：确定性事务交错——“撤销先完成的事件不被补活”：补投事务完成快照筛选
        后、锁事件行前，另一连接提交撤销；事件行 FOR UPDATE 锁内复核必须
        剔除该事件。另验证“补投先提交、撤销后到达”时新 pending 仍被翻转。
Part C：HTTP 端到端（api + receiver + worker）：晚建订阅补投旧事件、422/404/
        409、补投结果在投递查询可见、Worker 严格按事件主键升序实际投递、
        补投边界后实时事件排在其后、重复区间不新增。
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import asyncpg
import httpx
import pgserver

ROOT = Path(__file__).resolve().parent
VENV_PY = os.environ.get("VENV_PY", "/tmp/venv/bin/python")

COMMON_ENV = {
    "LEASE_SECONDS": "30",
    "HTTP_TIMEOUT_SECONDS": "5",
    "WORKER_POLL_SECONDS": "0.1",
    "MAX_ATTEMPTS": "6",
    "BACKOFF_BASE_SECONDS": "1",
    "BACKOFF_CAP_SECONDS": "8",
    "RECEIVER_SECRET": "dev-signing-secret",
    "RECEIVER_TIMESTAMP_TOLERANCE_SECONDS": "600",
    "WEBHOOK_ALLOWED_HOSTS": "127.0.0.1,localhost",
    "WEBHOOK_ALLOWED_CIDRS": "",
    "WEBHOOK_MAX_REDIRECTS": "5",
    "WEBHOOK_KEYRING_JSON": '{"active_key_id":"k1","keys":{"k1":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}}',
    "PAYLOAD_STORE_MODE": "local",
    "PAYLOAD_LOCAL_DIR": str(ROOT / ".test-payloads-backfill"),
    "PYTHONUNBUFFERED": "1",
}

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        failures.append(name)


# ---------------------------------------------------------------------------
# Part A/B：仓储层 + 确定性事务交错
# ---------------------------------------------------------------------------


async def _connect(dsn: str):
    conn = await asyncpg.connect(dsn)
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    return conn


async def _make_subscription(conn, source: str, *, url: str, active: bool = True,
                             paused: bool = False, revision: int = 1) -> int:
    sid = (
        await conn.fetchrow(
            """
            INSERT INTO subscriptions
                (source, target_url, secret, active, paused, current_revision)
            VALUES ($1, $2, '', $3, $4, $5)
            RETURNING id
            """,
            source, url, active, paused, revision,
        )
    )["id"]
    for rev in range(1, revision + 1):
        await conn.execute(
            """
            INSERT INTO subscription_versions
                (subscription_id, revision, target_url, secret, max_attempts,
                 backoff_base_seconds, backoff_cap_seconds)
            VALUES ($1, $2, $3, '', 6, 1.0, 8.0)
            """,
            sid, rev, url,
        )
    if revision > 1:
        await conn.execute(
            "UPDATE subscription_versions SET superseded_at=now() "
            "WHERE subscription_id=$1 AND revision<$2",
            sid, revision,
        )
    return sid


async def _make_event(conn, source: str, eid: str, *, cancelled: bool = False) -> int:
    eid_pk = (
        await conn.fetchrow(
            "INSERT INTO events (source, event_id, payload) VALUES ($1,$2,'{}'::jsonb) "
            "RETURNING id",
            source, eid,
        )
    )["id"]
    if cancelled:
        await conn.execute("UPDATE events SET cancelled_at=now() WHERE id=$1", eid_pk)
    return eid_pk


async def _initial_delivery(conn, event_pk: int, sid: int, status: str = "succeeded") -> None:
    ver = await conn.fetchval(
        "SELECT id FROM subscription_versions WHERE subscription_id=$1 "
        "ORDER BY revision DESC LIMIT 1",
        sid,
    )
    await conn.execute(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id, chain_id, chain_seq, status)
        VALUES ($1, $2, $3, nextval('delivery_chain_seq'), 1, $4)
        """,
        event_pk, sid, ver, status,
    )


async def _db_tests(dsn: str) -> None:
    from app import repository as repo
    from app.db import init_schema

    conn = await _connect(dsn)
    await init_schema(conn)
    source = "bfdb"

    # 订阅与事件：sid_late 在事件之后才“注册”（无初始投递）；sid_existing 对 e2
    # 已有初始投递。
    e1 = await _make_event(conn, source, "e1")
    e2 = await _make_event(conn, source, "e2")
    e3 = await _make_event(conn, source, "e3", cancelled=True)
    e4 = await _make_event(conn, source, "e4")
    e_other = await _make_event(conn, "other-src", "x")

    sid_late = await _make_subscription(conn, source, url="http://127.0.0.1:9/late")
    sid_existing = await _make_subscription(
        conn, source, url="http://127.0.0.1:9/existing"
    )
    sid_paused = await _make_subscription(
        conn, source, url="http://127.0.0.1:9/paused", paused=True
    )
    sid_inactive = await _make_subscription(
        conn, source, url="http://127.0.0.1:9/off", active=False
    )
    sid_rev2 = await _make_subscription(
        conn, source, url="http://127.0.0.1:9/rev2", revision=2
    )

    await _initial_delivery(conn, e2, sid_existing)

    # A1：筛选——闭区间内、本 source、未撤销、尚无初始投递
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_late, first_event_id=e1, last_event_id=e4
    )
    check(
        "A1 补建 e1/e4（跳过已撤销 e3），e2 属同 source 且无投递也补建",
        r["created"] == 3 and r["config_revision"] == 1,
        str(r),
    )
    rows = await conn.fetch(
        "SELECT event_id_fk FROM deliveries WHERE subscription_id=$1 ORDER BY seq",
        sid_late,
    )
    check(
        "A1 补投段按事件主键升序",
        [x["event_id_fk"] for x in rows] == [e1, e2, e4],
        str([x["event_id_fk"] for x in rows]),
    )
    check(
        "A1 全部为 pending 初始投递(chain_seq=1)",
        all(
            x["status"] == "pending" and x["chain_seq"] == 1
            for x in await conn.fetch(
                "SELECT status, chain_seq FROM deliveries WHERE subscription_id=$1",
                sid_late,
            )
        ),
    )

    # A2：区间边界是闭区间且只覆盖指定范围
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_existing, first_event_id=e1, last_event_id=e1
    )
    check("A2 闭区间单点补建 e1（e2 已有不影响 e1）", r["created"] == 1, str(r))

    # A3：幂等——完全相同区间
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_late, first_event_id=e1, last_event_id=e4
    )
    check("A3 重复区间不新增", r["created"] == 0, str(r))
    # 重叠区间（全部已被覆盖）
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_late, first_event_id=e1, last_event_id=e2
    )
    check("A3 重叠区间不新增", r["created"] == 0, str(r))
    # 跨已撤销事件的区间也不新增
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_late, first_event_id=e3, last_event_id=e3
    )
    check("A3 单指已撤销事件不新增", r["created"] == 0, str(r))
    n_total = await conn.fetchval(
        "SELECT count(*) FROM deliveries WHERE subscription_id=$1", sid_late
    )
    check("A3 sid_late 投递总数仍为 3", n_total == 3, str(n_total))

    # A4：不跨 source（other-src 的事件绝不补到 source 订阅）
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_late,
        first_event_id=min(e1, e_other), last_event_id=max(e1, e_other),
    )
    leaked = await conn.fetchval(
        "SELECT count(*) FROM deliveries d JOIN events e ON e.id=d.event_id_fk "
        "WHERE d.subscription_id=$1 AND e.source='other-src'",
        sid_late,
    )
    check("A4 不补其他 source 的事件", r["created"] == 0 and leaked == 0, str(r))

    # A5：暂停订阅可补投（建 pending）
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_paused, first_event_id=e1, last_event_id=e4
    )
    check("A5 暂停订阅补建 e1/e2/e4", r["created"] == 3, str(r))
    # 终结其他活跃订阅的全部 pending，使后续候选只能来自暂停订阅：
    # 严格顺序下只要暂停订阅不被认领，claim_delivery 必须返回 None。
    await conn.execute(
        "UPDATE deliveries SET status='succeeded' WHERE status='pending' "
        "AND subscription_id <> $1",
        sid_paused,
    )
    # 多认领几轮，确保没有任何暂停订阅的投递被选中
    claims = [await repo.claim_delivery(conn, f"bf-probe-{i}") for i in range(3)]
    paused_pending = await conn.fetchval(
        "SELECT count(*) FROM deliveries WHERE subscription_id=$1 AND status='pending'",
        sid_paused,
    )
    check(
        "A5 暂停补投的 pending 不被认领",
        all(c is None for c in claims) and paused_pending == 3,
        f"claims={claims}, pending={paused_pending}",
    )

    # A6：停用 -> BackfillError(subscription_inactive)；未知 -> LookupError
    try:
        await repo.backfill_subscription(
            conn, subscription_id=sid_inactive, first_event_id=e1, last_event_id=e4
        )
        check("A6 停用订阅抛 BackfillError", False)
    except repo.BackfillError as exc:
        check("A6 停用订阅抛 subscription_inactive", exc.code == "subscription_inactive")
    try:
        await repo.backfill_subscription(
            conn, subscription_id=99999999, first_event_id=1, last_event_id=2
        )
        check("A6 未知订阅抛 LookupError", False)
    except LookupError:
        check("A6 未知订阅抛 LookupError", True)

    # A7：版本固定——revision=2 订阅的补投全部钉 rev2
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_rev2, first_event_id=e1, last_event_id=e4
    )
    rev_used = await conn.fetchval(
        "SELECT min(v.revision) FROM deliveries d "
        "JOIN subscription_versions v ON v.id=d.config_version_id "
        "WHERE d.subscription_id=$1",
        sid_rev2,
    )
    rev_max = await conn.fetchval(
        "SELECT max(v.revision) FROM deliveries d "
        "JOIN subscription_versions v ON v.id=d.config_version_id "
        "WHERE d.subscription_id=$1",
        sid_rev2,
    )
    check(
        "A7 补投固定当前版本 rev2（整批同一版本）",
        r["config_revision"] == 2 and rev_used == 2 and rev_max == 2,
        str(r),
    )

    # A8：队列顺序——补投接在既有队列之后（seq 更大）
    # sid_existing 原先 e2 已 succeeded（seq 最小），后补 e1。
    seqs = {
        x["event_id_fk"]: x["seq"]
        for x in await conn.fetch(
            "SELECT event_id_fk, seq FROM deliveries WHERE subscription_id=$1",
            sid_existing,
        )
    }
    check(
        "A8 既有投递 seq 在前、补投在后",
        seqs[e2] < seqs[e1],
        str(seqs),
    )

    # ---- B：撤销/补投先后裁决（确定性交错） ----
    # 新订阅 + 两个无投递事件，手动开补投事务并在“快照后、事件行锁前”让另一
    # 连接提交撤销，验证锁内复核剔除。
    sid_b = await _make_subscription(conn, source, url="http://127.0.0.1:9/b")
    b1 = await _make_event(conn, source, "b1")
    b2 = await _make_event(conn, source, "b2")

    tx = conn.transaction()
    await tx.start()
    await conn.fetchrow("SELECT id FROM subscriptions WHERE id=$1 FOR UPDATE", sid_b)
    # 复刻仓储函数在闭区间 [b1,b2] 上的快照筛选
    snap = [
        x["id"]
        for x in await conn.fetch(
            """
            SELECT e.id FROM events e
            WHERE e.source=$1 AND e.id BETWEEN $3 AND $4 AND e.cancelled_at IS NULL
              AND NOT EXISTS (SELECT 1 FROM deliveries d
                              WHERE d.event_id_fk=e.id AND d.subscription_id=$2
                                AND d.chain_seq=1)
            ORDER BY e.id
            """,
            source, sid_b, b1, b2,
        )
    ]
    check("B 补投快照同时见到 b1/b2", snap == [b1, b2], str(snap))

    other = await _connect(dsn)
    await repo.cancel_event(other, b1)  # 该订阅尚无投递，不与订阅行锁冲突，可提交
    await other.close()

    locked = [
        x["id"]
        for x in await conn.fetch(
            """
            SELECT e.id
            FROM unnest($1::bigint[]) WITH ORDINALITY AS k(id, ord)
            JOIN events e ON e.id=k.id
            WHERE e.cancelled_at IS NULL
            ORDER BY k.ord
            FOR UPDATE OF e
            """,
            snap,
        )
    ]
    check("B 撤销先提交：事件行锁复核剔除 b1", locked == [b2], str(locked))
    await tx.rollback()

    # 直接用仓储函数重放：b1 已撤销，只补 b2
    r = await repo.backfill_subscription(
        conn, subscription_id=sid_b, first_event_id=b1, last_event_id=b2
    )
    check("B 仓储函数对已撤销区间只补未撤销事件", r["created"] == 1, str(r))
    got = [
        x["event_id_fk"]
        for x in await conn.fetch(
            "SELECT event_id_fk FROM deliveries WHERE subscription_id=$1", sid_b
        )
    ]
    check("B 已撤销 b1 未被补活", got == [b2], str(got))

    # 补投先提交、撤销后到达：新 pending 被翻转为 cancelled
    b3 = await _make_event(conn, source, "b3")
    await repo.backfill_subscription(
        conn, subscription_id=sid_b, first_event_id=b3, last_event_id=b3
    )
    await repo.cancel_event(conn, b3)
    st = await conn.fetchval(
        "SELECT status FROM deliveries WHERE event_id_fk=$1 AND subscription_id=$2",
        b3, sid_b,
    )
    check("B 补投后撤销：补建的 pending 转 cancelled", st == "cancelled", st)

    await conn.close()


# ---------------------------------------------------------------------------
# Part C：HTTP 端到端
# ---------------------------------------------------------------------------


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.3)
    raise RuntimeError(f"服务未就绪: {url} ({last})")


def wait_for(condition, desc: str, timeout: float = 30.0, interval: float = 0.3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc}")


class Harness:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def spawn(self, module_cmd: list[str], name: str):
        env = os.environ.copy()
        env.update(COMMON_ENV)
        env["DATABASE_URL"] = self.dsn
        log = open(ROOT / f"test-backfill-{name}.log", "w")
        proc = subprocess.Popen(
            [VENV_PY, "-m", *module_cmd],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        self.procs.append(proc)

    def start(self):
        self.pg = pgserver.get_server(
            str(ROOT / ".test-pgdata-backfill"), cleanup_mode="stop"
        )
        self.dsn = self.pg.get_uri()
        api_port = free_port()
        rcv_port = free_port()
        self.api = f"http://127.0.0.1:{api_port}"
        self.rcv = f"http://127.0.0.1:{rcv_port}"
        self.spawn(
            ["uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(api_port)],
            "api",
        )
        self.spawn(
            ["uvicorn", "app.receiver:app", "--host", "127.0.0.1", "--port", str(rcv_port)],
            "receiver",
        )
        self.spawn(["app.worker"], "worker")
        wait_http(f"{self.api}/health")
        wait_http(f"{self.rcv}/health")

    def stop(self):
        for p in self.procs:
            p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        try:
            self.pg.cleanup()
        except Exception:  # noqa: BLE001
            pass


def run_http_tests(h: Harness) -> None:
    api = httpx.Client(base_url=h.api, timeout=10)
    rcv = httpx.Client(base_url=h.rcv, timeout=10)
    rcv.post("/received/reset")

    source = "bfhttp"

    # 1) 先提交 4 个旧事件（此时尚无订阅），撤销其中第 3 个
    event_pks: list[int] = []
    for i in range(4):
        r = api.post(
            "/api/v1/events",
            json={"source": source, "event_id": f"old-{i}", "payload": {"n": i}},
        )
        assert r.status_code == 201, r.text
        event_pks.append(r.json()["id"])
    check("C 提交 4 个旧事件（订阅尚未创建）", len(event_pks) == 4)
    cr = api.post(f"/api/v1/events/{event_pks[2]}/cancel")
    check("C 撤销 old-2", cr.status_code == 200 and bool(cr.json()["cancelled_at"]))

    # 2) 非法区间 / 缺字段 -> 422（先建订阅以排除 404 干扰）
    sub = api.post(
        "/api/v1/subscriptions",
        json={
            "source": source,
            "target_url": f"{h.rcv}/receive",
            "secret": "dev-signing-secret",
            "backoff_base_seconds": 1,
            "backoff_cap_seconds": 8,
        },
    )
    check("C 晚建订阅 201", sub.status_code == 201, sub.text)
    sid = sub.json()["id"]

    r = api.post(
        f"/api/v1/subscriptions/{sid}/backfill",
        json={"first_event_id": 10, "last_event_id": 2},
    )
    check("C first>last -> 422", r.status_code == 422, r.text)
    r = api.post(
        f"/api/v1/subscriptions/{sid}/backfill",
        json={"first_event_id": 0, "last_event_id": 2},
    )
    check("C first<1 -> 422", r.status_code == 422)
    r = api.post(
        f"/api/v1/subscriptions/{sid}/backfill", json={"first_event_id": 1}
    )
    check("C 缺 last_event_id -> 422", r.status_code == 422)

    # 3) 未知订阅 -> 404
    r = api.post(
        "/api/v1/subscriptions/999999/backfill",
        json={"first_event_id": 1, "last_event_id": 99},
    )
    check("C 未知订阅 -> 404", r.status_code == 404, r.text)

    # 4) 正式补投：应补 old-0/old-1/old-3（old-2 已撤销）
    r = api.post(
        f"/api/v1/subscriptions/{sid}/backfill",
        json={"first_event_id": event_pks[0], "last_event_id": event_pks[-1]},
    )
    body = r.json()
    check(
        "C 补投返回 200 且 created=3、固定 rev1",
        r.status_code == 200 and body["created"] == 3
        and body["config_revision"] == 1
        and body["subscription_id"] == sid,
        r.text,
    )

    # 5) 投递查询/事件投递可见
    deliveries = api.get(
        "/api/v1/deliveries", params={"subscription_id": sid}
    ).json()
    check(
        "C 投递历史可见 3 条 pending",
        len(deliveries) == 3 and all(d["status"] == "pending" for d in deliveries),
        str([(d["source_event_id"], d["status"]) for d in deliveries]),
    )
    check(
        "C 已撤销事件无投递",
        len(api.get(f"/api/v1/events/{event_pks[2]}/deliveries").json()) == 0,
    )

    # 6) 幂等：重复/重叠区间不新增
    r = api.post(
        f"/api/v1/subscriptions/{sid}/backfill",
        json={"first_event_id": event_pks[0], "last_event_id": event_pks[-1]},
    )
    check("C 重复区间 created=0", r.json()["created"] == 0, r.text)
    r = api.post(
        f"/api/v1/subscriptions/{sid}/backfill",
        json={"first_event_id": event_pks[0], "last_event_id": event_pks[1]},
    )
    check("C 重叠区间 created=0", r.json()["created"] == 0)
    check(
        "C 投递总数不变",
        len(api.get("/api/v1/deliveries", params={"subscription_id": sid}).json())
        == 3,
    )

    # 7) 边界后实时提交的事件排在补投之后，Worker 按补投段升序再投递实时事件
    r = api.post(
        "/api/v1/events",
        json={"source": source, "event_id": "new-after", "payload": {"n": 99}},
    )
    check("C 边界后实时提交 201", r.status_code == 201, r.text)
    new_pk = r.json()["id"]

    expected_order = ["old-0", "old-1", "old-3", "new-after"]
    items = wait_for(
        lambda: (
            lambda rec: rec["items"]
            if [x["event_id"] for x in rec["items"]][:4] == expected_order
            else None
        )(rcv.get("/received", params={"limit": 200}).json()),
        "接收器按 补投升序 + 边界后实时 顺序收齐",
    )
    check(
        "C 实际投递顺序：old-0,old-1,old-3,new-after",
        [x["event_id"] for x in items][:4] == expected_order,
        str([x["event_id"] for x in items][:4]),
    )
    check(
        "C 补投/实时投递均签名校验通过且 revision=1",
        all(x["signature_valid"] and x["config_revision"] == "1" for x in items[:4]),
    )
    # 全部成功
    def _all_succeeded() -> bool:
        ds = api.get(
            "/api/v1/deliveries", params={"subscription_id": sid}
        ).json()
        return (
            len(ds) == 4
            and {x["source_event_id"]: x["status"] for x in ds}
            == {k: "succeeded" for k in expected_order}
        )

    wait_for(_all_succeeded, "4 条投递全部 succeeded")
    check("C 补投与实时投递全部 succeeded", _all_succeeded())

    # 8) 暂停订阅补投：建 pending 但不认领
    sub2 = api.post(
        "/api/v1/subscriptions",
        json={
            "source": source,
            "target_url": f"{h.rcv}/paused",
            "secret": "dev-signing-secret",
        },
    )
    sid2 = sub2.json()["id"]
    api.post(f"/api/v1/subscriptions/{sid2}/pause")
    r = api.post(
        f"/api/v1/subscriptions/{sid2}/backfill",
        json={"first_event_id": event_pks[0], "last_event_id": event_pks[1]},
    )
    check("C 暂停订阅补投 created=2", r.status_code == 200 and r.json()["created"] == 2, r.text)
    time.sleep(1.5)
    ds = api.get("/api/v1/deliveries", params={"subscription_id": sid2}).json()
    check(
        "C 暂停补投保持 pending、零尝试",
        len(ds) == 2 and all(d["status"] == "pending" and d["attempts_made"] == 0 for d in ds),
        str([(d["status"], d["attempts_made"]) for d in ds]),
    )

    # 9) 停用订阅 -> 409（直接改库模拟停用身份）
    async def _deactivate():
        pool = await asyncpg.create_pool(dsn=h.dsn, min_size=1, max_size=2)
        await pool.execute("UPDATE subscriptions SET active=FALSE WHERE id=$1", sid)
        await pool.close()

    asyncio.run(_deactivate())
    r = api.post(
        f"/api/v1/subscriptions/{sid}/backfill",
        json={"first_event_id": event_pks[0], "last_event_id": event_pks[-1]},
    )
    check(
        "C 停用订阅 -> 409 subscription_inactive",
        r.status_code == 409
        and r.json()["detail"]["code"] == "subscription_inactive",
        r.text,
    )


def main() -> int:
    pg = pgserver.get_server(
        str(ROOT / ".test-pgdata-backfill-db"), cleanup_mode="stop"
    )
    dsn = pg.get_uri()
    try:
        asyncio.run(_db_tests(dsn))
    finally:
        pg.cleanup()

    h = Harness()
    h.start()
    try:
        run_http_tests(h)
    finally:
        h.stop()

    if failures:
        print(f"\n历史补投 {len(failures)} 项检查失败 ❌: {failures}")
        return 1
    print("\n历史补投全部检查通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
