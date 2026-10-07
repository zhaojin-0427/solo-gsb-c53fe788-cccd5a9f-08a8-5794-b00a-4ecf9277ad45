"""订阅历史事件补投（backfill）专项验证（非 Docker，便携 Postgres + 本地文件载荷后端）。

Part A：确定性数据库测试——补投的筛选/锁序/幂等/边界语义（直接调用
        repository.backfill_subscription）：
        基本补建（按主键升序、钉住当前版本、接在既有队列之后）、重复区间
        幂等、撤销先完成不补活、补投先提交则被撤销翻转、并发撤销/补投
        不变式、停用拒绝、未知订阅、版本边界、既有初始投递不重复。
Part B：四进程 HTTP 端到端（api + receiver + 2 worker）：晚建订阅补投旧事件、
        补投接在实时投递之后、重复区间 created=0、版本边界钉住、暂停可补投
        但不认领、停用 409、未知订阅 404、非法区间 422、撤销不补活、
        死信重放语义不变。
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
# Part A：确定性数据库测试（直接调用 repository，无需 HTTP/Worker）
# ---------------------------------------------------------------------------

_TEST_KEYRING_DICT = {
    "active_key_id": "k1",
    "keys": {"k1": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="},
}


async def _seed_subscription(conn, source: str) -> int:
    """建一个活跃订阅 + revision=1 版本；返回 subscription_id。"""
    target = f"http://127.0.0.1:9/{source}"
    sub = await conn.fetchrow(
        """
        INSERT INTO subscriptions (source, target_url, secret, current_revision)
        VALUES ($1, $2, '', 1)
        RETURNING id
        """,
        source, target,
    )
    sid = sub["id"]
    await conn.execute(
        """
        INSERT INTO subscription_versions
            (subscription_id, revision, target_url, secret, max_attempts,
             backoff_base_seconds, backoff_cap_seconds)
        VALUES ($1, 1, $2, '', 6, 1.0, 8.0)
        """,
        sid, target,
    )
    return sid


async def _seed_event(conn, source: str, event_id: str) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO events (source, event_id, payload)
        VALUES ($1, $2, '{}'::jsonb)
        RETURNING id
        """,
        source, event_id,
    )
    return row["id"]


async def _seed_events(conn, source: str, n: int, prefix: str = "evt") -> list[int]:
    return [await _seed_event(conn, source, f"{prefix}-{i}") for i in range(1, n + 1)]


async def _deliveries_of(conn, sid: int) -> list[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT id, event_id_fk, subscription_id, config_version_id, chain_id,
               chain_seq, seq, status, attempts_made
        FROM deliveries WHERE subscription_id = $1 ORDER BY seq
        """,
        sid,
    )


async def _db_tests(dsn: str) -> None:
    from app import repository as repo
    from app.db import init_schema
    from app.keyring import Keyring

    keyring = Keyring.from_dict(_TEST_KEYRING_DICT)

    conn = await asyncpg.connect(dsn)
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await init_schema(conn)

    # -- 1. 基本补建：按主键升序、pending、链首、钉住当前版本、接在既有队列之后 --
    sid = await _seed_subscription(conn, "bf-a")
    eids = await _seed_events(conn, "bf-a", 3)
    # 既有队列尾：手工放一条该订阅的旧投递（模拟实时提交已建的初始投递）
    ev_old = await _seed_event(conn, "bf-a", "evt-old")
    ver1 = await conn.fetchval(
        "SELECT id FROM subscription_versions WHERE subscription_id=$1 AND revision=1",
        sid,
    )
    existing = await conn.fetchrow(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id, chain_id, chain_seq, status)
        VALUES ($1, $2, $3, nextval('delivery_chain_seq'), 1, 'succeeded')
        RETURNING id, seq
        """,
        ev_old, sid, ver1,
    )
    result = await repo.backfill_subscription(
        conn, subscription_id=sid, from_event_id=eids[0], to_event_id=eids[-1]
    )
    check("A1: 补投返回新增数=3", result is not None and result["created"] == 3,
          json.dumps(result, default=str))
    check("A1: 钉住当前版本 rev1", result["config_revision"] == 1)
    ds = await _deliveries_of(conn, sid)
    backfilled = [d for d in ds if d["event_id_fk"] in eids]
    check(
        "A1: 补建 3 条 pending 链首投递且按主键升序（seq 随事件 id 递增）",
        len(backfilled) == 3
        and all(d["status"] == "pending" and d["chain_seq"] == 1 for d in backfilled)
        and [d["event_id_fk"] for d in backfilled] == eids
        and all(backfilled[i]["seq"] < backfilled[i + 1]["seq"] for i in range(2)),
        str([(d["event_id_fk"], d["status"], d["seq"]) for d in backfilled]),
    )
    check(
        "A1: 补投投递接在既有队列之后（seq 大于既有投递）",
        all(d["seq"] > existing["seq"] for d in backfilled),
        f"existing seq={existing['seq']}",
    )

    # -- 2. 幂等：重复区间/重叠区间不新增投递 --
    again = await repo.backfill_subscription(
        conn, subscription_id=sid, from_event_id=eids[0], to_event_id=eids[-1]
    )
    check("A2: 重复区间 created=0", again["created"] == 0)
    overlap = await repo.backfill_subscription(
        conn, subscription_id=sid, from_event_id=eids[1], to_event_id=eids[-1] + 100
    )
    check("A2: 重叠区间（含不存在的主键）created=0", overlap["created"] == 0)
    ds_after = await _deliveries_of(conn, sid)
    check("A2: 重复补投后投递总数不变", len(ds_after) == len(ds))

    # -- 3. 子区间：只补区间内事件；已有初始投递的事件不重复 --
    sid3 = await _seed_subscription(conn, "bf-c")
    e3 = await _seed_events(conn, "bf-c", 3)
    r3 = await repo.backfill_subscription(
        conn, subscription_id=sid3, from_event_id=e3[1], to_event_id=e3[1]
    )
    ds3 = await _deliveries_of(conn, sid3)
    check(
        "A3: 单点闭区间只补 1 条（中间事件）",
        r3["created"] == 1 and len(ds3) == 1 and ds3[0]["event_id_fk"] == e3[1],
    )

    # -- 4. 撤销先完成：不被补活（无任何投递） --
    sid4 = await _seed_subscription(conn, "bf-d")
    e4 = await _seed_events(conn, "bf-d", 3)
    cancelled = await repo.cancel_event(conn, e4[1])
    check("A4: 撤销成功", cancelled is not None and cancelled["cancelled_at"] is not None)
    r4 = await repo.backfill_subscription(
        conn, subscription_id=sid4, from_event_id=e4[0], to_event_id=e4[-1]
    )
    ds4 = await _deliveries_of(conn, sid4)
    check(
        "A4: 撤销先完成的事件不被补活（只补其余 2 条，撤销事件无投递）",
        r4["created"] == 2
        and len(ds4) == 2
        and all(d["event_id_fk"] != e4[1] for d in ds4),
        str([(d["event_id_fk"], d["status"]) for d in ds4]),
    )

    # -- 5. 补投先提交：随后的撤销把补建的 pending 翻转为 cancelled（不滞留） --
    sid5 = await _seed_subscription(conn, "bf-e")
    e5 = await _seed_events(conn, "bf-e", 2)
    r5 = await repo.backfill_subscription(
        conn, subscription_id=sid5, from_event_id=e5[0], to_event_id=e5[-1]
    )
    check("A5: 补投 2 条", r5["created"] == 2)
    await repo.cancel_event(conn, e5[0])
    ds5 = await _deliveries_of(conn, sid5)
    st5 = {d["event_id_fk"]: d["status"] for d in ds5}
    check(
        "A5: 补投后撤销 -> 该事件投递为 cancelled 终态，另一事件不受影响",
        st5.get(e5[0]) == "cancelled" and st5.get(e5[1]) == "pending",
        json.dumps(st5),
    )

    # -- 6. 并发撤销 vs 补投：任意交错下都不存在被补活的在途投递 --
    for round_no in range(5):
        sid6 = await _seed_subscription(conn, f"bf-r{round_no}")
        e6 = await _seed_events(conn, f"bf-r{round_no}", 3)
        conn2 = await asyncpg.connect(dsn)
        await conn2.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
        try:
            await asyncio.gather(
                repo.backfill_subscription(
                    conn, subscription_id=sid6,
                    from_event_id=e6[0], to_event_id=e6[-1],
                ),
                repo.cancel_event(conn2, e6[1]),
            )
        finally:
            await conn2.close()
        row6 = await conn.fetchrow(
            """
            SELECT COUNT(*) AS alive
            FROM deliveries
            WHERE event_id_fk = $1 AND subscription_id = $2
              AND status IN ('pending', 'in_flight')
            """,
            e6[1], sid6,
        )
        check(
            f"A6.{round_no}: 并发撤销/补投后被撤销事件无在途投递",
            row6["alive"] == 0,
        )

    # -- 6b. 并发同订阅同区间补投：唯一索引兜底，合计新增数 == 事件数，无重复 --
    sid6b = await _seed_subscription(conn, "bf-cc")
    e6b = await _seed_events(conn, "bf-cc", 4)
    conn3 = await asyncpg.connect(dsn)
    await conn3.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    try:
        r_a, r_b = await asyncio.gather(
            repo.backfill_subscription(
                conn, subscription_id=sid6b,
                from_event_id=e6b[0], to_event_id=e6b[-1],
            ),
            repo.backfill_subscription(
                conn3, subscription_id=sid6b,
                from_event_id=e6b[0], to_event_id=e6b[-1],
            ),
        )
    finally:
        await conn3.close()
    ds6b = await _deliveries_of(conn, sid6b)
    check(
        "A6b: 并发同订阅补投合计新增=4 且无重复投递",
        r_a["created"] + r_b["created"] == 4 and len(ds6b) == 4,
        f"created={r_a['created']}+{r_b['created']}, deliveries={len(ds6b)}",
    )

    # -- 7. 停用订阅：拒绝补投（BackfillError.subscription_inactive） --
    sid7 = await _seed_subscription(conn, "bf-g")
    e7 = await _seed_events(conn, "bf-g", 2)
    await conn.execute("UPDATE subscriptions SET active = FALSE WHERE id = $1", sid7)
    try:
        await repo.backfill_subscription(
            conn, subscription_id=sid7, from_event_id=e7[0], to_event_id=e7[-1]
        )
        check("A7: 停用订阅补投抛 BackfillError", False)
    except repo.BackfillError as exc:
        check("A7: 停用订阅补投抛 BackfillError", exc.code == "subscription_inactive",
              exc.code)
    ds7 = await _deliveries_of(conn, sid7)
    check("A7: 停用订阅未建任何投递", len(ds7) == 0)

    # -- 8. 未知订阅：返回 None（HTTP 层映射 404） --
    none_result = await repo.backfill_subscription(
        conn, subscription_id=99999999, from_event_id=1, to_event_id=10
    )
    check("A8: 未知订阅返回 None", none_result is None)

    # -- 9. 版本边界：发布 rev2 后补投钉住 rev2；既有初始投递不重复 --
    sid9 = await _seed_subscription(conn, "bf-h")
    e9 = await _seed_events(conn, "bf-h", 3)
    # e9[0] 模拟实时提交已建初始投递（钉 rev1）
    ver9_1 = await conn.fetchval(
        "SELECT id FROM subscription_versions WHERE subscription_id=$1 AND revision=1",
        sid9,
    )
    await conn.execute(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id, chain_id, chain_seq, status)
        VALUES ($1, $2, $3, nextval('delivery_chain_seq'), 1, 'succeeded')
        """,
        e9[0], sid9, ver9_1,
    )
    _row, new_rev = await repo.publish_version(
        conn,
        keyring=keyring,
        subscription_id=sid9,
        target_url="http://127.0.0.1:9/bf-h-v2",
        secret="dev-signing-secret",
        max_attempts=6,
        backoff_base_seconds=1.0,
        backoff_cap_seconds=8.0,
        expected_revision=1,
    )
    check("A9: 发布 rev2", new_rev == 2)
    r9 = await repo.backfill_subscription(
        conn, subscription_id=sid9, from_event_id=e9[0], to_event_id=e9[-1]
    )
    check(
        "A9: 边界后补投钉住 rev2，且只补缺失的 2 条（已有初始投递的不重复）",
        r9["created"] == 2 and r9["config_revision"] == 2,
        json.dumps(r9, default=str),
    )
    ds9 = await _deliveries_of(conn, sid9)
    ver2 = await conn.fetchval(
        "SELECT id FROM subscription_versions WHERE subscription_id=$1 AND revision=2",
        sid9,
    )
    check(
        "A9: 补建投递 config_version_id 为 rev2 的版本行",
        all(d["config_version_id"] == ver2 for d in ds9 if d["status"] == "pending"),
    )

    # -- 10. 边界后实时投递排在其后：补投提交后再建的投递 seq 更大 --
    ev_after = await _seed_event(conn, "bf-h", "evt-after")
    late = await conn.fetchrow(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id, chain_id, chain_seq, status)
        VALUES ($1, $2, $3, nextval('delivery_chain_seq'), 1, 'pending')
        RETURNING seq
        """,
        ev_after, sid9, ver2,
    )
    max_bf_seq = max(d["seq"] for d in await _deliveries_of(conn, sid9)
                     if d["event_id_fk"] in e9)
    check("A10: 边界后实时投递排在补投投递之后", late["seq"] > max_bf_seq)

    await conn.close()


# ---------------------------------------------------------------------------
# Part B：HTTP 端到端
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
        self.spawn(["app.worker"], "worker1")
        self.spawn(["app.worker"], "worker2")
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


def _submit(api: httpx.Client, source: str, event_id: str, payload: dict | None = None) -> int:
    r = api.post("/api/v1/events", json={
        "source": source, "event_id": event_id, "payload": payload or {},
    })
    r.raise_for_status()
    return r.json()["id"]


def run_http_tests(h: Harness) -> None:
    api = httpx.Client(base_url=h.api, timeout=10)
    rcv = httpx.Client(base_url=h.rcv, timeout=10)
    rcv.post("/received/reset")
    rcv.post("/control/fail", params={"enabled": False}).raise_for_status()

    # -- 参数校验：非法区间 422（在订阅存在与否之前由请求模型裁决） --
    check("B0: from>to -> 422",
          api.post("/api/v1/subscriptions/1/backfill",
                   json={"from_event_id": 5, "to_event_id": 2}).status_code == 422)
    check("B0: from=0 -> 422",
          api.post("/api/v1/subscriptions/1/backfill",
                   json={"from_event_id": 0, "to_event_id": 2}).status_code == 422)
    check("B0: 缺字段 -> 422",
          api.post("/api/v1/subscriptions/1/backfill",
                   json={"from_event_id": 1}).status_code == 422)
    check("B0: 未知订阅 -> 404",
          api.post("/api/v1/subscriptions/999999/backfill",
                   json={"from_event_id": 1, "to_event_id": 2}).status_code == 404)

    # -- 1. 晚建订阅补投旧事件；补投接在实时投递之后 --
    # 订阅尚不存在时提交 3 个事件（不产生任何投递）
    e1 = _submit(api, "bf", "bf-1", {"n": 1})
    e2 = _submit(api, "bf", "bf-2", {"n": 2})
    e3 = _submit(api, "bf", "bf-3", {"n": 3})
    check("B1: 订阅前提交事件不产生投递",
          api.get("/api/v1/deliveries", params={"source": "bf"}).json() == [])

    sub = api.post("/api/v1/subscriptions", json={
        "source": "bf", "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    sid = sub["id"]

    # 订阅后的实时事件：先建投递并入队（队列尾部边界）
    e_live = _submit(api, "bf", "bf-live", {"n": 99})
    d_live = wait_for(
        lambda: next((x for x in api.get("/api/v1/deliveries",
                     params={"source": "bf"}).json()
                     if x["source_event_id"] == "bf-live"), None),
        "bf-live 实时投递出现",
    )
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{d_live['id']}").json()["status"] == "succeeded",
        "bf-live 实时投递成功",
    )

    # 补投 [e1, e3]：返回新增数 3，钉住 rev1
    bf = api.post(f"/api/v1/subscriptions/{sid}/backfill",
                  json={"from_event_id": e1, "to_event_id": e3})
    body = bf.json()
    check(
        "B1: 补投 200 且 created=3、钉住 rev1",
        bf.status_code == 200 and body["created"] == 3
        and body["config_revision"] == 1 and body["source"] == "bf"
        and body["from_event_id"] == e1 and body["to_event_id"] == e3,
        bf.text,
    )

    # 投递查询可见：3 条补投投递（pending -> 最终被 worker 送达）
    ds = wait_for(
        lambda: (
            lambda rows: rows if len(rows) == 4 else None
        )(api.get("/api/v1/deliveries", params={"source": "bf"}).json()),
        "补投后共 4 条投递可见",
    )
    for evid in ("bf-1", "bf-2", "bf-3"):
        d = next(x for x in ds if x["source_event_id"] == evid)
        check(f"B1: 补投投递 {evid} 为链首且钉 rev1",
              d["chain_seq"] == 1 and d["config_revision"] == 1)
        wait_for(
            lambda d=d: api.get(f"/api/v1/deliveries/{d['id']}").json()["status"] == "succeeded",
            f"{evid} 补投送达",
        )
    # 严格顺序：实时投递 bf-live 在补投投递之前（补投接在既有队列之后），
    # 补投内部按事件主键升序
    items = wait_for(
        lambda: (
            lambda its: its
            if len([x for x in its if x["event"] == "bf"]) == 4 else None
        )(rcv.get("/received", params={"limit": 200}).json()["items"]),
        "接收器收到 bf 全部 4 条",
    )
    order = [x["event_id"] for x in items if x["event"] == "bf"]
    check("B1: 接收顺序 = 实时投递在前，补投按主键升序随后",
          order == ["bf-live", "bf-1", "bf-2", "bf-3"], str(order))

    # -- 2. 重复/重叠区间不新增投递 --
    again = api.post(f"/api/v1/subscriptions/{sid}/backfill",
                     json={"from_event_id": e1, "to_event_id": e3}).json()
    check("B2: 重复区间 created=0", again["created"] == 0, json.dumps(again))
    wider = api.post(f"/api/v1/subscriptions/{sid}/backfill",
                     json={"from_event_id": e1, "to_event_id": e_live}).json()
    check("B2: 含实时事件的更大区间 created=0（实时投递即初始投递）",
          wider["created"] == 0, json.dumps(wider))
    check("B2: 投递总数仍为 4",
          len(api.get("/api/v1/deliveries", params={"source": "bf"}).json()) == 4)

    # -- 3. 版本边界：发布 rev2 后补投钉住 rev2；边界后实时投递排在其后 --
    ev1 = _submit(api, "bfv", "bfv-1")
    ev2 = _submit(api, "bfv", "bfv-2")
    subv = api.post("/api/v1/subscriptions", json={
        "source": "bfv", "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    sidv = subv["id"]
    pub = api.post(f"/api/v1/subscriptions/{sidv}/versions", json={
        "target_url": f"{h.rcv}/receive", "secret": "dev-signing-secret",
        "expected_revision": 1,
    })
    check("B3: 发布 rev2", pub.status_code == 201 and pub.json()["revision"] == 2,
          pub.text)
    bfv = api.post(f"/api/v1/subscriptions/{sidv}/backfill",
                   json={"from_event_id": ev1, "to_event_id": ev2}).json()
    check("B3: 边界后补投钉住 rev2",
          bfv["created"] == 2 and bfv["config_revision"] == 2, json.dumps(bfv))
    # 边界后的实时提交：钉 rev2 且排在补投投递之后
    ev3 = _submit(api, "bfv", "bfv-3")
    dsv = wait_for(
        lambda: (
            lambda rows: rows if len(rows) == 3 else None
        )(api.get("/api/v1/deliveries", params={"source": "bfv"}).json()),
        "bfv 共 3 条投递",
    )
    check(
        "B3: 补投与实时投递均钉 rev2",
        all(x["config_revision"] == 2 for x in dsv),
    )
    for x in dsv:
        wait_for(
            lambda x=x: api.get(f"/api/v1/deliveries/{x['id']}").json()["status"] == "succeeded",
            f"bfv {x['source_event_id']} 送达",
        )
    items_v = rcv.get("/received", params={"limit": 500}).json()["items"]
    order_v = [x["event_id"] for x in items_v if x["event"] == "bfv"]
    check("B3: 接收顺序 = 补投（主键升序）在前、边界后实时投递随后",
          order_v == ["bfv-1", "bfv-2", "bfv-3"], str(order_v))

    # -- 4. 暂停可补投但不认领；恢复后按序送达 --
    ep1 = _submit(api, "bfp", "bfp-1")
    ep2 = _submit(api, "bfp", "bfp-2")
    subp = api.post("/api/v1/subscriptions", json={
        "source": "bfp", "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    sidp = subp["id"]
    api.post(f"/api/v1/subscriptions/{sidp}/pause").raise_for_status()
    bfp = api.post(f"/api/v1/subscriptions/{sidp}/backfill",
                   json={"from_event_id": ep1, "to_event_id": ep2})
    check("B4: 暂停中补投 200 且 created=2",
          bfp.status_code == 200 and bfp.json()["created"] == 2, bfp.text)
    time.sleep(2.0)  # worker 轮询 0.1s：若暂停失效会立刻被认领
    dsp = api.get("/api/v1/deliveries", params={"source": "bfp"}).json()
    check(
        "B4: 暂停中补投投递保持 pending、零尝试",
        len(dsp) == 2 and all(x["status"] == "pending" and x["attempts_made"] == 0
                              for x in dsp),
        json.dumps(dsp, ensure_ascii=False),
    )
    check(
        "B4: 暂停中接收器未收到 bfp 事件",
        not any(x["event"] == "bfp" for x in rcv.get("/received", params={"limit": 500}).json()["items"]),
    )
    api.post(f"/api/v1/subscriptions/{sidp}/resume").raise_for_status()
    for x in dsp:
        wait_for(
            lambda x=x: api.get(f"/api/v1/deliveries/{x['id']}").json()["status"] == "succeeded",
            f"bfp {x['source_event_id']} 恢复后送达",
        )

    # -- 5. 撤销先完成的事件不被补活；补投后撤销则翻转为 cancelled --
    ec1 = _submit(api, "bfc", "bfc-1")
    ec2 = _submit(api, "bfc", "bfc-2")
    ec3 = _submit(api, "bfc", "bfc-3")
    api.post(f"/api/v1/events/{ec2}/cancel").raise_for_status()
    subc = api.post("/api/v1/subscriptions", json={
        "source": "bfc", "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    sidc = subc["id"]
    # 先暂停再补投：补建投递保持 pending（worker 不认领），随后的撤销确定性
    # 地将其翻转为 cancelled（否则 worker 可能先认领成功，撤销语义允许其完成）。
    api.post(f"/api/v1/subscriptions/{sidc}/pause").raise_for_status()
    bfc = api.post(f"/api/v1/subscriptions/{sidc}/backfill",
                   json={"from_event_id": ec1, "to_event_id": ec3}).json()
    check("B5: 撤销先完成 -> 只补其余 2 条", bfc["created"] == 2, json.dumps(bfc))
    check("B5: 被撤销事件无任何投递",
          api.get(f"/api/v1/events/{ec2}/deliveries").json() == [])
    # 补投后撤销：补建的 pending 被翻转为 cancelled
    api.post(f"/api/v1/events/{ec1}/cancel").raise_for_status()
    ds_c1 = api.get(f"/api/v1/events/{ec1}/deliveries").json()
    check("B5: 补投后撤销 -> 补建投递为 cancelled 终态",
          len(ds_c1) == 1 and ds_c1[0]["status"] == "cancelled",
          json.dumps(ds_c1, ensure_ascii=False))
    # 恢复后未撤销的 ec3 正常送达（cancelled 是终态，不阻挡后续事件）
    api.post(f"/api/v1/subscriptions/{sidc}/resume").raise_for_status()
    d_ec3 = api.get(f"/api/v1/events/{ec3}/deliveries").json()[0]
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{d_ec3['id']}").json()["status"] == "succeeded",
        "bfc-3 送达",
    )

    # -- 6. 停用订阅：409 subscription_inactive --
    subi = api.post("/api/v1/subscriptions", json={
        "source": "bfi", "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    sidi = subi["id"]

    async def _deactivate() -> None:
        c = await asyncpg.connect(h.dsn)
        try:
            await c.execute("UPDATE subscriptions SET active = FALSE WHERE id = $1", sidi)
        finally:
            await c.close()

    asyncio.run(_deactivate())
    ri = api.post(f"/api/v1/subscriptions/{sidi}/backfill",
                  json={"from_event_id": 1, "to_event_id": 10})
    check(
        "B6: 停用订阅补投 -> 409 subscription_inactive",
        ri.status_code == 409 and ri.json()["detail"]["code"] == "subscription_inactive",
        ri.text,
    )

    # -- 7. 死信重放语义不变：补投投递是正常链首，可退避/死信/重放 --
    ed1 = _submit(api, "bfd", "bfd-1")
    rcv.post("/control/fail", params={"enabled": True}).raise_for_status()
    subd = api.post("/api/v1/subscriptions", json={
        "source": "bfd", "target_url": f"{h.rcv}/fail",
        "secret": "dev-signing-secret",
        "max_attempts": 2, "backoff_base_seconds": 1, "backoff_cap_seconds": 2,
    }).json()
    sidd = subd["id"]
    bfd = api.post(f"/api/v1/subscriptions/{sidd}/backfill",
                   json={"from_event_id": ed1, "to_event_id": ed1}).json()
    check("B7: 补投 1 条到失败目标", bfd["created"] == 1, json.dumps(bfd))
    d_dl = wait_for(
        lambda: next((x for x in api.get("/api/v1/deliveries",
                     params={"source": "bfd"}).json()
                     if x["status"] == "dead_lettered"), None),
        "补投投递进入死信",
        timeout=60,
    )
    rp = api.post(f"/api/v1/dead-letters/{d_dl['id']}/replay", json={})
    check("B7: 补投投递的死信重放 201 且同链 chain_seq+1",
          rp.status_code == 201 and rp.json()["chain_id"] == d_dl["chain_id"]
          and rp.json()["chain_seq"] == 2, rp.text)
    rcv.post("/control/fail", params={"enabled": False}).raise_for_status()
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{rp.json()['new_delivery_id']}").json()["status"] == "succeeded",
        "重放投递成功",
    )


def main() -> int:
    # 清理上次运行的残留（便携库数据目录与本地载荷目录），保证可重复运行
    import shutil
    for path in (
        ROOT / ".test-pgdata-backfill-db",
        ROOT / ".test-pgdata-backfill",
        ROOT / ".test-payloads-backfill",
    ):
        shutil.rmtree(path, ignore_errors=True)

    # Part A：独立便携库（与 HTTP 测试互不影响）
    pg = pgserver.get_server(str(ROOT / ".test-pgdata-backfill-db"), cleanup_mode="stop")
    dsn = pg.get_uri()
    try:
        asyncio.run(_db_tests(dsn))
    except Exception:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append("Part A 异常")
    finally:
        try:
            pg.cleanup()
        except Exception:  # noqa: BLE001
            pass

    # Part B
    h = Harness()
    h.start()
    try:
        run_http_tests(h)
    except Exception:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append("Part B 异常")
    finally:
        h.stop()

    print()
    if failures:
        print(f"失败 {len(failures)} 项: {failures}")
        return 1
    print("历史事件补投全部检查通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
