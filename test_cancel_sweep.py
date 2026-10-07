"""撤销后过期在飞投递的独立后台清理专项（非 Docker，便携 Postgres + 本地文件载荷后端）。

修复背景：撤销 API 只终结 pending 与撤销时刻租约已过期的 in_flight 投递；
“撤销后才过期”的 in_flight 投递原先仅由 Worker 认领事务内的清理终结——
Worker 全部停止（API 仍存活）且无人查询时，这类投递会永远滞留 in_flight
与旧租约。现在 API 进程内有独立的后台周期清理（app/main.py 的
cancel_sweep_loop，间隔 CANCEL_SWEEP_INTERVAL_SECONDS），与 Worker 是否
存活无关地把到期投递终结为 cancelled 并清空租约。

Part A：确定性数据库测试——sweep_expired_cancelled_in_flight_round：
        单批未扫完（> _SWEEP_EXPIRED_LIMIT 行）时同轮续扫直至清完；
        被并发锁跳过的行本轮不等待、下一轮回访终结；租约未过期的行
        绝不提前清理（尚可成功结算的请求不被取消），其成功结算保留。
Part B：HTTP 端到端（只起 API、不起 Worker）：撤销时租约未过期的在飞
        投递保留 in_flight；租约到期后无人查询也由 API 后台任务终结为
        cancelled 并清空租约；清理幂等（不新增尝试、不改 attempts_made）；
        未撤销事件的投递不受影响；暂停订阅下同样生效。
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
    "WEBHOOK_ALLOWED_HOSTS": "127.0.0.1,localhost",
    "WEBHOOK_ALLOWED_CIDRS": "",
    "WEBHOOK_KEYRING_JSON": '{"active_key_id":"k1","keys":{"k1":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}}',
    "PAYLOAD_STORE_MODE": "local",
    "PAYLOAD_LOCAL_DIR": str(ROOT / ".test-payloads-sweep"),
    # 后台清理高频巡检，测试不必久等
    "CANCEL_SWEEP_INTERVAL_SECONDS": "0.2",
    "PYTHONUNBUFFERED": "1",
}

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        failures.append(name)


# ---------------------------------------------------------------------------
# Part A：sweep_expired_cancelled_in_flight_round 的确定性数据库测试
# ---------------------------------------------------------------------------


async def _seed(conn, source: str, *, status: str = "pending",
                lease: str | None = None, lease_age_seconds: float = 0.0):
    """建订阅+版本+事件+投递；可指定投递状态与（过期的）租约。"""
    target = f"http://127.0.0.1:9/{source}"
    sub = await conn.fetchrow(
        """
        INSERT INTO subscriptions (source, target_url, secret, current_revision)
        VALUES ($1, $2, '', 1) RETURNING id
        """,
        source, target,
    )
    sid = sub["id"]
    ver = await conn.fetchval(
        """
        INSERT INTO subscription_versions
            (subscription_id, revision, target_url, secret, max_attempts,
             backoff_base_seconds, backoff_cap_seconds)
        VALUES ($1, 1, $2, '', 6, 1.0, 8.0) RETURNING id
        """,
        sid, target,
    )
    ev = await conn.fetchrow(
        """
        INSERT INTO events (source, event_id, payload)
        VALUES ($1, $2, '{}'::jsonb) RETURNING id
        """,
        source, f"evt-{source}",
    )
    dl = await conn.fetchrow(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id,
             chain_id, chain_seq, status)
        VALUES ($1, $2, $3, nextval('delivery_chain_seq'), 1, $4)
        RETURNING id, status, event_id_fk
        """,
        ev["id"], sid, ver, status,
    )
    if lease is not None:
        await conn.execute(
            """
            UPDATE deliveries
            SET leased_by=$2, leased_at=now() - make_interval(secs => $3),
                lease_expires_at=now() - make_interval(secs => $3)
                    + interval '30 seconds'
            WHERE id=$1
            """,
            dl["id"], lease, lease_age_seconds,
        )
    return ev["id"], sid, dl["id"]


async def _db_tests(dsn: str) -> None:
    from app import repository as repo
    from app.db import init_schema

    conn = await asyncpg.connect(dsn)
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await init_schema(conn)

    # 场景 1：单批未扫完时同轮续扫——超过单批上限的滞留行一轮清完。
    limit = repo._SWEEP_EXPIRED_LIMIT
    n_rows = limit + 7
    await conn.execute(
        """
        INSERT INTO subscriptions (source, target_url, secret, current_revision)
        SELECT 'bulk', 'http://127.0.0.1:9/bulk-' || g, '', 1
        FROM generate_series(1, $1) g
        """,
        n_rows,
    )
    await conn.execute(
        """
        INSERT INTO subscription_versions
            (subscription_id, revision, target_url, secret, max_attempts,
             backoff_base_seconds, backoff_cap_seconds)
        SELECT id, 1, target_url, '', 6, 1.0, 8.0
        FROM subscriptions WHERE source = 'bulk'
        """
    )
    eid = await conn.fetchval(
        "INSERT INTO events (source, event_id, payload)"
        " VALUES ('bulk', 'evt-bulk', '{}'::jsonb) RETURNING id"
    )
    await conn.execute(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id,
             chain_id, chain_seq, status, leased_by, leased_at, lease_expires_at)
        SELECT $1, s.id, v.id, nextval('delivery_chain_seq'), 1, 'in_flight',
               'ghost', now() - interval '60 seconds',
               now() - interval '30 seconds'
        FROM subscriptions s
        JOIN subscription_versions v
             ON v.subscription_id = s.id AND v.revision = 1
        WHERE s.source = 'bulk'
        """,
        eid,
    )
    await conn.execute(
        "UPDATE events SET cancelled_at = now() WHERE id = $1", eid
    )
    # 单批函数只清到上限（证明批量上限确实生效、单批扫不完）
    n1 = await repo.sweep_expired_cancelled_in_flight(conn)
    check("单批清理受批量上限约束", n1 == limit, f"n={n1} limit={limit}")
    # round 在同一轮内续扫剩余行，一轮调用即全部终结
    n2 = await repo.sweep_expired_cancelled_in_flight_round(conn)
    left = await conn.fetchval(
        "SELECT COUNT(*) FROM deliveries WHERE event_id_fk = $1 AND status = 'in_flight'",
        eid,
    )
    check(
        "round 同轮续扫剩余行（一轮清完）",
        n2 == n_rows - limit and left == 0,
        f"n={n2} left={left}",
    )
    bad = await conn.fetchval(
        """
        SELECT COUNT(*) FROM deliveries
        WHERE event_id_fk = $1
          AND (status <> 'cancelled' OR leased_by IS NOT NULL
               OR leased_at IS NOT NULL OR lease_expires_at IS NOT NULL)
        """,
        eid,
    )
    check("续扫后全部 cancelled 且租约字段清空", bad == 0, f"bad={bad}")
    check(
        "批量清理不新增任何尝试记录",
        await conn.fetchval(
            "SELECT COUNT(*) FROM delivery_attempts da"
            " JOIN deliveries d ON d.id = da.delivery_id"
            " WHERE d.event_id_fk = $1",
            eid,
        )
        == 0,
    )

    # 场景 2：被并发持锁跳过的行本轮不等待，锁释放后下一轮回访终结。
    eid2, _sid2, did2 = await _seed(
        conn, "sweep-lock", status="in_flight",
        lease="ghost", lease_age_seconds=60.0,
    )
    await conn.execute(
        "UPDATE events SET cancelled_at = now() WHERE id = $1", eid2
    )
    other = await asyncpg.connect(dsn)
    await other.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    hold = other.transaction()
    await hold.start()
    # 另一连接持行锁（模拟 record_attempt/settle 进行中）
    await other.fetchval(
        "SELECT leased_by FROM deliveries WHERE id = $1 FOR UPDATE", did2
    )
    n_skip = await repo.sweep_expired_cancelled_in_flight_round(conn)
    st = await conn.fetchrow("SELECT status FROM deliveries WHERE id = $1", did2)
    check(
        "持锁行本轮跳过（不等待、不终结）",
        n_skip == 0 and st["status"] == "in_flight",
        f"n={n_skip} {dict(st)}",
    )
    await hold.commit()
    await other.close()
    n_next = await repo.sweep_expired_cancelled_in_flight_round(conn)
    st2 = await conn.fetchrow(
        "SELECT status, leased_by FROM deliveries WHERE id = $1", did2
    )
    check(
        "锁释放后下一轮回访终结并清租约",
        n_next == 1 and st2["status"] == "cancelled" and st2["leased_by"] is None,
        f"n={n_next} {dict(st2)}",
    )

    # 场景 3：租约未过期的行绝不提前清理（尚可成功结算的请求不被取消），
    # 其在租约内的成功结算保留为 succeeded。
    eid3, _sid3, did3 = await _seed(
        conn, "sweep-unexpired", status="in_flight",
        lease="live:1", lease_age_seconds=10.0,
    )
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() + interval '30 seconds'"
        " WHERE id = $1",
        did3,
    )
    await conn.execute(
        "UPDATE events SET cancelled_at = now() WHERE id = $1", eid3
    )
    n3 = await repo.sweep_expired_cancelled_in_flight_round(conn)
    st3 = await conn.fetchrow(
        "SELECT status, leased_by FROM deliveries WHERE id = $1", did3
    )
    check(
        "未过期行不被清理（不提前取消）",
        n3 == 0 and st3["status"] == "in_flight" and st3["leased_by"] == "live:1",
        f"n={n3} {dict(st3)}",
    )
    from datetime import datetime, timedelta, timezone

    att3 = await repo.record_attempt(
        conn, delivery_id=did3, lease_token="live:1", worker_id="live",
        outcome="success", http_status=200, response_excerpt="ok",
        error_message=None,
        leased_at=datetime.now(timezone.utc) - timedelta(seconds=20),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=10),
    )
    stl3 = await repo.settle_delivery(
        conn, delivery_id=did3, lease_token="live:1", succeeded=True,
        not_before=None, max_attempts=6,
    )
    check(
        "未过期请求在租约内成功结算保留为 succeeded",
        att3 == 1 and stl3 == "succeeded"
        and await conn.fetchval(
            "SELECT status FROM deliveries WHERE id = $1", did3
        )
        == "succeeded",
        f"att={att3} settle={stl3}",
    )

    await conn.close()


# ---------------------------------------------------------------------------
# Part B：HTTP 端到端（只起 API、不起 Worker）
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


def force_in_flight_lease(
    dsn: str, delivery_id: int, *, worker: str, expires_in_seconds: float
) -> None:
    """直接改库：把投递置为 in_flight 并挂上指定到期时间的租约。

    模拟 Worker 认领后失联（HTTP 层无法确定性地制造“认领后未结算”的现场）。
    """

    async def _run() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                """
                UPDATE deliveries
                SET status = 'in_flight',
                    leased_by = $2,
                    leased_at = now() - interval '60 seconds',
                    lease_expires_at = now() + make_interval(secs => $3),
                    updated_at = now()
                WHERE id = $1
                """,
                delivery_id,
                worker,
                expires_in_seconds,
            )
        finally:
            await conn.close()

    asyncio.run(_run())


class Harness:
    """只启动 API 进程（含后台清理任务），故意不启动任何 Worker。"""

    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def start(self):
        self.pg = pgserver.get_server(
            str(ROOT / ".test-pgdata-sweep-http"), cleanup_mode="stop"
        )
        self.dsn = self.pg.get_uri()
        api_port = free_port()
        self.api = f"http://127.0.0.1:{api_port}"
        env = os.environ.copy()
        env.update(COMMON_ENV)
        env["DATABASE_URL"] = self.dsn
        log = open(ROOT / "test-sweep-api.log", "w")
        proc = subprocess.Popen(
            [VENV_PY, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(api_port)],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        self.procs.append(proc)
        wait_http(f"{self.api}/health")

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

    # 订阅后暂停：覆盖“暂停订阅下过期在飞投递同样被清理”（目标无需真实
    # 存在——本测试不启动 Worker，不会有任何真实投递）。
    sub = api.post("/api/v1/subscriptions", json={
        "source": "sweep", "target_url": "http://127.0.0.1:9/receive",
        "secret": "dev-signing-secret",
    })
    check("创建订阅 201", sub.status_code == 201, sub.text)
    sid = sub.json()["id"]
    paused = api.post(f"/api/v1/subscriptions/{sid}/pause")
    check("暂停订阅 200", paused.status_code == 200
          and paused.json()["paused"] is True, paused.text)

    ev = api.post("/api/v1/events", json={
        "source": "sweep", "event_id": "sweep-1", "payload": {"k": 1},
    })
    check("提交事件 201", ev.status_code == 201, ev.text)
    event_pk = ev.json()["id"]
    deliveries = api.get(f"/api/v1/events/{event_pk}/deliveries").json()
    check(
        "暂停期间仍建立 pending 投递",
        len(deliveries) == 1 and deliveries[0]["status"] == "pending",
        json.dumps(deliveries, ensure_ascii=False),
    )
    did = deliveries[0]["id"]

    # 模拟 Worker 认领后失联：in_flight + 租约 4 秒后过期（撤销时未过期）。
    force_in_flight_lease(h.dsn, did, worker="ghost-worker:1",
                          expires_in_seconds=4)

    cr = api.post(f"/api/v1/events/{event_pk}/cancel")
    check(
        "撤销 200 且带 cancelled_at",
        cr.status_code == 200 and cr.json()["cancelled_at"] is not None,
        cr.text,
    )

    # 租约未过期：撤销后投递仍 in_flight（不提前取消尚可成功结算的请求）。
    now = api.get(f"/api/v1/deliveries/{did}").json()
    check(
        "撤销时租约未过期：投递保留 in_flight 与租约",
        now["status"] == "in_flight" and now["leased_by"] == "ghost-worker:1"
        and now["lease_expires_at"] is not None,
        json.dumps(now, ensure_ascii=False),
    )

    # 不发起任何查询地等待：覆盖租约过期（4s）+ 多个后台清理周期（0.2s）。
    # 若清理依赖 Worker 轮询或查询触发，这里将永远滞留 in_flight。
    time.sleep(7)
    final = api.get(f"/api/v1/deliveries/{did}").json()
    check(
        "Worker 停止、无人查询时 API 后台清理终结过期在飞投递并清空租约",
        final["status"] == "cancelled" and final["leased_by"] is None
        and final["leased_at"] is None and final["lease_expires_at"] is None,
        json.dumps(final, ensure_ascii=False),
    )
    check(
        "清理不新增尝试、attempts_made 不变",
        final["attempts_made"] == 0
        and api.get(f"/api/v1/deliveries/{did}/attempts").json() == [],
    )
    check(
        "事件投递查询不再滞留 in_flight",
        all(d["status"] != "in_flight"
            for d in api.get(f"/api/v1/events/{event_pk}/deliveries").json()),
    )

    # 幂等：再跑若干清理周期，状态不变、无新尝试。
    time.sleep(1.5)
    again = api.get(f"/api/v1/deliveries/{did}").json()
    check(
        "重复清理幂等（终态不变、无新尝试）",
        again["status"] == "cancelled" and again["attempts_made"] == 0
        and api.get(f"/api/v1/deliveries/{did}/attempts").json() == [],
    )

    # 未撤销事件的投递不受清理影响（无 Worker，保持 pending）。
    ev2 = api.post("/api/v1/events", json={
        "source": "sweep", "event_id": "sweep-2", "payload": {"k": 2},
    }).json()
    d2 = api.get(f"/api/v1/events/{ev2['id']}/deliveries").json()[0]
    time.sleep(1.0)
    d2_now = api.get(f"/api/v1/deliveries/{d2['id']}").json()
    check(
        "未撤销事件的 pending 投递不被清理误伤",
        d2_now["status"] == "pending",
        json.dumps(d2_now, ensure_ascii=False),
    )
    # 撤销该事件：pending 由撤销事务直接终结（不依赖后台任务）。
    cr2 = api.post(f"/api/v1/events/{ev2['id']}/cancel")
    check(
        "撤销直接终结 pending 投递",
        cr2.status_code == 200
        and api.get(f"/api/v1/deliveries/{d2['id']}").json()["status"]
        == "cancelled",
        cr2.text,
    )


def main() -> int:
    # Part A
    pg = pgserver.get_server(str(ROOT / ".test-pgdata-sweep-db"), cleanup_mode="stop")
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
    print("撤销过期在飞投递后台清理全部检查通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
