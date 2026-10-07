"""事件撤销专项验证（非 Docker，便携 Postgres + 本地文件载荷后端）。

Part A：确定性数据库交错测试——撤销（cancel_event）与认领原语
        （_claim_select_candidate / _claim_lock_subscription）的行锁先后
        次序：撤销先提交则认领复核到事件 cancelled_at，放弃租约并翻转；
        认领先提交则撤销只能随后生效，已认领尝试照常结算（成功 succeeded、
        失败/租约过期 cancelled）。另测结算 ↔ 撤销、重放 ↔ 撤销交错，
        以及过期在飞投递的清理（sweep_expired_cancelled_in_flight）：
        撤销直接终结已过期 in_flight；暂停/停用订阅下无需认领即终结；
        清理幂等（不新增尝试）；旧租约令牌不得覆盖终态；未过期请求的
        成功结算保留；与 record_attempt 持锁的 SKIP LOCKED 交错。
Part B：四进程 HTTP 端到端（api + receiver + 双 worker）：撤销 API、
        pending 批量 cancelled、查询展示撤销时刻/终态、幂等、404、
        重复提交不复活、撤销后拒绝死信重放、已认领在途请求的成功/失败
        结算语义、暂停订阅下过期在飞投递的撤销终结与轮询清理（投递查询
        不再滞留 in_flight）、其他事件严格顺序不受影响、attempts/载荷
        引用保留。
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
from datetime import datetime, timedelta, timezone
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
    "PAYLOAD_LOCAL_DIR": str(ROOT / ".test-payloads-cancel"),
    "PYTHONUNBUFFERED": "1",
}

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        failures.append(name)


# ---------------------------------------------------------------------------
# Part A：确定性事务交错
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


async def _db_interleaving_tests(dsn: str) -> None:
    from app import repository as repo
    from app.db import init_schema

    conn = await asyncpg.connect(dsn)
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await init_schema(conn)

    # 交错 1：撤销先提交 —— 认领事务已锁候选投递行，撤销在另一连接提交；
    # 认领导订阅行后复核事件 cancelled_at，必须放弃认领并翻转候选行。
    eid, sid, did = await _seed(conn, "cancel-iv1")
    now = datetime.now(timezone.utc)
    claim_tx = conn.transaction()
    await claim_tx.start()
    cand = await repo._claim_select_candidate(conn, now)
    check("交错1: 选中撤销前的候选投递", cand is not None and cand["id"] == did)
    other = await asyncpg.connect(dsn)
    await other.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    crow = await repo.cancel_event(other, eid)
    check("交错1: 撤销写入成功且带 cancelled_at",
          crow is not None and crow["cancelled_at"] is not None)
    await other.close()
    sub = await repo._claim_lock_subscription(conn, sid)
    cancelled_at = await repo._claim_event_cancelled(conn, cand["event_id_fk"])
    check("交错1: 认领复核读到事件已撤销", cancelled_at is not None)
    assert sub is not None
    await repo._claim_cancel_candidate(conn, cand["event_id_fk"])
    await claim_tx.commit()
    st = await conn.fetchrow("SELECT status, leased_by FROM deliveries WHERE id=$1", did)
    check(
        "交错1: 候选被翻转为 cancelled 且无租约",
        st["status"] == "cancelled" and st["leased_by"] is None,
        str(dict(st)),
    )
    check("交错1: 撤销后不再产生新租约",
          await repo.claim_delivery(conn, "probe") is None)

    # 交错 2：认领先提交 —— 租约先落库，撤销随后（不触碰 in_flight 行）；
    # 已认领请求成功仍 succeeded；再来的失败尝试在撤销后 -> cancelled。
    eid2, sid2, did2 = await _seed(conn, "cancel-iv2")
    now2 = datetime.now(timezone.utc)
    claim = await repo.claim_delivery(conn, "probe-worker-1")
    check("交错2: 认领先获得租约",
          claim is not None and claim["delivery"]["id"] == did2)
    lease_token = claim["lease_token"]
    # 撤销与已提交认领并发：in_flight 行保留
    crow2 = await repo.cancel_event(conn, eid2)
    check("交错2: 认领先提交后撤销仍成功", crow2["cancelled_at"] is not None)
    inflight = await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", did2)
    check("交错2: 撤销不触碰在途 in_flight 行", inflight == "in_flight")
    settled = await repo.settle_delivery(
        conn, delivery_id=did2, lease_token=lease_token, succeeded=True,
        not_before=None, max_attempts=6,
    )
    check("交错2: 在途请求成功仍记 succeeded", settled == "succeeded")

    # 交错 3：认领先提交、在途请求失败 —— 撤销提交后失败结算 -> cancelled
    eid3, sid3, did3 = await _seed(conn, "cancel-iv3")
    claim3 = await repo.claim_delivery(conn, "probe-worker-3")
    assert claim3 is not None
    await repo.cancel_event(conn, eid3)
    # 插入一次失败尝试记录（模拟 record_attempt），再失败结算
    await conn.execute(
        """
        INSERT INTO delivery_attempts
            (delivery_id, attempt_number, worker_id, outcome, finished_at,
             leased_at, lease_expires_at)
        VALUES ($1,1,'probe-worker-3','timeout',now(), now(), now()+interval '30s')
        """,
        did3,
    )
    await conn.execute(
        "UPDATE deliveries SET attempts_made=1 WHERE id=$1", did3
    )
    settled3 = await repo.settle_delivery(
        conn, delivery_id=did3, lease_token=claim3["lease_token"],
        succeeded=False,
        not_before=datetime.now(timezone.utc) + timedelta(seconds=30),
        max_attempts=6,
    )
    check("交错3: 撤销后在途失败转 cancelled（不退避重试）",
          settled3 == "cancelled")
    fin = await conn.fetchrow("SELECT status, not_before FROM deliveries WHERE id=$1", did3)
    check("交错3: 终态行不残留未来 not_before",
          fin["status"] == "cancelled" and fin["not_before"] <= datetime.now(timezone.utc) + timedelta(seconds=1))
    # 不再被认领
    check("交错3: cancelled 后不会再被认领",
          await repo.claim_delivery(conn, "probe-worker-3b") is None)

    # 交错 4：撤销先提交、租约已过期的 in_flight —— 撤销事务直接终结
    # （无需等待接管认领），清除租约字段。
    eid4, sid4, did4 = await _seed(
        conn, "cancel-iv4", status="in_flight",
        lease="dead-worker", lease_age_seconds=60.0,
    )
    await repo.cancel_event(conn, eid4)
    st4 = await conn.fetchrow(
        "SELECT status, leased_by, leased_at, lease_expires_at "
        "FROM deliveries WHERE id=$1",
        did4,
    )
    check(
        "交错4: 撤销直接翻转过期 in_flight 并清除租约",
        st4["status"] == "cancelled" and st4["leased_by"] is None
        and st4["leased_at"] is None and st4["lease_expires_at"] is None,
        str(dict(st4)),
    )
    claim4 = await repo.claim_delivery(conn, "probe-worker-4")
    check("交错4: 终结后不再产生新租约", claim4 is None)

    # 交错 5：撤销 ↔ 死信重放 —— 撤销先则重放拒绝 event_cancelled。
    eid5, sid5, did5 = await _seed(conn, "cancel-iv5", status="dead_lettered")
    await repo.cancel_event(conn, eid5)
    try:
        await repo.replay_dead_letter(conn, did5)
        check("交错5: 撤销后死信重放被拒", False)
    except repo.ReplayError as exc:
        check("交错5: 撤销后死信重放 event_cancelled", exc.code == "event_cancelled")
    # 重放先提交则新链 pending 被随后撤销翻转
    eid6, sid6, did6 = await _seed(conn, "cancel-iv6", status="dead_lettered")
    new_row, _rev = await repo.replay_dead_letter(conn, did6)
    new_id = new_row["id"]
    check("交错6: 未撤销事件死信可重放（201 语义）",
          new_row["status"] == "pending")
    await repo.cancel_event(conn, eid6)
    check(
        "交错6: 撤销翻转重放产生的 pending 链记录，旧死信保留",
        await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", new_id)
        == "cancelled"
        and await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", did6)
        == "dead_lettered",
    )
    # 之后再重放也被拒
    try:
        await repo.replay_dead_letter(conn, did6)
        check("交错6: 撤销后再次重放仍被拒", False)
    except repo.ReplayError as exc:
        check("交错6: 撤销后再次重放 event_cancelled", exc.code == "event_cancelled")

    # 幂等 + 404
    ts = (await conn.fetchrow("SELECT cancelled_at FROM events WHERE id=$1", eid))["cancelled_at"]
    again = await repo.cancel_event(conn, eid)
    check("交错7: 重复撤销幂等（cancelled_at 稳定）",
          again["cancelled_at"] == ts)
    check("交错7: 未知事件撤销返回 None",
          await repo.cancel_event(conn, 99999999) is None)

    # 其他事件顺序：被撤销终态不阻挡同订阅后续 pending
    eid8, sid8, did8a = await _seed(conn, "cancel-iv8a", status="cancelled")
    ev = await conn.fetchrow(
        "INSERT INTO events (source,event_id,payload) VALUES"
        " ('cancel-iv8a','evt-next','{}'::jsonb) RETURNING id"
    )
    ver = await conn.fetchval(
        "SELECT id FROM subscription_versions WHERE subscription_id=$1 AND revision=1",
        sid8,
    )
    did8b = await conn.fetchval(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, config_version_id,
             chain_id, chain_seq, status)
        VALUES ($1,$2,$3,nextval('delivery_chain_seq'),1,'pending')
        RETURNING id
        """,
        ev["id"], sid8, ver,
    )
    c = await repo.claim_delivery(conn, "probe-worker-8")
    check("交错8: 撤销终态不阻挡同订阅后续事件认领",
          c is not None and c["delivery"]["id"] == did8b)

    # 交错 9：暂停订阅下的过期 in_flight —— 撤销直接终结（候选选择会过滤
    # 暂停订阅，若只靠接管认领该行将永远滞留；cancel_event 不经候选选择）。
    eid9, sid9, did9 = await _seed(
        conn, "cancel-iv9", status="in_flight",
        lease="ghost-worker", lease_age_seconds=60.0,
    )
    await conn.execute(
        "UPDATE subscriptions SET paused = TRUE, paused_at = now() WHERE id = $1",
        sid9,
    )
    await repo.cancel_event(conn, eid9)
    st9 = await conn.fetchrow(
        "SELECT status, leased_by, lease_expires_at FROM deliveries WHERE id=$1",
        did9,
    )
    check(
        "交错9: 暂停订阅的过期 in_flight 被撤销直接终结并清租约",
        st9["status"] == "cancelled" and st9["leased_by"] is None
        and st9["lease_expires_at"] is None,
        str(dict(st9)),
    )

    # 交错 10：撤销时租约未过期 → 保留；过期后由清理终结（停用订阅同样
    # 生效，清理不经候选选择）。清理幂等：不新增尝试、不改 attempts_made。
    eid10, sid10, did10 = await _seed(
        conn, "cancel-iv10", status="in_flight",
        lease="live-worker", lease_age_seconds=10.0,
    )
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() + interval '30 seconds'"
        " WHERE id = $1",
        did10,
    )
    await conn.execute(
        "UPDATE subscriptions SET active = FALSE WHERE id = $1", sid10
    )
    await repo.cancel_event(conn, eid10)
    check(
        "交错10: 未过期 in_flight 撤销时保留（停用订阅同）",
        await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", did10)
        == "in_flight",
    )
    n0 = await repo.sweep_expired_cancelled_in_flight(conn)
    check(
        "交错10: 清理不动未过期行",
        n0 == 0
        and await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", did10)
        == "in_flight",
    )
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second'"
        " WHERE id = $1",
        did10,
    )
    n1 = await repo.sweep_expired_cancelled_in_flight(conn)
    st10 = await conn.fetchrow(
        "SELECT status, leased_by, lease_expires_at, attempts_made "
        "FROM deliveries WHERE id=$1",
        did10,
    )
    check(
        "交错10: 过期后清理转 cancelled 并清租约（停用订阅同样生效）",
        n1 == 1 and st10["status"] == "cancelled" and st10["leased_by"] is None
        and st10["lease_expires_at"] is None,
        f"n={n1} {dict(st10)}",
    )
    check(
        "交错10: 清理不新增尝试、attempts_made 不变",
        st10["attempts_made"] == 0
        and await conn.fetchval(
            "SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id=$1", did10
        ) == 0,
    )
    n2 = await repo.sweep_expired_cancelled_in_flight(conn)
    check(
        "交错10: 重复清理幂等（0 行、无尝试新增）",
        n2 == 0
        and await conn.fetchval(
            "SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id=$1", did10
        ) == 0,
    )

    # 交错 11：清理后旧租约令牌不得覆盖终态（record_attempt / settle 均作废）。
    att11 = await repo.record_attempt(
        conn, delivery_id=did10, lease_token="live-worker",
        worker_id="live-worker", outcome="success", http_status=200,
        response_excerpt=None, error_message=None,
        leased_at=datetime.now(timezone.utc) - timedelta(seconds=70),
        lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=40),
    )
    check("交错11: 清理后旧令牌 record_attempt 作废", att11 is None)
    stl11 = await repo.settle_delivery(
        conn, delivery_id=did10, lease_token="live-worker", succeeded=True,
        not_before=None, max_attempts=6,
    )
    check(
        "交错11: 清理后旧令牌 settle 作废、终态不变",
        stl11 is None
        and await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", did10)
        == "cancelled",
    )

    # 交错 12：清理与 record_attempt/settle 交错 —— record_attempt 先提交
    # （租约仍匹配，尝试历史保留），清理随后仍翻转过期行；旧令牌的 settle
    # 回写被栅栏作废。
    eid12, sid12, did12 = await _seed(
        conn, "cancel-iv12", status="in_flight",
        lease="worker-x:1", lease_age_seconds=10.0,
    )
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() + interval '30 seconds'"
        " WHERE id = $1",
        did12,
    )
    await repo.cancel_event(conn, eid12)  # 未过期：保留 in_flight
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second'"
        " WHERE id = $1",
        did12,
    )
    att12 = await repo.record_attempt(
        conn, delivery_id=did12, lease_token="worker-x:1", worker_id="worker-x",
        outcome="timeout", http_status=None, response_excerpt=None,
        error_message="请求超时",
        leased_at=datetime.now(timezone.utc) - timedelta(seconds=40),
        lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
    )
    check("交错12: 清理前 record_attempt 成功（租约仍匹配）", att12 == 1)
    n12 = await repo.sweep_expired_cancelled_in_flight(conn)
    stl12 = await repo.settle_delivery(
        conn, delivery_id=did12, lease_token="worker-x:1", succeeded=False,
        not_before=datetime.now(timezone.utc) + timedelta(seconds=5),
        max_attempts=6,
    )
    st12 = await conn.fetchrow(
        "SELECT status, attempts_made FROM deliveries WHERE id=$1", did12
    )
    check(
        "交错12: 清理翻转后旧令牌 settle 作废，终态 cancelled",
        n12 == 1 and stl12 is None and st12["status"] == "cancelled",
        f"n={n12} settle={stl12} {dict(st12)}",
    )
    check(
        "交错12: 已记录的尝试历史保留",
        st12["attempts_made"] == 1
        and await conn.fetchval(
            "SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id=$1", did12
        ) == 1,
    )

    # 交错 13：清理与 record_attempt 持锁交错 —— SKIP LOCKED 跳过本轮
    # （不等待、不死锁），锁释放后下一轮清理仍终结。
    eid13, sid13, did13 = await _seed(
        conn, "cancel-iv13", status="in_flight",
        lease="worker-y:1", lease_age_seconds=10.0,
    )
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() + interval '30 seconds'"
        " WHERE id = $1",
        did13,
    )
    await repo.cancel_event(conn, eid13)
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second'"
        " WHERE id = $1",
        did13,
    )
    other = await asyncpg.connect(dsn)
    await other.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    hold_tx = other.transaction()
    await hold_tx.start()
    # 另一连接持行锁（模拟 record_attempt 进行中）
    await other.fetchval(
        "SELECT leased_by FROM deliveries WHERE id = $1 FOR UPDATE", did13
    )
    n_skip = await repo.sweep_expired_cancelled_in_flight(conn)
    check("交错13: 行被持锁时清理跳过（不等待）", n_skip == 0)
    await other.execute(
        "UPDATE deliveries SET attempts_made = attempts_made + 1 WHERE id = $1",
        did13,
    )
    await hold_tx.commit()
    await other.close()
    n_next = await repo.sweep_expired_cancelled_in_flight(conn)
    check(
        "交错13: 锁释放后下一轮清理终结",
        n_next == 1
        and await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", did13)
        == "cancelled",
    )

    # 交错 14：未过期在飞请求的成功结算保留（清理与撤销都不触碰未过期行）。
    eid14, sid14, did14ok = await _seed(
        conn, "cancel-iv14", status="in_flight",
        lease="worker-z:1", lease_age_seconds=10.0,
    )
    await conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() + interval '30 seconds'"
        " WHERE id = $1",
        did14ok,
    )
    await repo.cancel_event(conn, eid14)
    n14 = await repo.sweep_expired_cancelled_in_flight(conn)
    att14 = await repo.record_attempt(
        conn, delivery_id=did14ok, lease_token="worker-z:1", worker_id="worker-z",
        outcome="success", http_status=200, response_excerpt="ok",
        error_message=None,
        leased_at=datetime.now(timezone.utc) - timedelta(seconds=20),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=10),
    )
    stl14 = await repo.settle_delivery(
        conn, delivery_id=did14ok, lease_token="worker-z:1", succeeded=True,
        not_before=None, max_attempts=6,
    )
    check(
        "交错14: 未过期请求成功结算保留为 succeeded（清理不干扰）",
        n14 == 0 and att14 == 1 and stl14 == "succeeded"
        and await conn.fetchval("SELECT status FROM deliveries WHERE id=$1", did14ok)
        == "succeeded",
        f"n={n14} att={att14} settle={stl14}",
    )

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


def wait_for(condition, desc: str, timeout: float = 30.0, interval: float = 0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc}")


def force_in_flight_lease(
    dsn: str, delivery_id: int, *, worker: str, expired: bool
) -> None:
    """直接改库：把投递置为 in_flight 并挂上租约（模拟 worker 认领后失联）。

    expired=True 时租约已过期（lease_expires_at 在过去）；False 时仍在租约内。
    HTTP 层无法确定性地制造“认领后未结算”的现场，故直接改库构造。
    """

    async def _run() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            expiry = (
                "now() - interval '30 seconds'"
                if expired
                else "now() + interval '30 seconds'"
            )
            await conn.execute(
                f"""
                UPDATE deliveries
                SET status = 'in_flight',
                    leased_by = $2,
                    leased_at = now() - interval '60 seconds',
                    lease_expires_at = {expiry},
                    updated_at = now()
                WHERE id = $1
                """,
                delivery_id,
                worker,
            )
        finally:
            await conn.close()

    asyncio.run(_run())


def expire_lease_now(dsn: str, delivery_id: int) -> None:
    """直接改库：把当前租约的到期时刻拨到过去（模拟租约刚到期）。"""

    async def _run() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(
                "UPDATE deliveries "
                "SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
                delivery_id,
            )
        finally:
            await conn.close()

    asyncio.run(_run())


class Harness:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def spawn(self, module_cmd: list[str], name: str):
        env = os.environ.copy()
        env.update(COMMON_ENV)
        env["DATABASE_URL"] = self.dsn
        log = open(ROOT / f"test-cancel-{name}.log", "w")
        proc = subprocess.Popen(
            [VENV_PY, "-m", *module_cmd],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        self.procs.append(proc)

    def start(self):
        self.pg = pgserver.get_server(
            str(ROOT / ".test-pgdata-cancel"), cleanup_mode="stop"
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


def run_http_tests(h: Harness) -> None:
    api = httpx.Client(base_url=h.api, timeout=10)
    rcv = httpx.Client(base_url=h.rcv, timeout=10)
    rcv.post("/received/reset")
    rcv.post("/control/fail", params={"enabled": False}).raise_for_status()

    # 404
    r404 = api.post("/api/v1/events/999999/cancel")
    check("未知事件撤销 404", r404.status_code == 404, r404.text)
    check("未知事件 GET 404", api.get("/api/v1/events/999999").status_code == 404)

    # 订阅两条（同 source 两个目标），撤销应批量翻转
    s1 = api.post("/api/v1/subscriptions", json={
        "source": "orders", "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    s2 = api.post("/api/v1/subscriptions", json={
        "source": "orders", "target_url": f"{h.rcv}/receive?dup=2",
        "secret": "dev-signing-secret",
    }).json()

    # 先暂停 s1，制造撤销时仍 pending 的投递
    api.post(f"/api/v1/subscriptions/{s1['id']}/pause").raise_for_status()

    ev = api.post("/api/v1/events", json={
        "source": "orders", "event_id": "ord-cancel-1", "payload": {"order": 1},
    })
    check("提交事件 201", ev.status_code == 201 and ev.json()["cancelled_at"] is None,
          ev.text)
    event_pk = ev.json()["id"]
    deliveries = wait_for(
        lambda: api.get(f"/api/v1/events/{event_pk}/deliveries").json()
        if len(api.get(f"/api/v1/events/{event_pk}/deliveries").json()) == 2 else None,
        "两条投递均建立",
    )
    # s2 的投递会成功；s1 暂停保持 pending
    d_s2 = next(d for d in deliveries if d["subscription_id"] == s2["id"])
    d_s1 = next(d for d in deliveries if d["subscription_id"] == s1["id"])
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{d_s2['id']}").json()["status"]
        == "succeeded",
        "未暂停订阅投递成功",
    )
    time.sleep(0.5)
    check("暂停订阅投递仍 pending",
          api.get(f"/api/v1/deliveries/{d_s1['id']}").json()["status"] == "pending")

    # 撤销
    cr = api.post(f"/api/v1/events/{event_pk}/cancel")
    cj = cr.json()
    check(
        "撤销 200 且返回 cancelled_at",
        cr.status_code == 200 and cj["cancelled_at"] is not None
        and cj["id"] == event_pk and cj["source"] == "orders"
        and cj["event_id"] == "ord-cancel-1",
        cr.text,
    )
    got = api.get(f"/api/v1/events/{event_pk}").json()
    check("GET 事件显示 cancelled_at（载荷仍可读）",
          got["cancelled_at"] == cj["cancelled_at"]
          and got["payload"] == {"order": 1},
          json.dumps(got, ensure_ascii=False))

    # pending -> cancelled；succeeded 保留
    final_s1 = api.get(f"/api/v1/deliveries/{d_s1['id']}").json()
    final_s2b = api.get(f"/api/v1/deliveries/{d_s2['id']}").json()
    check(
        "pending 投递终态 cancelled、无租约字段",
        final_s1["status"] == "cancelled" and final_s1["leased_by"] is None
        and final_s1["lease_expires_at"] is None,
        json.dumps(final_s1, ensure_ascii=False),
    )
    check("已成功投递保持 succeeded", final_s2b["status"] == "succeeded")
    # 事件投递列表同样展示终态
    lst = api.get(f"/api/v1/events/{event_pk}/deliveries").json()
    statuses = sorted(d["status"] for d in lst)
    check("事件投递查询展示终态", statuses == ["cancelled", "succeeded"], str(statuses))
    # 全局投递过滤
    cancelled_list = api.get(
        "/api/v1/deliveries", params={"status": "cancelled", "source": "orders"}
    ).json()
    check("按 status=cancelled 过滤命中",
          any(d["id"] == d_s1["id"] for d in cancelled_list)
          and all(d["status"] == "cancelled" for d in cancelled_list))
    check("非法 status 仍 400",
          api.get("/api/v1/deliveries", params={"status": "nope"}).status_code == 400)

    # 幂等：重复撤销返回同一时间戳
    cr2 = api.post(f"/api/v1/events/{event_pk}/cancel")
    check(
        "重复撤销幂等 200（cancelled_at 不变）",
        cr2.status_code == 200
        and cr2.json()["cancelled_at"] == cj["cancelled_at"],
        cr2.text,
    )

    # 撤销后恢复 s1：cancelled 不被重新认领、不重试
    api.post(f"/api/v1/subscriptions/{s1['id']}/resume").raise_for_status()
    time.sleep(1.5)
    check("恢复订阅后 cancelled 投递不复活",
          api.get(f"/api/v1/deliveries/{d_s1['id']}").json()["status"]
          == "cancelled")

    # 同一 source/event_id 重复提交：返回原事件 duplicate=true，不复活投递
    dup = api.post("/api/v1/events", json={
        "source": "orders", "event_id": "ord-cancel-1", "payload": {"order": 1},
    })
    dj = dup.json()
    check(
        "重复提交撤销事件：200 duplicate 且仍带 cancelled_at，不复活",
        dup.status_code == 200 and dj["duplicate"] is True
        and dj["id"] == event_pk and dj["cancelled_at"] == cj["cancelled_at"],
        dup.text,
    )
    time.sleep(0.8)
    after = api.get(f"/api/v1/events/{event_pk}/deliveries").json()
    check("重复提交后投递集合不变（仍 2 条，无新 pending）",
          len(after) == 2 and all(d["status"] in {"cancelled", "succeeded"} for d in after))

    # 撤销后死信重放被拒（409）。先造一个已撤销事件的死信：
    # 先建 /fail 订阅（max_attempts=2 快速死信），再提交事件 -> 打满死信
    # -> 撤销 -> 重放拒绝。
    rcv.post("/control/fail", params={"enabled": True}).raise_for_status()
    fsub = api.post("/api/v1/subscriptions", json={
        "source": "orders", "target_url": f"{h.rcv}/fail",
        "secret": "dev-signing-secret",
        "max_attempts": 2, "backoff_base_seconds": 1, "backoff_cap_seconds": 1,
    }).json()
    dlev = api.post("/api/v1/events", json={
        "source": "orders", "event_id": "ord-dl-then-cancel", "payload": {"n": 9},
    }).json()
    # 对 dlev 事件找到 fail 订阅的死信投递
    dlds = wait_for(
        lambda: next(
            (d for d in api.get(f"/api/v1/events/{dlev['id']}/deliveries").json()
             if d["subscription_id"] == fsub["id"]),
            None,
        ),
        "死信订阅投递建立",
    )
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{dlds['id']}").json()["status"]
        == "dead_lettered",
        "进入死信",
        timeout=60,
    )
    attempts_before = api.get(f"/api/v1/deliveries/{dlds['id']}/attempts").json()
    check("死信尝试历史已保留", len(attempts_before) == 2,
          f"{len(attempts_before)} 条")
    cr3 = api.post(f"/api/v1/events/{dlev['id']}/cancel")
    check("撤销带死信的事件 200", cr3.status_code == 200, cr3.text)
    rp = api.post(f"/api/v1/dead-letters/{dlds['id']}/replay", json={})
    check(
        "撤销后死信重放 409",
        rp.status_code == 409 and "撤销" in rp.text,
        rp.text,
    )
    time.sleep(0.8)
    # 重放没产生新投递
    dl_now = api.get(f"/api/v1/deliveries/{dlds['id']}").json()
    check("重放被拒不改死信终态", dl_now["status"] == "dead_lettered")
    attempts_after = api.get(f"/api/v1/deliveries/{dlds['id']}/attempts").json()
    check("尝试历史撤销后完整保留", len(attempts_after) == 2)
    rcv.post("/control/fail", params={"enabled": False}).raise_for_status()

    # 在途请求的撤销语义：长租约 + 慢端点不易控；改为暂停订阅后提交、认领前
    # 无法实现“在途”。这里用 worker 实际投递 + 接收器 /fail/{times}：
    # 事件首次失败（退避 pending）后撤销 -> 退避 pending 被翻转，worker 不再重试，
    # 接收器对该 delivery 只收到 1 次。
    flaky_sub = api.post("/api/v1/subscriptions", json={
        "source": "orders", "target_url": f"{h.rcv}/fail/5",
        "secret": "dev-signing-secret",
        "max_attempts": 6, "backoff_base_seconds": 1, "backoff_cap_seconds": 2,
    }).json()
    fev = api.post("/api/v1/events", json={
        "source": "orders", "event_id": "ord-inflight-cancel", "payload": {"a": 2},
    }).json()
    fd = wait_for(
        lambda: next(
            (d for d in api.get(f"/api/v1/events/{fev['id']}/deliveries").json()
             if d["subscription_id"] == flaky_sub["id"]),
            None,
        ),
        "flaky 投递建立",
    )
    # 等第一次尝试发生（投递变为 pending 退避，attempts_made=1）
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{fd['id']}").json()["attempts_made"] >= 1
        and api.get(f"/api/v1/deliveries/{fd['id']}").json()["status"] == "pending",
        "第一次尝试失败后进入退避",
        timeout=30,
    )
    cr4 = api.post(f"/api/v1/events/{fev['id']}/cancel")
    check("撤销退避中的事件 200", cr4.status_code == 200)
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{fd['id']}").json()["status"]
        == "cancelled",
        "退避 pending 被翻转为 cancelled",
    )
    attempts_n = api.get(f"/api/v1/deliveries/{fd['id']}").json()["attempts_made"]
    time.sleep(3.0)  # 超过退避间隔：若边界失效会出现第 2 次投递
    recv_items = rcv.get("/received", params={"limit": 200}).json()["items"]
    hits = [x for x in recv_items
            if x["event_id"] == "ord-inflight-cancel"
            and x["delivery_id"] == str(fd["id"])]
    check(
        "撤销后不再重试（接收器只收到撤销前的尝试）",
        len(hits) == attempts_n and attempts_n >= 1,
        f"attempts={attempts_n}, hits={len(hits)}",
    )
    final_fd = api.get(f"/api/v1/deliveries/{fd['id']}").json()
    check("最终终态 cancelled、尝试历史保留",
          final_fd["status"] == "cancelled"
          and len(api.get(f"/api/v1/deliveries/{fd['id']}/attempts").json()) == attempts_n)

    # ---- 过期在飞投递滞留修复（暂停订阅 + worker 认领后失联）----
    # 场景 1：撤销时租约已过期 —— cancel 直接终结并清租约，查询不再滞留。
    stuck_sub = api.post("/api/v1/subscriptions", json={
        "source": "stuck", "target_url": f"{h.rcv}/receive",
        "secret": "dev-signing-secret",
    }).json()
    api.post(f"/api/v1/subscriptions/{stuck_sub['id']}/pause").raise_for_status()
    sev = api.post("/api/v1/events", json={
        "source": "stuck", "event_id": "stuck-expired-1", "payload": {"s": 1},
    }).json()
    sd = wait_for(
        lambda: next(
            (d for d in api.get(f"/api/v1/events/{sev['id']}/deliveries").json()
             if d["subscription_id"] == stuck_sub["id"]),
            None,
        ),
        "stuck 投递建立",
    )
    # 模拟 worker 认领后宕机：in_flight + 已过期租约（HTTP 层无法制造，直接改库）
    force_in_flight_lease(h.dsn, sd["id"], worker="ghost-worker", expired=True)
    before = api.get(f"/api/v1/deliveries/{sd['id']}").json()
    check("过期在飞投递撤销前滞留 in_flight（修复前现场）",
          before["status"] == "in_flight" and before["leased_by"] == "ghost-worker",
          json.dumps(before, ensure_ascii=False))
    scr = api.post(f"/api/v1/events/{sev['id']}/cancel")
    check("撤销含过期在飞投递的事件 200", scr.status_code == 200, scr.text)
    final_sd = api.get(f"/api/v1/deliveries/{sd['id']}").json()
    check(
        "撤销直接终结过期在飞投递并清除租约",
        final_sd["status"] == "cancelled" and final_sd["leased_by"] is None
        and final_sd["leased_at"] is None and final_sd["lease_expires_at"] is None,
        json.dumps(final_sd, ensure_ascii=False),
    )
    check(
        "事件投递查询不再滞留 in_flight",
        all(d["status"] != "in_flight"
            for d in api.get(f"/api/v1/events/{sev['id']}/deliveries").json()),
    )
    check("被终结的过期在飞投递不新增尝试记录",
          api.get(f"/api/v1/deliveries/{sd['id']}/attempts").json() == []
          and final_sd["attempts_made"] == 0)

    # 场景 2：撤销时租约未过期 —— 保留 in_flight；租约到期后由 worker 每轮
    # 认领事务内的清理终结（订阅仍暂停，清理不经过候选选择），不新增尝试。
    sev2 = api.post("/api/v1/events", json={
        "source": "stuck", "event_id": "stuck-expire-later", "payload": {"s": 2},
    }).json()
    sd2 = wait_for(
        lambda: next(
            (d for d in api.get(f"/api/v1/events/{sev2['id']}/deliveries").json()
             if d["subscription_id"] == stuck_sub["id"]),
            None,
        ),
        "stuck 投递 2 建立",
    )
    force_in_flight_lease(h.dsn, sd2["id"], worker="live-worker", expired=False)
    api.post(f"/api/v1/events/{sev2['id']}/cancel").raise_for_status()
    after_cancel2 = api.get(f"/api/v1/deliveries/{sd2['id']}").json()
    check(
        "撤销时租约未过期的在飞投递保留 in_flight",
        after_cancel2["status"] == "in_flight"
        and after_cancel2["leased_by"] == "live-worker",
        json.dumps(after_cancel2, ensure_ascii=False),
    )
    expire_lease_now(h.dsn, sd2["id"])
    wait_for(
        lambda: api.get(f"/api/v1/deliveries/{sd2['id']}").json()["status"]
        == "cancelled",
        "暂停订阅的过期在飞投递被轮询清理终结",
    )
    fin2 = api.get(f"/api/v1/deliveries/{sd2['id']}").json()
    check(
        "清理后租约字段清空、不新增尝试",
        fin2["leased_by"] is None and fin2["lease_expires_at"] is None
        and fin2["attempts_made"] == 0
        and api.get(f"/api/v1/deliveries/{sd2['id']}/attempts").json() == [],
        json.dumps(fin2, ensure_ascii=False),
    )
    recv_stuck = rcv.get("/received", params={"limit": 500}).json()["items"]
    check(
        "两条被清理的在飞投递从未真正发送",
        len([x for x in recv_stuck
             if x["event_id"] in {"stuck-expired-1", "stuck-expire-later"}]) == 0,
    )

    # 其他事件严格顺序：新事件正常送达；已撤销终态不阻挡后续
    good = api.post("/api/v1/events", json={
        "source": "orders", "event_id": "ord-after-cancel", "payload": {"ok": True},
    }).json()
    gds = api.get(f"/api/v1/events/{good['id']}/deliveries").json()
    # 接收器订阅（s2） 与 flaky/fail 订阅都建了投递；/receive 两个目标应成功
    for d in gds:
        if d["subscription_id"] in (s1["id"], s2["id"]):
            wait_for(
                lambda d=d: api.get(f"/api/v1/deliveries/{d['id']}").json()["status"]
                == "succeeded",
                f"后续事件投递 {d['id']} 成功",
                timeout=30,
            )
    check("后续事件正常提交且可查询（不被撤销事件影响）",
          api.get(f"/api/v1/events/{good['id']}").json()["cancelled_at"] is None)

    # 载荷对象引用与状态接口仍正常（撤销不删载荷）
    status = api.get("/api/v1/payloads/status").json()
    check("载荷状态接口正常（引用计数 > 0）",
          status["total_references"] >= 4 and status["payload_objects"] >= 1,
          json.dumps({k: status[k] for k in
                      ("total_events", "payload_objects", "total_references")}))


def main() -> int:
    # Part A
    pg = pgserver.get_server(str(ROOT / ".test-pgdata-cancel-db"), cleanup_mode="stop")
    dsn = pg.get_uri()
    try:
        asyncio.run(_db_interleaving_tests(dsn))
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
    print("事件撤销全部检查通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
