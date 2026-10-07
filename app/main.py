"""FastAPI 入口。"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import api, payloads, repository as repo
from app.config import settings
from app.db import close_pool, get_pool, init_db
from app.keyring import get_keyring
from app.objectstore import init_object_store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

log = logging.getLogger("webhook.api")

OWNER_ID = (
    f"api:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
)


async def cancel_sweep_loop() -> None:
    """API 进程后台任务：独立终结“已撤销事件 + 租约已过期”的在飞投递。

    Worker 存活时，claim_delivery 每轮认领事务开头已执行同一清理（同一
    仓储函数、并发安全且幂等）；本任务保证 Worker 全部停止（或长期无
    认领）时，撤销后才过期的 in_flight 投递仍会在租约到期后被终结为
    cancelled 并清空租约，不会在投递查询中长期滞留 in_flight。

    只翻转租约已过期的行：租约未过期的在飞请求（仍可能成功结算）绝不
    被提前取消。单批未扫完时同轮续扫、被并发锁跳过的行留待下一周期，
    见 repository.sweep_expired_cancelled_in_flight_round。
    """
    pool = get_pool()
    interval = settings.cancel_sweep_interval_seconds
    log.info("撤销事件过期在飞投递的后台清理任务已启动（间隔 %ss）", interval)
    while True:
        await asyncio.sleep(interval)
        try:
            async with pool.acquire() as conn:
                n = await repo.sweep_expired_cancelled_in_flight_round(conn)
            if n:
                log.info("后台清理终结 %s 条已撤销事件的过期在飞投递", n)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 后台任务不能因单轮失败退出
            log.exception("撤销事件过期在飞投递清理失败")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    # 对象存储桶/目录就绪检查：不可用则启动失败（避免提交期才暴露）。
    await init_object_store()
    # 启动时加载本地密钥环：配置非法直接启动失败（避免运行期才暴露）；
    # 未配置则降级为只读（写入接口 503），详见 app/keyring.py。
    get_keyring()
    # 提交状态机后台恢复：崩溃/重启残留的“已登记/已上传未终结”提交由此续跑。
    recovery_task = asyncio.create_task(payloads.recovery_loop(OWNER_ID))
    # 撤销事件过期在飞投递的后台清理：Worker 停止时仍独立终结到期投递。
    sweep_task = asyncio.create_task(cancel_sweep_loop())
    try:
        yield
    finally:
        for task in (recovery_task, sweep_task):
            task.cancel()
        for task in (recovery_task, sweep_task):
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        await close_pool()


app = FastAPI(
    title="可靠 Webhook 投递服务",
    version="1.0.0",
    description=(
        "事件幂等提交、内容寻址载荷对象存储、严格顺序投递、租约抢占、"
        "指数退避、死信重放、事件撤销（cancel）、订阅历史事件补投"
        "（backfill）。交互文档见 /docs。"
    ),
    lifespan=lifespan,
)

app.include_router(api.router)


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
