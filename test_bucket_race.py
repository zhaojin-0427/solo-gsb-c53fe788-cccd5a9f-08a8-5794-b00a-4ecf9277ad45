"""竞态聚焦验证：Compose 冷启动建桶竞态（HEAD 404 x2 -> PUT 一方成功一方 409）。

背景缺陷：api 与 worker 同时启动、载荷桶尚不存在时，两者在 ensure_bucket
都先 HEAD bucket 读到 404，随后一方的 PUT 成功、另一方的 PUT 收到
`409 BucketAlreadyOwnedByYou`。旧代码把所有非 2xx 的 PUT 结果一律抛
ObjectStoreError，导致落败进程启动失败（api 退出，事件接口不可用）。

修复约定（见 app/objectstore.py 的 ensure_bucket）：

- `_request` 仍是桶与对象读写共用的**原始传输层**（返回原始 httpx 响应，
  不做结构化错误映射），对象路径的 GET 404 -> ObjectNotFound 等映射不变；
- 结构化错误码只在**桶路径**解析：
  - 409 + `BucketAlreadyOwnedByYou`（同凭据抢先建桶）-> 立即对**本次 PUT**
    复查 HEAD，200 才继续；
  - 409 + `BucketAlreadyExists`（其他账号占用）/ 其他错误码 / 无法解析 ->
    报错；
  - 复查 HEAD 非 200（仍 404 / 403 / 5xx）-> 报错；
  - HEAD 初始 403 等非 404 状态 -> 报错；
  - 已有桶（HEAD 200）与正常创建（PUT 200）照常可用。

本测试不依赖网络/MinIO：以内存中的脚本化 S3（替换 S3ObjectStore._request）
精确模拟上述交错，并额外用一个带屏障的“状态机 S3”驱动两个 ensure_bucket
真实并发，断言双方都成功且 PUT 只成功一次。对象读写的错误映射单独回归。

非 Docker：仅依赖项目 requirements（httpx），不需要 PostgreSQL/MinIO。
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        failures.append(name)


# 与 MinIO/AWS S3 实际错误响应一致的最小 XML 形态
def _error_xml(code: str, message: str) -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Error><Code>" + code + "</Code><Message>" + message + "</Message>"
        "<BucketName>webhook-payloads</BucketName>"
        "<Resource>/webhook-payloads</Resource><RequestId>t-1</RequestId></Error>"
    )


def make_store():
    from app.objectstore import S3ObjectStore

    return S3ObjectStore(
        endpoint_url="http://minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="webhook-payloads",
    )


class ScriptedS3:
    """按 (method) 顺序返回预设响应的假 S3；记录全部调用。"""

    def __init__(self, script: list):
        # script: [("HEAD", status, text), ("PUT", status, text), ...]
        self.script = list(script)
        self.calls: list[tuple[str, str]] = []

    def __call__(self, method, url, **kwargs):
        import httpx

        idx = sum(1 for _ in self.calls)
        self.calls.append((method, url))
        if idx >= len(self.script):
            raise AssertionError(f"脚本已耗尽，却又收到 {method} {url}")
        want_method, status, text = self.script[idx]
        if method != want_method:
            raise AssertionError(f"第 {idx} 个调用期望 {want_method}，实际 {method}")
        resp = httpx.Response(status_code=status, text=text or "", request=None)
        # _request 是 async；用 future 包一层
        async def _ret():
            return resp

        return _ret()


def patch_request(store, handler):
    """把 store._request 替换为 handler(method, url, **kw)（handler 可为协程函数）。"""
    async def _fake(method, url, **kwargs):
        result = handler(method, url, **kwargs)
        if asyncio.iscoroutine(result):
            return await result
        return result

    store._request = _fake


async def expect_error(name, store, handler, *, status=None):
    from app.objectstore import ObjectStoreError

    patch_request(store, handler)
    try:
        await store.ensure_bucket()
    except ObjectStoreError as exc:
        ok = status is None or exc.status == status
        check(name, ok, f"status={exc.status} message={exc}")
        return exc
    check(name, False, "期望 ObjectStoreError 但 ensure_bucket 成功")
    return None


async def case_existing_bucket_and_normal_create() -> None:
    store = make_store()

    # 1) 已有桶：HEAD 200，直接返回，不应发起 PUT
    s3 = ScriptedS3([("HEAD", 200, "")])
    patch_request(store, s3)
    await store.ensure_bucket()
    check("已有桶（HEAD 200）成功且不发起 PUT", [m for m, _ in s3.calls] == ["HEAD"])

    # 2) 正常冷启动：HEAD 404 -> PUT 200
    s3 = ScriptedS3([("HEAD", 404, ""), ("PUT", 200, "")])
    patch_request(store, s3)
    await store.ensure_bucket()
    check(
        "正常创建（HEAD 404 -> PUT 200）成功",
        [m for m, _ in s3.calls] == ["HEAD", "PUT"],
    )

    # 3) PUT 204 也算创建成功
    s3 = ScriptedS3([("HEAD", 404, ""), ("PUT", 204, "")])
    patch_request(store, s3)
    await store.ensure_bucket()
    check("PUT 204 视为创建成功", [m for m, _ in s3.calls] == ["HEAD", "PUT"])


async def case_race_loser_then_recheck_ok() -> None:
    """竞态落败方：HEAD 404 -> PUT 409 BucketAlreadyOwnedByYou -> HEAD 200。"""
    s3 = ScriptedS3(
        [
            ("HEAD", 404, ""),
            ("PUT", 409, _error_xml(
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded "
                "and you already own it.",
            )),
            ("HEAD", 200, ""),
        ]
    )
    store = make_store()
    patch_request(store, s3)
    await store.ensure_bucket()
    check(
        "竞态落败（409 OwnedByYou + 复查 HEAD 200）成功，且复查紧跟本次 PUT",
        [m for m, _ in s3.calls] == ["HEAD", "PUT", "HEAD"],
    )


async def case_other_owner_conflict_errors() -> None:
    # 1) BucketAlreadyExists（桶名被其他账号占用）-> 报错，且不复查
    async def h1(method, url, **kw):
        import httpx

        if method == "HEAD":
            return httpx.Response(404, request=None)
        return httpx.Response(
            409, text=_error_xml(
                "BucketAlreadyExists",
                "The requested bucket name is not available.",
            ),
            request=None,
        )

    store = make_store()
    exc = await expect_error(
        "409 BucketAlreadyExists（他人占用）报错且不复查",
        store,
        h1,
        status=409,
    )
    if exc is not None:
        check("错误信息含 S3 错误码", "BucketAlreadyExists" in str(exc))

    # 2) 其他 409 错误码（例如 OperationAborted）-> 报错
    async def h2(method, url, **kw):
        import httpx

        if method == "HEAD":
            return httpx.Response(404, request=None)
        return httpx.Response(
            409, text=_error_xml("OperationAborted", "conflicting operation"),
            request=None,
        )

    await expect_error("409 其他错误码（OperationAborted）报错", make_store(), h2)

    # 3) 409 但响应体不是合法 XML -> 报错（绝不猜测为幂等成功）
    async def h3(method, url, **kw):
        import httpx

        if method == "HEAD":
            return httpx.Response(404, request=None)
        return httpx.Response(409, text="<!doctype html><html>gateway</html>",
                              request=None)

    await expect_error("409 无法解析错误码报错", make_store(), h3, status=409)

    # 4) 409 空响应体 -> 报错
    async def h4(method, url, **kw):
        import httpx

        if method == "HEAD":
            return httpx.Response(404, request=None)
        return httpx.Response(409, text="", request=None)

    await expect_error("409 空响应体报错", make_store(), h4, status=409)

    # 5) 初始 HEAD 403（凭据/权限问题）-> 报错，不尝试 PUT
    seen = {"put": 0}

    async def h5(method, url, **kw):
        import httpx

        if method == "PUT":
            seen["put"] += 1
        return httpx.Response(403, request=None)

    await expect_error("初始 HEAD 403 报错且不发起 PUT", make_store(), h5,
                       status=403)
    check("HEAD 403 时不发起 PUT", seen["put"] == 0)


async def case_recheck_failures() -> None:
    import httpx

    owned = _error_xml(
        "BucketAlreadyOwnedByYou",
        "Your previous request to create the named bucket succeeded.",
    )

    # 复查仍 404（桶尚未可见 / 实际不可用）
    async def h404(method, url, **kw):
        if method == "HEAD":
            return httpx.Response(404, request=None)
        return httpx.Response(409, text=owned, request=None)

    await _expect("复查 HEAD 仍 404 -> 报错", h404, status=404)

    # 复查 403（权限问题）
    state = {"n": 0}

    async def h403(method, url, **kw):
        if method == "PUT":
            return httpx.Response(409, text=owned, request=None)
        state["n"] += 1
        # 初始 HEAD 是 404，复查 HEAD 是 403
        return httpx.Response(404 if state["n"] == 1 else 403, request=None)

    await _expect("复查 HEAD 403 -> 报错", h403, status=403)

    # 复查 500（MinIO 暂时不可用）
    state = {"n": 0}

    async def h500(method, url, **kw):
        if method == "PUT":
            return httpx.Response(409, text=owned, request=None)
        state["n"] += 1
        return httpx.Response(404 if state["n"] == 1 else 500, request=None)

    await _expect("复查 HEAD 500 -> 报错", h500, status=500)


async def _expect(name, handler, *, status):
    store = make_store()
    patch_request(store, handler)
    from app.objectstore import ObjectStoreError

    try:
        await store.ensure_bucket()
    except ObjectStoreError as exc:
        check(name, exc.status == status, f"status={exc.status}")
        return
    check(name, False, "期望报错但成功")


async def case_other_put_errors() -> None:
    import httpx

    # PUT 403（凭据无权建桶）沿用原有错误路径
    async def h403(method, url, **kw):
        return httpx.Response(404 if method == "HEAD" else 403, request=None)

    await _expect("PUT 403 报错", h403, status=403)

    # PUT 500
    async def h500(method, url, **kw):
        return httpx.Response(
            404 if method == "HEAD" else 500,
            text="internal error",
            request=None,
        )

    await _expect("PUT 500 报错", h500, status=500)


class RacingS3:
    """带屏障的“状态机 S3”：驱动两个 ensure_bucket 真实并发。

    - 两个进程的初始 HEAD 都必须在任一 PUT 之前返回 404（复现冷启动双 404）；
    - 第一个 PUT 成功（200）并把桶标记为已存在；第二个 PUT 等待第一个完全
      落地后返回 409 BucketAlreadyOwnedByYou；
    - 落败方随后的复查 HEAD 返回 200。
    """

    def __init__(self):
        self.bucket_exists = False
        self.puts = 0
        self.put_success = 0
        self.head_after_put = 0
        self._initial_heads = asyncio.Event()
        self._first_put_done = asyncio.Event()

    async def __call__(self, method, url, **kwargs):
        import httpx

        if method == "HEAD":
            if self.puts == 0:
                # 初始 HEAD：先放两个调用者都到达这里，再一起放行，保证它们
                # 都在任一 PUT 前读到 404（确定性复现双 404 竞态窗口）。
                self._initial_heads_seen = getattr(self, "_initial_heads_seen", 0) + 1
                if self._initial_heads_seen == 2:
                    self._initial_heads.set()
                try:
                    await asyncio.wait_for(self._initial_heads.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pass
                return httpx.Response(404, request=None)
            self.head_after_put += 1
            return httpx.Response(200 if self.bucket_exists else 404, request=None)

        # PUT
        self.puts += 1
        if self.puts == 1:
            # 抢先建桶成功者
            self.bucket_exists = True
            self.put_success += 1
            self._first_put_done.set()
            return httpx.Response(200, request=None)
        # 第二个 PUT 必须等第一个落地后才收到 409（对应真实时序）
        await asyncio.wait_for(self._first_put_done.wait(), timeout=5)
        return httpx.Response(
            409,
            text=_error_xml(
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded.",
            ),
            request=None,
        )


async def case_real_concurrent_ensure() -> None:
    store_a = make_store()
    store_b = make_store()
    server = RacingS3()
    patch_request(store_a, server)
    patch_request(store_b, server)

    await asyncio.gather(store_a.ensure_bucket(), store_b.ensure_bucket())
    check(
        "并发 ensure_bucket 双方都成功（api 不再因竞态退出）",
        server.put_success == 1 and server.head_after_put == 1,
        f"put_success={server.put_success} recheck_heads={server.head_after_put}",
    )


async def case_object_error_mapping_unchanged() -> None:
    """回归：共享层未把响应结构化，对象路径错误映射保持原样。"""
    import hashlib

    from app.objectstore import ObjectNotFound, ObjectStoreError

    store = make_store()
    key = hashlib.sha256(b"abc").hexdigest()

    # GET 对象 404 -> ObjectNotFound（不被桶逻辑干扰）
    async def h_get_404(method, url, **kw):
        import httpx

        return httpx.Response(404, request=None)

    patch_request(store, h_get_404)
    try:
        await store.get_object(key)
        check("GET 对象 404 -> ObjectNotFound", False)
    except ObjectNotFound as exc:
        check(
            "GET 对象 404 -> ObjectNotFound（对象错误映射不变）",
            exc.status == 404 and exc.key == key,
        )

    # GET 对象 409 -> 通用 ObjectStoreError（桶路径特判不外溢到对象路径）
    async def h_get_409(method, url, **kw):
        import httpx

        return httpx.Response(
            409, text=_error_xml("BucketAlreadyOwnedByYou", "x"), request=None
        )

    patch_request(store, h_get_409)
    try:
        await store.get_object(key)
        check("GET 对象 409 -> ObjectStoreError", False)
    except ObjectStoreError as exc:
        check(
            "GET 对象 409（即便错误码同名）仍是 ObjectStoreError，不被桶特判吞掉",
            type(exc) is ObjectStoreError and exc.status == 409,
        )

    # HEAD 对象 404 -> False
    async def h_head_404(method, url, **kw):
        import httpx

        return httpx.Response(404, request=None)

    patch_request(store, h_head_404)
    check("HEAD 对象 404 -> False（映射不变）", await store.head_object(key) is False)


async def _run() -> int:
    cases = [
        case_existing_bucket_and_normal_create,
        case_race_loser_then_recheck_ok,
        case_other_owner_conflict_errors,
        case_recheck_failures,
        case_other_put_errors,
        case_real_concurrent_ensure,
        case_object_error_mapping_unchanged,
    ]
    for c in cases:
        try:
            await c()
        except Exception:  # noqa: BLE001
            import traceback

            traceback.print_exc()
            failures.append(c.__name__)
    print()
    if failures:
        print(f"失败 {len(failures)} 项: {failures}")
        return 1
    print("Compose 冷启动建桶竞态修复检查全部通过 ✅")
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    sys.exit(main())
