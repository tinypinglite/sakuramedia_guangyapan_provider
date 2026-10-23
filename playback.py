"""播放：默认 302 直连 CDN，显式请求 proxy 时由本插件中继字节流。

实测：直链不校验 Referer / UA / Cookie，支持 Range（206 + Content-Range），有效期 6 小时。
宿主要求 `playback_deliveries` 必须包含 `proxy`，所以两种方式都要能工作。
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import httpx
from starlette.responses import RedirectResponse, Response, StreamingResponse

from src.plugins.provider_protocol import ProviderOperationError

from .exceptions import (
    GuangyapanAuthError,
    GuangyapanError,
    GuangyapanNotFoundError,
    GuangyapanRateLimitError,
)
from .guangya import USER_AGENT

PROVIDER_KEY = "guangyapan"
# 只转发与字节范围相关的请求头；Cookie / Origin / 自定义头一律不外传。
_FORWARD_REQUEST_HEADERS = ("range", "if-range")
# 只回传播放器需要的响应头；hop-by-hop 与压缩相关的一律丢弃。
_FORWARD_RESPONSE_HEADERS = (
    "content-type",
    "content-length",
    "content-range",
    "accept-ranges",
    "etag",
    "last-modified",
)
_PROXY_CHUNK = 256 * 1024
_STREAM_TIMEOUT = httpx.Timeout(30.0, connect=15.0, read=120.0, write=30.0, pool=30.0)
_DIRECT_URL_CACHE_TTL_SECONDS = 10 * 60
_DIRECT_CACHE_LIMIT = 64
_DIRECT_MAX_CONCURRENCY = 4
_RETRYABLE_UPSTREAM_STATUSES = frozenset({403, 410})


@dataclass(slots=True)
class _DirectEntry:
    url: str
    usable_until: float
    slots: asyncio.Semaphore = field(
        default_factory=lambda: asyncio.Semaphore(_DIRECT_MAX_CONCURRENCY)
    )


class _PlaybackCache:
    def __init__(self) -> None:
        self._direct: OrderedDict[tuple[object, ...], _DirectEntry] = OrderedDict()
        self._tasks: dict[tuple[object, ...], asyncio.Task[tuple[str, int]]] = {}

    def get(self, key: tuple[object, ...]) -> _DirectEntry | None:
        entry = self._direct.get(key)
        if entry is None or entry.usable_until <= time.monotonic():
            self._direct.pop(key, None)
            return None
        self._direct.move_to_end(key)
        return entry

    def put(self, key: tuple[object, ...], url: str, duration_seconds: int) -> _DirectEntry:
        ttl = max(60, min(duration_seconds - 300, _DIRECT_URL_CACHE_TTL_SECONDS))
        entry = _DirectEntry(url=url, usable_until=time.monotonic() + ttl)
        self._direct[key] = entry
        self._direct.move_to_end(key)
        while len(self._direct) > _DIRECT_CACHE_LIMIT:
            self._direct.popitem(last=False)
        return entry

    def discard(self, key: tuple[object, ...]) -> None:
        self._direct.pop(key, None)

    def task_for(self, key: tuple[object, ...]) -> asyncio.Task[tuple[str, int]] | None:
        return self._tasks.get(key)

    def register_task(
        self, key: tuple[object, ...], task: asyncio.Task[tuple[str, int]]
    ) -> None:
        self._tasks[key] = task

    def clear_task(self, key: tuple[object, ...]) -> None:
        self._tasks.pop(key, None)


_CACHE = _PlaybackCache()


def _provider_error(
    operation: str, code: str, message: str, *, retryable: bool = False
) -> ProviderOperationError:
    return ProviderOperationError(
        provider_key=PROVIDER_KEY,
        operation=operation,
        code=code,  # type: ignore[arg-type]
        safe_message=message,
        retryable=retryable,
    )


def _cloud_error(operation: str, exc: GuangyapanError) -> ProviderOperationError:
    if isinstance(exc, GuangyapanAuthError):
        return _provider_error(operation, "authentication_failed", "光鸭登录已失效，请重新填写 refresh_token")
    if isinstance(exc, GuangyapanNotFoundError):
        return _provider_error(operation, "source_not_found", "光鸭媒体文件不存在")
    if isinstance(exc, GuangyapanRateLimitError):
        return _provider_error(operation, "unavailable", "光鸭接口限流，请稍后重试", retryable=True)
    return _provider_error(operation, "unavailable", "光鸭播放服务暂不可用", retryable=True)


class GuangyapanPlayback:
    def __init__(self, client, *, credential_fingerprint: str = "default") -> None:
        self._client = client
        self._credential_fingerprint = credential_fingerprint

    async def handle(self, *, media, context) -> Response:
        file_id = str((media.storage_ref or {}).get("file_id") or "")
        if not file_id:
            raise _provider_error("playback", "source_not_found", "光鸭媒体引用无效")
        key = (
            self._credential_fingerprint,
            media.library.library_id,
            media.media_id,
            file_id,
        )
        entry = await self._direct_entry(key=key, file_id=file_id)
        if context.delivery == "redirect":
            return RedirectResponse(entry.url, status_code=302)
        return await self._proxy(key=key, file_id=file_id, request=context.request)

    async def _direct_entry(self, *, key: tuple[object, ...], file_id: str) -> _DirectEntry:
        entry = _CACHE.get(key)
        if entry is not None:
            return entry
        task = _CACHE.task_for(key)
        if task is None:
            task = asyncio.create_task(asyncio.to_thread(self._client.download_url, file_id))
            _CACHE.register_task(key, task)
        try:
            url, duration_seconds = await task
        except GuangyapanError as exc:
            raise _cloud_error("playback", exc) from exc
        except Exception as exc:
            raise _provider_error(
                "playback", "unavailable", "光鸭直链解析失败", retryable=True
            ) from exc
        finally:
            _CACHE.clear_task(key)
        return _CACHE.put(key, url, duration_seconds)

    async def _proxy(self, *, key: tuple[object, ...], file_id: str, request) -> Response:
        upstream_headers = {
            name: request.headers[name]
            for name in _FORWARD_REQUEST_HEADERS
            if name in request.headers
        }
        upstream_headers["user-agent"] = USER_AGENT
        is_head = request.method.upper() == "HEAD"

        for attempt in range(2):
            entry = await self._direct_entry(key=key, file_id=file_id)
            await entry.slots.acquire()
            client = httpx.AsyncClient(
                timeout=_STREAM_TIMEOUT, trust_env=False, follow_redirects=True
            )
            upstream = None
            try:
                upstream = await client.send(
                    client.build_request(
                        "HEAD" if is_head else "GET", entry.url, headers=upstream_headers
                    ),
                    stream=not is_head,
                )
            except httpx.RequestError as exc:
                entry.slots.release()
                await client.aclose()
                raise _provider_error(
                    "playback", "unavailable", "光鸭直链网络不可用", retryable=True
                ) from exc
            except Exception as exc:
                entry.slots.release()
                await client.aclose()
                raise _provider_error(
                    "playback", "unavailable", "光鸭直链请求失败", retryable=True
                ) from exc

            if (
                upstream.status_code in _RETRYABLE_UPSTREAM_STATUSES
                and attempt == 0
            ):
                await upstream.aclose()
                await client.aclose()
                entry.slots.release()
                _CACHE.discard(key)
                continue

            if upstream.status_code == 416:
                headers = {
                    name: upstream.headers[name]
                    for name in _FORWARD_RESPONSE_HEADERS
                    if name in upstream.headers
                }
                media_type = upstream.headers.get("content-type", "application/octet-stream")
                await upstream.aclose()
                await client.aclose()
                entry.slots.release()
                return Response(status_code=416, headers=headers, media_type=media_type)

            if upstream.status_code not in {200, 206}:
                status_code = upstream.status_code
                await upstream.aclose()
                await client.aclose()
                entry.slots.release()
                if status_code == 404:
                    raise _provider_error(
                        "playback", "source_not_found", "光鸭媒体文件不存在"
                    )
                raise _provider_error(
                    "playback",
                    "unavailable",
                    f"光鸭直链返回 HTTP {status_code}",
                    retryable=status_code >= 500,
                )

            headers = {
                name: upstream.headers[name]
                for name in _FORWARD_RESPONSE_HEADERS
                if name in upstream.headers
            }
            media_type = upstream.headers.get("content-type", "application/octet-stream")
            if is_head:
                await upstream.aclose()
                await client.aclose()
                entry.slots.release()
                return Response(
                    status_code=upstream.status_code, headers=headers, media_type=media_type
                )

            async def body_iterator(upstream=upstream, client=client, entry=entry):
                try:
                    async for chunk in upstream.aiter_bytes(_PROXY_CHUNK):
                        yield chunk
                finally:
                    await upstream.aclose()
                    await client.aclose()
                    entry.slots.release()

            return StreamingResponse(
                body_iterator(),
                status_code=upstream.status_code,
                headers=headers,
                media_type=media_type,
            )

        raise _provider_error("playback", "unavailable", "光鸭直链多次失效", retryable=True)
