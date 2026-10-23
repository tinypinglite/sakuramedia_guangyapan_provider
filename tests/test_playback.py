from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from sakuramedia_guangyapan_provider import playback as playback_module
from sakuramedia_guangyapan_provider.playback import GuangyapanPlayback


@pytest.fixture(autouse=True)
def clear_playback_cache():
    playback_module._CACHE._direct.clear()
    playback_module._CACHE._tasks.clear()
    yield
    playback_module._CACHE._direct.clear()
    playback_module._CACHE._tasks.clear()


class DirectClient:
    def __init__(self) -> None:
        self.calls = 0

    def download_url(self, file_id: str) -> tuple[str, int]:
        self.calls += 1
        return f"https://cdn.example/{self.calls}/{file_id}", 21600


def _media() -> SimpleNamespace:
    return SimpleNamespace(
        storage_ref={"file_id": "2"},
        library=SimpleNamespace(library_id=7),
        media_id=11,
    )


def _context(
    *, delivery: str, method: str = "GET", headers: dict[str, str] | None = None
):
    request = SimpleNamespace(method=method, headers=headers or {})
    return SimpleNamespace(delivery=delivery, request=request)


def test_redirect_reuses_cached_direct_url() -> None:
    client = DirectClient()
    playback = GuangyapanPlayback(client, credential_fingerprint="cache-test")

    first = asyncio.run(
        playback.handle(media=_media(), context=_context(delivery="redirect"))
    )
    second = asyncio.run(
        playback.handle(media=_media(), context=_context(delivery="redirect"))
    )

    assert first.status_code == 302
    assert first.headers["location"] == second.headers["location"]
    assert client.calls == 1


def test_proxy_relays_range_and_head(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_ranges: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_ranges.append(request.headers.get("range", ""))
        return httpx.Response(
            206,
            content=b"0123456789",
            headers={"Content-Range": "bytes 0-9/100", "Accept-Ranges": "bytes"},
            request=request,
        )

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        playback_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    playback = GuangyapanPlayback(DirectClient(), credential_fingerprint="proxy-test")

    async def run():
        get_response = await playback.handle(
            media=_media(),
            context=_context(delivery="proxy", headers={"range": "bytes=0-9"}),
        )
        body = b"".join([chunk async for chunk in get_response.body_iterator])
        head_response = await playback.handle(
            media=_media(),
            context=_context(delivery="proxy", method="HEAD"),
        )
        return get_response, body, head_response

    get_response, body, head_response = asyncio.run(run())

    assert get_response.status_code == 206
    assert body == b"0123456789"
    assert get_response.headers["content-range"] == "bytes 0-9/100"
    assert head_response.status_code == 206
    assert seen_ranges == ["bytes=0-9", ""]


def test_proxy_relays_416(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            416,
            headers={"Content-Range": "bytes */100"},
            request=request,
        )

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        playback_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    playback = GuangyapanPlayback(DirectClient(), credential_fingerprint="416-test")

    response = asyncio.run(
        playback.handle(
            media=_media(),
            context=_context(delivery="proxy", headers={"range": "bytes=999-"}),
        )
    )

    assert response.status_code == 416
    assert response.headers["content-range"] == "bytes */100"


def test_proxy_refreshes_url_after_403(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DirectClient()

    def handler(request: httpx.Request) -> httpx.Response:
        if "/1/" in str(request.url):
            return httpx.Response(403, request=request)
        return httpx.Response(
            206,
            content=b"ok",
            headers={"Content-Range": "bytes 0-1/2"},
            request=request,
        )

    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        playback_module.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    playback = GuangyapanPlayback(client, credential_fingerprint="refresh-test")

    async def run():
        response = await playback.handle(
            media=_media(),
            context=_context(delivery="proxy", headers={"range": "bytes=0-1"}),
        )
        return response, b"".join([chunk async for chunk in response.body_iterator])

    response, body = asyncio.run(run())

    assert response.status_code == 206
    assert body == b"ok"
    assert client.calls == 2
