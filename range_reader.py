"""可 seek 的 HTTP Range 读取器（喂给 PyAV / 哈希采样）。

实测约束：
- 直链支持 Range（206 + Content-Range），GET/HEAD 都不校验 Referer/UA；
- **刚落盘的文件在 30–60 秒内会「声明长度却返回 0 字节」**，必须当可重试错误，不能当 EOF；
- 签名 URL 有效期 6 小时，长任务中途过期要重新申请。
"""

from __future__ import annotations

import random
import re
import time
from collections.abc import Callable

import httpx

from .exceptions import (
    GuangyapanObjectNotReadyError,
    GuangyapanReadBudgetExceeded,
    GuangyapanRequestError,
)

_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_DEFAULT_RETRY_DELAYS = (3.0, 5.0, 8.0, 10.0, 12.0)


class GuangyapanRangeReader:
    """文件式对象；同一时刻只有一个上行 Range 请求在飞。"""

    def __init__(
        self,
        *,
        url_provider: Callable[[], tuple[str, int]] | None = None,
        url: str = "",
        file_size_bytes: int,
        chunk_size: int = 2 * 1024 * 1024,
        max_fetched_bytes: int | None = None,
        retry_delays: tuple[float, ...] = _DEFAULT_RETRY_DELAYS,
        url_expires_at: float = 0.0,
    ) -> None:
        if not url and url_provider is None:
            raise ValueError("range reader 需要 url 或 url_provider")
        if file_size_bytes <= 0:
            raise ValueError("range reader 文件大小无效")
        self._url_provider = url_provider
        self._url = url
        self._url_expires_at = url_expires_at
        self._file_size = file_size_bytes
        self._chunk_size = max(64 * 1024, chunk_size)
        self._max_fetched_bytes = max_fetched_bytes
        self._retry_delays = retry_delays
        self._client = httpx.Client(timeout=httpx.Timeout(120.0, connect=15.0), trust_env=False, follow_redirects=True)
        self._position = 0
        self._buffer = b""
        self._buffer_start = 0
        self.fetched_bytes = 0

    # ---------------------------------------------------------------- 文件接口

    @property
    def file_size(self) -> int:
        return self._file_size

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._position = offset
        elif whence == 1:
            self._position += offset
        elif whence == 2:
            self._position = self._file_size + offset
        else:
            raise ValueError("不支持的 seek 起点")
        self._position = max(0, self._position)
        return self._position

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = self._file_size - self._position
        output = bytearray()
        while size > 0 and self._position < self._file_size:
            offset = self._position - self._buffer_start
            if 0 <= offset < len(self._buffer):
                take = min(size, len(self._buffer) - offset)
                output += self._buffer[offset : offset + take]
                self._position += take
                size -= take
                continue
            self._fetch(self._position, size)
        return bytes(output)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "GuangyapanRangeReader":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ---------------------------------------------------------------- 内部

    def _current_url(self) -> str:
        if self._url and (not self._url_expires_at or time.time() < self._url_expires_at):
            return self._url
        if self._url_provider is None:
            return self._url
        url, duration = self._url_provider()
        self._url = url
        self._url_expires_at = time.time() + max(60, duration - 300)
        return self._url

    def _remaining_budget(self) -> int | None:
        if self._max_fetched_bytes is None:
            return None
        return self._max_fetched_bytes - self.fetched_bytes

    def _attempt(self, start: int, count: int, max_bytes: int | None) -> bytes:
        end = min(start + count, self._file_size) - 1
        try:
            with self._client.stream(
                "GET", self._current_url(), headers={"Range": f"bytes={start}-{end}"}
            ) as response:
                if response.status_code in {403, 404, 410}:
                    # 签名过期：丢弃缓存 URL 后由外层重试
                    self._url = ""
                    self._url_expires_at = 0.0
                    raise GuangyapanObjectNotReadyError("光鸭直链已失效")
                if response.status_code not in {200, 206}:
                    raise GuangyapanRequestError(f"光鸭直链读取失败 HTTP {response.status_code}")
                content_length = response.headers.get("Content-Length")
                if max_bytes is not None and content_length:
                    try:
                        if int(content_length) > max_bytes:
                            raise GuangyapanReadBudgetExceeded("光鸭读取超过本次预算")
                    except ValueError:
                        pass
                body_buffer = bytearray()
                for chunk in response.iter_bytes():
                    if max_bytes is not None and len(body_buffer) + len(chunk) > max_bytes:
                        raise GuangyapanReadBudgetExceeded("光鸭读取超过本次预算")
                    body_buffer.extend(chunk)
                body = bytes(body_buffer)
                if response.status_code == 206:
                    match = _CONTENT_RANGE.fullmatch(str(response.headers.get("Content-Range", "")).strip())
                    if match is None:
                        raise GuangyapanRequestError("光鸭 Range 响应无效")
                    actual_start, actual_end, actual_total = (int(value) for value in match.groups())
                    if (
                        actual_start != start
                        or actual_end < actual_start
                        or actual_end > end
                        or actual_total != self._file_size
                    ):
                        raise GuangyapanRequestError("光鸭 Range 响应不一致")
                    expected = actual_end - actual_start + 1
                    if len(body) != expected:
                        raise GuangyapanObjectNotReadyError("光鸭 Range 响应不完整")
                elif start != 0 or len(body) != self._file_size:
                    raise GuangyapanRequestError("光鸭未正确支持 Range")
                if not body:
                    raise GuangyapanObjectNotReadyError("光鸭直链返回空数据")
                return body
        except httpx.RequestError as exc:
            raise GuangyapanRequestError("光鸭直链网络读取失败") from exc

    def _fetch(self, start: int, requested: int) -> None:
        budget = self._remaining_budget()
        if budget is not None and budget <= 0:
            raise GuangyapanReadBudgetExceeded("光鸭读取超过本次预算")
        count = max(self._chunk_size, requested)
        if budget is not None:
            count = min(count, budget)

        last_error: Exception | None = None
        attempts = len(self._retry_delays) + 1
        for attempt in range(attempts):
            try:
                body = self._attempt(start, count, budget)
            except GuangyapanObjectNotReadyError as exc:
                # 刚落盘的文件会连续返回 0 字节，重试到它可读为止
                last_error = exc
            except GuangyapanRequestError as exc:
                if attempt + 1 >= attempts:
                    raise
                last_error = exc
            else:
                self._buffer = body
                self._buffer_start = start
                self.fetched_bytes += len(body)
                return
            if attempt + 1 < attempts:
                base = self._retry_delays[attempt]
                time.sleep(base + random.uniform(0.0, base * 0.25))
        raise GuangyapanObjectNotReadyError("光鸭文件暂不可读（刚落盘）") from last_error
