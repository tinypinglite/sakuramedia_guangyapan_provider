"""Remote random-access source for Guangyapan virtual-MP4 merged playback."""

from __future__ import annotations

from collections.abc import Callable, Iterable

from .exceptions import GuangyapanRequestError
from .range_reader import GuangyapanRangeReader


class GuangyapanMergedSource:
    """Expose one Guangyapan file through the generic MP4 merge source protocol."""

    def __init__(
        self,
        *,
        size: int,
        identity: tuple[object, ...],
        reader_factory: Callable[[int], GuangyapanRangeReader],
    ) -> None:
        if size <= 0:
            raise ValueError("Guangyapan merged source size must be positive")
        self.size = size
        self.identity = identity
        self._reader_factory = reader_factory

    def read_at(self, offset: int, length: int) -> bytes:
        if length <= 0:
            return b""
        with self._reader_factory(length) as reader:
            reader.seek(offset)
            data = reader.read(length)
        if len(data) != length:
            raise GuangyapanRequestError("光鸭合并播放读取不完整")
        return data

    def iter_range(self, offset: int, length: int) -> Iterable[bytes]:
        if length <= 0:
            return
        with self._reader_factory(length) as reader:
            reader.seek(offset)
            remaining = length
            while remaining > 0:
                chunk = reader.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise GuangyapanRequestError("光鸭合并播放读取中断")
                remaining -= len(chunk)
                yield chunk
