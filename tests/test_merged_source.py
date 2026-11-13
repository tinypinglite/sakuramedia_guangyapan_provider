from sakuramedia_guangyapan_provider.merged_source import GuangyapanMergedSource


class _FakeReader:
    def __init__(self) -> None:
        self._data = b"abcdefghij"
        self._position = 0

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        return None

    def seek(self, position: int) -> int:
        self._position = position
        return position

    def read(self, size: int = -1) -> bytes:
        end = len(self._data) if size < 0 else self._position + size
        data = self._data[self._position : end]
        self._position += len(data)
        return data


def test_guangyapan_merged_source_reads_and_streams() -> None:
    source = GuangyapanMergedSource(
        size=10,
        identity=("test", 10),
        reader_factory=lambda _length: _FakeReader(),
    )

    assert source.read_at(2, 3) == b"cde"
    assert b"".join(source.iter_range(2, 3)) == b"cde"
