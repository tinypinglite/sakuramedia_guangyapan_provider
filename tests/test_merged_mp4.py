"""merged_mp4 虚拟合并核心逻辑测试。

测试 mp4 用系统 ffmpeg 现场生成（CI 无 ffmpeg 时整体跳过）。覆盖：
解析、合并布局、Range 段映射与手工拼接一致性、规格门槛。
"""

import shutil
import struct
import subprocess

import pytest
from sakuramedia_guangyapan_provider import merged_mp4 as m


def _have_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


def _make_part(tmp_path, name, duration, size="320x240", pattern="testsrc", freq=440):
    out = tmp_path / f"{name}.mp4"
    subprocess.run(
        [
            "ffmpeg", "-v", "error",
            "-f", "lavfi", "-i", f"{pattern}=duration={duration}:size={size}:rate=30",
            "-f", "lavfi", "-i", f"sine=frequency={freq}:duration={duration}",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac",
            "-movflags", "faststart", "-y", str(out),
        ],
        check=True,
        capture_output=True,
    )
    return out


class _MemorySource:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.size = len(data)
        self.identity = ("test", len(data))

    def read_at(self, offset: int, length: int) -> bytes:
        return self._data[offset : offset + length]

    def iter_range(self, offset: int, length: int):
        data = self.read_at(offset, length)
        yield data


def _parse(path):
    return m.parse_source(_MemorySource(path.read_bytes()))


def _materialize(layout):
    """把布局完整还原成字节，验证 resolve_range 与布局一致。"""
    data = bytearray()
    for kind, arg, off, ln in layout.resolve_range(0, layout.total_size):
        if kind == "mem":
            data += arg
        else:
            data += arg.read_at(off, ln)
    return bytes(data)


def _hand_built(layout):
    expected = layout.header + struct.pack(
        ">I4s", 8 + sum(p.payload_len for p in layout.parts), b"mdat"
    )
    for part in layout.parts:
        expected += part.source.read_at(part.payload_start, part.payload_len)
    return expected


def _box(box_type: bytes, payload: bytes = b"") -> bytes:
    return struct.pack(">I4s", len(payload) + 8, box_type) + payload


def test_parse_file_rejects_multiple_mdat_boxes(tmp_path):
    path = tmp_path / "multiple-mdat.mp4"
    path.write_bytes(
        _box(b"ftyp", b"isom")
        + _box(b"moov")
        + _box(b"mdat", b"first")
        + _box(b"mdat", b"second")
    )

    with pytest.raises(m.Mp4MergeError, match="单个 mdat"):
        _parse(path)


def test_parse_file_rejects_fragmented_mp4(tmp_path):
    path = tmp_path / "fragmented.mp4"
    path.write_bytes(
        _box(b"ftyp", b"isom")
        + _box(b"moov")
        + _box(b"moof")
        + _box(b"mdat", b"fragment")
    )

    with pytest.raises(m.Mp4MergeError, match="分片 MP4"):
        _parse(path)


@pytest.mark.skipif(not _have_ffmpeg(), reason="需要 ffmpeg 生成测试 mp4")
class TestMergedMp4Build:
    def test_layout_materializes_to_logical_file(self, tmp_path):
        p1 = _make_part(tmp_path, "p1", 2)
        p2 = _make_part(tmp_path, "p2", 3)
        parts = [_parse(p1), _parse(p2)]
        layout = m.build_merged_layout(parts)

        assert layout.total_size == len(layout.header) + 8 + sum(
            p.payload_len for p in layout.parts
        )
        # resolve_range 全量还原 == 手工拼接的逻辑文件
        assert _materialize(layout) == _hand_built(layout)

    def test_partial_ranges_align(self, tmp_path):
        p1 = _make_part(tmp_path, "p1", 2)
        p2 = _make_part(tmp_path, "p2", 3)
        layout = m.build_merged_layout([_parse(p1), _parse(p2)])
        full = _hand_built(layout)

        for start, end in [
            (0, 100),                                    # 纯头部
            (len(layout.header) - 10, len(layout.header) + 10),  # 头 + mdat 头交界
            (layout.mdat_payload_start, layout.mdat_payload_start + 500),  # part1 段
            (layout.total_size - 100, layout.total_size),  # 尾部
        ]:
            data = bytearray()
            for kind, arg, off, ln in layout.resolve_range(start, end):
                if kind == "mem":
                    data += arg
                else:
                    data += arg.read_at(off, ln)
            assert bytes(data) == full[start:end]

    def test_need_at_least_two_parts(self, tmp_path):
        p1 = _make_part(tmp_path, "p1", 2)
        with pytest.raises(m.Mp4MergeError):
            m.build_merged_layout([_parse(p1)])

    def test_mismatched_spec_rejected(self, tmp_path):
        p1 = _make_part(tmp_path, "p1", 2)
        p2 = _make_part(tmp_path, "p2", 2, size="640x480")
        with pytest.raises(m.Mp4MergeError) as exc:
            m.build_merged_layout([_parse(p1), _parse(p2)])
        assert exc.value.error_code == "merged_mp4_mismatched_spec"

    def test_header_contains_merged_moov(self, tmp_path):
        p1 = _make_part(tmp_path, "p1", 2)
        p2 = _make_part(tmp_path, "p2", 3)
        layout = m.build_merged_layout([_parse(p1), _parse(p2)])
        # header = ftyp + moov，moov 必须位于 mdat 前（faststart 布局，Pico 播放器硬性要求）
        assert layout.header[4:8] == b"ftyp"
        assert b"moov" in layout.header
