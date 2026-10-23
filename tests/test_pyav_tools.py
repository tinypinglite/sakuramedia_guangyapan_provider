"""关键帧抽帧的离线单测：用 PyAV 现造一段小视频，验证偏移唯一且落在关键帧上。"""

from __future__ import annotations

import io
from pathlib import Path

import av
import pytest
from PIL import Image

from sakuramedia_guangyapan_provider.pyav_tools import generate_thumbnails


class BytesReader(io.BytesIO):
    """把内存里的 mp4 当成远端读取器喂给 PyAV。"""

    def __enter__(self) -> "BytesReader":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def build_video(*, frames: int, keyframe_interval: int, fps: int = 10) -> bytes:
    buffer = io.BytesIO()
    container = av.open(buffer, mode="w", format="mp4")
    stream = container.add_stream("libx264", rate=fps)
    stream.width = 160
    stream.height = 120
    stream.pix_fmt = "yuv420p"
    stream.options = {"g": str(keyframe_interval)}
    for index in range(frames):
        image = Image.new("RGB", (160, 120), (index * 4 % 256, index * 2 % 256, 40))
        frame = av.VideoFrame.from_image(image)
        frame.pts = index
        for packet in stream.encode(frame):
            container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()
    return buffer.getvalue()


@pytest.mark.parametrize("keyframe_interval", [10, 30])
def test_thumbnail_offsets_are_unique_and_keyframe_aligned(
    tmp_path: Path, keyframe_interval: int
) -> None:
    data = build_video(frames=50, keyframe_interval=keyframe_interval)
    workspace = tmp_path / "ws"
    workspace.mkdir()

    artifacts = generate_thumbnails(
        reader_factory=lambda: BytesReader(data),
        duration_seconds=5,
        interval_seconds=1,
        workspace=workspace,
    )

    offsets = [offset for offset, _path in artifacts]
    assert offsets, "至少应产出一张缩略图"
    assert len(offsets) == len(set(offsets)), offsets
    assert offsets == sorted(offsets), offsets
    assert all(0 <= offset <= 6 for offset in offsets), offsets
    for offset, relative_path in artifacts:
        assert workspace.joinpath(relative_path).is_file()
        assert relative_path == f"thumbnail-{offset}.webp"


def test_short_gop_matches_nominal_grid(tmp_path: Path) -> None:
    """1 秒一个关键帧、1 秒抽样 → 每个标称点都能拿到自己的关键帧。"""

    data = build_video(frames=50, keyframe_interval=10)
    workspace = tmp_path / "ws"
    workspace.mkdir()

    artifacts = generate_thumbnails(
        reader_factory=lambda: BytesReader(data),
        duration_seconds=5,
        interval_seconds=1,
        workspace=workspace,
    )

    assert [offset for offset, _ in artifacts] == [0, 1, 2, 3, 4]


def test_long_gop_still_produces_unique_frames(tmp_path: Path) -> None:
    """3 秒一个关键帧、1 秒抽样 → 命中同一关键帧时向前回退，偏移仍然唯一。"""

    data = build_video(frames=60, keyframe_interval=30)
    workspace = tmp_path / "ws"
    workspace.mkdir()

    artifacts = generate_thumbnails(
        reader_factory=lambda: BytesReader(data),
        duration_seconds=6,
        interval_seconds=1,
        workspace=workspace,
    )

    offsets = [offset for offset, _ in artifacts]
    assert len(offsets) == len(set(offsets)), offsets
    assert offsets == sorted(offsets)
    assert len(offsets) >= 2, offsets
