"""PyAV 相关的媒体处理：缩略图、剪辑、时长/分辨率/编码探测、封面源。

全部通过 `GuangyapanRangeReader` 远端按需读取；同时只保持一个容器顺序解码，避免把整个文件
拉下来（实测 CDN 单连接约 11 MiB/s，小块随机读延迟 280–520 ms）。
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .exceptions import GuangyapanRequestError

_THUMBNAIL_WIDTH = 640
_THUMBNAIL_HEIGHT = 360
_THUMBNAIL_QUALITY = 82
_MAX_FRAME_STEPS = 600  # 单个采样点最多解码多少帧，防止坏文件卡死


def _require_av():
    try:
        import av  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - 宿主环境自带 av
        raise GuangyapanRequestError("宿主环境缺少 PyAV，无法处理媒体") from exc
    return av


def _require_image():
    try:
        from PIL import Image  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - 宿主环境自带 Pillow
        raise GuangyapanRequestError("宿主环境缺少 Pillow，无法生成缩略图") from exc
    return Image


def open_container(reader):
    av = _require_av()
    container = av.open(reader, options={"probesize": str(8 * 1024 * 1024)})
    if not container.streams.video:
        container.close()
        raise GuangyapanRequestError("光鸭文件没有视频流")
    return container


def generate_thumbnails(
    *,
    reader_factory: Callable[[], Any],
    duration_seconds: int,
    interval_seconds: int,
    workspace: Path,
    progress_callback: Callable[[str], None] | None = None,
) -> list[tuple[int, str]]:
    """按间隔抽关键帧写 WebP，返回 (offset_seconds, relative_path) 列表。

    每个采样点只取「该时间点之前最近的关键帧」，**不做向前解码**：实测把单张缩略图的远端
    读取量从约 1.3 MiB 降到约 0.2 MiB（关键帧本身的中位大小 192 KiB）。代价是画面比标称
    时间点早一个关键帧间隔（实测 ≤2 秒）——返回的 offset 是这一帧的**真实时刻**，所以
    「点哪张缩略图跳到哪一刻」依然是精确的。
    """
    av = _require_av()
    image_module = _require_image()
    duration = max(0, int(duration_seconds))
    offsets = list(range(0, duration, max(1, interval_seconds))) or [0]

    artifacts: list[tuple[int, str]] = []
    used_offsets: set[int] = set()
    with reader_factory() as reader:
        container = open_container(reader)
        try:
            stream = container.streams.video[0]
            for index, offset in enumerate(offsets):
                sampled = _grab_keyframe(av, container, stream, float(offset), used_offsets)
                if sampled is None:
                    continue
                frame_time, frame_image = sampled
                destination = workspace / f"thumbnail-{frame_time}.webp"
                try:
                    frame_image.thumbnail(
                        (_THUMBNAIL_WIDTH, _THUMBNAIL_HEIGHT), image_module.Resampling.LANCZOS
                    )
                    frame_image.save(
                        destination,
                        format="WEBP",
                        quality=_THUMBNAIL_QUALITY,
                        method=4,
                    )
                finally:
                    frame_image.close()
                if destination.is_file() and destination.stat().st_size > 0:
                    artifacts.append((frame_time, destination.name))
                if progress_callback is not None and (index % 10 == 0 or index == len(offsets) - 1):
                    progress_callback(f"正在生成缩略图 · 已生成 {len(artifacts)}/{len(offsets)} 张")
        finally:
            container.close()
    return artifacts


def _grab_keyframe(av, container, stream, target_seconds: float, used_offsets: set[int]):
    """seek 到目标之前最近的关键帧，只解这一帧；返回 (帧时刻, 图像)。"""
    try:
        container.seek(int(target_seconds * av.time_base), backward=True)
    except Exception as exc:  # noqa: BLE001 - 远端 seek 失败按无产物处理
        raise GuangyapanRequestError("光鸭媒体 seek 失败") from exc
    steps = 0
    for frame in container.decode(stream):
        steps += 1
        if steps > _MAX_FRAME_STEPS:
            return None
        if frame.is_corrupt:
            continue
        frame_time = int(frame.time or 0)
        if frame_time in used_offsets:
            # 关键帧间隔比抽样间隔还长的文件：两个采样点会命中同一帧，向前多解几帧保证偏移唯一
            continue
        used_offsets.add(frame_time)
        return frame_time, frame.to_image()
    return None


def create_clip(
    *,
    reader_factory: Callable[[], Any],
    start_offset_seconds: int,
    end_offset_seconds: int,
    workspace: Path,
) -> str:
    """把 [start, end) 区间 remux 成 MP4（不转码），返回相对路径。"""
    av = _require_av()
    start = max(0, int(start_offset_seconds))
    end = max(start + 1, int(end_offset_seconds))
    destination = workspace / f"clip-{start}-{end}.mp4"

    with reader_factory() as reader:
        container = open_container(reader)
        output = av.open(str(destination), mode="w", format="mp4")
        try:
            out_streams: dict[int, Any] = {}
            for stream in container.streams:
                out_streams[stream.index] = output.add_stream_from_template(stream)
            container.seek(int(start * av.time_base), backward=True)
            for packet in container.demux():
                if packet.size == 0 or packet.dts is None:
                    continue
                time_base = packet.time_base
                seconds = float(packet.dts * time_base) if time_base else 0.0
                if seconds < start:
                    continue
                if seconds >= end:
                    break
                packet.stream = out_streams[packet.stream.index]
                output.mux(packet)
        finally:
            container.close()
            output.close()

    if not destination.is_file() or destination.stat().st_size <= 0:
        raise GuangyapanRequestError("光鸭片段生成失败")
    return destination.name


def probe_video(*, reader_factory: Callable[[], Any]) -> dict[str, Any]:
    """读取时长 / 分辨率 / 编码信息（私有接口不返回这些，只能本地探）。"""
    av = _require_av()
    with reader_factory() as reader:
        container = open_container(reader)
        try:
            stream = container.streams.video[0]
            duration = _duration_seconds(av, container, stream)
            width = int(stream.codec_context.width or 0)
            height = int(stream.codec_context.height or 0)
            info: dict[str, Any] = {}
            if width and height:
                info["resolution"] = {"width": width, "height": height}
            if duration:
                info["duration"] = duration
            if stream.codec_context.name:
                info["videoCodec"] = stream.codec_context.name
            bit_rate = getattr(stream, "bit_rate", None) or getattr(container, "bit_rate", None)
            if bit_rate:
                info["bitRate"] = int(bit_rate)
            frame_rate = getattr(stream, "average_rate", None)
            if frame_rate:
                info["frameRate"] = int(round(float(frame_rate)))
            return {
                "duration_seconds": duration,
                "resolution": f"{width}x{height}" if width and height else None,
                "video_info": info,
            }
        finally:
            container.close()


def _duration_seconds(av, container, stream) -> int:
    stream_duration = getattr(stream, "duration", None)
    stream_time_base = getattr(stream, "time_base", None)
    if stream_duration and stream_time_base:
        seconds = int(stream_duration * stream_time_base)
        if seconds > 0:
            return seconds
    container_duration = getattr(container, "duration", None)
    time_base = getattr(av, "time_base", None)
    if container_duration and time_base:
        seconds = int(container_duration / time_base)
        if seconds > 0:
            return seconds
    return 0
