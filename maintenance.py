"""光鸭云盘的维护任务（手动触发，都要求显式确认）。

1. `clear_recycle_bin`：清空光鸭回收站。实测 `clear_recycle_bin` 是**异步/最终一致**的
   （调用后约 1 分钟才真正清空），所以必须轮询到回收站为空才算成功（`endpoints.md` §5.5）。
2. `cleanup_empty_media_dirs`：删除媒体目录下的空子目录（导入/删除影片后残留的 `jav/<番号>/`）。
   删除前会对每个候选目录**重新列一次目录**，避免扫描快照与真实状态不一致时误删；
   删除走**回收站**（一步）而不是永久删除——扫的是网络快照，误删要能捞回来，
   确定性清理由「清空回收站」任务负责。
"""

from __future__ import annotations

import time
from typing import Any, Literal

from loguru import logger
from pydantic import BaseModel, ConfigDict

from .exceptions import GuangyapanError, safe_error_message
from .guangya import DELETE_BATCH_LIMIT, GuangyapanClient
from .library_tree import load_library_tree

PROVIDER_KEY = "guangyapan"
CLEAR_RECYCLE_TIMEOUT_SECONDS = 300.0
CLEAR_RECYCLE_POLL_SECONDS = 5.0
class MaintenanceConfirmParams(BaseModel):
    """破坏性手动任务的显式确认参数（与 115 的清理任务同一套约定）。"""

    model_config = ConfigDict(extra="forbid")

    confirm: Literal[True]


class _Progress:
    def __init__(self, reporter: Any | None) -> None:
        self.reporter = reporter
        self.text = "准备"
        self.detail = ""
        self.current = 0
        self.total = 0
        self._last_report = float("-inf")

    def __call__(self, *, text: str | None = None, detail: str | None = None,
                 current: int | None = None, total: int | None = None,
                 summary: dict[str, int] | None = None, force: bool = False) -> None:
        if text is not None:
            self.text = text
        if detail is not None:
            self.detail = detail
        if current is not None:
            self.current = current
        if total is not None:
            self.total = total
        now = time.monotonic()
        if not force and now - self._last_report < 2.0:
            return
        self._last_report = now
        if self.reporter is None:
            return
        payload: dict[str, Any] = {
            "text": self.text,
            "current": self.current,
            "total": self.total,
        }
        if self.detail:
            payload["detail"] = self.detail
        if summary is not None:
            payload["summary_patch"] = dict(summary)
        self.reporter.progress_callback(payload)


def clear_recycle_bin(reporter: Any | None, params: dict[str, Any] | None) -> dict[str, int]:
    """清空所有光鸭媒体库账号的回收站。"""

    _require_confirmation(params)
    progress = _Progress(reporter)
    stats = {"libraries": 0, "entries_before": 0, "cleared_libraries": 0, "failed_libraries": 0}
    libraries = _load_media_libraries()
    stats["libraries"] = len(libraries)
    for index, library in enumerate(libraries, 1):
        progress(text="清理回收站", detail=f"媒体库 {index}/{len(libraries)}（ID {library.id}）", force=True)
        try:
            refresh_token = _library_token(library)
            with GuangyapanClient(refresh_token) as client:
                before = client.recycle_bin_total()
                stats["entries_before"] += before
                if before == 0:
                    logger.info("光鸭回收站清理跳过（已为空）library_id={}", library.id)
                    continue
                client.clear_recycle_bin()
                if not _wait_recycle_bin_empty(client):
                    raise GuangyapanError("光鸭回收站清空超时")
                stats["cleared_libraries"] += 1
                logger.info("光鸭回收站已清空 library_id={} entries={}", library.id, before)
        except (GuangyapanError, TypeError, ValueError) as exc:
            stats["failed_libraries"] += 1
            logger.warning(
                "光鸭回收站清理失败 library_id={} error_type={} reason={}",
                library.id,
                type(exc).__name__,
                safe_error_message(exc),
            )
        progress(summary=stats, force=True)
    progress(
        text=f"回收站清理结束：清空 {stats['cleared_libraries']} 个库，失败 {stats['failed_libraries']} 个",
        force=True,
    )
    logger.info("光鸭回收站清理完成 stats={}", stats)
    return stats


def cleanup_empty_media_dirs(reporter: Any | None, params: dict[str, Any] | None) -> dict[str, int]:
    """删除媒体目录下的空子目录（保留媒体目录本身）。"""

    _require_confirmation(params)
    progress = _Progress(reporter)
    stats = {
        "libraries": 0,
        "scanned_directories": 0,
        "deleted_directories": 0,
        "skipped_directories": 0,
        "failed_libraries": 0,
    }
    libraries = _load_media_libraries()
    stats["libraries"] = len(libraries)
    for index, library in enumerate(libraries, 1):
        progress(text="扫描空目录", detail=f"媒体库 {index}/{len(libraries)}（ID {library.id}）", force=True)
        try:
            refresh_token, media_root_id = _library_config(library)
            with GuangyapanClient(refresh_token) as client:
                progress(text="扫描空目录", detail="正在读取媒体库快照", force=True)
                before = load_library_tree(
                    client,
                    progress_callback=lambda current, _total: progress(
                        text="扫描空目录", current=current, total=0
                    ),
                )
                before_empty, nested_skipped = before.empty_directory_summary(media_root_id)
                progress(text="复核空目录", detail="正在读取第二份媒体库快照", force=True)
                after = load_library_tree(
                    client,
                    progress_callback=lambda current, _total: progress(
                        text="复核空目录", current=current, total=0
                    ),
                )
                after_empty = after.empty_directory_ids(media_root_id)
                candidates = sorted(before_empty & after_empty)
                stats["scanned_directories"] += after.directory_count(media_root_id)
                stats["skipped_directories"] += nested_skipped + len(before_empty - after_empty)
                total = len(candidates)
                done = 0
                for chunk in _chunks(candidates, DELETE_BATCH_LIMIT):
                    progress(text="删除空目录", current=done, total=total, force=True)
                    client.wait_task(client.delete_files(chunk))
                    for directory_id in chunk:
                        logger.info(
                            "光鸭空目录清理已删除 library_id={} directory_id={}",
                            library.id,
                            directory_id,
                        )
                    stats["deleted_directories"] += len(chunk)
                    done += len(chunk)
                    progress(current=done, total=total, force=True)
        except (GuangyapanError, TypeError, ValueError) as exc:
            stats["failed_libraries"] += 1
            logger.warning(
                "光鸭空目录清理失败 library_id={} error_type={} reason={}",
                library.id,
                type(exc).__name__,
                safe_error_message(exc),
            )
        progress(summary=stats, force=True)
    progress(
        text=(
            f"空目录清理结束：扫描 {stats['scanned_directories']} 个目录，"
            f"删除 {stats['deleted_directories']} 个，保留 {stats['skipped_directories']} 个，"
            f"失败媒体库 {stats['failed_libraries']} 个"
        ),
        force=True,
    )
    logger.info("光鸭空目录清理完成 stats={}", stats)
    return stats


def _wait_recycle_bin_empty(client: GuangyapanClient) -> bool:
    deadline = time.monotonic() + CLEAR_RECYCLE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        time.sleep(CLEAR_RECYCLE_POLL_SECONDS)
        if client.recycle_bin_total() == 0:
            return True
    return False


def _load_media_libraries() -> tuple[Any, ...]:
    from src.model import MediaLibrary

    return tuple(
        MediaLibrary.select()
        .where(MediaLibrary.provider_key == PROVIDER_KEY)
        .order_by(MediaLibrary.id.asc())
    )


def _library_config(library: Any) -> tuple[str, str]:
    config = library.provider_config
    if not isinstance(config, dict):
        raise TypeError(f"光鸭媒体库配置无效 library_id={library.id}")
    refresh_token = config.get("refresh_token")
    media_root_id = config.get("media_root_id")
    if not isinstance(refresh_token, str) or not refresh_token:
        raise ValueError(f"光鸭媒体库缺少 refresh_token library_id={library.id}")
    if not isinstance(media_root_id, str) or not media_root_id:
        raise ValueError(f"光鸭媒体库缺少媒体目录 library_id={library.id}")
    return refresh_token, media_root_id


def _library_token(library: Any) -> str:
    config = library.provider_config
    if not isinstance(config, dict):
        raise TypeError(f"光鸭媒体库配置无效 library_id={library.id}")
    refresh_token = config.get("refresh_token")
    if not isinstance(refresh_token, str) or not refresh_token:
        raise ValueError(f"光鸭媒体库缺少 refresh_token library_id={library.id}")
    return refresh_token


def _require_confirmation(params: dict[str, Any] | None) -> None:
    MaintenanceConfirmParams.model_validate(params or {})


def _chunks(values: list[str], size: int):
    for start in range(0, len(values), size):
        yield values[start : start + size]
