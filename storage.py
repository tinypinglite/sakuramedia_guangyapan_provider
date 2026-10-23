"""光鸭云盘 StorageProvider：宿主要求的 12 项必选能力与本次实现的可选能力。

接口行为依据真机实测结论，几个直接影响实现方式的点：
- 同名复制 / 创建**不会失败**，只会自动改名成 `xxx(1)` → 幂等必须靠 `operation_key` journal；
- `create_dir(failIfNameExist=true)` 会返回 159 且**带上已有目录 ID** → 目录落位天然幂等；
- 服务端复制 1 GiB 只要约 1.6 秒 → 大文件搬运不是瓶颈；
- 详情接口不返回视频元数据，`thumbnail` 字段也不可用 → 缩略图 / 时长 / 分辨率都自己用 PyAV 算；
- 删除只有回收站语义：普通 ID 进回收站，回收站 ID 才是永久删除。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from loguru import logger

from src.plugins.provider_protocol import (
    BrowseEntry,
    BrowsePage,
    ClipArtifact,
    ImportFile,
    ImportFileContent,
    ImportPlacement,
    JsonObject,
    LibraryHandle,
    MediaHandle,
    ProviderOperationError,
    StagedMedia,
    StagedMediaTransfer,
    StorageSpaceUsage,
    ThumbnailArtifact,
    ThumbnailGeneration,
    ThumbnailGenerationDeferred,
)

from .exceptions import (
    GuangyapanAuthError,
    GuangyapanError,
    GuangyapanNotFoundError,
    GuangyapanObjectNotReadyError,
    GuangyapanRateLimitError,
    GuangyapanReadBudgetExceeded,
    GuangyapanRequestError,
    safe_error_message,
)
from .guangya import GuangyapanClient
from .journal import OperationJournal
from .library_tree import load_library_tree
from .playback import GuangyapanPlayback
from .pyav_tools import create_clip as _create_clip
from .pyav_tools import generate_thumbnails as _generate_thumbnails
from .pyav_tools import probe_video as _probe_video
from .range_reader import GuangyapanRangeReader

PROVIDER_KEY = "guangyapan"
REF_VERSION = 1
DIR_REF_KIND = "guangyapan_dir"
ENTRY_REF_KIND = "guangyapan_entry"
MEDIA_REF_KIND = "guangyapan_media"
FILE_RECEIPT_KIND = "guangyapan_file"
STAGE_RECEIPT_KIND = "guangyapan_stage"
TRANSFER_RECEIPT_KIND = "guangyapan_transfer_stage"

_HASH_DOMAIN = b"media-file-hash-v1"
_HASH_HEAD_TAIL_BYTES = 3 * 1024 * 1024
_HASH_MIDDLE_BYTES = 1024 * 1024
_HASH_FULL_THRESHOLD = 8 * 1024 * 1024
_HASH_READY_DELAYS = (3.0, 5.0, 8.0, 10.0, 12.0)

_READ_IMPORT_FILE_LIMIT_BYTES = 8 * 1024 * 1024
_COVER_MAX_FETCHED_BYTES = 64 * 1024 * 1024
_PROBE_MAX_FETCHED_BYTES = 64 * 1024 * 1024
_THUMBNAIL_MAX_FETCHED_BYTES = 1024 * 1024 * 1024
_CLIP_MAX_FETCHED_BYTES = 1024 * 1024 * 1024

# 固定采样间隔（秒）：与 115 插件一致，不由用户配置。
_THUMBNAIL_INTERVAL_SECONDS = 10
# 抽帧只取关键帧，单次 seek 只需读一张关键帧（实测中位 192-256 KiB）。
# 读块取 64 KiB（range_reader 的下限）：块越大单次 seek 多读越多，64 KiB 最省流量。
_THUMBNAIL_READ_CHUNK_BYTES = 64 * 1024
_IMPORT_DIRECTORY_CACHE_BATCHES = 4
_IMPORT_OP_DIRECTORY_PREFIX = "op-"
_MAX_ANCESTOR_DEPTH = 64

_VIDEO_SUFFIXES = frozenset(
    {
        ".3gp", ".avi", ".flv", ".m2ts", ".m4v", ".mkv", ".mov", ".mp4", ".mpeg",
        ".mpg", ".mts", ".rm", ".rmvb", ".ts", ".vob", ".webm", ".wmv",
    }
)
_SUBTITLE_SUFFIXES = frozenset({".srt"})


# ---------------------------------------------------------------- ref 工具


def _error(operation: str, code: str, message: str, *, retryable: bool = False) -> ProviderOperationError:
    return ProviderOperationError(
        provider_key=PROVIDER_KEY,
        operation=operation,
        code=code,  # type: ignore[arg-type]
        safe_message=message,
        retryable=retryable,
    )


def _cloud_error(operation: str, exc: GuangyapanError) -> ProviderOperationError:
    if isinstance(exc, GuangyapanAuthError):
        return _error(operation, "authentication_failed", "光鸭登录已失效，请重新填写 refresh_token")
    if isinstance(exc, GuangyapanNotFoundError):
        return _error(operation, "source_not_found", "光鸭文件或目录不存在")
    if isinstance(exc, GuangyapanObjectNotReadyError):
        return _error(operation, "unavailable", "光鸭文件暂不可读，请稍后重试", retryable=True)
    if isinstance(exc, GuangyapanRateLimitError):
        return _error(operation, "unavailable", "光鸭接口限流，请稍后重试", retryable=True)
    if isinstance(exc, GuangyapanReadBudgetExceeded):
        # 安全阀触发：不是服务故障，标成可重试只会让宿主反复重跑同一份大流量
        return _error(
            operation,
            "unavailable",
            "光鸭读取超出本次任务预算，已停止读取",
            retryable=False,
        )
    return _error(operation, "unavailable", "光鸭服务暂不可用", retryable=True)


def _is_video(item: JsonObject) -> bool:
    if item.get("fileType") == 2:
        return True
    return PurePosixPath(str(item.get("fileName") or "")).suffix.lower() in _VIDEO_SUFFIXES


def _is_importable(item: JsonObject) -> bool:
    suffix = PurePosixPath(str(item.get("fileName") or "")).suffix.lower()
    return _is_video(item) or suffix in _SUBTITLE_SUFFIXES


def _modified_at(item: JsonObject) -> datetime | None:
    value = item.get("utime") or item.get("ctime")
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    return datetime.fromtimestamp(int(value), tz=timezone.utc)


def _dir_ref(parent_ref: JsonObject) -> str:
    if (
        not isinstance(parent_ref, dict)
        or parent_ref.get("v") != REF_VERSION
        or parent_ref.get("kind") != DIR_REF_KIND
    ):
        raise _error("browse", "source_not_found", "光鸭目录引用无效")
    file_id = parent_ref.get("file_id")
    return str(file_id) if isinstance(file_id, str) else ""


def _entry_ref(item: JsonObject) -> JsonObject:
    ref: JsonObject = {
        "v": REF_VERSION,
        "kind": ENTRY_REF_KIND,
        "file_id": str(item.get("fileId") or ""),
        "name": str(item.get("fileName") or ""),
        "size_bytes": int(item.get("fileSize") or 0),
        "is_video": _is_video(item),
    }
    if "parentId" in item:
        ref["parent_id"] = str(item.get("parentId") or "")
    gcid = item.get("gcid")
    if isinstance(gcid, str) and gcid:
        ref["gcid"] = gcid
    return ref


def _entry_file_id(source_ref: JsonObject, *, operation: str) -> str:
    if (
        not isinstance(source_ref, dict)
        or source_ref.get("v") != REF_VERSION
        or source_ref.get("kind") not in {ENTRY_REF_KIND, MEDIA_REF_KIND}
    ):
        raise _error(operation, "source_not_found", "光鸭文件引用无效")
    file_id = source_ref.get("file_id")
    if not isinstance(file_id, str) or not file_id:
        raise _error(operation, "source_not_found", "光鸭文件引用无效")
    return file_id


def _media_ref(file_id: str) -> JsonObject:
    return {"v": REF_VERSION, "kind": MEDIA_REF_KIND, "file_id": file_id}


def _operation_directory(operation_key: str) -> str:
    digest = hashlib.sha256(operation_key.encode("utf-8")).hexdigest()[:24]
    return f"{_IMPORT_OP_DIRECTORY_PREFIX}{digest}"


def _placement_parts(placement: ImportPlacement) -> tuple[str, ...]:
    relative = str(getattr(placement, "relative_path", "") or "").strip()
    parts = tuple(part for part in PurePosixPath(relative).parts if part not in {"", "."})
    if not parts or any(part == ".." for part in parts):
        raise _error("stage_import_file", "invalid_config", "光鸭导入目标路径无效")
    return parts


def _import_request(
    *,
    source: ImportFile,
    source_parent_id: str,
    placement: ImportPlacement,
    source_disposition: str,
) -> JsonObject:
    return {
        "source_file_id": _entry_file_id(source.source_ref, operation="stage_import_file"),
        "source_parent_id": source_parent_id,
        "source_name": source.name,
        "source_size_bytes": int(source.size_bytes),
        "placement": str(getattr(placement, "relative_path", "") or ""),
        "source_disposition": source_disposition,
    }


class GuangyapanStorageProvider:
    def __init__(
        self,
        *,
        library: LibraryHandle,
        data_dir: Path,
    ) -> None:
        config = library.provider_config
        if not isinstance(config, dict):
            raise _error("build_storage", "invalid_config", "光鸭媒体库配置无效")
        refresh_token = config.get("refresh_token")
        media_root_id = config.get("media_root_id")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise _error("build_storage", "invalid_config", "光鸭媒体库缺少 refresh_token")
        if not isinstance(media_root_id, str):
            raise _error("build_storage", "invalid_config", "光鸭媒体库缺少媒体目录")
        self.library = library
        self.data_dir = Path(data_dir)
        self._media_root_id = media_root_id
        self._client = GuangyapanClient(refresh_token)
        self._playback = GuangyapanPlayback(
            self._client,
            credential_fingerprint=hashlib.sha256(refresh_token.encode("utf-8")).hexdigest(),
        )
        self._journal = OperationJournal(self.data_dir, namespace=f"import-{library.library_id}")
        self._transfer_journal = OperationJournal(
            self.data_dir, namespace=f"transfer-{library.library_id}"
        )
        self._delete_journal = OperationJournal(
            self.data_dir, namespace=f"delete-{library.library_id}"
        )
        self._import_directory_lock = threading.Lock()
        self._import_directory_caches: OrderedDict[
            str, dict[tuple[str, str], str]
        ] = OrderedDict()

    # ---------------------------------------------------------------- 浏览

    def browse(
        self, *, parent_ref: JsonObject | None, cursor: str | None, limit: int
    ) -> BrowsePage:
        if not isinstance(limit, int) or not 1 <= limit <= 200:
            raise _error("browse", "invalid_config", "浏览分页参数无效")
        parent_id = "" if parent_ref is None else _dir_ref(parent_ref)
        try:
            page = 0 if cursor in {None, ""} else int(cursor)
            if page < 0:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise _error("browse", "invalid_config", "浏览游标无效") from exc

        try:
            items, total = self._client.list_files(
                parent_id=parent_id, page=page, page_size=limit
            )
        except GuangyapanError as exc:
            self._log_failure("browse", exc, parent_id)
            raise _cloud_error("browse", exc) from exc

        entries = tuple(self._browse_entry(item, parent_id=parent_id) for item in items)
        consumed = page * limit + len(items)
        next_cursor = str(page + 1) if items and consumed < total else None
        return BrowsePage(entries=entries, next_cursor=next_cursor)

    @staticmethod
    def _browse_entry(item: JsonObject, *, parent_id: str) -> BrowseEntry:
        is_dir = item.get("resType") == 2
        source_ref = {
            "v": REF_VERSION,
            "kind": DIR_REF_KIND if is_dir else ENTRY_REF_KIND,
            "file_id": str(item.get("fileId") or ""),
            "name": str(item.get("fileName") or ""),
        }
        if not is_dir:
            source_ref["parent_id"] = parent_id
            gcid = item.get("gcid")
            if isinstance(gcid, str) and gcid:
                source_ref["gcid"] = gcid
        return BrowseEntry(
            source_ref=source_ref,
            name=str(item.get("fileName") or ""),
            entry_type="directory" if is_dir else "file",
            size_bytes=None if is_dir else int(item.get("fileSize") or 0),
            modified_at=_modified_at(item),
            is_video=False if is_dir else _is_video(item),
        )

    # ---------------------------------------------------------------- 导入源

    def scan_import_source(
        self, *, source_ref: JsonObject, progress_callback=None
    ) -> tuple[ImportFile, ...]:
        if not isinstance(source_ref, dict):
            raise _error("scan_import_source", "source_not_found", "光鸭导入源无效")
        kind = source_ref.get("kind")
        if source_ref.get("v") != REF_VERSION or kind not in {DIR_REF_KIND, ENTRY_REF_KIND}:
            raise _error("scan_import_source", "source_not_found", "光鸭导入源无效")

        reporter = _ScanReporter(progress_callback)
        try:
            if kind == ENTRY_REF_KIND:
                file_id = _entry_file_id(source_ref, operation="scan_import_source")
                detail = self._client.file_detail(file_id)
                reporter.start("读取文件详情")
                reporter.finish(1)
                if not detail or detail.get("resType") == 2:
                    raise _error("scan_import_source", "source_not_found", "光鸭导入文件不存在")
                return (self._import_file(detail, relative_path=str(detail.get("fileName") or "")),)
            directory_id = _dir_ref(source_ref)
            reporter.start("扫描目录")
            tree = load_library_tree(
                self._client,
                progress_callback=lambda current, _total: reporter.tick(current, 0),
            )
            files = tuple(
                sorted(
                    (
                        self._import_file(item, relative_path=relative)
                        for item, relative in tree.subtree(directory_id)
                        if item.get("resType") != 2 and _is_importable(item)
                    ),
                    key=lambda item: (item.relative_path.casefold(), item.relative_path),
                )
            )
            reporter.finish(len(files))
            return files
        except GuangyapanError as exc:
            self._log_failure("scan_import_source", exc, "")
            raise _cloud_error("scan_import_source", exc) from exc

    def validate_import_source(
        self, *, source_ref: JsonObject, source_disposition: str
    ) -> None:
        operation = "validate_import_source"
        if not isinstance(source_ref, dict):
            raise _error(operation, "source_not_found", "光鸭导入源无效")
        kind = source_ref.get("kind")
        if source_ref.get("v") != REF_VERSION or kind not in {
            DIR_REF_KIND,
            ENTRY_REF_KIND,
        }:
            raise _error(operation, "source_not_found", "光鸭导入源无效")
        file_id = source_ref.get("file_id")
        if not isinstance(file_id, str):
            raise _error(operation, "source_not_found", "光鸭导入源无效")
        media_root = self._media_root_id
        if media_root == "":
            raise self._overlap_error()
        if kind == DIR_REF_KIND:
            if file_id == "":
                raise self._overlap_error()
            if self._is_at_or_below(file_id, media_root) or self._is_at_or_below(
                media_root, file_id
            ):
                raise self._overlap_error()
            return
        if not file_id:
            raise _error(operation, "source_not_found", "光鸭导入源无效")
        parent_id = source_ref.get("parent_id")
        if not isinstance(parent_id, str):
            parent_id = self._parent_id(file_id)
        if parent_id and self._is_at_or_below(parent_id, media_root):
            raise self._overlap_error()

    def _is_at_or_below(self, start_id: str, ancestor_id: str) -> bool:
        current = start_id
        visited: set[str] = set()
        for _ in range(_MAX_ANCESTOR_DEPTH):
            if current == ancestor_id:
                return True
            if not current:
                return False
            if current in visited:
                raise self._unavailable_error()
            visited.add(current)
            current = self._parent_id(current)
        raise self._unavailable_error()

    def _parent_id(self, file_id: str) -> str:
        detail = self._file_detail(file_id)
        parent = detail.get("parentId")
        if isinstance(parent, str):
            return parent
        full = detail.get("fullParentIds")
        if isinstance(full, str):
            parts = [part for part in full.split("/") if part]
            return parts[-1] if parts else ""
        # 顶层目录/文件实测没有 parentId/fullParentIds：视为根。
        return ""

    def _file_detail(self, file_id: str) -> JsonObject:
        try:
            detail = self._client.file_detail(file_id)
        except GuangyapanError as exc:
            self._log_failure("validate_import_source", exc, file_id)
            raise _cloud_error("validate_import_source", exc) from exc
        if not isinstance(detail, dict) or not detail:
            raise _error(
                "validate_import_source", "source_not_found", "光鸭导入源不存在"
            )
        return detail

    @staticmethod
    def _overlap_error() -> ProviderOperationError:
        return _error(
            "validate_import_source",
            "invalid_config",
            "光鸭导入源与媒体库目录重叠，请选择媒体库目录之外的位置",
        )

    @staticmethod
    def _unavailable_error() -> ProviderOperationError:
        return _error(
            "validate_import_source",
            "unavailable",
            "无法校验导入源与媒体库目录的关系，请稍后重试",
            retryable=True,
        )

    @staticmethod
    def _import_file(item: JsonObject, *, relative_path: str) -> ImportFile:
        return ImportFile(
            source_ref=_entry_ref(item),
            name=str(item.get("fileName") or ""),
            relative_path=relative_path,
            size_bytes=int(item.get("fileSize") or 0),
            is_video=_is_video(item),
        )

    def read_import_file(self, *, source: ImportFile) -> ImportFileContent:
        file_id = _entry_file_id(source.source_ref, operation="read_import_file")
        size = int(source.size_bytes)
        if size <= 0 or size > _READ_IMPORT_FILE_LIMIT_BYTES:
            raise _error("read_import_file", "unsupported", "光鸭侧车文件过大或为空")
        try:
            with self._reader(file_id, file_size=size) as reader:
                content = reader.read()
        except GuangyapanError as exc:
            self._log_failure("read_import_file", exc, file_id)
            raise _cloud_error("read_import_file", exc) from exc
        if len(content) != size:
            raise _error("read_import_file", "unavailable", "光鸭侧车文件读取不完整", retryable=True)
        return ImportFileContent(
            content=content,
            deletion_receipt={
                "v": REF_VERSION,
                "kind": FILE_RECEIPT_KIND,
                "file_id": file_id,
            },
        )

    def delete_import_file(self, *, receipt: JsonObject) -> None:
        file_id = self._receipt_file_id(receipt, operation="delete_import_file")
        self._delete_files("delete_import_file", [file_id])

    def get_import_source_identity(self, *, source: ImportFile) -> str | None:
        ref = source.source_ref
        payload = json.dumps(
            {
                "file_id": ref.get("file_id"),
                "name": source.name,
                "relative_path": source.relative_path,
                "size_bytes": source.size_bytes,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"guangyapan-import-source-v1:{hashlib.sha256(payload).hexdigest()}"

    # ---------------------------------------------------------------- 落位 / 收尾

    def stage_import_file(
        self,
        *,
        source: ImportFile,
        placement: ImportPlacement,
        source_disposition: str,
        operation_key: str,
    ) -> StagedMedia:
        if source_disposition not in {"keep", "delete_after_commit"}:
            raise _error("stage_import_file", "invalid_config", "光鸭导入处置方式无效")
        if not isinstance(operation_key, str) or not operation_key:
            raise _error("stage_import_file", "invalid_config", "光鸭导入操作标识无效")

        source_id = _entry_file_id(source.source_ref, operation="stage_import_file")
        if _placement_parts(placement)[-1] != source.name:
            raise _error(
                "stage_import_file",
                "invalid_config",
                "光鸭导入目标文件名与源文件名不一致",
            )
        recorded = self._journal.get(operation_key)
        if recorded is not None:
            recorded_request = recorded.get("request")
            if isinstance(recorded_request, dict):
                expected_request = {
                    "source_file_id": source_id,
                    "source_name": source.name,
                    "source_size_bytes": int(source.size_bytes),
                    "placement": str(getattr(placement, "relative_path", "") or ""),
                    "source_disposition": source_disposition,
                }
                current_parent_id = source.source_ref.get("parent_id")
                if isinstance(current_parent_id, str):
                    expected_request["source_parent_id"] = current_parent_id
                if any(
                    recorded_request.get(key) != value
                    for key, value in expected_request.items()
                ):
                    raise _error(
                        "stage_import_file",
                        "invalid_config",
                        "同一光鸭导入操作标识对应了不同请求",
                    )
            staged = self._staged_from_record(recorded, operation="stage_import_file")
            if staged is not None:
                logger.info(
                    "光鸭导入复用已完成落位 library_id={} operation_key={} file_id={}",
                    self.library.library_id, operation_key, recorded.get("file_id"),
                )
                return staged
            if source_disposition != "keep":
                raise _error(
                    "stage_import_file",
                    "source_not_found",
                    "光鸭已移动的导入源文件不存在",
                )
            self._journal.remove(operation_key)

        try:
            source_parent_id = self._source_parent_id(source, file_id=source_id)
        except GuangyapanError as exc:
            self._log_failure("stage_import_file", exc, source_id)
            raise _cloud_error("stage_import_file", exc) from exc
        request = _import_request(
            source=source,
            source_parent_id=source_parent_id,
            placement=placement,
            source_disposition=source_disposition,
        )

        try:
            operation_dir_id, target_name, placement_parent_id = (
                self._ensure_placement_directory(placement, operation_key=operation_key)
            )
            if source_disposition == "keep":
                file_id = self._copy_into(
                    source_id,
                    operation_dir_id,
                    target_name,
                    source=source,
                )
            else:
                file_id = self._move_into(
                    source_id,
                    operation_dir_id,
                    target_name,
                    source=source,
                )
        except GuangyapanError as exc:
            self._log_failure("stage_import_file", exc, source_id)
            raise _cloud_error("stage_import_file", exc) from exc

        probed = self._probe_or_empty(file_id, size_bytes=int(source.size_bytes))
        record = {
            "operation_key": operation_key,
            "file_id": file_id,
            "parent_id": operation_dir_id,
            "placement_parent_id": placement_parent_id,
            "operation_dir_id": operation_dir_id,
            "source_parent_id": source_parent_id,
            "target_name": target_name,
            "size_bytes": int(source.size_bytes),
            "disposition": source_disposition,
            "request": request,
        }
        self._journal.put(operation_key, record)
        logger.info(
            "光鸭导入落位完成 library_id={} operation_key={} file_id={} disposition={}",
            self.library.library_id, operation_key, file_id, source_disposition,
        )
        return StagedMedia(
            storage_ref=_media_ref(file_id),
            receipt={
                "v": REF_VERSION,
                "kind": STAGE_RECEIPT_KIND,
                "operation_key": operation_key,
                "file_id": file_id,
                "parent_id": operation_dir_id,
                "operation_dir_id": operation_dir_id,
                "operation_dir_parent_id": placement_parent_id,
                "source_parent_id": source_parent_id,
                "target_name": target_name,
                "disposition": source_disposition,
                "size_bytes": int(source.size_bytes),
            },
            size_bytes=int(source.size_bytes),
            duration_seconds=probed.get("duration_seconds") or None,
            video_info=probed.get("video_info") or None,
            resolution=probed.get("resolution") or None,
        )

    def _source_parent_id(self, source: ImportFile, *, file_id: str) -> str:
        parent_id = source.source_ref.get("parent_id")
        if isinstance(parent_id, str):
            return parent_id
        detail = self._client.file_detail(file_id)
        if not detail or detail.get("resType") == 2:
            raise GuangyapanNotFoundError("光鸭导入源文件不存在")
        return str(detail.get("parentId") or "")

    def _batch_namespace(self, operation_key: str) -> str:
        namespace, separator, _index = operation_key.rpartition(":")
        return namespace if separator and namespace else operation_key

    def _import_directory_cache(self, operation_key: str) -> dict[tuple[str, str], str]:
        namespace = self._batch_namespace(operation_key)
        with self._import_directory_lock:
            cache = self._import_directory_caches.get(namespace)
            if cache is None:
                cache = {}
                self._import_directory_caches[namespace] = cache
            self._import_directory_caches.move_to_end(namespace)
            while len(self._import_directory_caches) > _IMPORT_DIRECTORY_CACHE_BATCHES:
                self._import_directory_caches.popitem(last=False)
            return cache

    def _cached_directory(
        self,
        parent_id: str,
        name: str,
        cache: dict[tuple[str, str], str],
    ) -> str:
        key = (parent_id, name)
        cached = cache.get(key)
        if cached:
            return cached
        directory = self._client.create_dir(name, parent_id, fail_if_name_exist=True)
        file_id = directory.get("fileId")
        if not isinstance(file_id, str) or not file_id:
            raise _error("stage_import_file", "unavailable", "光鸭目录创建失败", retryable=True)
        cache[key] = file_id
        return file_id

    def _ensure_placement_directory(
        self, placement: ImportPlacement, *, operation_key: str
    ) -> tuple[str, str, str]:
        parts = _placement_parts(placement)
        cache = self._import_directory_cache(operation_key)
        for attempt in range(2):
            try:
                placement_parent_id = self._media_root_id
                for part in parts[:-1]:
                    placement_parent_id = self._cached_directory(
                        placement_parent_id, part, cache
                    )
                operation_dir_id = self._cached_directory(
                    placement_parent_id,
                    _operation_directory(operation_key),
                    cache,
                )
                return operation_dir_id, parts[-1], placement_parent_id
            except GuangyapanNotFoundError:
                if attempt:
                    raise
                cache.clear()
        raise AssertionError("unreachable directory resolution")

    @staticmethod
    def _matches_import_item(
        item: JsonObject,
        *,
        target_name: str,
        source_size: int,
        source_gcid: str,
    ) -> bool:
        if item.get("resType") == 2:
            return False
        if str(item.get("fileName") or "") != target_name:
            return False
        if int(item.get("fileSize") or 0) != source_size:
            return False
        item_gcid = item.get("gcid")
        if source_gcid and isinstance(item_gcid, str) and item_gcid:
            return item_gcid.upper() == source_gcid.upper()
        return True

    def _copy_into(
        self,
        source_id: str,
        operation_dir_id: str,
        target_name: str,
        *,
        source: ImportFile,
    ) -> str:
        source_gcid = str(source.source_ref.get("gcid") or "")
        existing = self._matching_import_files(
            operation_dir_id,
            target_name=target_name,
            source_size=int(source.size_bytes),
            source_gcid=source_gcid,
        )
        if len(existing) == 1:
            return existing[0]
        if len(existing) > 1:
            raise GuangyapanRequestError("光鸭 operation 目录包含多个同名复制结果")
        task_id = self._client.copy_files([source_id], operation_dir_id)
        self._client.wait_task(task_id)
        last_matches: list[str] = []
        for attempt in range(3):
            matches = self._matching_import_files(
                operation_dir_id,
                target_name=target_name,
                source_size=int(source.size_bytes),
                source_gcid=source_gcid,
            )
            if len(matches) == 1:
                return matches[0]
            last_matches = matches
            if len(matches) > 1:
                break
            if attempt < 2:
                time.sleep(1.0)
        if len(last_matches) > 1:
            raise GuangyapanRequestError("光鸭 operation 目录包含多个同名复制结果")
        raise _error("stage_import_file", "unavailable", "光鸭复制结果未找到", retryable=True)

    def _move_into(
        self,
        source_id: str,
        operation_dir_id: str,
        target_name: str,
        *,
        source: ImportFile,
    ) -> str:
        detail = self._client.file_detail(source_id)
        if not detail or detail.get("resType") == 2:
            raise GuangyapanNotFoundError("光鸭导入源文件不存在")
        if detail.get("parentId") == operation_dir_id:
            self._verify_staged_file(
                source_id,
                expected_parent_id=operation_dir_id,
                expected_name=target_name,
                expected_size=int(source.size_bytes),
                source_gcid=str(source.source_ref.get("gcid") or ""),
            )
            return source_id
        task_id = self._client.move_files([source_id], operation_dir_id)
        self._client.wait_task(task_id)
        self._verify_staged_file(
            source_id,
            expected_parent_id=operation_dir_id,
            expected_name=target_name,
            expected_size=int(source.size_bytes),
            source_gcid=str(source.source_ref.get("gcid") or ""),
        )
        return source_id

    def _matching_import_files(
        self,
        operation_dir_id: str,
        *,
        target_name: str,
        source_size: int,
        source_gcid: str,
    ) -> list[str]:
        return [
            str(item.get("fileId") or "")
            for item in self._directory_items(operation_dir_id)
            if item.get("fileId")
            and self._matches_import_item(
                item,
                target_name=target_name,
                source_size=source_size,
                source_gcid=source_gcid,
            )
        ]

    def _verify_staged_file(
        self,
        file_id: str,
        *,
        expected_parent_id: str,
        expected_name: str,
        expected_size: int,
        source_gcid: str,
    ) -> None:
        detail = self._client.file_detail(file_id)
        if not detail or detail.get("resType") == 2:
            raise GuangyapanNotFoundError("光鸭落位文件不存在")
        parent_id = detail.get("parentId")
        if isinstance(parent_id, str) and parent_id != expected_parent_id:
            raise GuangyapanRequestError("光鸭落位文件父目录不匹配")
        name = detail.get("fileName")
        if isinstance(name, str) and name != expected_name:
            raise GuangyapanRequestError("光鸭落位文件名不匹配")
        size = detail.get("fileSize")
        if size is not None and int(size) != expected_size:
            raise GuangyapanRequestError("光鸭落位文件大小不匹配")
        gcid = detail.get("gcid")
        if (
            source_gcid
            and isinstance(gcid, str)
            and gcid
            and gcid.upper() != source_gcid.upper()
        ):
            raise GuangyapanRequestError("光鸭落位文件哈希不匹配")

    def finalize_import(self, *, receipt: JsonObject) -> None:
        self._stage_receipt(receipt, operation="finalize_import")

    def abort_import(self, *, receipt: JsonObject) -> None:
        stage = self._stage_receipt(receipt, operation="abort_import")
        file_id = stage["file_id"]
        disposition = stage["disposition"]
        operation_dir_id = stage["operation_dir_id"]
        if disposition == "keep":
            # 删除的是插件自己刚复制出来的副本，永久删除不会影响用户源文件。
            self._delete_files_permanently("abort_import", [file_id])
            self._cleanup_operation_directory(operation_dir_id)
        else:
            source_parent_id = stage["source_parent_id"]
            if not isinstance(source_parent_id, str):
                raise _error(
                    "abort_import",
                    "unavailable",
                    "历史光鸭导入回执缺少源目录，拒绝删除已移动文件",
                    retryable=True,
                )
            try:
                detail = self._client.file_detail(file_id)
                if not detail or detail.get("resType") == 2:
                    raise GuangyapanNotFoundError("光鸭导入源文件不存在")
                current_parent_id = detail.get("parentId")
                if current_parent_id != source_parent_id:
                    task_id = self._client.move_files([file_id], source_parent_id)
                    self._client.wait_task(task_id)
                self._verify_staged_file(
                    file_id,
                    expected_parent_id=source_parent_id,
                    expected_name=stage["target_name"],
                    expected_size=stage["size_bytes"],
                    source_gcid="",
                )
            except GuangyapanError as exc:
                self._log_failure("abort_import", exc, file_id)
                raise _cloud_error("abort_import", exc) from exc
            self._cleanup_operation_directory(operation_dir_id)
        self._journal.remove(stage["operation_key"])

    def _cleanup_operation_directory(self, operation_dir_id: object) -> None:
        if not isinstance(operation_dir_id, str) or not operation_dir_id:
            return
        try:
            if self._directory_items(operation_dir_id):
                logger.warning(
                    "光鸭 operation 目录非空，保留 directory_id={}", operation_dir_id
                )
                return
            task_id = self._client.delete_files([operation_dir_id])
            self._client.wait_task(task_id)
        except GuangyapanNotFoundError:
            return
        except GuangyapanError as exc:
            logger.warning(
                "光鸭 operation 空目录清理失败 directory_id={} reason={}",
                operation_dir_id,
                safe_error_message(exc),
            )

    def delete_media(self, *, media: MediaHandle) -> None:
        file_id = _entry_file_id(media.storage_ref, operation="delete_media")
        self._delete_files("delete_media", [file_id])

    def _delete_files(self, operation: str, file_ids: list[str]) -> None:
        for file_id in file_ids:
            record = self._delete_journal.get(file_id)
            if isinstance(record, dict) and record.get("state") == "trashed":
                continue
            if isinstance(record, dict) and record.get("state") == "pending":
                task_id = record.get("task_id")
                if isinstance(task_id, str) and task_id:
                    try:
                        status = self._client.task_status(task_id)
                    except GuangyapanNotFoundError:
                        status = 3
                    except GuangyapanError as exc:
                        self._log_failure(operation, exc, file_id)
                        raise _cloud_error(operation, exc) from exc
                    if status == 2:
                        self._delete_journal.put(file_id, {"state": "trashed"})
                        continue
                    if status in {0, 1}:
                        raise _error(
                            operation,
                            "unavailable",
                            "光鸭删除任务仍在处理中，请稍后重试",
                            retryable=True,
                        )
                try:
                    already_trashed = self._file_is_trashed_or_missing(file_id)
                except GuangyapanError as exc:
                    self._log_failure(operation, exc, file_id)
                    raise _cloud_error(operation, exc) from exc
                if already_trashed:
                    self._delete_journal.put(file_id, {"state": "trashed"})
                    continue
            self._delete_journal.put(
                file_id,
                {"state": "pending", "operation": operation},
            )
            try:
                task_id = self._client.delete_files([file_id])
                self._delete_journal.put(
                    file_id,
                    {"state": "pending", "operation": operation, "task_id": task_id},
                )
                self._client.wait_task(task_id)
            except GuangyapanNotFoundError:
                self._delete_journal.put(file_id, {"state": "trashed"})
                continue
            except GuangyapanError as exc:
                self._log_failure(operation, exc, file_id)
                raise _cloud_error(operation, exc) from exc
            self._delete_journal.put(file_id, {"state": "trashed"})

    def _file_is_trashed_or_missing(self, file_id: str) -> bool:
        if self._file_in_recycle_bin(file_id):
            return True
        try:
            detail = self._client.file_detail(file_id)
        except GuangyapanNotFoundError:
            return True
        return not bool(detail)

    def _file_in_recycle_bin(self, file_id: str) -> bool:
        page = 0
        while True:
            items, total = self._client.list_files(
                dir_type=4,
                page=page,
                page_size=1000,
            )
            if any(str(item.get("fileId") or "") == file_id for item in items):
                return True
            consumed = page * 1000 + len(items)
            if not items or consumed >= total:
                return False
            page += 1

    def _delete_files_permanently(self, operation: str, file_ids: list[str]) -> None:
        """两步永久删除（`delete_file` → 回收站 → 对同一 ID 再删一次）；见 endpoints.md §5.4。"""
        try:
            self._client.delete_files_permanently(file_ids)
        except GuangyapanNotFoundError:
            logger.info(
                "光鸭文件已不存在 library_id={} operation={}",
                self.library.library_id, operation,
            )
            return
        except GuangyapanError as exc:
            self._log_failure(operation, exc, "")
            raise _cloud_error(operation, exc) from exc

    # ---------------------------------------------------------------- 哈希

    def compute_file_hash(self, *, media: MediaHandle) -> str:
        file_id = _entry_file_id(media.storage_ref, operation="compute_file_hash")
        size = media.file_size_bytes
        if not isinstance(size, int) or size < 0:
            raise _error("compute_file_hash", "invalid_config", "光鸭媒体文件大小无效")

        started = time.monotonic()
        try:
            with self._reader(file_id, file_size=size, retry_delays=_HASH_READY_DELAYS) as reader:
                if size == 0:
                    digest = self._wrap_hash("full", size, hashlib.sha1(b"").digest())
                elif size < _HASH_FULL_THRESHOLD:
                    digest = self._wrap_hash("full", size, hashlib.sha1(self._read_exact(reader, 0, size)).digest())
                else:
                    head = hashlib.sha1(self._read_exact(reader, 0, _HASH_HEAD_TAIL_BYTES)).digest()
                    tail = hashlib.sha1(
                        self._read_exact(reader, size - _HASH_HEAD_TAIL_BYTES, _HASH_HEAD_TAIL_BYTES)
                    ).digest()
                    slot_count = (size - 6 * _HASH_MIDDLE_BYTES) // _HASH_MIDDLE_BYTES
                    slot_1 = int.from_bytes(head[:8], "big") % slot_count
                    slot_2 = int.from_bytes(tail[:8], "big") % (slot_count - 1)
                    if slot_2 >= slot_1:
                        slot_2 += 1
                    middle_1 = hashlib.sha1(
                        self._read_exact(reader, 3 * _HASH_MIDDLE_BYTES + slot_1 * _HASH_MIDDLE_BYTES, _HASH_MIDDLE_BYTES)
                    ).digest()
                    middle_2 = hashlib.sha1(
                        self._read_exact(reader, 3 * _HASH_MIDDLE_BYTES + slot_2 * _HASH_MIDDLE_BYTES, _HASH_MIDDLE_BYTES)
                    ).digest()
                    payload = (
                        _HASH_DOMAIN + b"\x00sampled\x00" + size.to_bytes(8, "big")
                        + head + tail + middle_1 + middle_2
                    )
                    digest = f"media-file-hash-v1:{hashlib.sha1(payload).hexdigest()}"
        except GuangyapanNotFoundError as exc:
            raise _error("compute_file_hash", "source_not_found", "光鸭媒体文件不存在") from exc
        except GuangyapanError as exc:
            self._log_failure("compute_file_hash", exc, file_id)
            raise _cloud_error("compute_file_hash", exc) from exc
        logger.info(
            "光鸭文件哈希完成 library_id={} media_id={} elapsed_seconds={:.2f}",
            self.library.library_id, media.media_id, time.monotonic() - started,
        )
        return digest

    @staticmethod
    def _wrap_hash(mode: str, size: int, digest: bytes) -> str:
        payload = _HASH_DOMAIN + b"\x00" + mode.encode() + b"\x00" + size.to_bytes(8, "big") + digest
        return f"media-file-hash-v1:{hashlib.sha1(payload).hexdigest()}"

    @staticmethod
    def _read_exact(reader: GuangyapanRangeReader, offset: int, length: int) -> bytes:
        reader.seek(offset)
        data = reader.read(length)
        if len(data) != length:
            raise GuangyapanObjectNotReadyError("光鸭读取长度不足")
        return data

    # ---------------------------------------------------------------- 播放

    async def handle_playback(self, *, media: MediaHandle, context):
        return await self._playback.handle(media=media, context=context)

    # ---------------------------------------------------------------- 缩略图 / 剪辑

    def generate_thumbnails(
        self,
        *,
        media: MediaHandle,
        workspace: Path,
        progress_callback: Callable[[str], None] | None = None,
    ) -> ThumbnailGeneration:
        file_id = _entry_file_id(media.storage_ref, operation="generate_thumbnails")
        duration = int(media.duration_seconds or 0)
        if duration <= 0:
            probed = self._probe_or_empty(file_id, size_bytes=media.file_size_bytes)
            duration = int(probed.get("duration_seconds") or 0)
        if duration <= 0:
            raise _error("generate_thumbnails", "unsupported", "光鸭媒体时长未知，无法生成缩略图")

        offsets = list(range(0, duration, _THUMBNAIL_INTERVAL_SECONDS)) or [0]
        try:
            artifacts = _generate_thumbnails(
                reader_factory=lambda: self._reader(
                    file_id,
                    file_size=media.file_size_bytes,
                    max_fetched_bytes=_THUMBNAIL_MAX_FETCHED_BYTES,
                    chunk_size=_THUMBNAIL_READ_CHUNK_BYTES,
                ),
                duration_seconds=duration,
                interval_seconds=_THUMBNAIL_INTERVAL_SECONDS,
                workspace=Path(workspace),
                progress_callback=progress_callback,
            )
        except GuangyapanObjectNotReadyError as exc:
            raise ThumbnailGenerationDeferred(
                "光鸭文件刚落盘，暂不能生成缩略图",
                error_code="provider_not_ready",
                max_deferred_attempts=6,
                deferred_backoff_base_seconds=15,
            ) from exc
        except GuangyapanError as exc:
            self._log_failure("generate_thumbnails", exc, file_id)
            raise _cloud_error("generate_thumbnails", exc) from exc

        return ThumbnailGeneration(
            expected_count=len(offsets),
            artifacts=tuple(
                ThumbnailArtifact(offset_seconds=offset, relative_path=path)
                for offset, path in artifacts
            ),
        )

    def create_clip(
        self,
        *,
        media: MediaHandle,
        start_offset_seconds: int,
        end_offset_seconds: int,
        workspace: Path,
    ) -> ClipArtifact:
        file_id = _entry_file_id(media.storage_ref, operation="create_clip")
        try:
            relative_path = _create_clip(
                reader_factory=lambda: self._reader(
                    file_id,
                    file_size=media.file_size_bytes,
                    max_fetched_bytes=_CLIP_MAX_FETCHED_BYTES,
                ),
                start_offset_seconds=start_offset_seconds,
                end_offset_seconds=end_offset_seconds,
                workspace=Path(workspace),
            )
        except GuangyapanError as exc:
            self._log_failure("create_clip", exc, file_id)
            raise _cloud_error("create_clip", exc) from exc
        return ClipArtifact(relative_path=relative_path)

    # ---------------------------------------------------------------- 可选：探测 / 封面 / 空间

    def probe_video_info(self, *, media: MediaHandle) -> JsonObject | None:
        file_id = _entry_file_id(media.storage_ref, operation="probe_video_info")
        probed = self._probe_or_empty(file_id, size_bytes=media.file_size_bytes)
        info = probed.get("video_info")
        return info if isinstance(info, dict) and info else None

    def probe_duration_seconds(self, *, media: MediaHandle) -> int:
        file_id = _entry_file_id(media.storage_ref, operation="probe_duration_seconds")
        probed = self._probe_or_empty(file_id, size_bytes=media.file_size_bytes)
        return int(probed.get("duration_seconds") or 0)

    def probe_resolution(self, *, media: MediaHandle) -> str | None:
        file_id = _entry_file_id(media.storage_ref, operation="probe_resolution")
        probed = self._probe_or_empty(file_id, size_bytes=media.file_size_bytes)
        value = probed.get("resolution")
        return str(value) if value else None

    @contextmanager
    def open_cover_source(self, *, media: MediaHandle):
        file_id = _entry_file_id(media.storage_ref, operation="open_cover_source")
        reader = self._reader(
            file_id,
            file_size=media.file_size_bytes,
            max_fetched_bytes=_COVER_MAX_FETCHED_BYTES,
        )
        try:
            yield reader
        finally:
            reader.close()

    def get_space_usage(self) -> StorageSpaceUsage:
        try:
            assets = self._client.assets()
        except GuangyapanError as exc:
            self._log_failure("get_space_usage", exc, "")
            raise _cloud_error("get_space_usage", exc) from exc
        total = assets.get("totalSpaceSize")
        used = assets.get("usedSpaceSize")
        total_bytes = int(total) if isinstance(total, (int, float)) and total > 0 else None
        used_bytes = int(used) if isinstance(used, (int, float)) and used >= 0 else None
        free_bytes = (
            max(0, total_bytes - used_bytes)
            if total_bytes is not None and used_bytes is not None
            else None
        )
        return StorageSpaceUsage(
            total_bytes=total_bytes, used_bytes=used_bytes, free_bytes=free_bytes
        )

    # ---------------------------------------------------------------- 可选：媒体引用盘点

    def scan_managed_media_ref_keys(self) -> set[str]:
        try:
            tree = load_library_tree(self._client)
            return {
                str(item.get("fileId"))
                for item in tree.subtree_items(self._media_root_id)
                if item.get("fileId") and item.get("resType") != 2
            }
        except GuangyapanError as exc:
            self._log_failure("scan_managed_media_ref_keys", exc, self._media_root_id)
            raise _cloud_error("scan_managed_media_ref_keys", exc) from exc

    @staticmethod
    def managed_media_ref_key(*, media_ref: JsonObject) -> str:
        return _entry_file_id(media_ref, operation="managed_media_ref_key")

    def scan_media_refs(self, *, source_ref: JsonObject) -> tuple[JsonObject, ...]:
        directory_id = _dir_ref(source_ref)
        try:
            tree = load_library_tree(self._client)
            return tuple(
                _media_ref(str(item.get("fileId")))
                for item in tree.subtree_items(directory_id)
                if item.get("fileId") and item.get("resType") != 2 and _is_video(item)
            )
        except GuangyapanError as exc:
            self._log_failure("scan_media_refs", exc, directory_id)
            raise _cloud_error("scan_media_refs", exc) from exc

    # ---------------------------------------------------------------- 可选：跨库迁移（仅秒传）

    def stage_transfer(
        self,
        *,
        source,
        placement: ImportPlacement,
        operation_key: str,
    ) -> StagedMediaTransfer:
        if not isinstance(operation_key, str) or not operation_key:
            raise _error("stage_transfer", "invalid_config", "光鸭迁移操作标识无效")
        recorded = self._transfer_journal.get(operation_key)
        if recorded is not None:
            staged = self._staged_transfer_from_record(recorded)
            if staged is not None:
                return staged

        info = source.info
        if _placement_parts(placement)[-1] != info.file_name:
            raise _error(
                "stage_transfer",
                "invalid_config",
                "光鸭迁移目标文件名与源文件名不一致",
            )
        try:
            parent_id, target_name, _placement_parent_id = self._ensure_placement_directory(
                placement, operation_key=operation_key
            )
            md5 = self._hash_transfer_source(source)
            source.assert_unchanged()
            token = self._client.upload_token(
                name=target_name, size=info.size_bytes, parent_id=parent_id, md5=md5
            )
            if not token.get("instant"):
                logger.info(
                    "光鸭迁移未命中秒传，跳过上传 library_id={} operation_key={} name={}",
                    self.library.library_id, operation_key, info.file_name,
                )
                return StagedMediaTransfer(status="not_available")
            task_id = str(token.get("taskId") or "")
            if not task_id:
                return StagedMediaTransfer(status="not_available")
            uploaded = self._client.wait_upload(task_id)
        except GuangyapanError as exc:
            self._log_failure("stage_transfer", exc, "")
            raise _cloud_error("stage_transfer", exc) from exc

        file_id = str(uploaded.get("fileId") or "")
        if not file_id:
            return StagedMediaTransfer(status="not_available")
        try:
            self._verify_staged_file(
                file_id,
                expected_parent_id=parent_id,
                expected_name=target_name,
                expected_size=int(info.size_bytes),
                source_gcid="",
            )
        except GuangyapanError as exc:
            self._log_failure("stage_transfer", exc, file_id)
            raise _cloud_error("stage_transfer", exc) from exc
        record = {
            "file_id": file_id,
            "parent_id": parent_id,
            "file_name": target_name,
            "size_bytes": info.size_bytes,
        }
        self._transfer_journal.put(operation_key, record)
        logger.info(
            "光鸭迁移秒传成功 library_id={} operation_key={} file_id={}",
            self.library.library_id, operation_key, file_id,
        )
        return StagedMediaTransfer(
            status="staged",
            storage_ref=_media_ref(file_id),
            receipt={
                "v": REF_VERSION,
                "kind": TRANSFER_RECEIPT_KIND,
                "operation_key": operation_key,
                "file_id": file_id,
                "parent_id": parent_id,
            },
            file_name=target_name,
            size_bytes=info.size_bytes,
        )

    @staticmethod
    def _hash_transfer_source(source) -> str:
        digest = hashlib.md5()
        with source.open_reader() as reader:
            while True:
                chunk = reader.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest().upper()

    def finalize_transfer(self, *, receipt: JsonObject) -> None:
        file_id = self._receipt_file_id(receipt, operation="finalize_transfer")
        try:
            if not self._client.file_detail(file_id):
                raise _error("finalize_transfer", "unavailable", "光鸭迁移文件校验失败", retryable=True)
        except GuangyapanNotFoundError as exc:
            raise _error("finalize_transfer", "source_not_found", "光鸭迁移文件不存在") from exc
        except GuangyapanError as exc:
            self._log_failure("finalize_transfer", exc, file_id)
            raise _cloud_error("finalize_transfer", exc) from exc

    def abort_transfer(self, *, receipt: JsonObject) -> None:
        file_id = self._receipt_file_id(receipt, operation="abort_transfer")
        self._delete_files("abort_transfer", [file_id])
        parent_id = receipt.get("parent_id")
        self._cleanup_operation_directory(parent_id)
        operation_key = receipt.get("operation_key")
        if isinstance(operation_key, str) and operation_key:
            self._transfer_journal.remove(operation_key)

    # ---------------------------------------------------------------- 内部工具

    def _reader(
        self,
        file_id: str,
        *,
        file_size: int,
        max_fetched_bytes: int | None = None,
        retry_delays: tuple[float, ...] | None = None,
        chunk_size: int | None = None,
    ) -> GuangyapanRangeReader:
        kwargs: dict[str, Any] = {}
        if retry_delays is not None:
            kwargs["retry_delays"] = retry_delays
        if chunk_size is not None:
            kwargs["chunk_size"] = chunk_size
        return GuangyapanRangeReader(
            url_provider=lambda: self._client.download_url(file_id),
            file_size_bytes=file_size,
            max_fetched_bytes=max_fetched_bytes,
            **kwargs,
        )

    def _probe_or_empty(self, file_id: str, *, size_bytes: int) -> dict[str, Any]:
        if size_bytes <= 0:
            return {}
        try:
            return _probe_video(
                reader_factory=lambda: self._reader(
                    file_id,
                    file_size=size_bytes,
                    max_fetched_bytes=_PROBE_MAX_FETCHED_BYTES,
                )
            )
        except Exception as exc:  # noqa: BLE001 - 探测失败不阻断导入，交给媒体信息回填任务
            logger.info(
                "光鸭媒体探测跳过 library_id={} file_id={} reason={}",
                self.library.library_id, file_id, safe_error_message(exc),
            )
            return {}

    def _directory_items(self, parent_id: str) -> list[JsonObject]:
        items: list[JsonObject] = []
        page = 0
        while True:
            batch, total = self._client.list_files(
                parent_id=parent_id,
                page=page,
                page_size=1000,
            )
            items.extend(batch)
            consumed = page * 1000 + len(batch)
            if not batch or consumed >= total:
                return items
            page += 1

    @staticmethod
    def _receipt_file_id(receipt: JsonObject, *, operation: str) -> str:
        if not isinstance(receipt, dict):
            raise _error(operation, "invalid_config", "光鸭操作回执无效")
        file_id = receipt.get("file_id")
        if not isinstance(file_id, str) or not file_id:
            raise _error(operation, "invalid_config", "光鸭操作回执无效")
        return file_id

    def _stage_receipt(self, receipt: JsonObject, *, operation: str) -> dict[str, Any]:
        file_id = self._receipt_file_id(receipt, operation=operation)
        disposition = receipt.get("disposition")
        if disposition not in {"keep", "delete_after_commit"}:
            raise _error(operation, "invalid_config", "光鸭导入回执处置方式无效")
        operation_key = receipt.get("operation_key")
        if not isinstance(operation_key, str):
            operation_key = ""
        size_bytes = receipt.get("size_bytes")
        if size_bytes is None:
            size_bytes = 0
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
            raise _error(operation, "invalid_config", "光鸭导入回执文件大小无效")
        return {
            "file_id": file_id,
            "disposition": disposition,
            "operation_key": operation_key,
            "operation_dir_id": receipt.get("operation_dir_id"),
            "source_parent_id": receipt.get("source_parent_id"),
            "target_name": str(receipt.get("target_name") or ""),
            "size_bytes": size_bytes,
        }

    def _staged_from_record(self, record: JsonObject, *, operation: str) -> StagedMedia | None:
        file_id = record.get("file_id")
        if not isinstance(file_id, str) or not file_id:
            return None
        try:
            detail = self._client.file_detail(file_id)
            if not detail or detail.get("resType") == 2:
                return None
        except GuangyapanNotFoundError:
            return None
        except GuangyapanError as exc:
            self._log_failure(operation, exc, file_id)
            raise _cloud_error(operation, exc) from exc
        size_bytes = int(record.get("size_bytes") or 0)
        disposition = str(record.get("disposition") or "keep")
        source_parent_id = record.get("source_parent_id")
        operation_dir_id = record.get("operation_dir_id") or ""
        target_name = str(record.get("target_name") or "")
        return StagedMedia(
            storage_ref=_media_ref(file_id),
            receipt={
                "v": REF_VERSION,
                "kind": STAGE_RECEIPT_KIND,
                "operation_key": record.get("operation_key") or "",
                "file_id": file_id,
                "parent_id": operation_dir_id,
                "operation_dir_id": operation_dir_id,
                "operation_dir_parent_id": record.get("placement_parent_id") or "",
                "source_parent_id": source_parent_id,
                "target_name": target_name,
                "disposition": disposition,
                "size_bytes": size_bytes,
            },
            size_bytes=size_bytes,
            duration_seconds=None,
            video_info=None,
        )

    def _staged_transfer_from_record(self, record: JsonObject) -> StagedMediaTransfer | None:
        file_id = record.get("file_id")
        if not isinstance(file_id, str) or not file_id:
            return None
        try:
            detail = self._client.file_detail(file_id)
            if not detail or detail.get("resType") == 2:
                return None
        except GuangyapanNotFoundError:
            return None
        except GuangyapanError:
            return None
        return StagedMediaTransfer(
            status="staged",
            storage_ref=_media_ref(file_id),
            receipt={
                "v": REF_VERSION,
                "kind": TRANSFER_RECEIPT_KIND,
                "operation_key": record.get("operation_key") or "",
                "file_id": file_id,
                "parent_id": record.get("parent_id") or "",
            },
            file_name=str(record.get("file_name") or ""),
            size_bytes=int(record.get("size_bytes") or 0),
        )

    def _log_failure(self, operation: str, exc: GuangyapanError, identifier: str) -> None:
        logger.warning(
            "光鸭操作失败 library_id={} operation={} target={} error_type={} reason={}",
            self.library.library_id,
            operation,
            identifier,
            type(exc).__name__,
            safe_error_message(exc),
        )


class _ScanReporter:
    """按宿主规范节流上报扫描进度（阶段开始/结束立即上报，循环内约 2 秒一次）。"""

    _REPORT_INTERVAL_SECONDS = 2.0
    _LOG_INTERVAL_SECONDS = 10.0

    def __init__(self, callback) -> None:
        self._callback = callback
        self._last_report = float("-inf")
        self._last_log = float("-inf")
        self._stage = "扫描导入源"
        self._checked = 0

    def start(self, stage: str) -> None:
        self._stage = stage
        self._emit(force=True, text=f"{stage} · 正在读取光鸭目录")

    def tick(self, checked: int, found: int) -> None:
        self._checked = checked
        self._emit(text=f"{self._stage} · 已检查 {checked} 个条目 · 找到 {found} 个文件")

    def finish(self, total: int) -> None:
        self._emit(force=True, text=f"{self._stage} · 已完成 · 共 {total} 个文件")

    def _emit(self, *, text: str, force: bool = False) -> None:
        now = time.monotonic()
        if force or now - self._last_log >= self._LOG_INTERVAL_SECONDS:
            logger.info("光鸭扫描进度 {}", text)
            self._last_log = now
        if self._callback is None:
            return
        if force or now - self._last_report >= self._REPORT_INTERVAL_SECONDS:
            try:
                self._callback({"current": self._checked, "total": 0, "text": text})
            except Exception:  # noqa: BLE001 - 上报失败不影响扫描
                logger.debug("光鸭扫描进度上报失败")
            self._last_report = now


__all__ = [
    "GuangyapanStorageProvider",
]
