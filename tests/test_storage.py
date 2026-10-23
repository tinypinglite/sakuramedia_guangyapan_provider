from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from sakuramedia_guangyapan_provider import range_reader as range_reader_module
from sakuramedia_guangyapan_provider.exceptions import (
    GuangyapanObjectNotReadyError,
    GuangyapanReadBudgetExceeded,
    GuangyapanRequestError,
)
from sakuramedia_guangyapan_provider.playback import GuangyapanPlayback
from sakuramedia_guangyapan_provider.range_reader import GuangyapanRangeReader
from sakuramedia_guangyapan_provider.storage import (
    GuangyapanStorageProvider,
    _entry_ref,
    _media_ref,
    _operation_directory,
)

from src.plugins.provider_protocol import (
    ImportFile,
    ImportPlacement,
    LibraryHandle,
    MediaHandle,
    ProviderOperationError,
)

MEDIA_ROOT = "100"


def _library() -> LibraryHandle:
    return LibraryHandle(
        library_id=7,
        provider_key="guangyapan",
        provider_config={"refresh_token": "gy.test", "media_root_id": MEDIA_ROOT},
        account_key="sub",
    )


class FakeClient:
    """只实现被测代码真正调用的接口，行为照实测结论：同名目录复用、同名文件自动改名。"""

    def __init__(self) -> None:
        self.tree: dict[str, list[dict]] = {
            "": [
                {"fileId": "1", "fileName": "Movies", "resType": 2},
                {"fileId": "9", "fileName": "root.mp4", "resType": 1, "fileSize": 100, "fileType": 2},
            ],
            MEDIA_ROOT: [],
            "1": [
                {"fileId": "2", "fileName": "A.mp4", "resType": 1, "fileSize": 2048, "fileType": 2, "utime": 1700000000},
                {"fileId": "3", "fileName": "A.srt", "resType": 1, "fileSize": 12, "fileType": 4},
                {"fileId": "4", "fileName": "Poster.jpg", "resType": 1, "fileSize": 30, "fileType": 1},
                {"fileId": "5", "fileName": "Sub", "resType": 2},
            ],
            "5": [{"fileId": "6", "fileName": "B.mkv", "resType": 1, "fileSize": 4096, "fileType": 2}],
        }
        self.created_dirs: list[tuple[str, str, str]] = []
        self.copied: list[tuple[str, str]] = []
        self.moved: list[tuple[str, str]] = []
        self.deleted: list[list[str]] = []
        self.recycle_bin: list[dict] = []
        self.list_calls: dict[str, int] = {}
        self.upload_tokens: list[dict] = []
        self.instant_upload = True
        self.last_uploaded_id = ""
        self._next_id = 1000

    # --- 读
    def list_files(self, *, parent_id="", page=0, page_size=100, **_kwargs):
        self.list_calls[parent_id] = self.list_calls.get(parent_id, 0) + 1
        if _kwargs.get("dir_type") == 4:
            items = list(self.recycle_bin)
            return items[page * page_size : (page + 1) * page_size], len(items)
        if parent_id == "*":
            items = []
            for current_parent, entries in self.tree.items():
                for entry in entries:
                    item = dict(entry)
                    item.setdefault("parentId", current_parent)
                    items.append(item)
            items.sort(key=lambda item: str(item.get("fileName") or "").casefold())
            return items[page * page_size : (page + 1) * page_size], len(items)
        items = list(self.tree.get(parent_id, []))
        output = []
        for entry in items:
            item = dict(entry)
            item.setdefault("parentId", parent_id)
            output.append(item)
        return output[page * page_size : (page + 1) * page_size], len(output)

    def all_entries(self, *, parent_id="", page_size=1000):
        yield from self.tree.get(parent_id, [])

    def file_detail(self, file_id):
        for parent_id, items in self.tree.items():
            for item in items:
                if item["fileId"] == file_id:
                    detail = dict(item)
                    detail.setdefault("parentId", parent_id)
                    return detail
        return {}

    def download_url(self, file_id):
        return f"https://cdn.example/{file_id}", 21600

    # --- 写
    def _new_id(self) -> str:
        self._next_id += 1
        return str(self._next_id)

    def create_dir(self, name, parent_id="", *, fail_if_name_exist=False):
        for item in self.tree.get(parent_id, []):
            if item["resType"] == 2 and item["fileName"] == name:
                return {"fileId": item["fileId"]}
        file_id = self._new_id()
        self.created_dirs.append((parent_id, name, file_id))
        self.tree.setdefault(parent_id, []).append(
            {"fileId": file_id, "fileName": name, "resType": 2, "parentId": parent_id}
        )
        return {"fileId": file_id}

    def copy_files(self, file_ids, parent_id):
        for file_id in file_ids:
            source = self.file_detail(file_id)
            target = dict(source)
            target["fileId"] = self._new_id()
            existing = {item["fileName"] for item in self.tree.setdefault(parent_id, [])}
            if target["fileName"] in existing:
                target["fileName"] = f"{target['fileName']}(1)"
            target["parentId"] = parent_id
            self.tree[parent_id].append(target)
            self.copied.append((file_id, target["fileId"]))
        return "task-copy"

    def move_files(self, file_ids, parent_id):
        for file_id in file_ids:
            source = self.file_detail(file_id)
            for items in self.tree.values():
                items[:] = [item for item in items if item["fileId"] != file_id]
            source["parentId"] = parent_id
            self.tree.setdefault(parent_id, []).append(source)
            self.moved.append((file_id, parent_id))
        return "task-move"

    def delete_files(self, file_ids):
        self.deleted.append(list(file_ids))
        for file_id in file_ids:
            recycled = next(
                (item for item in self.recycle_bin if item["fileId"] == file_id),
                None,
            )
            if recycled is not None:
                self.recycle_bin.remove(recycled)
                continue
            for items in self.tree.values():
                items[:] = [item for item in items if item["fileId"] != file_id]
            self.recycle_bin.append({"fileId": file_id})
        return "task-delete"

    def delete_files_permanently(self, file_ids):
        # 真实客户端是两步：普通 ID → 回收站，再对同一 ID 删一次
        self.delete_files(list(file_ids))
        self.delete_files(list(file_ids))

    def wait_task(self, task_id, **_kwargs):
        return None

    def task_status(self, task_id):
        return 2

    def upload_token(self, *, name, size, parent_id, md5=None):
        self.upload_tokens.append({"name": name, "size": size, "parent_id": parent_id, "md5": md5})
        if not self.instant_upload:
            return {"instant": False, "taskId": "task-upload"}
        file_id = self._new_id()
        self.last_uploaded_id = file_id
        self.tree.setdefault(parent_id, []).append(
            {"fileId": file_id, "fileName": name, "resType": 1, "fileSize": size, "fileType": 2}
        )
        return {"instant": True, "taskId": "task-upload", "fileId": file_id}

    def wait_upload(self, task_id, **_kwargs):
        return {"fileId": self.last_uploaded_id}


class LocalReader:
    """把远端 Range 读取换成普通文件，用来验证哈希采样算法本身。"""

    def __init__(self, path: Path) -> None:
        self._handle = path.open("rb")

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._handle.seek(offset, whence)

    def read(self, size: int = -1) -> bytes:
        return self._handle.read(size)

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> "LocalReader":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


@pytest.fixture
def provider(tmp_path: Path) -> GuangyapanStorageProvider:
    instance = GuangyapanStorageProvider(
        library=_library(), data_dir=tmp_path / "plugin-data"
    )
    instance._client = FakeClient()
    instance._playback = GuangyapanPlayback(instance._client)
    instance._probe_or_empty = lambda file_id, *, size_bytes: {}  # type: ignore[method-assign]
    return instance


def _entry(file_id: str, name: str, size: int, *, is_video: bool = True, parent: str = "1") -> ImportFile:
    detail = {"fileId": file_id, "fileName": name, "fileSize": size, "resType": 1, "parentId": parent}
    return ImportFile(
        source_ref=_entry_ref(detail),
        name=name,
        relative_path=name,
        size_bytes=size,
        is_video=is_video,
    )


def _media(provider: GuangyapanStorageProvider, file_id: str, size: int, duration: int = 0) -> MediaHandle:
    return MediaHandle(
        media_id=1,
        library=provider.library,
        storage_ref=_media_ref(file_id),
        file_name="A.mp4",
        file_size_bytes=size,
        duration_seconds=duration,
    )


# ---------------------------------------------------------------- 浏览 / 扫描


def test_browse_cursor_follows_pages(provider: GuangyapanStorageProvider) -> None:
    first = provider.browse(parent_ref=None, cursor=None, limit=1)
    assert [entry.name for entry in first.entries] == ["Movies"]
    assert first.next_cursor == "1"

    second = provider.browse(parent_ref=None, cursor=first.next_cursor, limit=1)
    assert [entry.name for entry in second.entries] == ["root.mp4"]
    assert second.next_cursor is None
    assert second.entries[0].is_video is True


def test_scan_directory_builds_relative_paths(provider: GuangyapanStorageProvider) -> None:
    directory_ref = {"v": 1, "kind": "guangyapan_dir", "file_id": "1", "name": "Movies"}
    files = provider.scan_import_source(source_ref=directory_ref)

    assert [(item.relative_path, item.is_video) for item in files] == [
        ("A.mp4", True),
        ("A.srt", False),
        ("Sub/B.mkv", True),
    ]


def test_scan_progress_reports_current_and_text() -> None:
    from sakuramedia_guangyapan_provider.storage import _ScanReporter

    events: list[dict] = []
    reporter = _ScanReporter(lambda payload: events.append(payload))
    reporter.start("扫描目录")
    reporter.tick(10, 2)
    reporter.finish(3)

    assert events[0]["text"].startswith("扫描目录")
    assert events[-1]["text"].endswith("共 3 个文件")
    assert events[1]["current"] == 10 and events[1]["total"] == 0


# ---------------------------------------------------------------- 导入落位


def test_stage_import_creates_directory_and_copies(provider: GuangyapanStorageProvider) -> None:
    staged = provider.stage_import_file(
        source=_entry("2", "A.mp4", 2048),
        placement=ImportPlacement(relative_path="videos/A.mp4"),
        source_disposition="keep",
        operation_key="job:0",
    )
    client: FakeClient = provider._client  # type: ignore[assignment]
    assert len(client.created_dirs) == 2
    assert client.created_dirs[0][:2] == (MEDIA_ROOT, "videos")
    assert client.created_dirs[1][1].startswith("op-")
    assert [file_id for file_id, _target in client.copied] == ["2"]
    assert staged.size_bytes == 2048
    assert staged.storage_ref["file_id"] == client.copied[0][1]


def test_stage_import_is_idempotent_per_operation_key(provider: GuangyapanStorageProvider) -> None:
    first = provider.stage_import_file(
        source=_entry("2", "A.mp4", 2048),
        placement=ImportPlacement(relative_path="videos/A.mp4"),
        source_disposition="keep",
        operation_key="job:0",
    )
    second = provider.stage_import_file(
        source=_entry("2", "A.mp4", 2048),
        placement=ImportPlacement(relative_path="videos/A.mp4"),
        source_disposition="keep",
        operation_key="job:0",
    )

    client: FakeClient = provider._client  # type: ignore[assignment]
    assert len(client.copied) == 1, "同一次导入重试不应重复复制"
    assert first.storage_ref == second.storage_ref
    assert len(client.created_dirs) == 2


def test_stage_import_rejects_reused_operation_key_for_other_request(
    provider: GuangyapanStorageProvider,
) -> None:
    provider.stage_import_file(
        source=_entry("2", "A.mp4", 2048),
        placement=ImportPlacement(relative_path="videos/A.mp4"),
        source_disposition="keep",
        operation_key="job:conflict",
    )

    with pytest.raises(ProviderOperationError) as excinfo:
        provider.stage_import_file(
            source=_entry("2", "A.mp4", 2048),
            placement=ImportPlacement(relative_path="other/A.mp4"),
            source_disposition="keep",
            operation_key="job:conflict",
        )

    assert excinfo.value.code == "invalid_config"


def test_stage_import_retry_reuses_record_when_source_ref_has_no_parent(
    provider: GuangyapanStorageProvider,
) -> None:
    source = _entry("6", "B.mkv", 4096)
    source = ImportFile(
        source_ref={
            key: value for key, value in source.source_ref.items() if key != "parent_id"
        },
        name=source.name,
        relative_path=source.relative_path,
        size_bytes=source.size_bytes,
        is_video=source.is_video,
    )
    first = provider.stage_import_file(
        source=source,
        placement=ImportPlacement(relative_path="videos/B.mkv"),
        source_disposition="delete_after_commit",
        operation_key="job:no-parent",
    )

    second = provider.stage_import_file(
        source=source,
        placement=ImportPlacement(relative_path="videos/B.mkv"),
        source_disposition="delete_after_commit",
        operation_key="job:no-parent",
    )

    assert second.storage_ref == first.storage_ref
    client: FakeClient = provider._client  # type: ignore[assignment]
    assert client.moved == [("6", first.receipt["operation_dir_id"])]


def test_stage_import_moves_source_when_requested(provider: GuangyapanStorageProvider) -> None:
    staged = provider.stage_import_file(
        source=_entry("6", "B.mkv", 4096),
        placement=ImportPlacement(relative_path="videos/B.mkv"),
        source_disposition="delete_after_commit",
        operation_key="job:1",
    )
    client: FakeClient = provider._client  # type: ignore[assignment]
    assert len(client.moved) == 1
    assert client.moved[0][0] == "6"
    assert client.moved[0][1] == staged.receipt["operation_dir_id"]
    assert client.copied == []
    assert staged.storage_ref["file_id"] == "6"


def test_abort_import_deletes_staged_file(provider: GuangyapanStorageProvider) -> None:
    staged = provider.stage_import_file(
        source=_entry("2", "A.mp4", 2048),
        placement=ImportPlacement(relative_path="videos/A.mp4"),
        source_disposition="keep",
        operation_key="job:2",
    )
    provider.abort_import(receipt=staged.receipt)
    client: FakeClient = provider._client  # type: ignore[assignment]
    # 两步永久删除：自产垃圾不进回收站
    assert client.deleted == [
        [staged.storage_ref["file_id"]],
        [staged.storage_ref["file_id"]],
        [staged.receipt["operation_dir_id"]],
    ]
    assert provider._journal.get("job:2") is None


def test_abort_import_moves_delete_after_commit_source_back(
    provider: GuangyapanStorageProvider,
) -> None:
    staged = provider.stage_import_file(
        source=_entry("6", "B.mkv", 4096),
        placement=ImportPlacement(relative_path="videos/B.mkv"),
        source_disposition="delete_after_commit",
        operation_key="job:move-back",
    )
    client: FakeClient = provider._client  # type: ignore[assignment]
    operation_dir_id = staged.receipt["operation_dir_id"]

    provider.abort_import(receipt=staged.receipt)

    assert client.moved[-1] == ("6", "1")
    assert client.file_detail("6")["parentId"] == "1"
    assert all("6" not in chunk for chunk in client.deleted)
    assert client.deleted == [[operation_dir_id]]
    assert provider._journal.get("job:move-back") is None

    provider.abort_import(receipt=staged.receipt)
    assert client.moved[-1] == ("6", "1")


def test_delete_media_retry_does_not_permanently_delete_recycled_file(
    provider: GuangyapanStorageProvider,
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    client.delete_files(["2"])
    provider._delete_journal.put("2", {"state": "pending"})
    before = list(client.deleted)

    provider.delete_media(media=_media(provider, "2", 2048))

    assert client.deleted == before
    assert provider._delete_journal.get("2") == {"state": "trashed"}


def test_delete_media_pending_request_retries_while_file_is_normal(
    provider: GuangyapanStorageProvider,
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    provider._delete_journal.put("2", {"state": "pending"})

    provider.delete_media(media=_media(provider, "2", 2048))

    assert client.deleted == [["2"]]
    assert provider._delete_journal.get("2") == {"state": "trashed"}


def test_delete_media_waits_for_pending_remote_task(
    provider: GuangyapanStorageProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    provider._delete_journal.put(
        "2",
        {"state": "pending", "operation": "delete_media", "task_id": "task-1"},
    )
    monkeypatch.setattr(client, "task_status", lambda _task_id: 1)

    with pytest.raises(ProviderOperationError) as excinfo:
        provider.delete_media(media=_media(provider, "2", 2048))

    assert excinfo.value.code == "unavailable"
    assert excinfo.value.retryable is True
    assert client.deleted == []


def test_stage_import_uses_isolated_directory_for_large_target(
    provider: GuangyapanStorageProvider,
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    old_file_id = "old-A.mp4"
    client.tree[MEDIA_ROOT] = [{"fileId": "200", "fileName": "videos", "resType": 2}]
    client.tree["200"] = [
        {"fileId": old_file_id, "fileName": "A.mp4", "resType": 1, "fileSize": 2048}
    ]
    client.tree["200"].extend(
        {
            "fileId": f"old-{index}",
            "fileName": f"old-{index}.mp4",
            "resType": 1,
            "fileSize": 1,
        }
        for index in range(249)
    )

    staged = provider.stage_import_file(
        source=_entry("2", "A.mp4", 2048),
        placement=ImportPlacement(relative_path="videos/A.mp4"),
        source_disposition="keep",
        operation_key="job:large-target",
    )

    assert staged.storage_ref["file_id"] != old_file_id
    assert client.file_detail(old_file_id)["fileName"] == "A.mp4"
    assert client.list_calls.get("200", 0) == 0
    assert client.list_calls[staged.receipt["operation_dir_id"]] == 2


def test_stage_import_reuses_existing_operation_copy_after_crash(
    provider: GuangyapanStorageProvider,
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    operation_key = "job:crash-copy"
    operation_dir_id = _operation_directory(operation_key)
    client.tree[MEDIA_ROOT] = [{"fileId": "200", "fileName": "videos", "resType": 2}]
    client.tree["200"] = [
        {"fileId": operation_dir_id, "fileName": operation_dir_id, "resType": 2}
    ]
    client.tree[operation_dir_id] = [
        {"fileId": "202", "fileName": "A.mp4", "resType": 1, "fileSize": 2048}
    ]

    staged = provider.stage_import_file(
        source=_entry("2", "A.mp4", 2048),
        placement=ImportPlacement(relative_path="videos/A.mp4"),
        source_disposition="keep",
        operation_key=operation_key,
    )

    assert staged.storage_ref["file_id"] == "202"
    assert client.copied == []


def test_legacy_import_receipt_does_not_cleanup_final_parent(
    provider: GuangyapanStorageProvider,
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    record = {
        "file_id": "9",
        "parent_id": "1",
        "target_name": "root.mp4",
        "size_bytes": 100,
        "disposition": "keep",
    }
    staged = provider._staged_from_record(record, operation="stage_import_file")
    assert staged is not None

    provider.abort_import(receipt=staged.receipt)

    assert all("1" not in chunk for chunk in client.deleted)
    assert client.file_detail("1") is not None


def test_batch_directory_cache_reuses_shared_parent(
    provider: GuangyapanStorageProvider,
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    for index in range(20):
        provider.stage_import_file(
            source=_entry("2", "A.mp4", 2048),
            placement=ImportPlacement(relative_path="videos/A.mp4"),
            source_disposition="keep",
            operation_key=f"task:9:download:5:{index}",
        )

    parent_creations = [
        entry for entry in client.created_dirs if entry[:2] == (MEDIA_ROOT, "videos")
    ]
    operation_creations = [
        entry for entry in client.created_dirs if entry[1].startswith("op-")
    ]
    assert len(parent_creations) == 1
    assert len(operation_creations) == 20


def test_delete_media_uses_trash_delete(provider: GuangyapanStorageProvider) -> None:
    provider.delete_media(media=_media(provider, "2", 2048))
    client: FakeClient = provider._client  # type: ignore[assignment]
    assert client.deleted == [["2"]]


# ---------------------------------------------------------------- 哈希


def _expected_hash(data: bytes) -> str:
    """按协议草案独立实现一遍，用作对照。"""
    size = len(data)
    if size < 8 * 1024 * 1024:
        digest = hashlib.sha1(data).digest()
        payload = b"media-file-hash-v1\x00full\x00" + size.to_bytes(8, "big") + digest
        return f"media-file-hash-v1:{hashlib.sha1(payload).hexdigest()}"

    mib = 1024 * 1024
    head = hashlib.sha1(data[: 3 * mib]).digest()
    tail = hashlib.sha1(data[size - 3 * mib :]).digest()
    slot_count = (size - 6 * mib) // mib
    slot_1 = int.from_bytes(head[:8], "big") % slot_count
    candidate = int.from_bytes(tail[:8], "big") % (slot_count - 1)
    slot_2 = candidate if candidate < slot_1 else candidate + 1
    middle_1 = hashlib.sha1(data[3 * mib + slot_1 * mib :][:mib]).digest()
    middle_2 = hashlib.sha1(data[3 * mib + slot_2 * mib :][:mib]).digest()
    payload = (
        b"media-file-hash-v1\x00sampled\x00"
        + size.to_bytes(8, "big")
        + head
        + tail
        + middle_1
        + middle_2
    )
    return f"media-file-hash-v1:{hashlib.sha1(payload).hexdigest()}"


@pytest.mark.parametrize("size", [0, 7 * 1024 * 1024, 9 * 1024 * 1024])
def test_compute_file_hash_matches_protocol(
    provider: GuangyapanStorageProvider, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, size: int
) -> None:
    data = bytes((index * 31 + 7) % 251 for index in range(min(size, 4096))) * (
        size // min(size, 4096) if size else 0
    )
    data = (data + b"\x00" * size)[:size]
    target = tmp_path / "sample.bin"
    target.write_bytes(data)

    monkeypatch.setattr(
        provider,
        "_reader",
        lambda file_id, *, file_size, **kwargs: LocalReader(target),
    )

    assert provider.compute_file_hash(media=_media(provider, "2", size)) == _expected_hash(data)


# ---------------------------------------------------------------- Range 读取器


def test_range_reader_retries_empty_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = GuangyapanRangeReader(
        url="https://cdn.example/file",
        file_size_bytes=4,
        chunk_size=1024,
        retry_delays=(0.01, 0.01),
    )
    attempts = {"count": 0}

    def fake_attempt(start: int, count: int, _max_bytes: int | None) -> bytes:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise GuangyapanObjectNotReadyError("空响应")
        return b"data"

    monkeypatch.setattr(reader, "_attempt", fake_attempt)
    try:
        assert reader.read(4) == b"data"
    finally:
        reader.close()
    assert attempts["count"] == 3


def test_range_reader_rejects_overwide_206(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["range"] == "bytes=0-65535"
        return httpx.Response(
            206,
            content=b"x" * 65537,
            headers={"Content-Range": "bytes 0-65536/1000000"},
            request=request,
        )

    real_client = httpx.Client
    monkeypatch.setattr(
        range_reader_module.httpx,
        "Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    reader = GuangyapanRangeReader(
        url="https://cdn.example/file",
        file_size_bytes=1_000_000,
        chunk_size=64 * 1024,
        max_fetched_bytes=1_000_000,
        retry_delays=(),
    )
    try:
        with pytest.raises(GuangyapanRequestError):
            reader.read(1)
    finally:
        reader.close()


def test_range_reader_rejects_full_response_over_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 12, request=request)

    real_client = httpx.Client
    monkeypatch.setattr(
        range_reader_module.httpx,
        "Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    reader = GuangyapanRangeReader(
        url="https://cdn.example/file",
        file_size_bytes=12,
        chunk_size=1,
        max_fetched_bytes=10,
        retry_delays=(),
    )
    try:
        with pytest.raises(GuangyapanReadBudgetExceeded):
            reader.read(1)
    finally:
        reader.close()


def test_range_reader_enforces_budget_while_streaming(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Chunks(httpx.SyncByteStream):
        def __iter__(self):
            yield b"123456"
            yield b"789012"

        def close(self) -> None:
            return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=Chunks(), request=request)

    real_client = httpx.Client
    monkeypatch.setattr(
        range_reader_module.httpx,
        "Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    reader = GuangyapanRangeReader(
        url="https://cdn.example/file",
        file_size_bytes=12,
        chunk_size=1,
        max_fetched_bytes=10,
        retry_delays=(),
    )
    try:
        with pytest.raises(GuangyapanReadBudgetExceeded):
            reader.read(1)
    finally:
        reader.close()


# ---------------------------------------------------------------- 播放 / 迁移


def test_playback_redirects_by_default(provider: GuangyapanStorageProvider) -> None:
    context = SimpleNamespace(delivery="redirect", request=None, url_for=lambda path: path)
    response = asyncio.run(
        provider.handle_playback(media=_media(provider, "2", 2048), context=context)
    )
    assert response.status_code == 302
    assert response.headers["location"] == "https://cdn.example/2"


class FakeTransferSource:
    def __init__(self, path: Path, *, name: str = "transfer.mp4") -> None:
        self._path = path
        self.info = SimpleNamespace(file_name=name, size_bytes=path.stat().st_size)
        self.asserted = 0

    def open_reader(self):
        return LocalReader(self._path)

    def assert_unchanged(self) -> None:
        self.asserted += 1


def test_stage_transfer_uses_instant_upload_only(
    provider: GuangyapanStorageProvider, tmp_path: Path
) -> None:
    payload = b"x" * 1024
    source_path = tmp_path / "transfer.mp4"
    source_path.write_bytes(payload)
    result = provider.stage_transfer(
        source=FakeTransferSource(source_path),
        placement=ImportPlacement(relative_path="videos/transfer.mp4"),
        operation_key="move:0",
    )
    client: FakeClient = provider._client  # type: ignore[assignment]
    assert result.status == "staged"
    assert client.upload_tokens[-1]["md5"] == hashlib.md5(payload).hexdigest().upper()
    assert result.file_name == "transfer.mp4"


def test_stage_transfer_reports_not_available_without_instant_upload(
    provider: GuangyapanStorageProvider, tmp_path: Path
) -> None:
    client: FakeClient = provider._client  # type: ignore[assignment]
    client.instant_upload = False
    source_path = tmp_path / "other.mp4"
    source_path.write_bytes(b"y" * 512)
    result = provider.stage_transfer(
        source=FakeTransferSource(source_path, name="other.mp4"),
        placement=ImportPlacement(relative_path="videos/other.mp4"),
        operation_key="move:1",
    )
    assert result.status == "not_available"
    assert result.storage_ref is None


def test_read_budget_exceeded_is_not_retryable() -> None:
    """预算用尽是安全阀，不是服务故障：标成可重试会让宿主反复重跑同一份大流量。"""

    from sakuramedia_guangyapan_provider.exceptions import GuangyapanReadBudgetExceeded
    from sakuramedia_guangyapan_provider.storage import _cloud_error

    error = _cloud_error("generate_thumbnails", GuangyapanReadBudgetExceeded("光鸭读取超过本次预算"))

    assert error.code == "unavailable"
    assert error.retryable is False
    assert "预算" in error.safe_message


class DetailClient(FakeClient):
    """按 file_id 定制详情响应，用于覆盖父链边界。"""

    def __init__(
        self,
        details: dict[str, dict] | None = None,
        errors: dict[str, Exception] | None = None,
    ) -> None:
        super().__init__()
        self.details = details or {}
        self.errors = errors or {}
        self.detail_calls: list[str] = []

    def file_detail(self, file_id):
        self.detail_calls.append(file_id)
        error = self.errors.get(file_id)
        if error is not None:
            raise error
        if file_id in self.details:
            return self.details[file_id]
        return super().file_detail(file_id)


def _install_detail_client(
    provider: GuangyapanStorageProvider,
    details: dict[str, dict] | None = None,
    errors: dict[str, Exception] | None = None,
) -> DetailClient:
    client = DetailClient(details=details, errors=errors)
    provider._client = client
    provider._playback = GuangyapanPlayback(client)
    return client


def _dir_source_ref(file_id: str) -> dict:
    return {"v": 1, "kind": "guangyapan_dir", "file_id": file_id}


def _entry_source_ref(file_id: str, *, parent_id: str | None = None) -> dict:
    ref = {"v": 1, "kind": "guangyapan_entry", "file_id": file_id}
    if parent_id is not None:
        ref["parent_id"] = parent_id
    return ref


@pytest.mark.parametrize(
    "source_disposition", ["keep", "delete_after_commit", "in_place"]
)
def test_validate_import_source_rejects_media_root(
    provider: GuangyapanStorageProvider, source_disposition: str
) -> None:
    client = _install_detail_client(provider, details={MEDIA_ROOT: {"fileId": MEDIA_ROOT}})

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_dir_source_ref(MEDIA_ROOT),
            source_disposition=source_disposition,
        )

    assert error.value.code == "invalid_config"
    assert client.detail_calls == []


def test_validate_import_source_rejects_descendant_of_media_root(
    provider: GuangyapanStorageProvider,
) -> None:
    client = _install_detail_client(
        provider,
        details={MEDIA_ROOT: {"fileId": MEDIA_ROOT}, "50": {"fileId": "50", "parentId": MEDIA_ROOT}},
    )

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_dir_source_ref("50"),
            source_disposition="keep",
        )

    assert error.value.code == "invalid_config"
    assert client.detail_calls == ["50"]


def test_validate_import_source_rejects_account_root(
    provider: GuangyapanStorageProvider,
) -> None:
    client = _install_detail_client(provider)

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_dir_source_ref(""),
            source_disposition="keep",
        )

    assert error.value.code == "invalid_config"
    assert client.detail_calls == []


def test_validate_import_source_allows_disjoint_directory(
    provider: GuangyapanStorageProvider,
) -> None:
    client = _install_detail_client(
        provider,
        details={MEDIA_ROOT: {"fileId": MEDIA_ROOT}, "1": {"fileId": "1"}},
    )

    provider.validate_import_source(
        source_ref=_dir_source_ref("1"),
        source_disposition="keep",
    )

    assert client.detail_calls == ["1", MEDIA_ROOT]


def test_validate_import_source_rejects_everything_when_media_root_is_account_root(
    tmp_path: Path,
) -> None:
    provider = GuangyapanStorageProvider(
        library=LibraryHandle(
            library_id=7,
            provider_key="guangyapan",
            provider_config={"refresh_token": "gy.test", "media_root_id": ""},
            account_key="sub",
        ),
        data_dir=tmp_path / "plugin-data",
    )
    client = _install_detail_client(provider)

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_dir_source_ref("1"),
            source_disposition="keep",
        )

    assert error.value.code == "invalid_config"
    assert client.detail_calls == []


def test_validate_import_source_uses_entry_parent_id(
    provider: GuangyapanStorageProvider,
) -> None:
    client = _install_detail_client(
        provider,
        details={MEDIA_ROOT: {"fileId": MEDIA_ROOT}, "1": {"fileId": "1"}},
    )

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_entry_source_ref("50", parent_id=MEDIA_ROOT),
            source_disposition="keep",
        )
    assert error.value.code == "invalid_config"

    provider.validate_import_source(
        source_ref=_entry_source_ref("2", parent_id="1"),
        source_disposition="keep",
    )
    assert client.detail_calls == ["1"]


def test_validate_import_source_treats_top_level_entry_without_parent_as_root(
    provider: GuangyapanStorageProvider,
) -> None:
    client = _install_detail_client(
        provider,
        details={
            "9": {"fileId": "9", "fileName": "root.mp4", "resType": 1, "fileSize": 100},
        },
    )

    provider.validate_import_source(
        source_ref=_entry_source_ref("9"),
        source_disposition="keep",
    )

    assert client.detail_calls == ["9"]


def test_validate_import_source_resolves_entry_parent_from_file_detail(
    provider: GuangyapanStorageProvider,
) -> None:
    client = _install_detail_client(
        provider,
        details={
            MEDIA_ROOT: {"fileId": MEDIA_ROOT},
            "50": {"fileId": "50", "parentId": MEDIA_ROOT},
        },
    )

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_entry_source_ref("50"),
            source_disposition="keep",
        )

    assert error.value.code == "invalid_config"
    assert client.detail_calls == ["50"]


def test_validate_import_source_uses_full_parent_ids_fallback(
    provider: GuangyapanStorageProvider,
) -> None:
    client = _install_detail_client(
        provider,
        details={
            MEDIA_ROOT: {"fileId": MEDIA_ROOT},
            "60": {"fileId": "60", "fullParentIds": f"0/{MEDIA_ROOT}"},
            "61": {"fileId": "61", "fullParentIds": "0/1"},
            "1": {"fileId": "1"},
        },
    )

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_entry_source_ref("60"),
            source_disposition="keep",
        )
    assert error.value.code == "invalid_config"

    provider.validate_import_source(
        source_ref=_entry_source_ref("61"),
        source_disposition="keep",
    )
    assert client.detail_calls == ["60", "61", "1"]


def test_validate_import_source_rejects_missing_source(
    provider: GuangyapanStorageProvider,
) -> None:
    _install_detail_client(provider, details={"50": {}})

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_entry_source_ref("50"),
            source_disposition="keep",
        )

    assert error.value.code == "source_not_found"


def test_validate_import_source_maps_api_failure_to_unavailable(
    provider: GuangyapanStorageProvider,
) -> None:
    _install_detail_client(
        provider,
        errors={"50": GuangyapanRequestError("boom")},
    )

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_entry_source_ref("50"),
            source_disposition="keep",
        )

    assert error.value.code == "unavailable"
    assert error.value.retryable is True


def test_validate_import_source_rejects_parent_cycle(
    provider: GuangyapanStorageProvider,
) -> None:
    _install_detail_client(
        provider,
        details={
            "A": {"fileId": "A", "parentId": "B"},
            "B": {"fileId": "B", "parentId": "A"},
        },
    )

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_entry_source_ref("A"),
            source_disposition="keep",
        )

    assert error.value.code == "unavailable"
    assert error.value.retryable is True


def test_validate_import_source_rejects_over_deep_parent_chain(
    provider: GuangyapanStorageProvider,
) -> None:
    details = {
        str(index): {"fileId": str(index), "parentId": str(index + 1)}
        for index in range(65)
    }
    client = _install_detail_client(provider, details=details)

    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref=_entry_source_ref("0"),
            source_disposition="keep",
        )

    assert error.value.code == "unavailable"
    assert error.value.retryable is True
    # 先解析 entry 自身父目录 1 次，再沿父链最多走 64 步。
    assert len(client.detail_calls) == 65


def test_validate_import_source_rejects_invalid_ref(
    provider: GuangyapanStorageProvider,
) -> None:
    with pytest.raises(ProviderOperationError) as error:
        provider.validate_import_source(
            source_ref={"v": 2, "kind": "guangyapan_dir", "file_id": "1"},
            source_disposition="keep",
        )

    assert error.value.code == "source_not_found"
