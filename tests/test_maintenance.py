"""维护任务单测：回收站清空要轮询确认，空目录清理只删「整棵子树都空」的最上层目录。"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from sakuramedia_guangyapan_provider import maintenance as maintenance_module
from sakuramedia_guangyapan_provider.maintenance import (
    cleanup_empty_media_dirs,
    clear_recycle_bin,
)

MEDIA_ROOT = "100"


class FakeClient:
    def __init__(self) -> None:
        self.tree: dict[str, list[dict[str, Any]]] = {MEDIA_ROOT: []}
        self.recycle_bin: list[dict[str, Any]] = []
        self.recycle_clears = 0
        self.recycle_cleared_after = 1
        self.deleted: list[list[str]] = []
        self.list_calls: dict[str, int] = {}
        self.grow_after_first_list: dict[str, dict[str, Any]] = {}

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def list_files(self, *, parent_id: str = "", page: int = 0, page_size: int = 100,
                   dir_type: int | None = None, **_kwargs):
        if dir_type == 4:
            items = list(self.recycle_bin)
            return items[:page_size], len(items)
        if parent_id == "*":
            seen = self.list_calls.get(parent_id, 0) + 1
            self.list_calls[parent_id] = seen
            items = []
            for current_parent, entries in self.tree.items():
                for entry in entries:
                    item = dict(entry)
                    item.setdefault("parentId", current_parent)
                    items.append(item)
            if seen > 1:
                for extra_parent, extra in self.grow_after_first_list.items():
                    expanded = dict(extra)
                    expanded.setdefault("parentId", extra_parent)
                    items.append(expanded)
            items.sort(key=lambda item: str(item.get("fileName") or "").casefold())
            return items[page * page_size : (page + 1) * page_size], len(items)
        seen = self.list_calls.get(parent_id, 0) + 1
        self.list_calls[parent_id] = seen
        extra = self.grow_after_first_list.get(parent_id)
        if extra is not None and seen > 1:
            return [extra], 1
        items = list(self.tree.get(parent_id, []))
        return items[page * page_size : (page + 1) * page_size], len(items)

    def recycle_bin_total(self) -> int:
        return len(self.recycle_bin)

    def clear_recycle_bin(self) -> None:
        self.recycle_clears += 1

    def delete_files(self, file_ids: list[str]) -> str:
        self.deleted.append(list(file_ids))
        for file_id in file_ids:
            for items in self.tree.values():
                items[:] = [item for item in items if item["fileId"] != file_id]
        return "task-delete"

    def wait_task(self, task_id: str, **_kwargs) -> None:
        return None


class FakeReporter:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def progress_callback(self, payload: dict[str, Any]) -> None:
        self.payloads.append(payload)


def _library(library_id: int = 7, *, refresh_token: str = "gy.test") -> SimpleNamespace:
    return SimpleNamespace(
        id=library_id,
        provider_config={"refresh_token": refresh_token, "media_root_id": MEDIA_ROOT},
    )


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    fake = FakeClient()
    monkeypatch.setattr(maintenance_module, "GuangyapanClient", lambda _token: fake)
    monkeypatch.setattr(maintenance_module, "CLEAR_RECYCLE_POLL_SECONDS", 0.0)
    monkeypatch.setattr(maintenance_module, "_load_media_libraries", lambda: (_library(),))
    return fake


def test_clear_recycle_bin_requires_confirmation() -> None:
    with pytest.raises(ValidationError):
        clear_recycle_bin(None, {})
    with pytest.raises(ValidationError):
        clear_recycle_bin(None, {"confirm": False})
    with pytest.raises(ValidationError):
        cleanup_empty_media_dirs(None, None)


def test_clear_recycle_bin_skips_empty_bin(client: FakeClient) -> None:
    stats = clear_recycle_bin(None, {"confirm": True})

    assert stats == {"libraries": 1, "entries_before": 0, "cleared_libraries": 0, "failed_libraries": 0}
    assert client.recycle_clears == 0


def test_clear_recycle_bin_polls_until_empty(client: FakeClient) -> None:
    client.recycle_bin = [{"fileId": "1"}, {"fileId": "2"}]
    drained = {"value": False}

    def fake_total() -> int:
        if client.recycle_clears and not drained["value"]:
            # 第一次查询仍然是旧的（实测调用后约 1 分钟才真正清空）
            drained["value"] = True
            return 2
        return 0 if client.recycle_clears else 2

    client.recycle_bin_total = fake_total  # type: ignore[assignment]
    reporter = FakeReporter()

    stats = clear_recycle_bin(reporter, {"confirm": True})

    assert client.recycle_clears == 1
    assert stats["entries_before"] == 2
    assert stats["cleared_libraries"] == 1
    assert stats["failed_libraries"] == 0
    assert any("回收站" in payload["text"] for payload in reporter.payloads)


def test_clear_recycle_bin_counts_failed_library(client: FakeClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        maintenance_module,
        "_load_media_libraries",
        lambda: (_library(refresh_token=""),),
    )

    stats = clear_recycle_bin(None, {"confirm": True})

    assert stats["failed_libraries"] == 1


def test_cleanup_deletes_only_topmost_empty_directories(client: FakeClient) -> None:
    # 100（媒体根）
    #  ├─ 1 jav/            → 2（有文件）
    #  └─ 3 空目录/         → 4（空）—— 父目录一起删，4 不单独列
    client.tree = {
        MEDIA_ROOT: [
            {"fileId": "1", "fileName": "jav", "resType": 2},
            {"fileId": "3", "fileName": "空目录", "resType": 2},
        ],
        "1": [{"fileId": "2", "fileName": "ABC-123", "resType": 2}],
        "2": [{"fileId": "9", "fileName": "ABC-123.mp4", "resType": 1, "fileSize": 10}],
        "3": [{"fileId": "4", "fileName": "sub", "resType": 2}],
        "4": [],
    }

    stats = cleanup_empty_media_dirs(None, {"confirm": True})

    assert client.deleted == [["3"]]
    assert stats["deleted_directories"] == 1
    assert stats["scanned_directories"] == 5
    assert stats["skipped_directories"] == 1
    assert stats["failed_libraries"] == 0


def test_cleanup_keeps_media_root_even_when_empty(client: FakeClient) -> None:
    client.tree = {MEDIA_ROOT: []}

    stats = cleanup_empty_media_dirs(None, {"confirm": True})

    assert client.deleted == []
    assert stats["deleted_directories"] == 0


def test_cleanup_sees_empty_directory_after_large_first_page(
    client: FakeClient,
) -> None:
    client.tree = {
        MEDIA_ROOT: [
            {
                "fileId": f"file-{index}",
                "fileName": f"file-{index}.mp4",
                "resType": 1,
                "fileSize": 1,
            }
            for index in range(250)
        ]
        + [{"fileId": "3", "fileName": "空目录", "resType": 2}],
        "3": [],
    }

    stats = cleanup_empty_media_dirs(None, {"confirm": True})

    assert client.deleted == [["3"]]
    assert stats["deleted_directories"] == 1


def test_cleanup_rechecks_before_deleting(client: FakeClient) -> None:
    client.tree = {
        MEDIA_ROOT: [{"fileId": "3", "fileName": "空目录", "resType": 2}],
        "3": [],
    }
    # 扫描之后、删除之前目录里出现了文件（导入任务正在写）
    client.grow_after_first_list = {"3": {"fileId": "9", "fileName": "x.mp4", "resType": 1, "fileSize": 1}}

    stats = cleanup_empty_media_dirs(None, {"confirm": True})

    assert client.deleted == []
    assert stats["deleted_directories"] == 0
    assert stats["skipped_directories"] == 1


def test_cleanup_counts_failed_library(client: FakeClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        maintenance_module,
        "_load_media_libraries",
        lambda: (SimpleNamespace(id=9, provider_config={}),),
    )

    stats = cleanup_empty_media_dirs(None, {"confirm": True})

    assert stats["failed_libraries"] == 1
