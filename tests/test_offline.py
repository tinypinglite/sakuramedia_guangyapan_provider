"""离线下载（云添加）组件的单测：状态映射、幂等、0 字节窗口、删除语义。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from sakuramedia_guangyapan_provider.exceptions import GuangyapanCloudResourceNotFound
from sakuramedia_guangyapan_provider.offline import (
    GuangyapanOfflineDownloadComponent,
    GuangyapanOfflineDownloadProvider,
)
from sakuramedia_guangyapan_provider import offline as offline_module

from src.plugins.provider_protocol import (
    DownloadClientHandle,
    DownloadSubmission,
    LibraryHandle,
    ProviderOperationError,
)

DOWNLOADS_ROOT = "500"
HASH_A = "a" * 40
HASH_B = "b" * 40
HASH_C = "c" * 40


def _library(*, downloads_root_id: str = DOWNLOADS_ROOT) -> LibraryHandle:
    config: dict[str, Any] = {"refresh_token": "gy.test", "media_root_id": "100"}
    if downloads_root_id:
        config["downloads_root_id"] = downloads_root_id
    return LibraryHandle(
        library_id=7,
        provider_key="guangyapan",
        provider_config=config,
        account_key="sub",
    )


class FakeClient:
    """只实现被测代码真正调用的接口，行为照 endpoints.md §7 的实测结论。"""

    def __init__(self) -> None:
        self.tree: dict[str, list[dict[str, Any]]] = {DOWNLOADS_ROOT: []}
        self.resolved: list[str] = []
        self.uploaded_torrents: list[bytes] = []
        self.created_tasks: list[dict[str, Any]] = []
        self.deleted_task_ids: list[str] = []
        self.permanent_deletes: list[list[str]] = []
        self.tasks: list[dict[str, Any]] = []
        self.task_id_queries: list[list[str]] = []
        self.bt_res_info: dict[str, Any] = {}
        self.membership = 2
        self.resolve_error: Exception | None = None
        self._next_id = 1000
        self._next_task = 1

    # --- 生命周期
    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    # --- 账号 / 目录
    def assets(self) -> dict[str, Any]:
        return {"vipStatus": self.membership, "vipLeftTime": 86400 * 30}

    def list_files(self, *, parent_id: str = "", page: int = 0, page_size: int = 100, **_kwargs):
        if parent_id == "*":
            items = []
            for current_parent, entries in self.tree.items():
                for entry in entries:
                    item = dict(entry)
                    item.setdefault("parentId", current_parent)
                    items.append(item)
            return items[page * page_size : (page + 1) * page_size], len(items)
        items = list(self.tree.get(parent_id, []))
        return items[page * page_size : (page + 1) * page_size], len(items)

    def create_dir(self, name: str, parent_id: str = "", *, fail_if_name_exist: bool = False):
        for item in self.tree.get(parent_id, []):
            if item["resType"] == 2 and item["fileName"] == name:
                return {"fileId": item["fileId"]}
        self._next_id += 1
        file_id = str(self._next_id)
        self.tree.setdefault(parent_id, []).append(
            {"fileId": file_id, "fileName": name, "resType": 2}
        )
        return {"fileId": file_id}

    def add_file(self, parent_id: str, *, name: str, size: int) -> None:
        self._next_id += 1
        self.tree.setdefault(parent_id, []).append(
            {"fileId": str(self._next_id), "fileName": name, "resType": 1, "fileSize": size}
        )

    # --- 云添加
    def cloud_resolve(self, url: str) -> dict[str, Any]:
        self.resolved.append(url)
        if self.resolve_error is not None:
            raise self.resolve_error
        return {"btResInfo": dict(self.bt_res_info), "url": url}

    def cloud_resolve_torrent(self, payload: bytes, **_kwargs) -> dict[str, Any]:
        self.uploaded_torrents.append(payload)
        return {"btResInfo": dict(self.bt_res_info)}

    def cloud_create_task(self, url: str, *, parent_id: str = "", new_name: str | None = None):
        self._next_task += 1
        task_id = f"task-{self._next_task}"
        self.created_tasks.append({"url": url, "parent_id": parent_id, "new_name": new_name})
        return {"taskId": task_id}

    def cloud_tasks(self, *, task_ids: list[str] | None = None, **_kwargs) -> dict[str, Any]:
        if task_ids is not None:
            self.task_id_queries.append(list(task_ids))
        tasks = [dict(item) for item in self.tasks]
        if task_ids is not None:
            tasks = [item for item in tasks if str(item.get("taskId")) in set(task_ids)]
        return {"list": tasks}

    def iter_cloud_tasks(self, *, task_ids: list[str] | None = None, **_kwargs):
        yield self.cloud_tasks(task_ids=task_ids)

    def cloud_delete_tasks(self, task_ids: list[str]) -> None:
        self.deleted_task_ids.extend(task_ids)

    def delete_files(self, file_ids: list[str]) -> str:
        self.permanent_deletes.append(list(file_ids))
        for file_id in file_ids:
            for items in self.tree.values():
                items[:] = [item for item in items if item["fileId"] != file_id]
        return "task-delete"

    def delete_files_permanently(self, file_ids: list[str]) -> None:
        # 实测是两步：普通 ID → 回收站，再对同一 ID 删一次
        self.delete_files(list(file_ids))
        self.delete_files(list(file_ids))

    def wait_task(self, task_id: str, **_kwargs) -> None:
        return None


def _provider(tmp_path: Path, client: FakeClient, *, downloads_root_id: str = DOWNLOADS_ROOT):
    return GuangyapanOfflineDownloadProvider(
        refresh_token="gy.test",
        downloads_root_id=downloads_root_id,
        library=_library(downloads_root_id=downloads_root_id),
        data_dir=tmp_path / "plugin-data",
        client_factory=lambda _token: client,
    )


def test_submit_magnet_creates_task_directory_and_is_idempotent(tmp_path: Path) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_A.upper(), "fileName": "ABC-123.mkv", "fileSize": 99}
    provider = _provider(tmp_path, client)
    submission = DownloadSubmission(
        source_uri=f"magnet:?xt=urn:btih:{HASH_A.upper()}", display_name="ABC-123-ab12cd"
    )

    first = provider.submit(submission=submission)

    assert first.remote_id == HASH_A
    assert first.state == "queued"
    assert first.completed_source_ref is None
    assert client.created_tasks == [
        {"url": f"magnet:?xt=urn:btih:{HASH_A}", "parent_id": "1001", "new_name": None}
    ]
    assert [item["fileName"] for item in client.tree[DOWNLOADS_ROOT]] == ["ABC-123-ab12cd"]

    second = provider.submit(submission=submission)

    assert second.remote_id == HASH_A
    # 重试不能再建目录、也不能再建任务
    assert len(client.created_tasks) == 1
    assert len(client.tree[DOWNLOADS_ROOT]) == 1


def test_submit_sanitises_display_name(tmp_path: Path) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_A, "fileName": "a.mkv", "fileSize": 1}
    provider = _provider(tmp_path, client)

    provider.submit(
        submission=DownloadSubmission(
            source_uri=f"magnet:?xt=urn:btih:{HASH_A}", display_name="ABC/123: bad*name"
        )
    )

    assert [item["fileName"] for item in client.tree[DOWNLOADS_ROOT]] == ["ABC 123 bad name"]


def test_submit_http_torrent_link_uploads_seed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_B, "fileName": "b.mkv", "fileSize": 5}
    monkeypatch.setattr(offline_module, "_fetch_torrent", lambda _url: ("torrent", b"seed-bytes"))
    provider = _provider(tmp_path, client)

    task = provider.submit(
        submission=DownloadSubmission(
            source_uri="https://indexer.example/download/1", display_name="B-000001"
        )
    )

    assert client.uploaded_torrents == [b"seed-bytes"]
    assert task.remote_id == HASH_B
    assert client.created_tasks[0]["url"] == f"magnet:?xt=urn:btih:{HASH_B}"


def test_submit_http_link_redirected_to_magnet_skips_torrent_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_C, "fileName": "c.mkv", "fileSize": 5}
    magnet = f"magnet:?xt=urn:btih:{HASH_C}"
    monkeypatch.setattr(offline_module, "_fetch_torrent", lambda _url: ("magnet", magnet))
    provider = _provider(tmp_path, client)

    provider.submit(
        submission=DownloadSubmission(source_uri="https://indexer.example/2", display_name="C-1")
    )

    assert client.uploaded_torrents == []
    assert client.resolved == [magnet]
    assert client.created_tasks[0]["url"] == magnet


def test_submit_rejects_invalid_magnet_and_bt_not_found(tmp_path: Path) -> None:
    provider = _provider(tmp_path, FakeClient())

    with pytest.raises(ProviderOperationError) as invalid:
        provider.submit(
            submission=DownloadSubmission(source_uri="magnet:?xt=urn:btih:zzz", display_name="x")
        )
    assert invalid.value.code == "invalid_config"

    client = FakeClient()
    client.resolve_error = GuangyapanCloudResourceNotFound("BT文件不存在")
    provider = _provider(tmp_path, client)
    with pytest.raises(ProviderOperationError) as missing:
        provider.submit(
            submission=DownloadSubmission(
                source_uri=f"magnet:?xt=urn:btih:{HASH_A}", display_name="x"
            )
        )
    assert missing.value.code == "source_not_found"
    assert "BT" in missing.value.safe_message


def test_list_tasks_maps_status_and_postpones_completion(tmp_path: Path) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_A, "fileName": "a.mkv", "fileSize": 9}
    provider = _provider(tmp_path, client)
    provider.submit(
        submission=DownloadSubmission(
            source_uri=f"magnet:?xt=urn:btih:{HASH_A}", display_name="A-1"
        )
    )
    directory_id = client.created_tasks[0]["parent_id"]
    client.tasks = [
        {"taskId": "task-2", "fileName": "A-1", "status": 2, "progress": 23},
        {"taskId": "not-ours", "fileName": "用户自己加的", "status": 1},
    ]

    postponed = provider.list_tasks()

    assert [task.remote_id for task in postponed] == [HASH_A]
    assert postponed[0].state == "downloading"
    assert postponed[0].completed_source_ref is None

    client.add_file(directory_id, name="a.mkv", size=9)
    completed = provider.list_tasks()

    assert completed[0].state == "completed"
    assert completed[0].progress == pytest.approx(0.23)
    assert completed[0].completed_source_ref == {
        "v": 1,
        "kind": "guangyapan_dir",
        "file_id": directory_id,
        "name": "A-1",
    }


def test_list_tasks_maps_failed_and_partial_states(tmp_path: Path) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_A, "fileName": "a.mkv", "fileSize": 9}
    provider = _provider(tmp_path, client)
    provider.submit(
        submission=DownloadSubmission(source_uri=f"magnet:?xt=urn:btih:{HASH_A}", display_name="A-1")
    )
    directory_id = client.created_tasks[0]["parent_id"]
    client.add_file(directory_id, name="a.mkv", size=9)

    for status, expected in ((0, "queued"), (1, "downloading"), (3, "failed"), (4, "failed"), (5, "completed")):
        client.tasks = [{"taskId": "task-2", "fileName": "A-1", "status": status}]
        task = provider.list_tasks()[0]
        assert task.state == expected, status


def test_list_tasks_queries_all_managed_task_ids(tmp_path: Path) -> None:
    client = FakeClient()
    provider = _provider(tmp_path, client)
    for index in range(75):
        info_hash = f"{index:040x}"
        task_id = f"task-{index}"
        directory_id = f"dir-{index}"
        provider._journal.put(  # noqa: SLF001 - test seeds the provider ledger directly
            info_hash,
            {
                "task_id": task_id,
                "dir_id": directory_id,
                "dir_name": f"download-{index}",
            },
        )
        client.tree.setdefault(directory_id, []).append(
            {
                "fileId": f"file-{index}",
                "fileName": f"file-{index}.mkv",
                "resType": 1,
                "fileSize": 1,
            }
        )
        client.tasks.append(
            {"taskId": task_id, "fileName": f"download-{index}", "status": 1}
        )

    tasks = provider.list_tasks()

    assert len(tasks) == 75
    assert len(client.task_id_queries) == 1
    assert len(client.task_id_queries[0]) == 75


def test_list_tasks_waits_for_nested_zero_byte_file(tmp_path: Path) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_A, "fileName": "a.mkv", "fileSize": 9}
    provider = _provider(tmp_path, client)
    provider.submit(
        submission=DownloadSubmission(
            source_uri=f"magnet:?xt=urn:btih:{HASH_A}", display_name="A-1"
        )
    )
    root_id = client.created_tasks[0]["parent_id"]
    child = client.create_dir("sub", root_id)
    client.add_file(child["fileId"], name="a.mkv", size=0)
    client.tasks = [{"taskId": "task-2", "fileName": "A-1", "status": 2}]

    assert provider.list_tasks()[0].state == "downloading"

    client.tree[child["fileId"]][0]["fileSize"] = 9
    assert provider.list_tasks()[0].state == "completed"


def test_delete_task_removes_record_and_files(tmp_path: Path) -> None:
    client = FakeClient()
    client.bt_res_info = {"infoHash": HASH_A, "fileName": "a.mkv", "fileSize": 9}
    provider = _provider(tmp_path, client)
    provider.submit(
        submission=DownloadSubmission(source_uri=f"magnet:?xt=urn:btih:{HASH_A}", display_name="A-1")
    )
    directory_id = client.created_tasks[0]["parent_id"]
    client.tasks = [{"taskId": "task-2", "fileName": "A-1", "status": 1}]

    provider.delete_task(remote_id=HASH_A, delete_files=True)

    assert client.deleted_task_ids == ["task-2"]
    assert client.permanent_deletes == [[directory_id], [directory_id]]
    # 记录清掉后这个任务不再上报
    client.tasks = []
    assert provider.list_tasks() == ()


def test_delete_task_without_record_is_noop(tmp_path: Path) -> None:
    client = FakeClient()
    provider = _provider(tmp_path, client)

    provider.delete_task(remote_id=HASH_B, delete_files=True)

    assert client.deleted_task_ids == []
    assert client.permanent_deletes == []

    with pytest.raises(ProviderOperationError) as excinfo:
        provider.delete_task(remote_id="not-a-hash", delete_files=False)
    assert excinfo.value.code == "invalid_config"


def test_component_requires_downloads_root_and_reports_membership(tmp_path: Path) -> None:
    client = FakeClient()
    component = GuangyapanOfflineDownloadComponent(
        data_dir=tmp_path / "plugin-data", client_factory=lambda _token: client
    )

    with pytest.raises(ProviderOperationError) as excinfo:
        component.build(
            client=DownloadClientHandle(
                client_id=1, library=_library(downloads_root_id=""), provider_config={}
            )
        )
    assert excinfo.value.code == "invalid_config"
    assert "下载目录" in excinfo.value.safe_message

    provider = component.build(
        client=DownloadClientHandle(client_id=1, library=_library(), provider_config={})
    )
    assert isinstance(provider, GuangyapanOfflineDownloadProvider)

    report = component.test_client(submitted_config={}, library=_library())
    assert report.status == "ok"
    assert "会员" in report.checks[0].message

    client.membership = 1
    assert "非会员" in component.test_client(submitted_config={}, library=_library()).checks[0].message
