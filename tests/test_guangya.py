"""用 MockTransport 锁住错误码语义与云添加请求形态（真机冒烟抓到过的坑）。"""

from __future__ import annotations

import json

import httpx
import pytest

from sakuramedia_guangyapan_provider.exceptions import (
    GuangyapanCloudResourceNotFound,
    GuangyapanCloudUrlUnsupported,
)
from sakuramedia_guangyapan_provider.guangya import (
    CLOUD_BT_NOT_FOUND_CODE,
    CLOUD_TASK_STATUSES,
    CLOUD_URL_ERROR_CODE,
    DUPLICATE_DIR_CODE,
    GuangyapanClient,
    INSTANT_UPLOAD_CODE,
    UPLOAD_IN_PROGRESS_CODE,
)


@pytest.fixture
def client_factory(monkeypatch: pytest.MonkeyPatch):
    def build(handler) -> GuangyapanClient:
        client = GuangyapanClient("gy.test-token")
        # 只替换业务域名请求，账号域（token 刷新）保持真实客户端但不发请求
        client._client = httpx.Client(
            transport=httpx.MockTransport(handler), headers={"user-agent": "test"}
        )
        client._access_token = "test-access"
        client._expires_at = 10 ** 12
        return client

    return build


def test_create_dir_returns_existing_directory_on_159(client_factory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": DUPLICATE_DIR_CODE,
                "msg": "文件夹名称重复",
                "data": {"fileId": "42", "fileName": "videos", "resType": 2},
            },
        )

    client = client_factory(handler)
    assert client.create_dir("videos", "1", fail_if_name_exist=True)["fileId"] == "42"
    client.close()


def test_upload_token_reports_instant_upload_on_156(client_factory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"code": INSTANT_UPLOAD_CODE, "msg": "上传已完成", "data": {"taskId": "t1", "gcid": "AB"}},
        )

    client = client_factory(handler)
    token = client.upload_token(name="a.mp4", size=100, parent_id="", md5="A" * 32)
    assert token["instant"] is True and token["taskId"] == "t1"
    client.close()


def test_upload_info_treats_147_as_still_processing(client_factory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"code": UPLOAD_IN_PROGRESS_CODE, "msg": "文件上传中"})

    client = client_factory(handler)
    assert client.upload_info("t1") == {}
    client.close()


def test_refresh_requires_valid_grant(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400, json={"error": "invalid_grant", "error_description": "refresh token invalid"}
        )

    client = GuangyapanClient("gy.test-token")
    client._access_token = ""
    client._expires_at = 0
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(Exception) as excinfo:
        client.access_token()
    assert "失效" in str(excinfo.value)
    client.close()


def test_cloud_resolve_maps_352_to_resource_not_found(client_factory) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"code": CLOUD_BT_NOT_FOUND_CODE, "msg": "BT文件不存在"})

    client = client_factory(handler)
    with pytest.raises(GuangyapanCloudResourceNotFound):
        client.cloud_resolve("magnet:?xt=urn:btih:" + "A" * 40)
    assert seen == ["/cloudcollection/v1/resolve_res"]
    client.close()


def test_cloud_create_task_maps_350_and_omits_new_name(client_factory) -> None:
    bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        code = CLOUD_URL_ERROR_CODE if request.headers.get("x-probe") else None
        if code:
            return httpx.Response(200, json={"code": code, "msg": "URL链接错误"})
        return httpx.Response(200, json={"msg": "success", "data": {"taskId": "t1"}})

    client = client_factory(handler)
    payload = client.cloud_create_task("magnet:?xt=urn:btih:" + "A" * 40, parent_id="42")
    assert payload["taskId"] == "t1"
    assert bodies == [{"url": "magnet:?xt=urn:btih:" + "A" * 40, "parentId": "42"}]

    client._client.headers["x-probe"] = "1"
    with pytest.raises(GuangyapanCloudUrlUnsupported):
        client.cloud_create_task("file:///tmp/x.torrent", parent_id="42")
    client.close()


def test_cloud_resolve_torrent_uses_torrent_field(client_factory) -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content-type"] = request.headers.get("content-type", "")
        body = request.content.decode("utf-8", "replace")
        captured["body"] = body
        return httpx.Response(
            200,
            json={
                "msg": "success",
                "data": {"btResInfo": {"infoHash": "B" * 40, "fileName": "a.mkv", "fileSize": 7}},
            },
        )

    client = client_factory(handler)
    data = client.cloud_resolve_torrent(b"d4:infod4:name4:x.e")
    assert data["btResInfo"]["infoHash"] == "B" * 40
    assert 'name="torrent"' in captured["body"]
    assert captured["content-type"].startswith("multipart/form-data; boundary=")
    client.close()


def test_cloud_tasks_requests_every_status_or_exact_ids(client_factory) -> None:
    bodies: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"msg": "success", "data": {"list": []}})

    client = client_factory(handler)
    client.cloud_tasks()
    client.cloud_tasks(task_ids=["t1", "t2"])
    assert bodies[0]["status"] == list(CLOUD_TASK_STATUSES)
    assert bodies[1] == {"taskIds": ["t1", "t2"]}
    client.close()


def test_delete_files_permanently_deletes_twice(client_factory) -> None:
    calls: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/delete_file"):
            calls.append(json.loads(request.content)["fileIds"])
            return httpx.Response(200, json={"msg": "success", "data": {"taskId": "task-1"}})
        # get_task_status：第一次就返回终态，避免测试里空转
        return httpx.Response(200, json={"msg": "success", "data": {"status": 2}})

    client = client_factory(handler)
    client.delete_files_permanently(["7"])
    assert calls == [["7"], ["7"]]
    client.close()
