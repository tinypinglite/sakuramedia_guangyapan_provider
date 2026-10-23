"""光鸭云盘私有接口客户端。

本模块是插件**唯一**的出网位置：所有上行报文（路径、字段、请求头、错误码）都来自
真机实测结论；平台改接口时只需要改这一个文件。

实测要点：
- 业务接口只需 `Authorization: Bearer` + `content-type`，不需要 did / dt / 签名；
- 成功响应没有 `code` 字段（`{"msg": "success", "data": {...}}`），错误才有 `code`；
- `code 127` 是限流（HTTP 200），恢复是立即的，短退避重试即可；
- `code 117` 是 token 无效，刷新后重试一次。
"""

from __future__ import annotations

import hashlib
import os
import random
import threading
import time
from collections.abc import Iterator
from typing import Any

import httpx

from .exceptions import (
    GuangyapanAuthError,
    GuangyapanCloudResourceNotFound,
    GuangyapanCloudUrlUnsupported,
    GuangyapanNotFoundError,
    GuangyapanRateLimitError,
    GuangyapanRequestError,
)

ACCOUNT_BASE = "https://account.guangyapan.com"
API_BASE = "https://api.guangyapan.com"
CLIENT_ID = "aMe-8VSlkrbQXpUR"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

TOKEN_EXPIRY_MARGIN_SECONDS = 120
# 实测：4 并发（≈18.7 req/s）安全，8 并发（≈44.8 req/s）触发 code 127。
# 取 5 req/s 作为账号级上限，留 3 倍以上余量。
MIN_REQUEST_INTERVAL_SECONDS = 0.2
RATE_LIMIT_CODE = 127
TOKEN_INVALID_CODE = 117
# 同名目录 + failIfNameExist=true：返回 159 **并在 data 里带上已有目录 ID**，是幂等落位的关键
DUPLICATE_DIR_CODE = 159
# 秒传命中 / 上传已完成：156 是**成功终态**，不是错误
INSTANT_UPLOAD_CODE = 156
# 上传任务仍在处理中：147 出现在 get_info_by_task_id 轮询里
UPLOAD_IN_PROGRESS_CODE = 147
NOT_FOUND_CODES = frozenset({143, 145})
DELETE_BATCH_LIMIT = 500
# 云添加：链接形态不受支持（实测纯 infoHash / file:// 都会得到它）
CLOUD_URL_ERROR_CODE = 350
# 云添加：平台的 BT 网络里找不到这个资源（合成种子/无人做种实测得到它，与协议能力无关）
CLOUD_BT_NOT_FOUND_CODE = 352
# 云添加任务状态（Web 包硬编码：0 排队中 / 1 进行中 / 2 已完成 / 3 已失败 / 4 已取消 / 5 部分已完成）
CLOUD_TASK_STATUSES = (0, 1, 2, 3, 4, 5)

_HTTP_TIMEOUT = httpx.Timeout(60.0, connect=15.0)
_PACERS: dict[str, "_Pacer"] = {}
_PACERS_LOCK = threading.Lock()


class _Pacer:
    """账号级最小请求间隔（跨媒体库、跨线程共享）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next_at - now
            if delay > 0:
                time.sleep(delay)
            self._next_at = max(now, self._next_at) + MIN_REQUEST_INTERVAL_SECONDS


def _pacer_for(key: str) -> _Pacer:
    with _PACERS_LOCK:
        pacer = _PACERS.get(key)
        if pacer is None:
            pacer = _Pacer()
            _PACERS[key] = pacer
        return pacer


def is_success(body: Any) -> bool:
    if not isinstance(body, dict):
        return False
    code = body.get("code")
    return code is None or code == 0


def data_of(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise GuangyapanRequestError("光鸭接口返回了非 JSON 结果")
    if not is_success(body):
        code = body.get("code")
        message = str(body.get("msg") or "未知错误")
        if code in NOT_FOUND_CODES:
            raise GuangyapanNotFoundError(message)
        if code == RATE_LIMIT_CODE:
            raise GuangyapanRateLimitError(message)
        raise GuangyapanRequestError(f"光鸭接口错误 code={code} msg={message}")
    data = body.get("data")
    return data if isinstance(data, dict) else {}


class GuangyapanClient:
    """同步客户端：token 自动续期 + 账号级限速 + 错误码映射。"""

    def __init__(self, refresh_token: str) -> None:
        if not isinstance(refresh_token, str) or not refresh_token.strip():
            raise GuangyapanAuthError("缺少光鸭 refresh_token")
        self._refresh_token = refresh_token.strip()
        self._access_token = ""
        self._expires_at = 0.0
        self._token_lock = threading.Lock()
        self._pacer = _pacer_for(hashlib.sha256(self._refresh_token.encode()).hexdigest())
        self._client = httpx.Client(
            timeout=_HTTP_TIMEOUT,
            trust_env=False,
            follow_redirects=True,
            headers={"user-agent": USER_AGENT, "accept": "application/json, text/plain, */*"},
        )

    # ---------------------------------------------------------------- 生命周期

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "GuangyapanClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # ---------------------------------------------------------------- token

    def access_token(self) -> str:
        with self._token_lock:
            if self._access_token and time.time() < self._expires_at:
                return self._access_token
            self._refresh_locked()
            return self._access_token

    def _refresh_locked(self) -> None:
        body = {
            "client_id": CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        }
        try:
            response = self._client.post(
                f"{ACCOUNT_BASE}/v1/auth/token",
                json=body,
                headers={
                    "content-type": "application/json",
                    "x-client-id": CLIENT_ID,
                    "x-client-version": "0.0.1",
                    "x-action": "401",
                },
            )
        except httpx.RequestError as exc:
            raise GuangyapanRequestError("光鸭账号服务网络不可用") from exc
        payload: Any
        try:
            payload = response.json()
        except ValueError as exc:
            raise GuangyapanRequestError(f"光鸭账号服务返回异常（HTTP {response.status_code}）") from exc
        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not isinstance(token, str) or not token:
            description = ""
            if isinstance(payload, dict):
                description = str(payload.get("error_description") or payload.get("error") or "")
            if "invalid_grant" in description or response.status_code == 400:
                raise GuangyapanAuthError("光鸭 refresh_token 已失效，请重新获取")
            raise GuangyapanRequestError(f"光鸭 token 刷新失败（HTTP {response.status_code}）")
        expires_in = payload.get("expires_in")
        seconds = float(expires_in) if isinstance(expires_in, (int, float)) and expires_in > 0 else 7200.0
        self._access_token = token
        self._expires_at = time.time() + max(60.0, seconds - TOKEN_EXPIRY_MARGIN_SECONDS)

    # ---------------------------------------------------------------- 请求

    def _send(
        self,
        method: str,
        url: str,
        *,
        json_body: Any,
        headers: dict[str, str],
        params: Any = None,
        content: bytes | None = None,
    ):
        try:
            if content is not None:
                return self._client.request(
                    method, url, content=content, headers=headers, params=params
                )
            return self._client.request(method, url, json=json_body, headers=headers, params=params)
        except httpx.RequestError as exc:
            raise GuangyapanRequestError("光鸭接口网络不可用") from exc

    def _call(
        self,
        method: str,
        url: str,
        *,
        json_body: Any = None,
        params: Any = None,
        authorized: bool = True,
        allow_codes: tuple[int, ...] = (),
        content: bytes | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        headers = {"content-type": content_type or "application/json"}
        if authorized:
            headers["authorization"] = f"Bearer {self.access_token()}"

        refreshed = False
        rate_limit_attempt = 0
        while True:
            self._pacer.wait()
            response = self._send(
                method, url, json_body=json_body, headers=headers, params=params, content=content
            )
            try:
                payload = response.json()
            except ValueError as exc:
                raise GuangyapanRequestError(
                    f"光鸭接口返回非 JSON（HTTP {response.status_code}）"
                ) from exc
            code = payload.get("code") if isinstance(payload, dict) else None

            if code == TOKEN_INVALID_CODE or (authorized and response.status_code == 401):
                if refreshed:
                    raise GuangyapanAuthError("光鸭登录已失效")
                with self._token_lock:
                    self._access_token = ""
                    self._refresh_locked()
                headers["authorization"] = f"Bearer {self._access_token}"
                refreshed = True
                continue
            if code == RATE_LIMIT_CODE:
                rate_limit_attempt += 1
                if rate_limit_attempt > 3:
                    raise GuangyapanRateLimitError("光鸭接口限流，请稍后重试")
                time.sleep(0.4 * rate_limit_attempt + random.uniform(0.0, 0.3))
                continue
            if not is_success(payload):
                error_code = payload.get("code") if isinstance(payload, dict) else response.status_code
                if error_code in allow_codes:
                    return payload if isinstance(payload, dict) else {}
                message = str(payload.get("msg") or "") if isinstance(payload, dict) else ""
                if error_code in NOT_FOUND_CODES:
                    raise GuangyapanNotFoundError(message or "文件不存在")
                if response.status_code == 401:
                    raise GuangyapanAuthError("光鸭登录已失效")
                raise GuangyapanRequestError(f"光鸭接口错误 code={error_code} msg={message}")
            return payload if isinstance(payload, dict) else {}

    # ---------------------------------------------------------------- 账号

    def user_me(self) -> dict[str, Any]:
        payload = self._call("GET", f"{ACCOUNT_BASE}/v1/user/me")
        return payload.get("data") if isinstance(payload.get("data"), dict) else payload

    def assets(self) -> dict[str, Any]:
        return data_of(self._call("POST", f"{API_BASE}/assets/v1/get_assets", json_body={}))

    # ---------------------------------------------------------------- 列表 / 详情

    def list_files(
        self,
        *,
        parent_id: str = "",
        page: int = 0,
        page_size: int = 100,
        order_by: int = 3,
        sort_type: int = 1,
        file_types: list[int] | None = None,
        res_type: int | None = None,
        dir_type: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        body: dict[str, Any] = {
            "parentId": parent_id,
            "page": page,
            "pageSize": page_size,
            "orderBy": order_by,
            "sortType": sort_type,
        }
        if file_types is not None:
            body["fileTypes"] = file_types
        if res_type is not None:
            body["resType"] = res_type
        if dir_type is not None:
            body["dirType"] = dir_type
        data = data_of(self._call("POST", f"{API_BASE}/userres/v1/file/get_file_list", json_body=body))
        items = data.get("list")
        total = data.get("total")
        return (items if isinstance(items, list) else []), int(total or 0)

    def file_detail(self, file_id: str) -> dict[str, Any]:
        data = data_of(
            self._call(
                "POST",
                f"{API_BASE}/userres/v1/file/get_file_detail",
                json_body={"fileId": file_id},
            )
        )
        info = data.get("fileInfo")
        return info if isinstance(info, dict) else {}

    def all_entries(self, *, parent_id: str = "", page_size: int = 1000) -> Iterator[dict[str, Any]]:
        """分页遍历一个目录（或全库：parent_id="*"）。"""
        page = 0
        while True:
            items, total = self.list_files(parent_id=parent_id, page=page, page_size=page_size)
            for item in items:
                yield item
            page += 1
            if len(items) < page_size or page * page_size >= total:
                return

    def resolve_path(self, path: str) -> str:
        """把 `/a/b` 这样的绝对路径逐级解析成 fileId；不存在返回空串。"""
        parts = [part for part in path.strip("/").split("/") if part]
        parent_id = ""
        for part in parts:
            found = ""
            for item in self.all_entries(parent_id=parent_id, page_size=1000):
                if item.get("resType") == 2 and item.get("fileName") == part:
                    found = str(item.get("fileId") or "")
                    break
            if not found:
                return ""
            parent_id = found
        return parent_id

    # ---------------------------------------------------------------- 直链

    def download_url(self, file_id: str) -> tuple[str, int]:
        data = data_of(
            self._call(
                "POST",
                f"{API_BASE}/userres/v1/get_res_download_url",
                json_body={"fileId": file_id},
            )
        )
        url = str(data.get("signedURL") or data.get("downloadUrl") or "")
        if not url:
            raise GuangyapanRequestError("光鸭未返回下载地址")
        duration = data.get("urlDuration")
        return url, int(duration) if isinstance(duration, (int, float)) else 21600

    # ---------------------------------------------------------------- 写操作

    def create_dir(self, dir_name: str, parent_id: str = "", *, fail_if_name_exist: bool = False) -> dict[str, Any]:
        """返回新建目录；`fail_if_name_exist=True` 时同名目录返回 159 且带已有 fileId。"""
        body: dict[str, Any] = {"dirName": dir_name, "parentId": parent_id}
        if fail_if_name_exist:
            body["failIfNameExist"] = True
        payload = self._call(
            "POST",
            f"{API_BASE}/userres/v1/file/create_dir",
            json_body=body,
            allow_codes=(DUPLICATE_DIR_CODE,),
        )
        if payload.get("code") == DUPLICATE_DIR_CODE:
            data = payload.get("data")
            if isinstance(data, dict) and data.get("fileId"):
                return data
            raise GuangyapanRequestError("光鸭同名目录已存在但未返回目录 ID")
        return data_of(payload)

    def _batch(self, path: str, file_ids: list[str], parent_id: str | None = None) -> str:
        if not file_ids or len(file_ids) > DELETE_BATCH_LIMIT:
            raise GuangyapanRequestError("光鸭批量操作数量无效")
        body: dict[str, Any] = {"fileIds": list(file_ids)}
        if parent_id is not None:
            body["parentId"] = parent_id
        data = data_of(self._call("POST", f"{API_BASE}{path}", json_body=body))
        task_id = data.get("taskId")
        return str(task_id) if task_id else ""

    def copy_files(self, file_ids: list[str], parent_id: str) -> str:
        return self._batch("/userres/v1/file/copy_file", file_ids, parent_id)

    def move_files(self, file_ids: list[str], parent_id: str) -> str:
        return self._batch("/userres/v1/file/move_file", file_ids, parent_id)

    def delete_files(self, file_ids: list[str]) -> str:
        """普通 ID → 回收站；回收站里的 ID → 永久删除。"""
        return self._batch("/userres/v1/file/delete_file", file_ids)

    def delete_files_permanently(self, file_ids: list[str]) -> None:
        """两步永久删除：先移入回收站，再对**同一个 ID** 删一次（实测两步的 fileId 相同）。

        清理自己刚生成的垃圾时用它，避免在回收站里躺 100 天（`endpoints.md` §5.4/§5.5）。
        """
        if not file_ids:
            return
        for attempt in (1, 2):
            try:
                task_id = self.delete_files(list(file_ids))
            except GuangyapanNotFoundError:
                # 第一次就找不到 = 本来就没有；第二次 = 上一步已经把回收站条目删掉了
                if attempt == 1:
                    raise
                return
            self.wait_task(task_id)

    def task_status(self, task_id: str) -> int:
        data = data_of(
            self._call("POST", f"{API_BASE}/userres/v1/get_task_status", json_body={"taskId": task_id})
        )
        status = data.get("status")
        return int(status) if isinstance(status, (int, float)) else 0

    # ---------------------------------------------------------------- 回收站

    def clear_recycle_bin(self) -> None:
        """清空回收站。实测是**异步/最终一致**的：调用后约 1 分钟才真正清空，必须轮询确认。"""
        data_of(self._call("POST", f"{API_BASE}/userres/v1/file/clear_recycle_bin", json_body={}))

    def recycle_bin_total(self) -> int:
        _items, total = self.list_files(dir_type=4, page_size=1)
        return total

    def wait_task(self, task_id: str, *, timeout_seconds: float = 120.0) -> None:
        """轮询异步任务到终态；失败抛错。"""
        if not task_id:
            return
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            status = self.task_status(task_id)
            if status == 2:
                return
            if status == 3:
                raise GuangyapanRequestError("光鸭任务执行失败")
            time.sleep(0.5)
        raise GuangyapanRequestError("光鸭任务超时")

    # ---------------------------------------------------------------- 上传（秒传）

    def upload_token(self, *, name: str, size: int, parent_id: str, md5: str | None = None) -> dict[str, Any]:
        res: dict[str, Any] = {"fileSize": size}
        if md5:
            res["md5"] = md5
        body = {"capacity": 2, "name": name, "parentId": parent_id, "res": res}
        payload = self._call(
            "POST",
            f"{API_BASE}/userres/v2/get_res_center_token",
            json_body=body,
            allow_codes=(INSTANT_UPLOAD_CODE,),
        )
        if payload.get("code") == INSTANT_UPLOAD_CODE or "已完成" in str(payload.get("msg") or ""):
            data = payload.get("data")
            return {"instant": True, **(data if isinstance(data, dict) else {})}
        return {"instant": False, **data_of(payload)}

    def upload_info(self, task_id: str) -> dict[str, Any]:
        payload = self._call(
            "POST",
            f"{API_BASE}/userres/v1/file/get_info_by_task_id",
            json_body={"taskId": task_id},
            allow_codes=(UPLOAD_IN_PROGRESS_CODE,),
        )
        if payload.get("code") == UPLOAD_IN_PROGRESS_CODE:
            return {}
        data = payload.get("data")
        return data if isinstance(data, dict) else {}

    def wait_upload(self, task_id: str, *, timeout_seconds: float = 180.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            info = self.upload_info(task_id)
            if info.get("fileId"):
                return info
            time.sleep(1.0)
        raise GuangyapanRequestError("光鸭上传任务超时")

    # ---------------------------------------------------------------- 云添加

    def cloud_resolve(self, url: str) -> dict[str, Any]:
        """`resolve_res`：HTTP/HTTPS 直链、magnet、ed2k。

        返回里按资源形态给 `urlResInfo` / `btResInfo` / `emuleResInfo`。
        注意：HTTP 上的 `.torrent` 链接会被当**普通文件**（只有 `urlResInfo`），要走 BT
        必须先自己拿种子字节（见 `cloud_resolve_torrent`）或直接拿到 magnet。
        """
        return self._cloud_call("/cloudcollection/v1/resolve_res", {"url": url})

    def cloud_resolve_torrent(
        self, payload: bytes, *, filename: str = "upload.torrent"
    ) -> dict[str, Any]:
        """上传 `.torrent` 解析出 `btResInfo`（含 infoHash）。

        multipart 字段名**必须**是 `torrent`（用 `file` → HTTP 400 `torrent is required`）。
        """
        boundary = "----sakuramedia" + os.urandom(12).hex()
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="torrent"; filename="{filename}"\r\n'
            "Content-Type: application/x-bittorrent\r\n\r\n"
        ).encode() + payload + f"\r\n--{boundary}--\r\n".encode()
        return self._cloud_call(
            "/cloudcollection/v1/resolve_torrent",
            None,
            content=body,
            content_type=f"multipart/form-data; boundary={boundary}",
        )

    def cloud_create_task(
        self, url: str, *, parent_id: str = "", new_name: str | None = None
    ) -> dict[str, Any]:
        """建云添加任务；落地位置就是 `parent_id` 目录。

        `new_name` 会**整名覆盖**落地文件名（实测连扩展名一起换掉），默认不要传。
        """
        body: dict[str, Any] = {"url": url, "parentId": parent_id}
        if new_name:
            body["newName"] = new_name
        return self._cloud_call("/cloudcollection/v1/create_task", body)

    def cloud_tasks(
        self,
        *,
        task_ids: list[str] | None = None,
        page: int = 0,
        page_size: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """任务列表：传 `task_ids` 精确查（Web 轮询用这个），否则分页取。

        `status` 过滤实测不可靠（会漏掉失败任务），这里固定请求全部状态、由调用方过滤。
        """
        if task_ids is not None:
            body: dict[str, Any] = {"taskIds": list(task_ids)}
            if cursor:
                body["cursor"] = cursor
        else:
            body = {"page": page, "pageSize": page_size, "status": list(CLOUD_TASK_STATUSES)}
        return self._cloud_call("/cloudcollection/v1/list_task", body)

    def iter_cloud_tasks(
        self,
        *,
        task_ids: list[str] | None = None,
        page_size: int = 100,
    ) -> Iterator[dict[str, Any]]:
        """Yield every requested task page, following hasMore/cursor when supplied."""
        if task_ids is not None:
            for start in range(0, len(task_ids), page_size):
                chunk = list(dict.fromkeys(task_ids[start : start + page_size]))
                if not chunk:
                    continue
                cursor: str | None = None
                while True:
                    data = self.cloud_tasks(task_ids=chunk, cursor=cursor)
                    yield data
                    items = data.get("list")
                    items = items if isinstance(items, list) else []
                    next_cursor = data.get("cursor")
                    has_more = data.get("hasMore")
                    if not items or has_more is False or not isinstance(next_cursor, str):
                        break
                    cursor = next_cursor
            return

        page = 0
        while True:
            data = self.cloud_tasks(page=page, page_size=page_size)
            yield data
            items = data.get("list")
            items = items if isinstance(items, list) else []
            total = int(data.get("total") or 0)
            has_more = data.get("hasMore")
            if has_more is False or not items or (page + 1) * page_size >= total:
                break
            page += 1

    def cloud_delete_tasks(self, task_ids: list[str]) -> None:
        if not task_ids:
            return
        self._cloud_call("/cloudcollection/v2/delete_task", {"taskIds": list(task_ids)})

    def _cloud_call(
        self,
        path: str,
        body: dict[str, Any] | None,
        *,
        content: bytes | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        payload = self._call(
            "POST",
            f"{API_BASE}{path}",
            json_body=body,
            content=content,
            content_type=content_type,
            allow_codes=(CLOUD_URL_ERROR_CODE, CLOUD_BT_NOT_FOUND_CODE),
        )
        code = payload.get("code")
        if code == CLOUD_BT_NOT_FOUND_CODE:
            raise GuangyapanCloudResourceNotFound("光鸭的 BT 网络里找不到这个资源")
        if code == CLOUD_URL_ERROR_CODE:
            raise GuangyapanCloudUrlUnsupported("光鸭不接受这个链接")
        return data_of(payload)
