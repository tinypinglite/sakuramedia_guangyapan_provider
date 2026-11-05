"""光鸭云盘离线下载（云添加）组件与 Provider。

依据光鸭私有接口的实测结论：

- 光鸭**原生支持 magnet**：`resolve_res` 返回 `btResInfo`，数据由平台自己的 BT 网络去拉；
  平台找不到资源时 `create_task` 直接 `code 352`（冷门种子 / 私有 tracker 会这样）。
- `.torrent` 文件走 multipart `resolve_torrent`（字段名必须是 `torrent`）；**HTTP 上的 `.torrent`
  链接不是 BT 入口**（`resolve_res` 会把它当普通文件），所以种子链接必须由插件自己拉字节。
- `list_task` **不返回落地目录** → 插件自建「每任务一个目录」并记 journal；
  `remote_id` 用 infoHash（与宿主侧资源 hash 记账一致），`completed_source_ref` 指向任务目录。
- `newName` 会整名覆盖（连扩展名一起），所以不传；落地文件名保持资源原名。
- `progress` 不可靠（完成态实测只有 23），一律按 `status` 判状态。
- 完成不等于立刻可读：落盘后 30–60 秒内读会 0 字节，所以「已完成」要等目录里条目就绪才上报，
  否则宿主会立刻导入一个 0 字节文件。
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit

import httpx
from loguru import logger

from src.plugins.provider_protocol import (
    ConfigField,
    DownloadClientHandle,
    DownloadSubmission,
    JsonObject,
    LibraryHandle,
    ProviderDiagnosticCheck,
    ProviderDiagnosticReport,
    ProviderOperationError,
    RemoteDownloadTask,
)

from .exceptions import (
    GuangyapanAuthError,
    GuangyapanCloudResourceNotFound,
    GuangyapanCloudUrlUnsupported,
    GuangyapanError,
    GuangyapanNotFoundError,
    GuangyapanRateLimitError,
    GuangyapanRequestError,
    safe_error_message,
)
from .guangya import GuangyapanClient
from .journal import OperationJournal
from .library_tree import LibraryTree, load_library_tree

PROVIDER_KEY = "guangyapan"
DOWNLOAD_REF_VERSION = 1
DOWNLOAD_SOURCE_KIND = "guangyapan_dir"

MAX_TORRENT_BYTES = 10 * 1024 * 1024
MAX_HTTP_REDIRECTS = 5
_MAX_DIR_NAME_LENGTH = 120

_BTIH_RE = re.compile(r"urn:btih:([A-Za-z0-9]+)", re.IGNORECASE)
_UNSAFE_NAME_CHARS = re.compile(r'[\\/:*?"<>|]')
# 0 排队中 / 1 进行中 / 2 已完成 / 3 已失败 / 4 已取消 / 5 部分已完成（Web 包硬编码，实测一致）
_STATUS_TO_STATE: dict[int, str] = {
    0: "queued",
    1: "downloading",
    2: "completed",
    3: "failed",
    4: "failed",
    5: "completed",
}


def _error(
    operation: str, code: str, message: str, *, retryable: bool = False
) -> ProviderOperationError:
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
    if isinstance(exc, GuangyapanCloudResourceNotFound):
        return _error(
            operation,
            "source_not_found",
            "光鸭在自己的 BT 网络里找不到这个资源（冷门种子或没有做种）",
        )
    if isinstance(exc, GuangyapanCloudUrlUnsupported):
        return _error(operation, "unsupported", "光鸭不接受这个下载链接")
    if isinstance(exc, GuangyapanNotFoundError):
        return _error(operation, "source_not_found", "光鸭文件或目录不存在")
    if isinstance(exc, GuangyapanRateLimitError):
        return _error(operation, "unavailable", "光鸭接口限流，请稍后重试", retryable=True)
    return _error(operation, "unavailable", "光鸭服务暂不可用", retryable=True)


def _require_config(library: LibraryHandle, *, operation: str) -> tuple[str, str]:
    config = library.provider_config if isinstance(library.provider_config, dict) else {}
    refresh_token = config.get("refresh_token")
    downloads_root_id = config.get("downloads_root_id")
    if not isinstance(refresh_token, str) or not refresh_token:
        raise _error(operation, "invalid_config", "光鸭媒体库缺少 refresh_token")
    if not isinstance(downloads_root_id, str) or not downloads_root_id:
        raise _error(operation, "invalid_config", "请先在媒体库配置里填写「光鸭下载目录」")
    return refresh_token, downloads_root_id


def _canonical_btih(value: str) -> str:
    """BTIH 归一化成 40 位小写 hex；magnet 里的 base32 形态也接受。"""
    text = value.strip()
    if len(text) == 40:
        try:
            bytes.fromhex(text)
        except ValueError as exc:
            raise ValueError("invalid btih") from exc
        return text.lower()
    if len(text) != 32 or not re.fullmatch(r"[A-Za-z2-7]+", text):
        raise ValueError("invalid btih")
    try:
        return base64.b32decode(text.upper() + "=" * ((8 - len(text) % 8) % 8)).hex()
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid btih") from exc


def _info_hash_of_magnet(value: str) -> str:
    match = _BTIH_RE.search(unquote(value))
    if match is None:
        raise ValueError("invalid magnet")
    return _canonical_btih(match.group(1))


def _info_hash_of_source(value: object) -> str:
    """Resolve a task `res` value to the journal key used by this provider."""
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if text.lower().startswith("magnet:"):
        try:
            return _info_hash_of_magnet(text)
        except ValueError:
            return ""
    if text.lower().startswith("ed2k://"):
        parts = text.split("|")
        if len(parts) >= 6:
            try:
                size = int(parts[3])
            except ValueError:
                return ""
            info_hash = parts[4].strip().lower()
            if re.fullmatch(r"[0-9a-f]{32}", info_hash) and size > 0:
                return f"ed2k:{info_hash}:{size}"
        return ""
    if text.lower().startswith(("http://", "https://")):
        try:
            return "http:" + _http_url_key(text)
        except ValueError:
            return ""
    return ""


def _http_url_key(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("invalid http url")
    host = parsed.hostname.lower() if parsed.hostname else ""
    if not host:
        raise ValueError("invalid http url")
    port = parsed.port
    if port is None or (parsed.scheme.lower() == "http" and port == 80) or (
        parsed.scheme.lower() == "https" and port == 443
    ):
        netloc = host
    else:
        netloc = f"{host}:{port}"
    normalized = urlunsplit(
        (parsed.scheme.lower(), netloc, parsed.path or "/", parsed.query, "")
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


def _ed2k_file_info(value: str) -> tuple[str, int]:
    parts = value.split("|")
    if len(parts) >= 6:
        try:
            return unquote(parts[2].strip()), int(parts[3])
        except ValueError:
            pass
    return "", 0


def _http_file_info(value: str) -> tuple[str, int]:
    name = unquote(urlsplit(value).path.rsplit("/", 1)[-1])
    return name, 0


def _magnet_of_info_hash(info_hash: str) -> str:
    return f"magnet:?xt=urn:btih:{info_hash}"


def _safe_dir_name(value: object) -> str:
    text = _UNSAFE_NAME_CHARS.sub(" ", str(value or "").replace("\x00", " "))
    return " ".join(text.split()).strip(" .")[:_MAX_DIR_NAME_LENGTH]


def _fetch_torrent(source_uri: str) -> tuple[str, Any]:
    """抓取种子链接：≤10 MiB、≤5 跳重定向（与宿主 `resource_hash` 同规则）。

    返回 `("magnet", magnet)` 或 `("torrent", bytes)`。
    """
    try:
        with httpx.Client(timeout=120.0, follow_redirects=False, trust_env=False) as client:
            request_url = source_uri
            for _ in range(MAX_HTTP_REDIRECTS + 1):
                parsed = urlsplit(request_url)
                if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
                    raise _error(
                        "submit_download", "unsupported", "光鸭下载只支持磁力链与 http/https 种子链接"
                    )
                response = client.get(request_url)
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        raise _error("submit_download", "unsupported", "种子链接重定向地址无效")
                    request_url = urljoin(request_url, location)
                    if request_url.lower().startswith("magnet:"):
                        return "magnet", request_url
                    continue
                if response.status_code == 404:
                    raise _error("submit_download", "source_not_found", "种子文件不存在")
                if response.status_code >= 500:
                    raise _error(
                        "submit_download", "unavailable", "种子文件服务暂时不可用", retryable=True
                    )
                if not 200 <= response.status_code < 300:
                    raise _error("submit_download", "unsupported", "种子文件地址不受支持")
                payload = response.content
                if len(payload) > MAX_TORRENT_BYTES:
                    raise _error("submit_download", "unsupported", "种子文件超过大小限制")
                return "torrent", payload
    except httpx.HTTPError as exc:
        raise _error("submit_download", "unavailable", "种子文件下载失败", retryable=True) from exc
    raise _error("submit_download", "unsupported", "种子链接重定向次数过多")


def _progress_of(item: JsonObject) -> float:
    value = item.get("progress")
    if not isinstance(value, (int, float)):
        return 0.0
    return min(1.0, max(0.0, float(value) / 100.0))


class GuangyapanOfflineDownloadComponent:
    """下载组件边界：没有额外配置字段（配置都在媒体库上）。"""

    config_fields: tuple[ConfigField, ...] = ()
    supported_source_kinds = frozenset({"magnet", "torrent", "ed2k", "http"})

    def __init__(
        self,
        *,
        data_dir: Path,
        client_factory: Callable[[str], GuangyapanClient] = GuangyapanClient,
    ) -> None:
        self._data_dir = Path(data_dir)
        self._client_factory = client_factory

    def prepare_client(
        self,
        *,
        submitted_config: dict[str, object],
        library: LibraryHandle,
        previous: DownloadClientHandle | None,
    ) -> dict[str, object]:
        if submitted_config:
            raise _error("prepare_download", "invalid_config", "光鸭离线下载无需额外配置")
        _require_config(library, operation="prepare_download")
        return {}

    def test_client(
        self,
        *,
        submitted_config: dict[str, object],
        library: LibraryHandle,
    ) -> ProviderDiagnosticReport:
        if submitted_config:
            raise _error("test_download", "invalid_config", "光鸭离线下载无需额外配置")
        refresh_token, downloads_root_id = _require_config(library, operation="test_download")
        try:
            with self._client_factory(refresh_token) as client:
                assets = client.assets()
                client.list_files(parent_id=downloads_root_id, page_size=1)
        except GuangyapanError as exc:
            raise _cloud_error("test_download", exc) from exc
        member = "会员" if assets.get("vipStatus") == 2 else "非会员/未知"
        return ProviderDiagnosticReport(
            status="ok",
            checks=(
                ProviderDiagnosticCheck(
                    key="guangyapan_cloud",
                    status="ok",
                    code="guangyapan_cloud_ok",
                    message=f"光鸭登录与下载目录可用（{member}）",
                ),
            ),
        )

    def build(self, *, client: DownloadClientHandle) -> "GuangyapanOfflineDownloadProvider":
        refresh_token, downloads_root_id = _require_config(
            client.library, operation="build_download"
        )
        return GuangyapanOfflineDownloadProvider(
            refresh_token=refresh_token,
            downloads_root_id=downloads_root_id,
            library=client.library,
            data_dir=self._data_dir,
            client_factory=self._client_factory,
        )


class GuangyapanOfflineDownloadProvider:
    def __init__(
        self,
        *,
        refresh_token: str,
        downloads_root_id: str,
        library: LibraryHandle,
        data_dir: Path,
        client_factory: Callable[[str], GuangyapanClient] = GuangyapanClient,
    ) -> None:
        self.library = library
        self._refresh_token = refresh_token
        self._downloads_root_id = downloads_root_id
        self._client_factory = client_factory
        self._journal = OperationJournal(
            Path(data_dir), namespace=f"downloads-{library.library_id}"
        )

    # ---------------------------------------------------------------- 提交

    def submit(self, *, submission: DownloadSubmission) -> RemoteDownloadTask:
        source_uri = (submission.source_uri or "").strip()
        if not source_uri:
            raise _error("submit_download", "invalid_config", "下载链接不能为空")
        source_kind = submission.source_kind
        if source_kind is None:
            source_kind = (
                "magnet"
                if source_uri.lower().startswith("magnet:")
                else "ed2k"
                if source_uri.lower().startswith("ed2k://")
                else "http"
                if source_uri.lower().startswith(("http://", "https://"))
                else "torrent"
            )
        display_name = _safe_dir_name(submission.display_name)

        with self._client_factory(self._refresh_token) as client:
            submit_uri, info_hash, file_name, file_size = self._resolve_source(
                client, source_uri, source_kind
            )
            resource_key = (submission.resource_key or "").strip()
            if not resource_key:
                resource_key = (
                    f"bt:{info_hash}"
                    if info_hash
                    else _info_hash_of_source(source_uri)
                )
            journal_key = info_hash or resource_key
            if not journal_key:
                raise _error(
                    "submit_download", "invalid_config", "无法识别下载资源标识"
                )
            recorded = self._journal.get(journal_key)
            if recorded is not None:
                # 幂等：宿主重试同一次提交时不能再建一个任务
                logger.info(
                    "光鸭下载任务复用已有记录 library_id={} source_kind={} key={} task_id={}",
                    self.library.library_id,
                    source_kind,
                    journal_key,
                    recorded.get("task_id"),
                )
                return self._task_of(recorded, journal_key, state="queued", progress=0.0)

            directory_name = display_name or _safe_dir_name(file_name) or journal_key
            directory_id = self._ensure_task_directory(client, directory_name)
            created = client.cloud_create_task(submit_uri, parent_id=directory_id)
            task_id = str(created.get("taskId") or "")
            if not task_id:
                raise _error("submit_download", "unavailable", "光鸭未返回云添加任务 ID", retryable=True)
            remote_id = info_hash or task_id
            record: dict[str, Any] = {
                "task_id": task_id,
                "dir_id": directory_id,
                "info_hash": info_hash,
                "resource_key": resource_key,
                "source_kind": source_kind,
                "remote_id": remote_id,
                "dir_name": directory_name,
                "file_name": file_name,
                "file_size": file_size,
            }
            self._journal.put(journal_key, record)

        logger.info(
            "光鸭下载任务已提交 library_id={} source_kind={} key={} task_id={} dir_id={} name={}",
            self.library.library_id,
            source_kind,
            journal_key,
            task_id,
            directory_id,
            record["dir_name"],
        )
        return self._task_of(record, journal_key, state="queued", progress=0.0)

    def _resolve_source(
        self, client: GuangyapanClient, source_uri: str, source_kind: str
    ) -> tuple[str, str | None, str, int]:
        """把 source_uri 归一成提交 URI、BT hash 和文件信息。"""
        try:
            if source_kind not in {"magnet", "torrent", "ed2k", "http"}:
                raise ValueError("invalid source kind")
            if source_kind in {"magnet", "torrent"} and source_uri.lower().startswith(
                "magnet:"
            ):
                info_hash = _info_hash_of_magnet(source_uri)
                return self._from_bt_res_info(client, _magnet_of_info_hash(info_hash), info_hash)
            if source_kind in {"ed2k", "http"}:
                data = client.cloud_resolve(source_uri)
                key = "emuleResInfo" if source_kind == "ed2k" else "urlResInfo"
                name, size = (
                    _ed2k_file_info(source_uri)
                    if source_kind == "ed2k"
                    else _http_file_info(source_uri)
                )
                info = data.get(key)
                if not isinstance(info, dict):
                    info = {}
                name = str(info.get("fileName") or info.get("name") or name)
                raw_size = info.get("fileSize") or info.get("size") or size
                file_size = int(raw_size) if isinstance(raw_size, (int, float)) else 0
                return source_uri, None, name, file_size
            kind, payload = _fetch_torrent(source_uri)
            if kind == "magnet":
                info_hash = _info_hash_of_magnet(str(payload))
                return self._from_bt_res_info(client, _magnet_of_info_hash(info_hash), info_hash)
            data = client.cloud_resolve_torrent(payload)
            magnet, info_hash, file_name, file_size = self._bt_tuple(data)
            return magnet, info_hash, file_name, file_size
        except ValueError as exc:
            raise _error("submit_download", "invalid_config", "下载链接无效") from exc
        except GuangyapanError as exc:
            logger.warning(
                "光鸭下载源解析失败 library_id={} error_type={} reason={}",
                self.library.library_id,
                type(exc).__name__,
                safe_error_message(exc),
            )
            raise _cloud_error("submit_download", exc) from exc

    def _from_bt_res_info(
        self, client: GuangyapanClient, magnet: str, info_hash: str
    ) -> tuple[str, str, str, int]:
        """magnet 形态：先让平台确认资源可用（找不到会抛 352），顺便拿文件名/大小。"""
        data = client.cloud_resolve(magnet)
        _magnet, _hash, file_name, file_size = self._bt_tuple(data, fallback_hash=info_hash)
        return magnet, info_hash, file_name, file_size

    @staticmethod
    def _bt_tuple(data: dict[str, Any], *, fallback_hash: str = "") -> tuple[str, str, str, int]:
        info = data.get("btResInfo")
        info = info if isinstance(info, dict) else {}
        info_hash = str(info.get("infoHash") or fallback_hash)
        if not info_hash:
            raise GuangyapanRequestError("光鸭未返回 BT 资源 hash")
        try:
            info_hash = _canonical_btih(info_hash)
        except ValueError as exc:
            raise GuangyapanRequestError("光鸭返回的 BT 资源 hash 无效") from exc
        file_size = info.get("fileSize")
        return (
            _magnet_of_info_hash(info_hash),
            info_hash,
            str(info.get("fileName") or ""),
            int(file_size) if isinstance(file_size, (int, float)) else 0,
        )

    def _ensure_task_directory(self, client: GuangyapanClient, name: str) -> str:
        data = client.create_dir(name, self._downloads_root_id, fail_if_name_exist=True)
        directory_id = str(data.get("fileId") or "")
        if not directory_id:
            raise _error("submit_download", "unavailable", "光鸭未返回下载目录 ID", retryable=True)
        return directory_id

    # ---------------------------------------------------------------- 列表

    def list_tasks(self) -> tuple[RemoteDownloadTask, ...]:
        records = self._journal.items()
        if not records:
            return ()
        by_task_id = {
            str(record.get("task_id") or ""): (key, record)
            for key, record in records.items()
            if record.get("task_id")
        }
        try:
            with self._client_factory(self._refresh_token) as client:
                items: list[JsonObject] = []
                task_ids = [task_id for task_id in by_task_id if task_id]
                if task_ids:
                    for data in client.iter_cloud_tasks(task_ids=task_ids):
                        batch = data.get("list")
                        if isinstance(batch, list):
                            items.extend(item for item in batch if isinstance(item, dict))
                matched: list[tuple[str, JsonObject, JsonObject]] = []
                for item in items:
                    entry = by_task_id.get(str(item.get("taskId") or ""))
                    if entry is None:
                        info_hash = _info_hash_of_source(item.get("res"))
                        if info_hash and info_hash in records:
                            entry = (info_hash, records[info_hash])
                    if entry is None:
                        # 用户在光鸭 App 里自己加的云添加任务，不归我们管
                        continue
                    matched.append((entry[0], entry[1], item))
                tree: LibraryTree | None = None
                if any(int(item.get("status") or 0) == 2 for _key, _record, item in matched):
                    tree = load_library_tree(client)
                return tuple(
                    self._remote_task(client, key, record, item, tree=tree)
                    for key, record, item in matched
                )
        except GuangyapanError as exc:
            logger.warning(
                "光鸭下载任务同步失败 library_id={} error_type={} reason={}",
                self.library.library_id,
                type(exc).__name__,
                safe_error_message(exc),
            )
            raise _cloud_error("list_downloads", exc) from exc

    def _remote_task(
        self,
        client: GuangyapanClient,
        journal_key: str,
        record: JsonObject,
        item: JsonObject,
        *,
        tree: LibraryTree | None,
    ) -> RemoteDownloadTask:
        try:
            status = int(item.get("status") or 0)
        except (TypeError, ValueError):
            status = 0
        state = _STATUS_TO_STATE.get(status, "failed")
        completed_ref: JsonObject | None = None
        if state == "completed":
            completed_ref = self._completed_ref(client, record, tree=tree)
            if completed_ref is None:
                # 落盘后 30–60 秒内读会 0 字节：先按进行中上报，下一轮再转完成，
                # 否则宿主会立刻导入一个 0 字节文件（endpoints.md §0.3）。
                logger.info(
                    "光鸭下载任务已到终态但产物未就绪，延后上报完成 library_id={} key={}",
                    self.library.library_id,
                    journal_key,
                )
                state = "downloading"
        name = str(record.get("dir_name") or item.get("fileName") or journal_key)
        return RemoteDownloadTask(
            remote_id=str(record.get("remote_id") or journal_key),
            name=name,
            state=state,  # type: ignore[arg-type]
            progress=_progress_of(item),
            completed_source_ref=completed_ref,
        )

    def _completed_ref(
        self,
        client: GuangyapanClient,
        record: JsonObject,
        *,
        tree: LibraryTree | None,
    ) -> JsonObject | None:
        directory_id = record.get("dir_id")
        if not isinstance(directory_id, str) or not directory_id:
            return None
        if not self._landed_ready(client, directory_id, tree=tree):
            return None
        return {
            "v": DOWNLOAD_REF_VERSION,
            "kind": DOWNLOAD_SOURCE_KIND,
            "file_id": directory_id,
            "name": str(record.get("dir_name") or ""),
        }

    @staticmethod
    def _landed_ready(
        client: GuangyapanClient,
        directory_id: str,
        *,
        tree: LibraryTree | None,
    ) -> bool:
        snapshot = tree or load_library_tree(client)
        files = [
            item
            for item in snapshot.subtree_items(directory_id)
            if item.get("resType") != 2
        ]
        if not files:
            return False
        return all(int(item.get("fileSize") or 0) > 0 for item in files)

    def _task_of(
        self,
        record: JsonObject,
        journal_key: str,
        *,
        state: str,
        progress: float,
    ) -> RemoteDownloadTask:
        return RemoteDownloadTask(
            remote_id=str(record.get("remote_id") or journal_key),
            name=str(record.get("dir_name") or journal_key),
            state=state,  # type: ignore[arg-type]
            progress=progress,
            completed_source_ref=None,
        )

    # ---------------------------------------------------------------- 删除

    def delete_task(self, *, remote_id: str, delete_files: bool) -> None:
        remote_id = str(remote_id or "").strip()
        if not remote_id:
            raise _error("delete_download", "invalid_config", "光鸭下载任务标识无效")
        journal_key, record = self._record_for_remote_id(remote_id)
        if record is None and not journal_key:
            raise _error("delete_download", "invalid_config", "光鸭下载任务标识无效")
        task_id = str(record.get("task_id") or "") if record else ""
        try:
            with self._client_factory(self._refresh_token) as client:
                if not task_id:
                    task_id = self._find_task_id(client, journal_key or remote_id)
                if task_id:
                    try:
                        client.cloud_delete_tasks([task_id])
                    except GuangyapanNotFoundError:
                        logger.info("光鸭下载任务记录已不存在 remote_id={}", remote_id)
                if delete_files and record:
                    directory_id = record.get("dir_id")
                    if isinstance(directory_id, str) and directory_id:
                        try:
                            client.delete_files_permanently([directory_id])
                        except GuangyapanError as exc:
                            # 尽力而为：任务记录已删成功，目录清理失败只告警，不阻塞宿主台账
                            logger.warning(
                                "光鸭下载目录清理失败 library_id={} key={} error_type={} reason={}",
                                self.library.library_id,
                                journal_key,
                                type(exc).__name__,
                                safe_error_message(exc),
                            )
        except GuangyapanError as exc:
            logger.warning(
                "光鸭下载任务删除失败 library_id={} key={} error_type={} reason={}",
                self.library.library_id,
                journal_key or remote_id,
                type(exc).__name__,
                safe_error_message(exc),
            )
            raise _cloud_error("delete_download", exc) from exc
        if journal_key:
            self._journal.remove(journal_key)

    def _record_for_remote_id(
        self, remote_id: str
    ) -> tuple[str, JsonObject | None]:
        record = self._journal.get(remote_id)
        if record is not None:
            return remote_id, record
        for key, candidate in self._journal.items().items():
            if str(candidate.get("task_id") or "") == remote_id:
                return key, candidate
            if str(candidate.get("resource_key") or "") == remote_id:
                return key, candidate
        try:
            legacy_key = _canonical_btih(remote_id)
        except ValueError:
            return "", None
        return legacy_key, self._journal.get(legacy_key)

    def _find_task_id(self, client: GuangyapanClient, journal_key: str) -> str:
        """journal 丢了（插件 data 被清）时按任务列表里的来源兜底找。"""
        try:
            pages = tuple(client.iter_cloud_tasks())
        except GuangyapanError:
            return ""
        for data in pages:
            items = data.get("list") if isinstance(data.get("list"), list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if _info_hash_of_source(item.get("res")) == journal_key:
                    return str(item.get("taskId") or "")
        return ""
