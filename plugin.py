"""光鸭云盘 Provider 的注册与媒体库配置。

登录方式：用户粘贴一次 `refresh_token`（见 README 的控制台取法）。实测该 token 不轮换、
`access_token` 有效 2 小时由插件自动续期，所以用户只需要填这一次。
"""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

from loguru import logger

from src.plugins import PluginContext, PluginExtension, PluginRegistration
from src.plugins.provider_protocol import (
    MEDIA_PROVIDER_EXTENSION_KEY,
    ConfigField,
    JsonObject,
    LibraryHandle,
    PreparedLibrary,
    ProviderOperationError,
)

from src.scheduler.contracts import JobDefinition

from .exceptions import GuangyapanAuthError, GuangyapanError, safe_error_message
from .guangya import GuangyapanClient
from .maintenance import (
    MaintenanceConfirmParams,
    cleanup_empty_media_dirs,
    clear_recycle_bin,
)
from .offline import GuangyapanOfflineDownloadComponent
from .storage import PROVIDER_KEY, GuangyapanStorageProvider

PLUGIN_ID = "sakuramedia_guangyapan_provider"
DISPLAY_NAME = "光鸭云盘"
MANIFEST = json.loads(Path(__file__).with_name("manifest.json").read_text(encoding="utf-8"))
VERSION = MANIFEST["version"]
HOST_API_VERSION = MANIFEST["host_api_version"]

_TOKEN_HELP = (
    "在浏览器登录 www.guangyapan.com 后按 F12 打开控制台，粘贴 README 里的一行脚本，"
    "把输出的 refresh_token 整段填到这里。只需填写一次，插件会自动续期。"
)

LIBRARY_CONFIG_FIELDS = (
    ConfigField(
        key="refresh_token",
        label="光鸭 refresh_token",
        input="secret",
        required=True,
        description=_TOKEN_HELP,
        hint="gy. 开头",
    ),
    ConfigField(
        key="media_root_path",
        label="光鸭媒体目录",
        input="path",
        required=True,
        description=(
            "导入媒体的目标目录，必须是光鸭云盘里**已经存在**的绝对路径；"
            "插件不会替你新建目录，找不到会在保存时报错。"
        ),
        hint="例如 /sakuramedia-media",
    ),
    ConfigField(
        key="downloads_root_path",
        label="光鸭下载目录",
        input="path",
        required=False,
        description=(
            "云添加（离线下载）任务的落地目录，必须是光鸭云盘里**已经存在**的绝对路径；"
            "插件不会替你新建目录。留空表示不用下载任务，填写后才能在「下载器」里使用本库。"
        ),
        hint="例如 /sakuramedia-downloads",
    ),
    ConfigField(
        key="account_uid",
        label="光鸭账号 ID",
        input="text",
        required=False,
        read_only=True,
    ),
    ConfigField(
        key="media_root_id",
        label="解析后的媒体目录 ID",
        input="text",
        required=False,
        read_only=True,
    ),
    ConfigField(
        key="downloads_root_id",
        label="解析后的下载目录 ID",
        input="text",
        required=False,
        read_only=True,
    ),
    ConfigField(
        key="account_state",
        label="账号状态",
        input="text",
        required=False,
        read_only=True,
    ),
)


class GuangyapanMediaProviderBundle:
    provider_key = PROVIDER_KEY
    display_name = DISPLAY_NAME
    library_config_fields = LIBRARY_CONFIG_FIELDS
    playback_deliveries = ("redirect", "proxy")

    def __init__(self, *, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.downloads = GuangyapanOfflineDownloadComponent(data_dir=self.data_dir)

    def prepare_library(
        self, *, submitted_config: JsonObject, previous: LibraryHandle | None
    ) -> PreparedLibrary:
        if not isinstance(submitted_config, dict):
            raise _error("prepare_library", "invalid_config", "光鸭配置无效")
        unknown = set(submitted_config) - {field.key for field in LIBRARY_CONFIG_FIELDS}
        if unknown:
            raise _error("prepare_library", "invalid_config", "光鸭配置字段无效")

        previous_config = previous.provider_config if previous is not None else {}
        if not isinstance(previous_config, dict):
            previous_config = {}
        refresh_token = _text_field(submitted_config, previous_config, "refresh_token")
        if not refresh_token:
            raise _error("prepare_library", "invalid_config", "请填写光鸭 refresh_token")
        media_root_path = _normalise_directory_path(
            submitted_config.get("media_root_path")
            if submitted_config.get("media_root_path") is not None
            else previous_config.get("media_root_path")
        )
        # 下载目录可选：留空 = 不用下载任务（新建库时可以不填，之后再补）
        downloads_root_path = _normalise_optional_directory_path(
            _text_field(submitted_config, previous_config, "downloads_root_path"),
            label="光鸭下载目录",
        )

        with GuangyapanClient(refresh_token) as client:
            try:
                account = client.user_me()
                account_uid = str(account.get("sub") or "")
                media_root_id = client.resolve_path(media_root_path)
                downloads_root_id = (
                    client.resolve_path(downloads_root_path) if downloads_root_path else ""
                )
                assets = client.assets()
            except GuangyapanAuthError as exc:
                logger.warning(
                    "光鸭媒体库配置准备失败 reason={}", safe_error_message(exc)
                )
                raise _error(
                    "prepare_library", "authentication_failed", "光鸭 refresh_token 已失效，请重新获取"
                ) from exc
            except GuangyapanError as exc:
                logger.warning(
                    "光鸭媒体库配置准备失败 error_type={} reason={}",
                    type(exc).__name__,
                    safe_error_message(exc),
                )
                raise _error(
                    "prepare_library", "unavailable", "光鸭服务暂不可用，请稍后重试", retryable=True
                ) from exc
        if not media_root_id:
            raise _error(
                "prepare_library",
                "invalid_config",
                f"光鸭媒体目录不存在：{media_root_path}，请先在光鸭云盘里创建该目录",
            )
        if downloads_root_path and not downloads_root_id:
            raise _error(
                "prepare_library",
                "invalid_config",
                f"光鸭下载目录不存在：{downloads_root_path}，请先在光鸭云盘里创建该目录",
            )
        account_state = _describe_account(assets)
        logger.info(
            "光鸭媒体库配置准备完成 account_uid={} media_root_id={} downloads_root_id={} state={}",
            account_uid,
            media_root_id,
            downloads_root_id,
            account_state,
        )
        return PreparedLibrary(
            provider_config={
                "refresh_token": refresh_token,
                "media_root_path": media_root_path,
                "downloads_root_path": downloads_root_path,
                "account_uid": account_uid,
                "media_root_id": media_root_id,
                "downloads_root_id": downloads_root_id,
                "account_state": account_state,
            },
            account_key=account_uid or None,
        )

    def build_storage(self, *, library: LibraryHandle) -> GuangyapanStorageProvider:
        return GuangyapanStorageProvider(library=library, data_dir=self.data_dir)


def register(context: PluginContext) -> PluginRegistration:
    bundle = GuangyapanMediaProviderBundle(data_dir=context.data_dir)
    return PluginRegistration(
        plugin_id=PLUGIN_ID,
        display_name=DISPLAY_NAME,
        version=VERSION,
        host_api_version=HOST_API_VERSION,
        jobs=(
            JobDefinition(
                task_key="sakuramedia_guangyapan_cleanup_empty_media_dirs",
                log_name="光鸭-清理空目录",
                cli_name="guangyapan-cleanup-empty-media-dirs",
                cli_help="删除光鸭媒体库目录下的空子目录（进回收站，可恢复）",
                manual_only=True,
                params_schema=MaintenanceConfirmParams,
                handler=cleanup_empty_media_dirs,
            ),
            JobDefinition(
                task_key="sakuramedia_guangyapan_clear_recycle_bin",
                log_name="光鸭-清空回收站",
                cli_name="guangyapan-clear-recycle-bin",
                cli_help="清空光鸭账号回收站（异步生效，任务会轮询到真正清空）",
                manual_only=True,
                params_schema=MaintenanceConfirmParams,
                handler=clear_recycle_bin,
            ),
        ),
        extensions=(PluginExtension(key=MEDIA_PROVIDER_EXTENSION_KEY, data=bundle),),
    )


def _describe_account(assets: JsonObject) -> str:
    status = assets.get("vipStatus")
    left_seconds = assets.get("vipLeftTime")
    if status == 2:
        if isinstance(left_seconds, (int, float)) and left_seconds > 0:
            days = int(left_seconds // 86400)
            return f"会员（剩余约 {days} 天）"
        return "会员"
    if status == 3:
        return "会员已失效（光鸭三方应用能力可能不可用）"
    if status == 1:
        return "非会员（光鸭三方应用能力可能不可用）"
    return "未知"


def _normalise_directory_path(value: object, *, label: str = "光鸭媒体目录") -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise _error("prepare_library", "invalid_config", f"{label}无效")
    path = PurePosixPath(value.strip())
    if path.anchor != "/" or ".." in path.parts:
        raise _error("prepare_library", "invalid_config", f"{label}必须是绝对路径")
    return str(path)


def _normalise_optional_directory_path(value: object, *, label: str) -> str:
    """空值表示关闭这项能力（光鸭的「下载目录」留空 = 不用下载任务）。"""
    if value is None or (isinstance(value, str) and not value.strip()):
        return ""
    return _normalise_directory_path(value, label=label)


def _text_field(submitted_config: JsonObject, previous_config: dict, key: str) -> str:
    """未提交（缺失或 null）时沿用旧值；显式提交空串表示清空。"""
    value = submitted_config.get(key)
    if value is None:
        previous = previous_config.get(key)
        return previous if isinstance(previous, str) else ""
    if not isinstance(value, str):
        raise _error("prepare_library", "invalid_config", "光鸭配置字段无效")
    return value.strip()


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
