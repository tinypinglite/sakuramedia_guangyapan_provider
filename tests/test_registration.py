from __future__ import annotations

import json
from pathlib import Path

from sakuramedia_guangyapan_provider.plugin import PLUGIN_ID, register

from src.plugins import PluginContext
from src.plugins.extensions.media_provider import validate_media_provider_extension
from src.plugins.provider_protocol import MEDIA_PROVIDER_EXTENSION_KEY


def test_registration_declares_provider(tmp_path: Path) -> None:
    manifest = json.loads((Path(__file__).parents[1] / "manifest.json").read_text(encoding="utf-8"))
    registration = register(
        PluginContext(plugin_id=PLUGIN_ID, settings={}, data_dir=tmp_path / "plugin-data")
    )
    extension = registration.extensions[0]
    bundle = validate_media_provider_extension(plugin_id=PLUGIN_ID, extension=extension)

    assert registration.host_api_version == manifest["host_api_version"]
    assert extension.key == MEDIA_PROVIDER_EXTENSION_KEY
    assert bundle.provider_key == "guangyapan"
    assert bundle.playback_deliveries == ("redirect", "proxy")
    assert bundle.downloads is not None
    assert tuple(bundle.downloads.config_fields) == ()

    fields = {field.key: field for field in bundle.library_config_fields}
    assert fields["refresh_token"].input == "secret"
    assert fields["refresh_token"].required is True
    assert fields["refresh_token"].read_only is False
    assert fields["media_root_path"].input == "path"
    assert fields["media_root_path"].required is True
    assert fields["downloads_root_path"].input == "path"
    assert fields["downloads_root_path"].required is False
    for key in ("account_uid", "media_root_id", "downloads_root_id", "account_state"):
        assert fields[key].read_only is True
        assert fields[key].required is False

    assert [job.task_key for job in registration.jobs] == [
        "sakuramedia_guangyapan_cleanup_empty_media_dirs",
        "sakuramedia_guangyapan_clear_recycle_bin",
    ]
    for job in registration.jobs:
        # 宿主 loader 禁止插件自填来源与引用宿主 Scheduler 字段
        assert job.plugin_id is None
        assert job.cron_setting is None
        assert job.manual_only is True
        assert job.params_schema is not None
