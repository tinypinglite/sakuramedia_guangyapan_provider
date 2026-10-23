"""媒体库配置准备：下载目录是可选项，不能因为没填就挡住保存。"""

from __future__ import annotations

from pathlib import Path

import pytest

from sakuramedia_guangyapan_provider import plugin as plugin_module
from sakuramedia_guangyapan_provider.plugin import GuangyapanMediaProviderBundle

from src.plugins.provider_protocol import LibraryHandle, ProviderOperationError

MEDIA_PATH = "/sakuramedia-media"
DOWNLOADS_PATH = "/sakuramedia-downloads"


class FakePrepareClient:
    def __init__(self, paths: dict[str, str]) -> None:
        self._paths = paths

    def __enter__(self) -> "FakePrepareClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def user_me(self) -> dict[str, str]:
        return {"sub": "asnK_test"}

    def resolve_path(self, path: str) -> str:
        return self._paths.get(path, "")

    def assets(self) -> dict[str, object]:
        return {"vipStatus": 2, "vipLeftTime": 86400 * 30}


@pytest.fixture
def bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GuangyapanMediaProviderBundle:
    paths = {MEDIA_PATH: "100", DOWNLOADS_PATH: "500"}
    monkeypatch.setattr(
        plugin_module, "GuangyapanClient", lambda _token: FakePrepareClient(paths)
    )
    return GuangyapanMediaProviderBundle(data_dir=tmp_path / "plugin-data")


def test_prepare_without_downloads_directory(bundle: GuangyapanMediaProviderBundle) -> None:
    prepared = bundle.prepare_library(
        submitted_config={"refresh_token": "gy.x", "media_root_path": MEDIA_PATH},
        previous=None,
    )

    assert prepared.provider_config["media_root_id"] == "100"
    assert prepared.provider_config["downloads_root_path"] == ""
    assert prepared.provider_config["downloads_root_id"] == ""


def test_prepare_resolves_downloads_directory(bundle: GuangyapanMediaProviderBundle) -> None:
    prepared = bundle.prepare_library(
        submitted_config={
            "refresh_token": "gy.x",
            "media_root_path": MEDIA_PATH,
            "downloads_root_path": DOWNLOADS_PATH + "/",
        },
        previous=None,
    )

    assert prepared.provider_config["downloads_root_path"] == DOWNLOADS_PATH
    assert prepared.provider_config["downloads_root_id"] == "500"


def test_prepare_rejects_missing_downloads_directory(bundle: GuangyapanMediaProviderBundle) -> None:
    with pytest.raises(ProviderOperationError) as excinfo:
        bundle.prepare_library(
            submitted_config={
                "refresh_token": "gy.x",
                "media_root_path": MEDIA_PATH,
                "downloads_root_path": "/nope",
            },
            previous=None,
        )

    assert excinfo.value.code == "invalid_config"
    assert "光鸭下载目录不存在" in excinfo.value.safe_message


def test_prepare_keeps_previous_downloads_directory(bundle: GuangyapanMediaProviderBundle) -> None:
    previous = LibraryHandle(
        library_id=7,
        provider_key="guangyapan",
        provider_config={
            "refresh_token": "gy.x",
            "media_root_path": MEDIA_PATH,
            "downloads_root_path": DOWNLOADS_PATH,
            "media_root_id": "100",
            "downloads_root_id": "500",
        },
        account_key="asnK_test",
    )

    prepared = bundle.prepare_library(
        submitted_config={"media_root_path": MEDIA_PATH},
        previous=previous,
    )

    assert prepared.provider_config["downloads_root_path"] == DOWNLOADS_PATH
    assert prepared.provider_config["downloads_root_id"] == "500"


def test_prepare_rejects_relative_downloads_directory(bundle: GuangyapanMediaProviderBundle) -> None:
    with pytest.raises(ProviderOperationError) as excinfo:
        bundle.prepare_library(
            submitted_config={
                "refresh_token": "gy.x",
                "media_root_path": MEDIA_PATH,
                "downloads_root_path": "downloads",
            },
            previous=None,
        )

    assert "光鸭下载目录必须是绝对路径" in excinfo.value.safe_message
