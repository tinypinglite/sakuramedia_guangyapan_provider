from __future__ import annotations

from typing import Any

import pytest

from sakuramedia_guangyapan_provider.exceptions import GuangyapanRequestError
from sakuramedia_guangyapan_provider.library_tree import load_library_tree

MEDIA_ROOT = "100"


class SnapshotClient:
    def __init__(self, entries: list[dict[str, Any]]) -> None:
        self.entries = entries
        self.calls: list[tuple[str, int, int]] = []

    def list_files(
        self,
        *,
        parent_id: str = "",
        page: int = 0,
        page_size: int = 100,
        **_kwargs,
    ):
        self.calls.append((parent_id, page, page_size))
        start = page * page_size
        return self.entries[start : start + page_size], len(self.entries)


def test_flat_snapshot_request_count_is_page_based_not_directory_based() -> None:
    entries: list[dict[str, Any]] = []
    for index in range(1500):
        directory_id = f"d{index}"
        entries.append(
            {
                "fileId": directory_id,
                "fileName": directory_id,
                "resType": 2,
                "parentId": MEDIA_ROOT,
            }
        )
        entries.append(
            {
                "fileId": f"f{index}",
                "fileName": f"f{index}.mp4",
                "resType": 1,
                "fileSize": 1,
                "parentId": directory_id,
            }
        )
    client = SnapshotClient(entries)

    tree = load_library_tree(client)
    files = [
        item for item in tree.subtree_items(MEDIA_ROOT) if item.get("resType") == 1
    ]

    assert len(files) == 1500
    assert len(client.calls) == 3
    assert {call[0] for call in client.calls} == {"*"}
    assert tree.relative_path("f0", MEDIA_ROOT) == "d0/f0.mp4"


def test_flat_snapshot_rejects_missing_parent_chain() -> None:
    client = SnapshotClient(
        [
            {
                "fileId": "f1",
                "fileName": "x.mp4",
                "resType": 1,
                "fileSize": 1,
                "parentId": "missing",
            }
        ]
    )
    tree = load_library_tree(client)

    with pytest.raises(GuangyapanRequestError):
        tuple(tree.subtree(MEDIA_ROOT))


def test_flat_snapshot_rejects_short_page() -> None:
    class ShortPageClient(SnapshotClient):
        def list_files(self, **kwargs):
            self.calls.append(("short", 0, 1000))
            return [], 1

    with pytest.raises(GuangyapanRequestError):
        load_library_tree(ShortPageClient([]))
