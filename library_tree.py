"""Flat library snapshots shared by scans, maintenance, and offline checks."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator
from typing import Any

from .exceptions import GuangyapanRequestError

LIBRARY_SNAPSHOT_PAGE_SIZE = 1000


def _entry_id(item: dict[str, Any]) -> str:
    value = item.get("fileId")
    if not isinstance(value, str) or not value:
        raise GuangyapanRequestError("光鸭全库列表缺少文件 ID")
    return value


def _parent_id(item: dict[str, Any]) -> str:
    parent = item.get("parentId")
    if isinstance(parent, str):
        return parent
    full_parent_ids = item.get("fullParentIds")
    if isinstance(full_parent_ids, str):
        parts = [part for part in full_parent_ids.split("/") if part]
        return parts[-1] if parts else ""
    return ""


class LibraryTree:
    """A complete, validated snapshot of one account's normal file tree."""

    def __init__(self, items: tuple[dict[str, Any], ...]) -> None:
        self.items = items
        self._by_id: dict[str, dict[str, Any]] = {}
        self._parent: dict[str, str] = {}
        self._children: dict[str, list[str]] = defaultdict(list)
        for item in items:
            file_id = _entry_id(item)
            if file_id in self._by_id:
                raise GuangyapanRequestError("光鸭全库列表包含重复文件 ID")
            self._by_id[file_id] = item
            parent_id = _parent_id(item)
            self._parent[file_id] = parent_id
            if parent_id:
                self._children[parent_id].append(file_id)
        self._relative_paths: dict[tuple[str, str], str | None] = {}

    def relative_path(self, file_id: str, root_id: str) -> str | None:
        """Return the path below root_id, or None when file_id is not in that subtree."""
        key = (file_id, root_id)
        if key in self._relative_paths:
            return self._relative_paths[key]

        chain: list[str] = []
        current = file_id
        visiting: set[str] = set()
        while current:
            if current in visiting:
                raise GuangyapanRequestError("光鸭全库父目录链存在循环")
            visiting.add(current)
            item = self._by_id.get(current)
            if item is None:
                raise GuangyapanRequestError("光鸭全库父目录链不完整")
            if current == root_id:
                self._relative_paths[key] = None
                return None
            chain.append(str(item.get("fileName") or ""))
            parent = self._parent.get(current, "")
            if not parent:
                result = "/".join(reversed(chain)) if root_id == "" else None
                self._relative_paths[key] = result
                return result
            if parent == root_id:
                result = "/".join(reversed(chain))
                self._relative_paths[key] = result
                return result
            current = parent

        result = "/".join(reversed(chain)) if root_id == "" else None
        self._relative_paths[key] = result
        return result

    def subtree(self, root_id: str) -> Iterator[tuple[dict[str, Any], str]]:
        """Yield all descendants of root_id together with their relative paths."""
        for item in self.items:
            file_id = _entry_id(item)
            if file_id == root_id:
                continue
            relative = self.relative_path(file_id, root_id)
            if relative is not None:
                yield item, relative

    def subtree_items(self, root_id: str) -> tuple[dict[str, Any], ...]:
        return tuple(item for item, _relative in self.subtree(root_id))

    def empty_directory_ids(self, root_id: str) -> set[str]:
        """Return the highest empty directories below root_id, excluding root_id."""
        candidates, _skipped = self.empty_directory_summary(root_id)
        return candidates

    def empty_directory_summary(self, root_id: str) -> tuple[set[str], int]:
        """Return topmost empty directories and the nested empties they subsume."""
        subtree = self.subtree_items(root_id)
        directory_ids = {root_id}
        depths = {root_id: 0}
        has_file: dict[str, bool] = defaultdict(bool)
        children: dict[str, list[str]] = defaultdict(list)
        for item in subtree:
            file_id = _entry_id(item)
            if item.get("resType") == 2:
                directory_ids.add(file_id)
                children[self._parent.get(file_id, "")].append(file_id)
                depths[file_id] = len(
                    (self.relative_path(file_id, root_id) or "").split("/")
                )
            else:
                has_file[self._parent.get(file_id, "")] = True

        empty: dict[str, bool] = {}
        for directory_id in sorted(
            directory_ids, key=lambda value: depths.get(value, 0), reverse=True
        ):
            subtree_empty = not has_file.get(directory_id, False)
            for child_id in children.get(directory_id, []):
                if not empty.get(child_id, False):
                    subtree_empty = False
                    break
            empty[directory_id] = subtree_empty

        candidates: set[str] = set()
        all_empty: set[str] = set()
        for directory_id in directory_ids:
            if directory_id == root_id or not empty.get(directory_id, False):
                continue
            all_empty.add(directory_id)
            parent_id = self._parent.get(directory_id, "")
            if parent_id == root_id or (
                parent_id in directory_ids and not empty.get(parent_id, False)
            ):
                candidates.add(directory_id)
        return candidates, len(all_empty - candidates)

    def directory_count(self, root_id: str) -> int:
        return (
            sum(
                1
                for item, _relative in self.subtree(root_id)
                if item.get("resType") == 2
            )
            + 1
        )


def load_library_tree(
    client: Any,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
) -> LibraryTree:
    """Load a stable, fully paginated snapshot through parentId="*"."""
    items: list[dict[str, Any]] = []
    page = 0
    seen: set[str] = set()
    while True:
        batch, total = client.list_files(
            parent_id="*",
            page=page,
            page_size=LIBRARY_SNAPSHOT_PAGE_SIZE,
            order_by=0,
            sort_type=0,
        )
        if not isinstance(batch, list) or any(
            not isinstance(item, dict) for item in batch
        ):
            raise GuangyapanRequestError("光鸭全库列表分页结果无效")
        for item in batch:
            file_id = _entry_id(item)
            if file_id in seen:
                raise GuangyapanRequestError("光鸭全库列表分页结果重复")
            seen.add(file_id)
            items.append(item)
        consumed = page * LIBRARY_SNAPSHOT_PAGE_SIZE + len(batch)
        if progress_callback is not None:
            progress_callback(consumed, int(total or 0))
        if not batch:
            if consumed < int(total or 0):
                raise GuangyapanRequestError("光鸭全库列表分页不完整")
            break
        if consumed >= int(total or 0):
            break
        page += 1
    return LibraryTree(tuple(items))


__all__ = ["LIBRARY_SNAPSHOT_PAGE_SIZE", "LibraryTree", "load_library_tree"]
