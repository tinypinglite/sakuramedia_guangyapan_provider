"""Atomic per-operation idempotency records shared by plugin instances."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any


class OperationJournal:
    """Store one operation per file so concurrent Provider instances cannot overwrite it."""

    def __init__(self, data_dir: Path, *, namespace: str) -> None:
        self._directory = Path(data_dir) / f"operations-{namespace}"
        self._legacy_path = Path(data_dir) / f"operations-{namespace}.json"
        self._lock = threading.RLock()
        self._migrate_legacy()

    def _path(self, operation_key: str) -> Path:
        digest = hashlib.sha256(operation_key.encode("utf-8")).hexdigest()
        return self._directory / f"{digest}.json"

    def _read(self, path: Path) -> tuple[str, dict[str, Any]] | None:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(raw, dict) or raw.get("version") != 1:
            return None
        operation_key = raw.get("operation_key")
        record = raw.get("record")
        if not isinstance(operation_key, str) or not operation_key or not isinstance(record, dict):
            return None
        return operation_key, dict(record)

    def _write(self, operation_key: str, record: dict[str, Any]) -> None:
        self._directory.mkdir(parents=True, exist_ok=True)
        destination = self._path(operation_key)
        temporary = destination.with_name(
            f".{destination.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        temporary.write_text(
            json.dumps(
                {"version": 1, "operation_key": operation_key, "record": record},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        temporary.replace(destination)

    def _migrate_legacy(self) -> None:
        with self._lock:
            try:
                raw = json.loads(self._legacy_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return
            records = raw.get("operations") if isinstance(raw, dict) else None
            if not isinstance(records, dict):
                return
            for operation_key, record in records.items():
                if isinstance(operation_key, str) and operation_key and isinstance(record, dict):
                    self._write(operation_key, dict(record))
            try:
                self._legacy_path.replace(self._legacy_path.with_suffix(".json.migrated"))
            except FileNotFoundError:
                pass

    def get(self, operation_key: str) -> dict[str, Any] | None:
        with self._lock:
            loaded = self._read(self._path(operation_key))
            if loaded is None or loaded[0] != operation_key:
                return None
            return dict(loaded[1])

    def items(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            if not self._directory.is_dir():
                return {}
            records: dict[str, dict[str, Any]] = {}
            for path in sorted(self._directory.glob("*.json")):
                loaded = self._read(path)
                if loaded is not None:
                    records[loaded[0]] = dict(loaded[1])
            return records

    def put(self, operation_key: str, record: dict[str, Any]) -> None:
        with self._lock:
            self._write(operation_key, dict(record))

    def remove(self, operation_key: str) -> None:
        with self._lock:
            try:
                self._path(operation_key).unlink()
            except FileNotFoundError:
                return


__all__ = ["OperationJournal"]
