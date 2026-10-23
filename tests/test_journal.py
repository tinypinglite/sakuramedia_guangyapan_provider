from __future__ import annotations

import json
from pathlib import Path

from sakuramedia_guangyapan_provider.journal import OperationJournal


def test_two_instances_do_not_overwrite_each_other(tmp_path: Path) -> None:
    first = OperationJournal(tmp_path, namespace="import-7")
    second = OperationJournal(tmp_path, namespace="import-7")

    first.put("job:1", {"file_id": "1"})
    second.put("job:2", {"file_id": "2"})

    reopened = OperationJournal(tmp_path, namespace="import-7")
    assert reopened.get("job:1") == {"file_id": "1"}
    assert reopened.get("job:2") == {"file_id": "2"}


def test_legacy_aggregate_journal_is_migrated(tmp_path: Path) -> None:
    legacy = tmp_path / "operations-import-7.json"
    legacy.write_text(
        json.dumps({"version": 1, "operations": {"job:1": {"file_id": "1"}}}),
        encoding="utf-8",
    )

    journal = OperationJournal(tmp_path, namespace="import-7")

    assert journal.get("job:1") == {"file_id": "1"}
    assert not legacy.exists()
    assert (tmp_path / "operations-import-7.json.migrated").is_file()


def test_journal_is_not_capped_at_500_records(tmp_path: Path) -> None:
    journal = OperationJournal(tmp_path, namespace="import-7")
    for index in range(600):
        journal.put(f"job:{index}", {"index": index})

    assert journal.get("job:0") == {"index": 0}
    assert journal.get("job:599") == {"index": 599}
    assert len(journal.items()) == 600
