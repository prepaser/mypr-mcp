import json
import os
import threading
from pathlib import Path

import pytest

from mypr_mcp.history import History


def test_batch_order_and_filtered_cursor_skip_unrelated_events(tmp_path):
    history = History(tmp_path)
    try:
        events = history.append_many(
            "execution",
            "output",
            [
                {
                    "id": str(index),
                    "client_id": "alice" if index % 3 == 0 else "bob",
                    "text": str(index),
                }
                for index in range(20)
            ],
        )
        assert [event["data"]["text"] for event in events] == [str(index) for index in range(20)]
        cursor, found = 0, []
        while cursor < events[-1]["seq"]:
            page = history.logs(cursor=cursor, client_id="alice", limit=2)
            assert page["cursor"] > cursor
            cursor = page["cursor"]
            found.extend(page["events"])
        assert [event["data"]["text"] for event in found] == [
            str(index) for index in range(0, 20, 3)
        ]
        assert history.logs(cursor=cursor, client_id="alice")["events"] == []
    finally:
        history.close()


def test_legacy_database_gains_request_index_without_losing_records(tmp_path):
    import json
    import sqlite3

    root = tmp_path / ".mypr"
    root.mkdir()
    old = {"id": "legacy", "client": "owner", "request_id": "", "code": "42", "state": "succeeded"}
    with sqlite3.connect(root / "history.sqlite3") as database:
        database.execute(
            "CREATE TABLE entities (entity_seq INTEGER PRIMARY KEY AUTOINCREMENT, "
            "id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, created REAL NOT NULL, "
            "updated REAL NOT NULL, data TEXT NOT NULL)"
        )
        database.execute(
            "INSERT INTO entities(id,kind,created,updated,data) VALUES(?,?,?,?,?)",
            ("legacy", "execution", 1, 1, json.dumps(old)),
        )
    history = History(tmp_path)
    try:
        assert history.find_request("owner", "") == old
        assert history.get("legacy") == old
        assert history.find_request("other", "") is None
        with sqlite3.connect(root / "history.sqlite3") as database:
            assert database.execute(
                "SELECT 1 FROM sqlite_master WHERE name='execution_request_idx'"
            ).fetchone()
    finally:
        history.close()


def test_record_merge_summary_and_get(tmp_path: Path):
    history = History(tmp_path)
    try:
        history.record(
            "execution",
            {"id": "a", "client_id": "c", "state": "running", "code": "x", "output": "y"},
        )
        history.record("execution", {"id": "a", "state": "succeeded", "result": 1})
        assert history.get("a") == {
            "id": "a",
            "client_id": "c",
            "state": "succeeded",
            "code": "x",
            "output": "y",
            "result": 1,
            "kind": "execution",
        }
        summary = history.list()["items"]
        assert summary == [
            {"id": "a", "client_id": "c", "state": "succeeded", "result": 1, "kind": "execution"}
        ]
    finally:
        history.close()


def test_record_can_keep_same_public_id_across_generations(tmp_path: Path):
    history = History(tmp_path)
    try:
        history.record("python", {"id": "legacy", "generation": "zero", "state": "succeeded"})
        history.record(
            "python",
            {"id": "repeatable", "generation": "one", "state": "succeeded"},
            event="succeeded",
            entity_id="python:one:repeatable",
        )
        history.record(
            "python",
            {"id": "repeatable", "generation": "two", "state": "running"},
            event="running",
            entity_id="python:two:repeatable",
        )
        history.record(
            "python",
            {"id": "legacy", "generation": "three", "state": "running"},
            entity_id="python:three:legacy",
        )

        assert history.get("repeatable")["generation"] == "two"
        assert history.get("python:one:repeatable")["generation"] == "one"
        assert history.get("legacy")["generation"] == "three"
        assert history.get("python:zero:legacy") == {
            "id": "legacy",
            "generation": "zero",
            "state": "succeeded",
            "kind": "python",
            "history_id": "python:zero:legacy",
        }
        assert {
            item["history_id"] for item in history.list()["items"] if item["id"] == "legacy"
        } == {"python:zero:legacy", "python:three:legacy"}
        assert len(history.list()["items"]) == 4
        assert [event["data"] for event in history.logs()["events"]] == [
            {"history_id": "python:one:repeatable", "generation": "one"},
            {"history_id": "python:two:repeatable", "generation": "two"},
        ]
    finally:
        history.close()


def test_recover_updates_generation_qualified_python_record(tmp_path: Path):
    history = History(tmp_path)
    try:
        history.record(
            "python",
            {"id": "repeatable", "generation": "one", "state": "running"},
            entity_id="python:one:repeatable",
        )

        assert history.recover() == 1
        assert history.get("python:one:repeatable")["state"] == "lost"
        assert history.get("repeatable")["state"] == "lost"
        assert history.recover() == 0
        with history._lock:
            count = history._db.execute(
                "SELECT COUNT(*) FROM entities WHERE json_extract(data, '$.id') = ?",
                ("repeatable",),
            ).fetchone()[0]
        assert count == 1
    finally:
        history.close()


def test_logs_cursor_filter_and_recovery(tmp_path: Path):
    history = History(tmp_path)
    try:
        history.record("python", {"id": "a", "client_id": "one", "state": "running"}, "started")
        history.record("python", {"id": "b", "client_id": "two", "state": "succeeded"}, "done")
        history.append("python", "output", {"id": "a", "client_id": "one", "text": "hello"})
        tail = history.logs(client_id="one")
        assert [event["event"] for event in tail["events"]] == ["started", "output"]
        assert tail["events"] == sorted(tail["events"], key=lambda event: event["seq"])
        history.record("shell", {"id": "c", "client_id": "one", "state": "running"}, "running")
        forward = history.logs(cursor=tail["cursor"], client_id="one")
        assert [event["id"] for event in forward["events"]] == ["c"]
        assert history.recover() == 2
        assert history.get("a")["state"] == "lost"
    finally:
        history.close()


def test_list_and_log_validation(tmp_path: Path):
    history = History(tmp_path)
    try:
        with pytest.raises(ValueError):
            history.list(limit=0)
        with pytest.raises(ValueError):
            history.logs(cursor="bad")
        with pytest.raises(ValueError):
            history.record("unknown", {"id": "x"})
    finally:
        history.close()


def test_empty_log_cursor_and_filtered_pages_survive_restart(tmp_path):
    history = History(tmp_path)
    cursor = history.logs(client_id="a")["cursor"]
    assert cursor == 0
    for index in range(5):
        history.append("connection", "connected", {"client_id": "a", "id": str(index)})
        history.append("connection", "connected", {"client_id": "b", "id": str(index)})
    page = history.logs(cursor=cursor, client_id="a", limit=2)
    assert [event["id"] for event in page["events"]] == ["0", "1"]
    history.close()
    history = History(tmp_path)
    try:
        next_page = history.logs(cursor=page["cursor"], client_id="a", limit=2)
        assert [event["id"] for event in next_page["events"]] == ["2", "3"]
        last = history.logs(cursor=next_page["cursor"], client_id="a", limit=2)
        assert [event["id"] for event in last["events"]] == ["4"]
        assert history.logs(cursor=last["cursor"], client_id="a")["events"] == []
    finally:
        history.close()


def test_storage_gc_normalizes_artifacts_and_scan_metadata(tmp_path):
    root = tmp_path / ".mypr"
    scan_dir = root / "scans"
    scan_dir.mkdir(parents=True)
    result = scan_dir / "request.jsonl"
    summary = scan_dir / "request.summary.json"
    artifact = scan_dir / "request.xml"
    for path in (result, summary, artifact):
        path.write_text("x", encoding="utf-8")
    (scan_dir / "job.json").write_text(
        json.dumps(
            {
                "id": "job",
                "result_path": str(result),
                "summary_path": str(summary),
                "artifact": str(artifact),
            }
        ),
        encoding="utf-8",
    )
    artifact_dir = root / "artifacts" / "exec"
    artifact_dir.mkdir(parents=True)
    owned_artifact = artifact_dir / "image.png"
    owned_artifact.write_bytes(b"png")
    history = History(tmp_path)
    try:
        history.record(
            "scan",
            {"id": "job", "kind": "scan", "state": "succeeded", "client_id": "client"},
        )
        history.record(
            "execution",
            {
                "id": "exec",
                "exec_id": "exec",
                "state": "succeeded",
                "client_id": "client",
                "code": "display('x')",
                "messages": ["keep"],
                "artifacts": [{"path": str(owned_artifact), "mime": "image/png"}],
            },
        )
        snapshot = history.storage_gc_snapshot()
        assert snapshot["uncertain"] is False
        assert ".mypr/scans/request.jsonl" in snapshot["references"]
        assert ".mypr/scans/request.xml" in snapshot["references"]
        assert ".mypr/artifacts/exec/image.png" in snapshot["references"]

        marked = history.storage_gc_before_delete(
            [
                {"path": ".mypr/scans/request.xml"},
                {"path": ".mypr/artifacts/exec/image.png"},
                {"path": ".mypr/scans/orphan.jsonl"},
            ]
        )
        assert marked == [".mypr/artifacts/exec/image.png", ".mypr/scans/request.xml"]
        assert history.get("job")["scan_output_evicted"]
        updated = history.get("exec")
        assert updated["artifact_evicted"]
        assert "output_evicted" not in updated
        assert updated["code"] == "display('x')"
        assert updated["messages"] == ["keep"]
        assert updated["client_id"] == "client"
    finally:
        history.close()


@pytest.mark.parametrize("metadata_kind", ["fifo", "oversized", "invalid"])
def test_scan_metadata_failure_makes_storage_gc_uncertain(tmp_path: Path, metadata_kind: str):
    scan_dir = tmp_path / ".mypr" / "scans"
    scan_dir.mkdir(parents=True)
    metadata = scan_dir / "job.json"
    if metadata_kind == "fifo":
        os.mkfifo(metadata)
    elif metadata_kind == "invalid":
        metadata.write_text("null", encoding="utf-8")
    else:
        metadata.write_bytes(b"{" + b"x" * (1024 * 1024))

    history = History(tmp_path)
    try:
        history.record("scan", {"id": "job", "state": "succeeded"})
        result: list[dict] = []

        def read_snapshot() -> None:
            result.append(history.storage_gc_snapshot())

        worker = threading.Thread(target=read_snapshot, daemon=True)
        worker.start()
        worker.join(1)
        assert not worker.is_alive()
        snapshot = result[0]
        assert snapshot["uncertain"] is True
        expected_code = (
            "scan_metadata_invalid_object"
            if metadata_kind == "invalid"
            else "scan_metadata_unreadable"
        )
        assert snapshot["warnings"][0]["code"] == expected_code
        assert ".mypr/scans/job.jsonl" in snapshot["references"]
        assert history.storage_gc_before_delete(
            [{"path": ".mypr/scans/job.jsonl"}]
        ) == []
    finally:
        history.close()
        metadata.unlink(missing_ok=True)


def test_evicted_scan_metadata_can_be_removed_before_the_next_gc(tmp_path: Path):
    scan_dir = tmp_path / ".mypr" / "scans"
    scan_dir.mkdir(parents=True)
    result = scan_dir / "job.jsonl"
    summary = scan_dir / "job.summary.json"
    artifact = scan_dir / "job.xml"
    for path in (result, summary, artifact):
        path.write_text("x", encoding="utf-8")
    (scan_dir / "job.json").write_text(
        json.dumps(
            {
                "id": "job",
                "result_path": str(result),
                "summary_path": str(summary),
                "artifact_path": str(artifact),
            }
        ),
        encoding="utf-8",
    )

    history = History(tmp_path)
    try:
        history.record("scan", {"id": "job", "state": "succeeded"})
        assert history.storage_gc_snapshot()["uncertain"] is False
        marked = history.storage_gc_before_delete(
            [{"path": ".mypr/scans/job.xml"}]
        )
        assert marked == [".mypr/scans/job.xml"]
        (scan_dir / "job.json").unlink()
        snapshot = history.storage_gc_snapshot()
        assert snapshot["uncertain"] is False
        assert snapshot["tombstones"][".mypr/scans/job.jsonl"] == {"id": "job"}
    finally:
        history.close()
