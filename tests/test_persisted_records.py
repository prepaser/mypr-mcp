from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from mypr_mcp.history import History
from mypr_mcp.messages import MessageStore
from mypr_mcp.services import Shells
from mypr_mcp.storage import Storage


def _damage(root, table: str, column: str, value: str, where: str, ident: int):
    with sqlite3.connect(root / ".mypr" / "history.sqlite3") as database:
        database.execute(
            f"UPDATE {table} SET {column} = ? WHERE {where} = ?",
            (value, ident),
        )
        database.commit()


def test_corrupt_message_data_keeps_inbox_metadata(tmp_path):
    history = History(tmp_path)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.close()
    store = MessageStore(tmp_path)
    sent = store.send("alice", "bob", "keep this")
    store.close()
    _damage(tmp_path, "messages", "data", "{", "id", sent["id"])

    store = MessageStore(tmp_path)
    try:
        page = store.read("bob")
    finally:
        store.close()

    message = page["messages"][0]
    assert message["id"] == sent["id"]
    assert message["from"] == "alice"
    assert message["to"] == "bob"
    assert message["text"] == "keep this"
    assert message["data"] is None
    assert message["data_corrupt"] is True
    assert page["warnings"][0]["code"] == "message_data_corrupt"


def test_large_corrupt_messages_keep_cursor_progress_with_bounded_warnings(tmp_path):
    history = History(tmp_path)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.close()
    store = MessageStore(tmp_path)
    sent = [store.send("alice", "bob", "x" * 16200) for _ in range(2)]
    store.close()
    for message in sent:
        _damage(tmp_path, "messages", "data", "{", "id", message["id"])

    store = MessageStore(tmp_path)
    try:
        page = store.read("bob", limit=20)
    finally:
        store.close()

    assert len(page["messages"]) == 2
    assert page["has_more"] is False
    assert page["warnings_truncated"] is True
    assert len(json.dumps(page, ensure_ascii=False).encode()) <= 32 * 1024


def test_many_corrupt_messages_do_not_fake_more_pages(tmp_path):
    history = History(tmp_path)
    history.reserve_client_id("alice")
    history.reserve_client_id("bob")
    history.close()
    store = MessageStore(tmp_path)
    sent = [store.send("alice", "bob", str(index)) for index in range(5)]
    store.close()
    for message in sent:
        _damage(tmp_path, "messages", "data", "{", "id", message["id"])

    store = MessageStore(tmp_path)
    try:
        page = store.read("bob", limit=20)
    finally:
        store.close()

    assert len(page["messages"]) == 5
    assert page["has_more"] is False
    assert page["warnings_truncated"] is True
    assert len(page["warnings"]) == 4


def test_corrupt_history_records_and_events_are_reported_per_row(tmp_path):
    history = History(tmp_path)
    history.record("execution", {"id": "entity", "state": "succeeded"})
    event = history.append("execution", "output", {"id": "entity", "text": "ok"})
    history.append("execution", "output", 42)
    history.close()
    _damage(tmp_path, "entities", "data", "{", "id", "entity")
    _damage(tmp_path, "events", "data", "{", "seq", event["seq"])

    history = History(tmp_path)
    try:
        listed = history.list()
        item = listed["items"][0]
        assert item["id"] == "entity"
        assert item["kind"] == "execution"
        assert item["corrupt"] is True
        assert item["corruption"] == "invalid_json"
        assert item["warning"]["code"] == "history_entity_corrupt"
        assert listed["warnings"][0]["code"] == "history_entity_corrupt"
        assert history.get("entity")["corrupt"] is True

        logs = history.logs()
        assert logs["events"][0]["data_corrupt"] is True
        assert logs["events"][0]["id"] == "entity"
        assert logs["warnings"][0]["code"] == "history_event_corrupt"
        assert logs["events"][1]["data"] == 42
    finally:
        history.close()


@pytest.mark.parametrize("payload", ["{", "[]"])
def test_corrupt_execution_history_fails_request_dedup_closed(tmp_path, payload):
    history = History(tmp_path)
    history.record(
        "execution",
        {"id": "entity", "client_id": "alice", "request_id": "request"},
    )
    history.close()
    _damage(tmp_path, "entities", "data", payload, "id", "entity")

    history = History(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="request deduplication"):
            history.find_request("alice", "request")
    finally:
        history.close()


def test_uncertain_history_blocks_storage_tombstones(tmp_path):
    history = History(tmp_path)
    history.record("execution", {"id": "entity", "state": "succeeded"})
    history.close()
    _damage(tmp_path, "entities", "data", "[]", "id", "entity")

    history = History(tmp_path)
    try:
        snapshot = history.storage_gc_snapshot()
        assert snapshot["uncertain"] is True
        assert snapshot["warnings"][0]["code"] == "history_entity_corrupt"
        assert history.storage_gc_before_delete(
            [{"path": ".mypr/runs/entity.jsonl"}]
        ) == []
        assert history.mark_storage_evicted([".mypr/runs/entity.jsonl"]) == []
    finally:
        history.close()


def test_python_storage_gc_uses_public_task_id_for_references(tmp_path):
    result = tmp_path / ".mypr" / "task-results" / "task.json"
    artifact = tmp_path / ".mypr" / "artifacts" / "task" / "image.bin"
    result.parent.mkdir(parents=True)
    artifact.parent.mkdir(parents=True)
    result.write_text("result", encoding="utf-8")
    artifact.write_bytes(b"artifact")
    history = History(tmp_path)
    try:
        history.record(
            "python",
            {
                "id": "task",
                "kind": "python",
                "generation": "generation",
                "state": "running",
                "result_ref": {"path": str(result)},
                "artifacts": [{"path": str(artifact)}],
            },
            entity_id="python:generation:task",
        )
        records = history.storage_records()
        assert records[0]["id"] == "task"
        assert records[0]["kind"] == "python"
        snapshot = history.storage_gc_snapshot()
        digest = hashlib.sha256(b"python:generation:task").hexdigest()
        assert f".mypr/runs/task-{digest}.jsonl" in snapshot["references"]
        assert snapshot["references"][".mypr/task-results/task.json"] == ["task"]
        assert snapshot["references"][".mypr/artifacts/task/image.bin"] == ["task"]
        assert "task" in snapshot["active_ids"]
    finally:
        history.close()


@pytest.mark.asyncio
async def test_storage_planner_and_apply_fail_closed_on_uncertain_history(tmp_path):
    output = tmp_path / ".mypr" / "runs" / ("a" * 32 + ".jsonl")
    output.parent.mkdir(parents=True)
    output.write_text('{"stream":"stdout","text":"keep"}\n', encoding="utf-8")

    class UncertainHistory:
        def storage_gc_snapshot(self):
            return {"uncertain": True, "active_ids": set(), "protected_paths": set()}

    storage = Storage(tmp_path, history=UncertainHistory())
    plan = await storage.gc(dry_run=True, max_bytes=None)
    assert plan["candidates"] == []
    result = await storage.gc(dry_run=False, max_bytes=None)
    assert result["deleted"] == []
    assert output.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata", "warning_code"),
    [
        (b"{", "shell_metadata_corrupt"),
        (b"[]", "shell_metadata_corrupt"),
        (b'{"state": []}', "shell_outcome_unknown"),
        (b'{"state": {}}', "shell_outcome_unknown"),
    ],
)
async def test_corrupt_shell_metadata_still_polls_journal(tmp_path, metadata, warning_code):
    shells = Shells(tmp_path)
    job_id = "a" * 32
    shells.jobs_root.mkdir(parents=True, exist_ok=True)
    (shells.jobs_root / f"{job_id}.jsonl").write_text(
        json.dumps({"stream": "stdout", "text": "journal survives"}) + "\n",
        encoding="utf-8",
    )
    (shells.jobs_root / f"{job_id}.json").write_bytes(metadata)
    try:
        result = await shells.poll(job_id)
    finally:
        await shells.close()

    assert result["state"] == "lost"
    assert result["outcome_unknown"] is True
    assert result["output"] == [{"stream": "stdout", "text": "journal survives"}]
    assert result["warnings"][0]["code"] == warning_code
