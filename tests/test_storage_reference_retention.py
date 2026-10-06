from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

import pytest

import mypr_mcp.storage as storage_module
from mypr_mcp.history import History
from mypr_mcp.storage import Storage


def _old(path: Path, content: str | bytes = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")
    timestamp = time.time() - 40 * 24 * 60 * 60
    os.utime(path, (timestamp, timestamp))


@pytest.mark.asyncio
async def test_gc_scopes_history_references_to_retained_files(tmp_path, monkeypatch):
    result = tmp_path / ".mypr" / "task-results" / "saved.json"
    artifact = tmp_path / ".mypr" / "artifacts" / "owner" / "saved.bin"
    owner_metadata = tmp_path / ".mypr" / "jobs" / "owner.json"
    _old(result, "result")
    _old(artifact, b"artifact")
    _old(owner_metadata, json.dumps({"id": "owner", "kind": "shell", "state": "succeeded"}))

    history = History(tmp_path)
    try:
        old = time.time() - 40 * 24 * 60 * 60
        history_id = "python:" + "a" * 32 + ":owner"
        history.record(
            "python",
            {
                "id": "owner",
                "kind": "python",
                "generation": "a" * 32,
                "history_id": history_id,
                "state": "succeeded",
                "result_ref": {"path": ".mypr/task-results/saved.json"},
            },
            entity_id=history_id,
            updated_at=old,
        )
        with history._lock:
            history._db.executemany(
                "INSERT INTO entities(id,kind,created,updated,data) VALUES(?,?,?,?,?)",
                [
                    (
                        f"old-{index}",
                        "shell",
                        old,
                        old,
                        json.dumps(
                            {"id": f"old-{index}", "kind": "shell", "state": "succeeded"},
                            separators=(",", ":"),
                        ),
                    )
                    for index in range(6)
                ],
            )
        monkeypatch.setattr(storage_module, "_MAX_PLAN_CANDIDATES", 4)
        storage = Storage(tmp_path, history=history)
        plan = await storage.gc(max_bytes=0)
        paths = {item["path"] for item in plan["candidates"]}
        assert ".mypr/task-results/saved.json" in paths
        assert ".mypr/artifacts/owner/saved.bin" in paths

        result_data = await storage.gc_apply(plan["plan_id"])

        assert not result.exists()
        assert not artifact.exists()
        assert {item["path"] for item in result_data["deleted"]} >= paths
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_preserves_active_history_owned_files_after_reference_growth(
    tmp_path, monkeypatch
):
    result = tmp_path / ".mypr" / "task-results" / "active.json"
    artifact = tmp_path / ".mypr" / "artifacts" / "active" / "saved.bin"
    _old(result, "result")
    _old(artifact, b"artifact")
    history = History(tmp_path)
    try:
        old = time.time() - 40 * 24 * 60 * 60
        history_id = "python:" + "b" * 32 + ":active"
        history.record(
            "python",
            {
                "id": "active",
                "kind": "python",
                "generation": "b" * 32,
                "history_id": history_id,
                "state": "running",
                "result_ref": {"path": ".mypr/task-results/active.json"},
            },
            entity_id=history_id,
            updated_at=old,
        )
        with history._lock:
            history._db.executemany(
                "INSERT INTO entities(id,kind,created,updated,data) VALUES(?,?,?,?,?)",
                [
                    (
                        f"old-{index}",
                        "shell",
                        old,
                        old,
                        json.dumps(
                            {"id": f"old-{index}", "kind": "shell", "state": "succeeded"},
                            separators=(",", ":"),
                        ),
                    )
                    for index in range(6)
                ],
            )
        monkeypatch.setattr(storage_module, "_MAX_PLAN_CANDIDATES", 4)
        plan = await Storage(tmp_path, history=history).gc(max_bytes=0)

        paths = {item["path"] for item in plan["candidates"]}
        assert "active" in plan["protected"]["active_ids"]
        assert ".mypr/task-results/active.json" not in paths
        assert ".mypr/artifacts/active/saved.bin" not in paths
        assert result.exists()
        assert artifact.exists()
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_artifact_eviction_does_not_expire_retained_journal(tmp_path):
    metadata = tmp_path / ".mypr" / "jobs" / "artifact-only.json"
    output = metadata.with_suffix(".jsonl")
    artifact = tmp_path / ".mypr" / "artifacts" / "artifact-only" / "image.bin"
    _old(metadata, json.dumps({"id": "artifact-only", "kind": "shell", "state": "succeeded"}))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("retained output", encoding="utf-8")
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(b"old artifact")
    old = time.time() - 40 * 24 * 60 * 60
    os.utime(artifact, (old, old))

    history = History(tmp_path)
    try:
        history.record(
            "shell",
            {"id": "artifact-only", "kind": "shell", "state": "succeeded"},
            updated_at=old,
        )
        max_bytes = metadata.stat().st_size + output.stat().st_size
        storage = Storage(tmp_path, history=history)
        plan = await storage.gc(max_bytes=max_bytes)
        assert {item["path"] for item in plan["candidates"]} == {
            ".mypr/artifacts/artifact-only/image.bin"
        }

        await storage.gc_apply(plan["plan_id"])

        record = history.get("artifact-only")
        assert output.exists()
        assert not artifact.exists()
        assert record is not None
        assert record.get("artifact_evicted") is True
        assert record.get("output_evicted") is not True
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_collects_artifact_owned_by_terminal_python_history(tmp_path):
    artifact = tmp_path / ".mypr" / "artifacts" / "python-owner" / "image.bin"
    _old(artifact, b"old artifact")
    history = History(tmp_path)
    try:
        old = time.time() - 40 * 24 * 60 * 60
        history_id = "python:" + "e" * 32 + ":python-owner"
        history.record(
            "python",
            {
                "id": "python-owner",
                "kind": "python",
                "generation": "e" * 32,
                "history_id": history_id,
                "state": "succeeded",
            },
            entity_id=history_id,
            updated_at=old,
        )
        storage = Storage(tmp_path, history=history)
        plan = await storage.gc(max_bytes=0)

        assert ".mypr/artifacts/python-owner/image.bin" in {
            item["path"] for item in plan["candidates"]
        }
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_keeps_artifact_for_history_owner_with_unknown_state(tmp_path):
    artifact = tmp_path / ".mypr" / "artifacts" / "unknown-owner" / "image.bin"
    _old(artifact, b"unknown state")
    history = History(tmp_path)
    try:
        old = time.time() - 40 * 24 * 60 * 60
        history_id = "python:" + "f" * 32 + ":unknown-owner"
        history.record(
            "python",
            {
                "id": "unknown-owner",
                "kind": "python",
                "generation": "f" * 32,
                "history_id": history_id,
                "state": "unknown",
            },
            entity_id=history_id,
            updated_at=old,
        )
        storage = Storage(tmp_path, history=history)
        plan = await storage.gc(max_bytes=0)

        assert ".mypr/artifacts/unknown-owner/image.bin" not in {
            item["path"] for item in plan["candidates"]
        }
        assert "known_owner_ids" not in plan["protected"]
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_does_not_cross_generation_for_unknown_python_owner(tmp_path):
    old_artifact = tmp_path / ".mypr" / "artifacts" / "exec-old" / "image.bin"
    unknown_artifact = tmp_path / ".mypr" / "artifacts" / "exec-unknown" / "image.bin"
    _old(old_artifact, b"old artifact")
    _old(unknown_artifact, b"unknown artifact")
    history = History(tmp_path)
    try:
        old = time.time() - 40 * 24 * 60 * 60
        history.record(
            "execution",
            {"id": "exec-old", "kind": "execution", "state": "succeeded"},
            updated_at=old,
        )
        history.record(
            "execution",
            {"id": "exec-unknown", "kind": "execution", "state": "succeeded"},
            updated_at=old,
        )
        history.record(
            "python",
            {
                "id": "job",
                "kind": "python",
                "generation": "1" * 32,
                "history_id": "python:" + "1" * 32 + ":job",
                "exec_id": "exec-old",
                "state": "succeeded",
            },
            entity_id="python:" + "1" * 32 + ":job",
            updated_at=old,
        )
        history.record(
            "python",
            {
                "id": "job",
                "kind": "python",
                "generation": "2" * 32,
                "history_id": "python:" + "2" * 32 + ":job",
                "exec_id": "exec-unknown",
                "state": "unknown",
            },
            entity_id="python:" + "2" * 32 + ":job",
            updated_at=old,
        )
        plan = await Storage(tmp_path, history=history).gc(max_bytes=0)
        paths = {item["path"] for item in plan["candidates"]}

        assert ".mypr/artifacts/exec-old/image.bin" in paths
        assert ".mypr/artifacts/exec-unknown/image.bin" not in paths
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_groups_journal_index_and_removes_owned_orphan_index(tmp_path):
    output = tmp_path / ".mypr" / "jobs" / "owned.jsonl"
    index = output.with_suffix(".idx")
    metadata = output.with_suffix(".json")
    _old(metadata, json.dumps({"id": "owned", "kind": "shell", "state": "succeeded"}))
    _old(output, "output")
    _old(index, b"index")
    orphan_metadata = metadata.with_name("orphan.json")
    orphan_index = orphan_metadata.with_suffix(".idx")
    _old(orphan_metadata, json.dumps({"id": "orphan", "kind": "shell", "state": "succeeded"}))
    _old(orphan_index, b"orphan index")

    history = History(tmp_path)
    try:
        old = time.time() - 40 * 24 * 60 * 60
        history.record(
            "shell",
            {"id": "owned", "kind": "shell", "state": "succeeded"},
            updated_at=old,
        )
        storage = Storage(tmp_path, history=history)
        plan = await storage.gc(max_bytes=0)
        candidates = {item["path"]: item for item in plan["candidates"]}
        assert candidates[".mypr/jobs/owned.jsonl"]["group"] == ".mypr/jobs/owned.jsonl"
        assert candidates[".mypr/jobs/owned.idx"]["group"] == ".mypr/jobs/owned.jsonl"
        assert candidates[".mypr/jobs/orphan.idx"]["reason"] == "orphan_index"

        result = await storage.gc_apply(plan["plan_id"], tombstones=[".mypr/jobs/owned.jsonl"])

        assert not output.exists()
        assert not index.exists()
        assert not orphan_index.exists()
        assert {item["path"] for item in result["deleted"]} >= set(candidates)
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_collects_python_orphan_index_from_history_owner(tmp_path):
    history = History(tmp_path)
    try:
        old = time.time() - 40 * 24 * 60 * 60
        history_id = "python:" + "c" * 32 + ":orphan"
        history.record(
            "python",
            {
                "id": "orphan",
                "kind": "python",
                "generation": "c" * 32,
                "history_id": history_id,
                "state": "succeeded",
            },
            entity_id=history_id,
            updated_at=old,
        )
        active_history_id = "python:" + "d" * 32 + ":live"
        history.record(
            "python",
            {
                "id": "live",
                "kind": "python",
                "generation": "d" * 32,
                "history_id": active_history_id,
                "state": "running",
            },
            entity_id=active_history_id,
            updated_at=old,
        )
        digest = hashlib.sha256(history_id.encode()).hexdigest()
        index = tmp_path / ".mypr" / "runs" / f"task-{digest}.idx"
        _old(index, b"orphan index")
        active_digest = hashlib.sha256(active_history_id.encode()).hexdigest()
        active_index = tmp_path / ".mypr" / "runs" / f"task-{active_digest}.idx"
        _old(active_index, b"active index")

        storage = Storage(tmp_path, history=history)
        plan = await storage.gc(max_bytes=0)
        relative = index.relative_to(tmp_path).as_posix()
        candidate = next(item for item in plan["candidates"] if item["path"] == relative)
        assert candidate["reason"] == "orphan_index"
        assert active_index.relative_to(tmp_path).as_posix() not in {
            item["path"] for item in plan["candidates"]
        }
        result = await storage.gc_apply(plan["plan_id"])

        assert not index.exists()
        assert active_index.exists()
        assert any(item["path"] == candidate["path"] for item in result["deleted"])
    finally:
        history.close()


@pytest.mark.asyncio
async def test_gc_keeps_index_when_journal_delete_fails(tmp_path, monkeypatch):
    output = tmp_path / ".mypr" / "jobs" / "owned.jsonl"
    index = output.with_suffix(".idx")
    metadata = output.with_suffix(".json")
    _old(metadata, json.dumps({"id": "owned", "kind": "shell", "state": "succeeded"}))
    _old(output, "output")
    _old(index, b"index")
    old = time.time() - 40 * 24 * 60 * 60
    os.utime(index, (old - 86400, old - 86400))

    history = History(tmp_path)
    try:
        history.record(
            "shell",
            {"id": "owned", "kind": "shell", "state": "succeeded"},
            updated_at=old,
        )
        storage = Storage(tmp_path, history=history)
        plan = await storage.gc(max_bytes=0)
        paths = {item["path"] for item in plan["candidates"]}
        assert paths == {".mypr/jobs/owned.jsonl", ".mypr/jobs/owned.idx"}

        original_unlink = Path.unlink
        failed = True

        def fail_journal_unlink(path, *args, **kwargs):
            nonlocal failed
            if path == output and failed:
                failed = False
                raise OSError("injected journal delete failure")
            return original_unlink(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", fail_journal_unlink)
        result = await storage.gc_apply(plan["plan_id"], tombstones=[".mypr/jobs/owned.jsonl"])

        assert output.exists()
        assert index.exists()
        assert any(
            item["path"] == ".mypr/jobs/owned.jsonl" and item["reason"] == "delete_failed: OSError"
            for item in result["skipped"]
        )
        assert any(
            item["path"] == ".mypr/jobs/owned.idx" and item["reason"] == "paired_file_invalid"
            for item in result["skipped"]
        )

        monkeypatch.setattr(Path, "unlink", original_unlink)
        retry = await storage.gc(max_bytes=0)
        retry_result = await storage.gc_apply(retry["plan_id"])

        assert not output.exists()
        assert not index.exists()
        assert {item["path"] for item in retry_result["deleted"]} >= paths
    finally:
        history.close()


def test_atomic_revision_json_preserves_surrogate_paths(tmp_path):
    path = tmp_path / "index.json"
    value = {"resource": os.fsdecode(b"name-\xff.txt")}

    Storage._atomic_json(path, value)

    assert json.loads(path.read_text(encoding="utf-8")) == value


def test_public_gc_plan_bounds_active_owner_diagnostics():
    active_ids = [f"client-{index}" for index in range(storage_module._MAX_PUBLIC_ITEMS + 1)]
    plan = storage_module.Storage._public_plan(
        {
            "protected": {
                "paths": [],
                "references": {},
                "active_ids": active_ids,
                "known_owner_ids": ["internal-only"],
            },
            "candidates": [],
            "tombstones": [],
            "database": {},
        }
    )

    protected = plan["protected"]
    assert protected["active_ids"] == active_ids[: storage_module._MAX_PUBLIC_ITEMS]
    assert protected["active_id_count"] == len(active_ids)
    assert protected["active_ids_truncated"] is True
    assert "known_owner_ids" not in protected
    assert plan["public_truncated"] is True
