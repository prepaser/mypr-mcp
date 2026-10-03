from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

import mypr_mcp.storage as storage_module
from mypr_mcp.storage import Storage


def old(path: Path, text: str = "x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    timestamp = time.time() - 40 * 24 * 60 * 60
    os.utime(path, (timestamp, timestamp))


def test_document_result_fifo_is_rejected_without_waiting_for_a_writer(tmp_path: Path):
    from mypr_mcp.document_tools import _ResultStore

    store = _ResultStore(tmp_path)
    ident = store.create({"kind": "extract", "source": {"path": "x"}, "items": []})
    path = store.root / f"{ident}.json"
    path.unlink()
    os.mkfifo(path)
    code = (
        "from pathlib import Path\n"
        "import sys\n"
        "from mypr_mcp.document_tools import _ResultStore\n"
        "_ResultStore(Path(sys.argv[1])).load(sys.argv[2])\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path), ident],
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert result.returncode != 0
    assert "invalid stored document result" in result.stderr


@pytest.mark.asyncio
async def test_usage_excludes_workspace_code_and_venv(tmp_path: Path):
    old(tmp_path / ".mypr" / "searches" / "one.json", "{}")
    old(tmp_path / ".mypr" / "venv" / "ignored", "12345")
    old(tmp_path / ".mypr" / "skills" / "review" / "SKILL.md", "text")

    result = await Storage(tmp_path).usage()

    assert result["total_bytes"] == 2
    assert result["categories"]["snapshots"]["files"] == 1


@pytest.mark.asyncio
async def test_usage_reports_protected_files_and_hardlinks_without_double_counting(
    tmp_path: Path,
):
    managed = tmp_path / ".mypr" / "searches" / "one.json"
    protected = tmp_path / ".mypr" / "venv" / "package.bin"
    managed.parent.mkdir(parents=True)
    protected.parent.mkdir(parents=True)
    managed.write_bytes(b"managed")
    protected.write_bytes(b"protected")
    link = tmp_path / ".mypr" / "venv" / "package-link.bin"
    link.hardlink_to(protected)

    result = await Storage(tmp_path).usage()

    assert result["managed"]["logical_bytes"] == len(b"managed")
    assert result["protected"]["logical_bytes"] == len(b"protected") * 2
    assert result["protected"]["unique_inodes"] == 2
    assert result["protected"]["allocated_bytes"] == protected.stat().st_blocks * 512
    assert result["protected"]["has_hardlinks"] is True
    assert result["workspace"]["files"] == 4


@pytest.mark.asyncio
async def test_usage_marks_managed_summary_truncated_when_protected_files_hit_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    protected = tmp_path / ".mypr" / "config.toml"
    managed = tmp_path / ".mypr" / "searches" / "one.json"
    protected.parent.mkdir(parents=True)
    managed.parent.mkdir(parents=True)
    protected.write_text("[workspace]\n", encoding="utf-8")
    managed.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(storage_module, "_MAX_FILES", 1)

    result = await Storage(tmp_path).usage()

    assert result["managed"]["files"] == 0
    assert result["managed"]["truncated"] is True
    assert result["workspace"]["truncated"] is True


@pytest.mark.asyncio
async def test_database_gc_is_previewed_by_id_and_applied_only_after_revalidation(
    tmp_path: Path,
):
    calls: list[tuple[str, object]] = []

    class History:
        def storage_history_snapshot(self, **kwargs):
            calls.append(("snapshot", kwargs))
            return {
                "entities": [{"id": "entity-1", "data_sha256": "digest"}],
                "events": [],
                "count": 1,
                "bytes": 32,
            }

        def storage_history_apply(self, plan):
            calls.append(("apply", plan))
            return {
                "entities": 1,
                "events": 0,
                "pruned_bytes": 32,
                "checkpoint": {"busy": False},
                "vacuum": {"performed": False},
                "reclaimed_bytes": 0,
            }

    storage = Storage(tmp_path, history=History())
    plan = await storage.gc(max_bytes=None)

    assert plan["database"]["selected_ids"] == ["entity-1"]
    assert [kind for kind, _ in calls] == ["snapshot"]

    result = await storage.gc_apply(plan["plan_id"])

    assert [kind for kind, _ in calls] == ["snapshot", "apply"]
    assert result["database"]["pruned_count"] == 1
    assert result["database"]["selected_ids"] == ["entity-1"]


@pytest.mark.asyncio
async def test_history_database_plan_uses_exact_entity_and_event_records(tmp_path: Path):
    calls: list[tuple[str, object]] = []

    class History:
        def storage_history_snapshot(self, *, retention_days):
            calls.append(("snapshot", retention_days))
            return {
                "entities": [{"id": "entity-1", "entity_seq": 4}],
                "events": [{"seq": 7, "time": 1.0, "id": "entity-1"}],
            }

        def storage_history_apply(self, plan):
            calls.append(("apply", plan))
            return {"entities": 1, "events": 1, "vacuum": {"performed": False}}

    storage = Storage(tmp_path, history=History())
    plan = await storage.gc(max_bytes=None, older_than_days=4)
    assert plan["database"]["selected_ids"] == ["entity-1", 7]
    assert plan["database"]["entities"][0]["entity_seq"] == 4
    assert plan["database"]["events"][0]["seq"] == 7
    assert [kind for kind, _ in calls] == ["snapshot"]

    result = await storage.gc_apply(plan["plan_id"])
    assert result["database"]["pruned_count"] == 2
    assert [kind for kind, _ in calls] == ["snapshot", "apply"]


@pytest.mark.asyncio
async def test_gc_requires_tombstone_before_deleting_output(tmp_path: Path):
    metadata = tmp_path / ".mypr" / "runs" / "a.json"
    output = tmp_path / ".mypr" / "runs" / "a.jsonl"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({"id": "a", "state": "succeeded"}), encoding="utf-8")
    old(output, '{"type":"stream","text":"output"}\n')

    storage = Storage(tmp_path)
    plan = await storage.gc(max_bytes=0)
    assert any(item["path"] == ".mypr/runs/a.jsonl" for item in plan["candidates"])
    assert ".mypr/runs/a.jsonl" in {item["path"] for item in plan["tombstones"]}

    result = await storage.gc_apply(plan["plan_id"])
    assert output.exists()
    assert any(item["reason"] == "tombstone_required" for item in result["skipped"])

    result = await storage.gc_apply(
        (await storage.gc(max_bytes=0))["plan_id"], tombstones=[".mypr/runs/a.jsonl"]
    )
    assert not output.exists()
    assert result["deleted_bytes"] > 0


@pytest.mark.asyncio
async def test_gc_revalidates_file_identity(tmp_path: Path):
    path = tmp_path / ".mypr" / "searches" / "one.json"
    old(path, "old")
    storage = Storage(tmp_path)
    plan = await storage.gc(max_bytes=0)
    path.write_text("changed", encoding="utf-8")

    result = await storage.gc_apply(plan["plan_id"])

    assert path.exists()
    assert result["deleted"] == []
    assert result["skipped"][0]["reason"] == "changed_since_plan"


@pytest.mark.asyncio
async def test_gc_preserves_shared_and_current_revision_objects(tmp_path: Path):
    root = tmp_path / ".mypr" / "revisions"
    index = root / "index" / "modules" / "resource.json"
    objects = root / "objects"
    index.parent.mkdir(parents=True)
    objects.mkdir(parents=True)
    shared = "a" * 64
    current = "b" * 64
    orphan = "c" * 64
    index.write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "modules",
                "resource": ".mypr/lib/ws_lib/demo.py",
                "count": 1,
                "revisions": [{"sequence": 1, "revision": shared, "size": 1, "created_at": "now"}],
            }
        ),
        encoding="utf-8",
    )
    for revision in (shared, current, orphan):
        old(objects / revision, "x")
    current_path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    current_path.parent.mkdir(parents=True)
    current_path.write_text("y", encoding="utf-8")
    current_hash = __import__("hashlib").sha256(b"y").hexdigest()
    (objects / current).unlink()
    (objects / current_hash).write_text("y", encoding="utf-8")
    timestamp = time.time() - 40 * 24 * 60 * 60
    os.utime(objects / current_hash, (timestamp, timestamp))

    plan = await Storage(tmp_path).gc(max_bytes=0)
    paths = {item["path"] for item in plan["candidates"]}

    assert f".mypr/revisions/objects/{orphan}" in paths
    assert f".mypr/revisions/objects/{shared}" not in paths
    assert f".mypr/revisions/objects/{current_hash}" not in paths


@pytest.mark.asyncio
async def test_gc_protects_active_record(tmp_path: Path):
    metadata = tmp_path / ".mypr" / "jobs" / "job.json"
    output = tmp_path / ".mypr" / "jobs" / "job.jsonl"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({"id": "job", "state": "running"}), encoding="utf-8")
    old(output, "active")

    plan = await Storage(tmp_path).gc(max_bytes=0)

    assert ".mypr/jobs/job.jsonl" not in {item["path"] for item in plan["candidates"]}


@pytest.mark.asyncio
async def test_gc_ignores_symlink_outside_workspace(tmp_path: Path):
    outside = tmp_path.parent / "outside-storage-file"
    outside.write_text("keep", encoding="utf-8")
    link = tmp_path / ".mypr" / "searches" / "outside.json"
    link.parent.mkdir(parents=True)
    link.symlink_to(outside)

    plan = await Storage(tmp_path).gc(max_bytes=0)

    assert str(link.relative_to(tmp_path)) not in {item["path"] for item in plan["candidates"]}
    assert outside.exists()


@pytest.mark.asyncio
async def test_gc_uses_history_marker_callback_before_output_delete(tmp_path: Path):
    output = tmp_path / ".mypr" / "runs" / "a.jsonl"
    metadata = output.with_suffix(".json")
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({"id": "a", "state": "succeeded"}), encoding="utf-8")
    old(output, "output")
    marked: list[str] = []

    class History:
        def storage_gc_snapshot(self):
            return {"references": {".mypr/runs/a.jsonl": ["a"]}}

        def storage_gc_before_delete(self, candidates):
            marked.extend(item["path"] for item in candidates)
            return marked

    result = await Storage(tmp_path, history=History()).gc(dry_run=False, max_bytes=0)

    assert ".mypr/runs/a.jsonl" in marked
    assert not output.exists()
    assert result["deleted_bytes"] > 0


@pytest.mark.asyncio
async def test_gc_protects_invalid_revision_index(tmp_path: Path):
    index = tmp_path / ".mypr" / "revisions" / "index" / "modules" / "bad.json"
    object_path = tmp_path / ".mypr" / "revisions" / "objects" / ("d" * 64)
    index.parent.mkdir(parents=True)
    object_path.parent.mkdir(parents=True)
    index.write_text("not-json", encoding="utf-8")
    old(object_path, "object")

    plan = await Storage(tmp_path).gc(max_bytes=0)

    assert str(object_path.relative_to(tmp_path)) not in {
        item["path"] for item in plan["candidates"]
    }


@pytest.mark.asyncio
async def test_gc_uses_oldest_recent_files_to_reach_quota(tmp_path: Path):
    first = tmp_path / ".mypr" / "searches" / "first.json"
    second = tmp_path / ".mypr" / "searches" / "second.json"
    first.parent.mkdir(parents=True)
    first.write_text("1" * 10, encoding="utf-8")
    second.write_text("2" * 10, encoding="utf-8")
    now = time.time()
    os.utime(first, (now - 10, now - 10))
    os.utime(second, (now - 5, now - 5))

    plan = await Storage(tmp_path).gc(older_than_days=30, max_bytes=10)

    assert [item["path"] for item in plan["candidates"]] == [
        ".mypr/searches/first.json"
    ]


@pytest.mark.asyncio
async def test_gc_requires_boolean_dry_run(tmp_path: Path):
    with pytest.raises(TypeError, match="dry_run"):
        await Storage(tmp_path).gc(dry_run=1)


@pytest.mark.asyncio
async def test_gc_keeps_document_result_pair_together(tmp_path: Path):
    result = tmp_path / ".mypr" / "document-results" / "doc.json"
    resume = result.with_suffix(".resume")
    result.parent.mkdir(parents=True)
    old(result, "result")
    old(resume, "resume")

    plan = await Storage(tmp_path).gc(max_bytes=5)

    paths = {item["path"] for item in plan["candidates"]}
    assert paths == {".mypr/document-results/doc.json", ".mypr/document-results/doc.resume"}


@pytest.mark.asyncio
async def test_gc_does_not_mark_changed_output(tmp_path: Path):
    output = tmp_path / ".mypr" / "runs" / "a.jsonl"
    metadata = output.with_suffix(".json")
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({"id": "a", "state": "succeeded"}), encoding="utf-8")
    old(output, "before")
    marked: list[str] = []

    class History:
        def storage_gc_before_delete(self, candidates):
            marked.extend(item["path"] for item in candidates)
            return marked

    storage = Storage(tmp_path, history=History())
    plan = await storage.gc(max_bytes=0)
    output.write_text("changed", encoding="utf-8")
    result = await storage.gc_apply(plan["plan_id"])

    assert marked == []
    assert output.exists()
    assert result["deleted"] == []


@pytest.mark.asyncio
async def test_gc_prunes_v2_index_before_sweeping_revision_objects(tmp_path: Path):
    index = tmp_path / ".mypr" / "revisions" / "index" / "files" / "demo.json"
    objects = tmp_path / ".mypr" / "revisions" / "objects"
    index.parent.mkdir(parents=True)
    objects.mkdir(parents=True)
    revisions = []
    for sequence in range(1, 101):
        revision = f"{sequence:064x}"
        revisions.append(
            {
                "sequence": sequence,
                "revision": revision,
                "size": 1,
                "created_at": "now",
            }
        )
        old(objects / revision, "x")
    index.write_text(
        json.dumps(
            {
                "version": 2,
                "kind": "files",
                "resource": "notes.txt",
                "count": len(revisions),
                "next_sequence": 101,
                "revisions": revisions,
            }
        ),
        encoding="utf-8",
    )
    current = tmp_path / "notes.txt"
    current.write_text("current", encoding="utf-8")

    result = await Storage(tmp_path).gc(dry_run=False, max_bytes=0, revision_keep=5)
    updated = json.loads(index.read_text(encoding="utf-8"))

    assert updated["version"] == 2
    assert updated["next_sequence"] == 101
    assert updated["count"] == 5
    assert [item["sequence"] for item in updated["revisions"]] == [96, 97, 98, 99, 100]
    assert result["revision_pruned"]
    assert not (objects / f"{1:064x}").exists()
    assert (objects / f"{100:064x}").exists()


@pytest.mark.asyncio
async def test_gc_migrates_v1_index_and_preserves_absent_current_record(tmp_path: Path):
    index = tmp_path / ".mypr" / "revisions" / "index" / "files" / "demo.json"
    objects = tmp_path / ".mypr" / "revisions" / "objects"
    index.parent.mkdir(parents=True)
    objects.mkdir(parents=True)
    old_revision = "e" * 64
    absent = {
        "sequence": 2,
        "revision": "absent",
        "absent": True,
        "size": 0,
        "created_at": "now",
    }
    old(objects / old_revision, "old")
    index.write_text(
        json.dumps(
            {
                "version": 1,
                "kind": "files",
                "resource": "missing.txt",
                "count": 2,
                "revisions": [
                    {
                        "sequence": 1,
                        "revision": old_revision,
                        "size": 3,
                        "created_at": "now",
                    },
                    absent,
                ],
            }
        ),
        encoding="utf-8",
    )

    await Storage(tmp_path).gc(dry_run=False, max_bytes=0, revision_keep=1)
    updated = json.loads(index.read_text(encoding="utf-8"))

    assert updated["version"] == 2
    assert updated["next_sequence"] == 3
    assert updated["revisions"][0]["revision"] == "absent"
    assert not (objects / old_revision).exists()


@pytest.mark.asyncio
async def test_gc_keeps_only_latest_current_revision_duplicate(tmp_path: Path):
    index = tmp_path / ".mypr" / "revisions" / "index" / "files" / "demo.json"
    objects = tmp_path / ".mypr" / "revisions" / "objects"
    index.parent.mkdir(parents=True)
    objects.mkdir(parents=True)
    current_hash = hashlib.sha256(b"current").hexdigest()
    revisions = []
    for sequence in range(1, 101):
        revision = current_hash if sequence in {1, 50} else f"{sequence:064x}"
        revisions.append(
            {
                "sequence": sequence,
                "revision": revision,
                "size": 7,
                "created_at": "now",
            }
        )
        old(objects / revision, "current" if revision == current_hash else "x")
    index.write_text(
        json.dumps(
            {
                "version": 2,
                "kind": "files",
                "resource": "demo.txt",
                "count": 100,
                "next_sequence": 101,
                "revisions": revisions,
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "demo.txt").write_text("current", encoding="utf-8")

    await Storage(tmp_path).gc(dry_run=False, max_bytes=0, revision_keep=5)
    updated = json.loads(index.read_text(encoding="utf-8"))

    assert [item["sequence"] for item in updated["revisions"]] == [50, 96, 97, 98, 99, 100]
