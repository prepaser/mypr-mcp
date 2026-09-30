from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

import mypr_mcp.storage as storage_module
from mypr_mcp.storage import Storage


def old(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    timestamp = time.time() - 40 * 24 * 60 * 60
    os.utime(path, (timestamp, timestamp))


@pytest.mark.asyncio
async def test_usage_does_not_hash_managed_files(tmp_path: Path, monkeypatch):
    path = tmp_path / ".mypr" / "searches" / "one.json"
    old(path, "content")
    calls: list[Path] = []

    def digest(path: Path, size: int) -> str | None:
        calls.append(path)
        raise AssertionError("usage must not read file content")

    monkeypatch.setattr(Storage, "_digest", staticmethod(digest))

    result = await Storage(tmp_path).usage()

    assert result["total_bytes"] == path.stat().st_size
    assert calls == []


@pytest.mark.asyncio
async def test_gc_hashes_selected_files_without_hashing_protected_files(
    tmp_path: Path, monkeypatch
):
    active = tmp_path / ".mypr" / "jobs" / "job.jsonl"
    metadata = active.with_suffix(".json")
    candidate = tmp_path / ".mypr" / "searches" / "one.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text('{"id":"job","state":"running"}', encoding="utf-8")
    old(active, "active")
    old(candidate, "candidate")
    calls: list[Path] = []
    original = Storage._digest

    def digest(path: Path, size: int) -> str | None:
        calls.append(path)
        return original(path, size)

    monkeypatch.setattr(Storage, "_digest", staticmethod(digest))

    await Storage(tmp_path).gc(max_bytes=0)

    assert candidate in calls
    assert active not in calls


@pytest.mark.asyncio
async def test_gc_skips_files_above_digest_limit_but_usage_counts_them(
    tmp_path: Path, monkeypatch
):
    candidate = tmp_path / ".mypr" / "searches" / "large.json"
    old(candidate, "xx")
    monkeypatch.setattr(storage_module, "_MAX_HASH_BYTES", 1)

    plan = await Storage(tmp_path).gc(max_bytes=0)
    usage = await Storage(tmp_path).usage()

    assert plan["candidates"] == []
    assert usage["total_bytes"] == 2


@pytest.mark.asyncio
async def test_gc_apply_does_not_add_candidates_created_after_plan(tmp_path: Path):
    planned = tmp_path / ".mypr" / "searches" / "planned.json"
    old(planned, "planned")
    storage = Storage(tmp_path)
    plan = await storage.gc(max_bytes=0)

    unplanned = tmp_path / ".mypr" / "searches" / "unplanned.json"
    old(unplanned, "unplanned")
    result = await storage.gc_apply(plan["plan_id"])

    assert not planned.exists()
    assert unplanned.exists()
    assert [item["path"] for item in result["deleted"]] == [
        ".mypr/searches/planned.json"
    ]


@pytest.mark.asyncio
async def test_changed_selected_file_is_not_marked(tmp_path: Path):
    output = tmp_path / ".mypr" / "runs" / "a.jsonl"
    metadata = output.with_suffix(".json")
    metadata.parent.mkdir(parents=True)
    metadata.write_text('{"id":"a","state":"succeeded"}', encoding="utf-8")
    old(output, "before")
    original_stat = output.stat()
    marked: list[str] = []

    class History:
        def storage_gc_before_delete(self, candidates):
            marked.extend(item["path"] for item in candidates)
            return marked

    storage = Storage(tmp_path, history=History())
    plan = await storage.gc(max_bytes=0)
    output.write_text("after!", encoding="utf-8")
    os.utime(
        output,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )

    result = await storage.gc_apply(plan["plan_id"])

    assert marked == []
    assert output.exists()
    assert result["deleted"] == []
    assert any(item["reason"] == "changed_since_plan" for item in result["skipped"])
