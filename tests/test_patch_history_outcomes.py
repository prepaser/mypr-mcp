from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path

import pytest

import mypr_mcp.patching as patching
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.revisions import RevisionIndexOutcomeUnknown, RevisionStore


def patch_body(*parts: str) -> str:
    return "*** Begin Patch\n" + "\n".join(parts) + "\n*** End Patch"


@pytest.mark.asyncio
async def test_single_file_unknown_history_outcome_keeps_new_content(tmp_path: Path, monkeypatch):
    fs = Filesystem(tmp_path)
    first = await fs.write("value.txt", "old\n")
    original = fs.write

    async def fail_index(path, text, **kwargs):
        if ".mypr/revisions/index/" in str(path):
            await original(path, text, **kwargs)
            (tmp_path / path).write_text("{}", encoding="utf-8")
            raise OSError("injected index outcome unknown")
        return await original(path, text, **kwargs)

    monkeypatch.setattr(fs, "write", fail_index)
    with pytest.raises(
        RevisionIndexOutcomeUnknown, match="history index outcome is unknown"
    ) as caught:
        await fs.write("value.txt", "new\n", expected_hash=first["revision"])
    assert (tmp_path / "value.txt").read_text() == "new\n"
    assert caught.value.code == "outcome_unknown"


@pytest.mark.asyncio
async def test_batch_unknown_history_outcome_keeps_changes_and_backups(tmp_path: Path, monkeypatch):
    (tmp_path / "value.txt").write_text("old\n")
    fs = Filesystem(tmp_path)
    store = fs._history_store()

    def fail_record(_plans):
        raise RevisionIndexOutcomeUnknown("injected index outcome unknown")

    store.record_changes_sync = fail_record
    monkeypatch.setattr(fs, "_history_store", lambda: store)
    with pytest.raises(RevisionIndexOutcomeUnknown, match="outcome unknown") as caught:
        await fs.apply_patch(
            patch_body(
                "*** Update File: value.txt",
                "@@ -1,1 +1,1 @@",
                "-old",
                "+new",
            )
        )
    assert (tmp_path / "value.txt").read_text() == "new\n"
    backups = await asyncio.to_thread(lambda: list(tmp_path.glob(".*.mypr-backup.*")))
    assert backups
    assert any("recovery backups preserved" in note for note in caught.value.__notes__)


@pytest.mark.asyncio
async def test_moved_parent_keeps_replacement_and_cleans_original_temp(tmp_path: Path, monkeypatch):
    fs = Filesystem(tmp_path)
    parent = tmp_path / "nested"
    displaced = tmp_path / "displaced"
    original_mkstemp = patching.tempfile.mkstemp
    swapped = False

    def swap_parent(*args, **kwargs):
        nonlocal swapped
        result = original_mkstemp(*args, **kwargs)
        if not swapped and kwargs.get("dir") == parent:
            swapped = True
            parent.rename(displaced)
            parent.mkdir()
        return result

    monkeypatch.setattr("mypr_mcp.patching.tempfile.mkstemp", swap_parent)
    with pytest.raises(FileNotFoundError):
        await fs.apply_patch(
            patch_body("*** Add File: nested/new.txt", "+new"),
            history=False,
        )
    assert parent.is_dir()
    assert displaced.is_dir()
    assert list(displaced.iterdir()) == []
    assert not (parent / "new.txt").exists()


def test_artifact_cleanup_preserves_reused_name_in_replacement_parent(tmp_path):
    parent, moved = tmp_path / "parent", tmp_path / "moved"
    parent.mkdir()
    temporary = parent / "temporary"
    temporary.write_text("owned")
    identity = (temporary.stat().st_dev, temporary.stat().st_ino)
    descriptor = patching._open_artifact_parent(parent)
    try:
        parent.rename(moved)
        parent.mkdir()
        temporary.write_text("external")
        patching._remove_artifact(temporary, descriptor, identity)
        assert temporary.read_text() == "external"
        assert not (moved / "temporary").exists()
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("kind", ["modules", "skills"])
async def test_resource_commit_preserves_unknown_outcome_code(tmp_path, monkeypatch, kind):
    fs = Filesystem(tmp_path)
    store = RevisionStore(tmp_path, fs, kind)

    async def fail_record(*_args):
        raise RevisionIndexOutcomeUnknown("injected unknown outcome")

    monkeypatch.setattr(store, "record", fail_record)
    with pytest.raises(RevisionIndexOutcomeUnknown) as caught:
        await store.commit("value.txt", None, "new", expected_hash=None)
    assert (tmp_path / "value.txt").read_text() == "new"
    assert caught.value.code == "outcome_unknown"
    assert caught.value.details["resource"] == "value.txt"


@pytest.mark.parametrize("operation", ["copy", "move"])
async def test_lifecycle_preserves_unknown_outcome_code(tmp_path, monkeypatch, operation):
    fs = Filesystem(tmp_path)
    (tmp_path / "source.txt").write_text("old")
    store = fs._history_store()

    def fail_record(_plans):
        raise RevisionIndexOutcomeUnknown("injected unknown outcome")

    monkeypatch.setattr(store, "record_changes_sync", fail_record)
    monkeypatch.setattr(fs, "_history_store", lambda: store)
    with pytest.raises(RevisionIndexOutcomeUnknown) as caught:
        await getattr(fs, operation)(
            "source.txt", "destination.txt", expected_hash=hashlib.sha256(b"old").hexdigest()
        )
    assert (tmp_path / "destination.txt").read_text() == "old"
    assert (tmp_path / "source.txt").exists() == (operation == "copy")
    assert caught.value.code == "outcome_unknown"
    assert caught.value.details["resource"] == "destination.txt"
