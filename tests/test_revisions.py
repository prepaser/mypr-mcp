from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.kernel_api import Skills
from mypr_mcp.modules import ModuleManager
from mypr_mcp.revisions import RevisionStore


class FakeShell:
    async def run(self, command, **kwargs):
        raise AssertionError("revision tests do not run child processes")


def module_manager(workspace: Path, fs: Filesystem | None = None) -> ModuleManager:
    root = workspace / ".mypr" / "lib" / "ws_lib"
    root.mkdir(parents=True, exist_ok=True)
    (root / "__init__.py").touch()
    return ModuleManager(workspace, fs or Filesystem(workspace), FakeShell())


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.asyncio
async def test_revision_storage_lock_does_not_leak_to_child_context(tmp_path: Path):
    store = RevisionStore(tmp_path, Filesystem(tmp_path), "files")

    async def acquire_from_child():
        async with store.transaction("child.txt"):
            return True

    async with store.transaction("parent.txt"):
        child = asyncio.create_task(acquire_from_child())
        await asyncio.sleep(0.05)
        assert not child.done()
    assert await asyncio.wait_for(child, 1) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["modules", "skills"])
async def test_legacy_revision_index_upgrades_and_prunes(kind: str, tmp_path: Path):
    store = RevisionStore(tmp_path, Filesystem(tmp_path), kind)
    resource = f"{kind}/demo.py"
    values = [f"value = {index}\n" for index in range(3)]
    for value in values:
        await store.record(resource, [value])
    index_path = tmp_path / store._index_path(resource)
    legacy = json.loads(index_path.read_text(encoding="utf-8"))
    legacy["version"] = 1
    legacy.pop("next_sequence", None)
    legacy.pop("pruned_before", None)
    index_path.write_text(json.dumps(legacy), encoding="utf-8")

    values.extend(f"value = {index}\n" for index in range(3, 55))
    for value in values[3:]:
        await store.record(resource, [value])
    migrated = "value = migrated\n"
    await store.record(resource, [migrated])
    current = json.loads(index_path.read_text(encoding="utf-8"))
    assert current["version"] == 2
    assert current["next_sequence"] > current["revisions"][-1]["sequence"]
    history = await store.history(resource, limit=100)
    assert len(history["items"]) == 56
    assert history["pruned_before"] == 0
    assert (await store.read_revision(resource, hashlib.sha256(values[-1].encode()).hexdigest()))[
        "recorded"
    ]


@pytest.mark.asyncio
async def test_module_history_restore_is_cas_and_does_not_reload(tmp_path: Path):
    modules = module_manager(tmp_path)
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    path.write_text("VALUE = 0\n", encoding="utf-8")
    original_hash = sha256("VALUE = 0\n")

    first = await modules.write("demo", "VALUE = 1\n", expected_hash=original_hash)
    second = await modules.write("demo", "VALUE = 2\n", expected_hash=first["revision"])
    active = modules.load("demo")

    history = await modules.history("demo", limit=2)
    assert [item["revision"] for item in history["items"]] == [
        second["revision"],
        first["revision"],
    ]
    assert history["has_more"] is True
    older = await modules.history("demo", limit=2, cursor=history["next_cursor"])
    assert [item["revision"] for item in older["items"]] == [original_hash]

    restored_source = await modules.read_revision("demo", first["revision"])
    assert restored_source["text"] == "VALUE = 1\n"
    with pytest.raises(ValueError, match="Revision mismatch"):
        await modules.restore("demo", first["revision"], expected_hash="0" * 64)
    with pytest.raises(FileExistsError, match="expected_hash"):
        await modules.restore("demo", first["revision"])

    restored = await modules.restore("demo", first["revision"], expected_hash=second["revision"])
    assert restored["changed"] is True
    assert restored["activated"] is False
    assert path.read_text(encoding="utf-8") == "VALUE = 1\n"
    assert sys.modules["ws_lib.demo"] is active
    assert active.VALUE == 2

    before = await modules.history("demo")
    await modules.write("demo", "VALUE = 1\n", expected_hash=first["revision"])
    after = await modules.history("demo")
    assert len(after["items"]) == len(before["items"])
    sys.modules.pop("ws_lib.demo", None)


@pytest.mark.asyncio
async def test_skill_history_restore_and_utf8_page_boundary(tmp_path: Path):
    skills = Skills(tmp_path, Filesystem(tmp_path))
    old = "# 한글 skill\n"
    new = "# New skill\n"
    first = await skills.write("demo", old)
    second = await skills.write("demo", new, expected_hash=first["revision"])
    history = await skills.history("demo")
    assert [item["revision"] for item in history["items"]] == [
        second["revision"],
        first["revision"],
    ]

    page = await skills.read_revision("demo", first["revision"], start_byte=2, max_bytes=1)
    assert page["text"] == "한"
    assert page["next_cursor"] == 5
    tail = await skills.read_revision(
        "demo", first["revision"], start_byte=page["next_cursor"], max_bytes=10
    )
    assert tail["text"] == "글 skill\n"

    with pytest.raises(ValueError, match="Revision mismatch"):
        await skills.restore("demo", first["revision"], expected_hash="0" * 64)
    restored = await skills.restore("demo", first["revision"], expected_hash=second["revision"])
    assert restored["changed"] is True
    assert restored["activated"] is False
    assert skills.read("demo") == old


@pytest.mark.asyncio
async def test_history_write_cancellation_finishes_revision_record(tmp_path: Path):
    target = ".mypr/lib/ws_lib/demo.py"

    class GatedFilesystem(Filesystem):
        def __init__(self, workspace: Path):
            super().__init__(workspace)
            self.gate_target = False
            self.target_written = asyncio.Event()
            self.release = asyncio.Event()

        async def write(self, path, text, **kwargs):
            result = await super().write(path, text, **kwargs)
            if self.gate_target and str(path) == target:
                self.target_written.set()
                await self.release.wait()
            return result

    fs = GatedFilesystem(tmp_path)
    modules = module_manager(tmp_path, fs)
    path = tmp_path / target
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("VALUE = 0\n", encoding="utf-8")
    first = await modules.write("demo", "VALUE = 1\n", expected_hash=sha256("VALUE = 0\n"))

    fs.gate_target = True
    pending = asyncio.create_task(
        modules.write("demo", "VALUE = 2\n", expected_hash=first["revision"])
    )
    await asyncio.wait_for(fs.target_written.wait(), timeout=2)
    pending.cancel()
    await asyncio.sleep(0)
    fs.release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending

    history = await modules.history("demo", limit=10)
    revisions = {item["revision"] for item in history["items"]}
    assert sha256("VALUE = 0\n") in revisions
    assert first["revision"] in revisions
    assert sha256("VALUE = 2\n") in revisions
    assert await modules.read_revision("demo", sha256("VALUE = 0\n"))


@pytest.mark.asyncio
async def test_revision_index_limit_fails_before_file_write(tmp_path: Path, monkeypatch):
    import mypr_mcp.revisions as revisions_module

    monkeypatch.setattr(revisions_module, "_MAX_INDEX_BYTES", 2)
    modules = module_manager(tmp_path)
    with pytest.raises(ValueError, match="metadata size limit"):
        await modules.write("demo", "VALUE = 1\n")
    assert not (tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py").exists()


@pytest.mark.asyncio
async def test_index_failure_rolls_back_existing_module_and_keeps_recovery_blob(tmp_path: Path):
    fs = Filesystem(tmp_path)
    modules = module_manager(tmp_path, fs)
    first = await modules.write("demo", "VALUE = 0\n")
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    original_write = fs.write

    async def fail_index(path, text, **kwargs):
        if ".mypr/revisions/index/" in str(path):
            raise OSError("injected index failure")
        return await original_write(path, text, **kwargs)

    fs.write = fail_index
    attempted = "VALUE = 1\n"
    with pytest.raises(RuntimeError, match="file change was rolled back"):
        await modules.write("demo", attempted, expected_hash=first["revision"])

    assert path.read_text(encoding="utf-8") == "VALUE = 0\n"
    history = await modules.history("demo")
    assert [item["revision"] for item in history["items"]] == [first["revision"]]
    recovered = await modules.read_revision("demo", sha256(attempted))
    assert recovered["text"] == attempted
    assert recovered["recorded"] is False


@pytest.mark.asyncio
async def test_index_failure_removes_new_module(tmp_path: Path):
    fs = Filesystem(tmp_path)
    modules = module_manager(tmp_path, fs)
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    original_write = fs.write

    async def fail_index(path, text, **kwargs):
        if ".mypr/revisions/index/" in str(path):
            raise OSError("injected index failure")
        return await original_write(path, text, **kwargs)

    fs.write = fail_index
    attempted = "VALUE = 1\n"
    with pytest.raises(RuntimeError, match="file change was rolled back"):
        await modules.write("demo", attempted)

    assert not path.exists()
    assert (await modules.history("demo"))["items"] == []
    recovered = await modules.read_revision("demo", sha256(attempted))
    assert recovered["text"] == attempted
    assert recovered["recorded"] is False


@pytest.mark.asyncio
async def test_index_failure_after_commit_is_reported_as_success(tmp_path: Path):
    fs = Filesystem(tmp_path)
    modules = module_manager(tmp_path, fs)
    first = await modules.write("demo", "VALUE = 0\n")
    original_write = fs.write

    async def commit_then_fail(path, text, **kwargs):
        result = await original_write(path, text, **kwargs)
        if ".mypr/revisions/index/" in str(path):
            raise OSError("injected post-commit failure")
        return result

    fs.write = commit_then_fail
    result = await modules.write("demo", "VALUE = 1\n", expected_hash=first["revision"])

    assert result["revision"] == sha256("VALUE = 1\n")
    assert (await modules.history("demo", limit=2))["items"][0]["revision"] == result["revision"]


@pytest.mark.asyncio
async def test_old_identical_history_tail_does_not_hide_rejected_write(tmp_path: Path):
    fs = Filesystem(tmp_path)
    modules = module_manager(tmp_path, fs)
    first = await modules.write("demo", "VALUE = 0\n")
    second = await modules.write("demo", "VALUE = 1\n", expected_hash=first["revision"])
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    path.write_text("VALUE = 0\n", encoding="utf-8")
    original_write = fs.write

    async def fail_index(path, text, **kwargs):
        if ".mypr/revisions/index/" in str(path):
            raise OSError("injected pre-commit failure")
        return await original_write(path, text, **kwargs)

    fs.write = fail_index
    with pytest.raises(RuntimeError, match="file change was rolled back"):
        await modules.write("demo", "VALUE = 1\n", expected_hash=first["revision"])

    assert path.read_text(encoding="utf-8") == "VALUE = 0\n"
    history = await modules.history("demo", limit=2)
    assert [item["revision"] for item in history["items"]] == [
        second["revision"],
        first["revision"],
    ]


@pytest.mark.asyncio
async def test_index_failure_preserves_concurrent_module_edit(tmp_path: Path):
    fs = Filesystem(tmp_path)
    modules = module_manager(tmp_path, fs)
    first = await modules.write("demo", "VALUE = 0\n")
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    original_write = fs.write
    concurrent = "VALUE = 9\n"

    async def concurrent_edit_then_fail(index_path, text, **kwargs):
        if ".mypr/revisions/index/" in str(index_path):
            path.write_text(concurrent, encoding="utf-8")
            raise OSError("injected index failure")
        return await original_write(index_path, text, **kwargs)

    fs.write = concurrent_edit_then_fail
    with pytest.raises(RuntimeError, match="Recovery did not overwrite the target") as error:
        await modules.write("demo", "VALUE = 1\n", expected_hash=first["revision"])

    assert path.read_text(encoding="utf-8") == concurrent
    assert first["revision"] in str(error.value)
    assert (await modules.read_revision("demo", first["revision"]))["text"] == "VALUE = 0\n"


@pytest.mark.asyncio
async def test_skill_index_failure_rolls_back_existing_file(tmp_path: Path):
    fs = Filesystem(tmp_path)
    skills = Skills(tmp_path, fs)
    first = await skills.write("demo", "# Old skill\n")
    path = tmp_path / ".mypr" / "skills" / "demo" / "SKILL.md"
    original_write = fs.write

    async def fail_index(path, text, **kwargs):
        if ".mypr/revisions/index/" in str(path):
            raise OSError("injected index failure")
        return await original_write(path, text, **kwargs)

    fs.write = fail_index
    with pytest.raises(RuntimeError, match="file change was rolled back"):
        await skills.write("demo", "# New skill\n", expected_hash=first["revision"])

    assert path.read_text(encoding="utf-8") == "# Old skill\n"


@pytest.mark.asyncio
async def test_new_file_rollback_preserves_concurrent_edit(tmp_path: Path):
    fs = Filesystem(tmp_path)
    modules = module_manager(tmp_path, fs)
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    original_write = fs.write
    concurrent = "VALUE = 9\n"

    async def concurrent_edit_then_fail(index_path, text, **kwargs):
        if ".mypr/revisions/index/" in str(index_path):
            path.write_text(concurrent, encoding="utf-8")
            raise OSError("injected index failure")
        return await original_write(index_path, text, **kwargs)

    fs.write = concurrent_edit_then_fail
    with pytest.raises(RuntimeError, match="Recovery did not overwrite the target"):
        await modules.write("demo", "VALUE = 1\n")

    assert path.read_text(encoding="utf-8") == concurrent


def test_create_rollback_restores_concurrent_broken_symlink(tmp_path: Path, monkeypatch):
    import mypr_mcp.revisions as revisions

    path = tmp_path / "created.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")
    original_replace = os.replace
    swapped = False

    def replace_with_broken_symlink(source, destination):
        nonlocal swapped
        if Path(source) == path and not swapped:
            swapped = True
            path.unlink()
            path.symlink_to("missing-target")
        return original_replace(source, destination)

    monkeypatch.setattr(revisions.os, "replace", replace_with_broken_symlink)
    with pytest.raises(RuntimeError, match="no longer a regular file"):
        revisions._unlink_if_revision(path, sha256("VALUE = 1\n"))

    assert path.is_symlink()
    assert os.readlink(path) == "missing-target"
