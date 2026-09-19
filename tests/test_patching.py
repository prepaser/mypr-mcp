from __future__ import annotations

import asyncio
import os
import threading
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.patching import apply_patch, parse_patch


def patch_body(*parts: str) -> str:
    return "*** Begin Patch\n" + "\n".join(parts) + "\n*** End Patch"


async def test_multi_file_add_update_delete_and_move(tmp_path: Path):
    (tmp_path / "old.txt").write_text("one\ntwo\n")
    (tmp_path / "remove.txt").write_text("gone\n")
    fs = Filesystem(tmp_path)
    patch = patch_body(
        "*** Add File: new.txt",
        "+created",
        "*** Update File: old.txt",
        "@@ -1,2 +1,2 @@",
        " one",
        "-two",
        "+changed",
        "*** Update File: old.txt",
        "@@ -1,2 +1,2 @@",
        " one",
        "-changed",
        "+changed again",
        "*** Update File: old.txt",
        "*** Move to: moved.txt",
        "@@ -1,2 +1,2 @@",
        " one",
        "-changed again",
        "+final",
        "*** Delete File: remove.txt",
    )
    # Duplicate operations are rejected before any filesystem mutation.
    with pytest.raises(ValueError, match="conflicting"):
        await apply_patch(fs, patch)

    patch = patch_body(
        "*** Add File: new.txt",
        "+created",
        "*** Update File: old.txt",
        "@@ -1,2 +1,2 @@",
        " one",
        "-two",
        "+changed",
        "*** Update File: old.txt",
        "*** Move to: moved.txt",
        "@@ -1,2 +1,2 @@",
        " one",
        "-changed",
        "+final",
        "*** Delete File: remove.txt",
    )
    with pytest.raises(ValueError, match="conflicting"):
        await apply_patch(fs, patch)

    # Each source can occur once; use a single update in the real transaction.
    patch = patch_body(
        "*** Add File: new.txt",
        "+created",
        "*** Update File: old.txt",
        "@@ -1,2 +1,2 @@",
        " one",
        "-two",
        "+changed",
        "*** Delete File: remove.txt",
    )
    result = await apply_patch(fs, patch)
    assert [item["operation"] for item in result["changes"]] == ["add", "update", "delete"]
    assert (tmp_path / "new.txt").read_text() == "created\n"
    assert (tmp_path / "old.txt").read_text() == "one\nchanged\n"
    assert not (tmp_path / "remove.txt").exists()

    moved = await apply_patch(
        fs,
        patch_body(
            "*** Update File: old.txt",
            "*** Move to: moved.txt",
            "@@ -1,2 +1,2 @@",
            " one",
            "-changed",
            "+final",
        ),
    )
    assert moved["changes"][0]["operation"] == "move"
    assert not (tmp_path / "old.txt").exists()
    assert (tmp_path / "moved.txt").read_text() == "one\nfinal\n"


async def test_failed_hunk_and_stale_hash_are_preflight_atomic(tmp_path: Path):
    (tmp_path / "one.txt").write_text("one\n")
    (tmp_path / "two.txt").write_text("two\n")
    fs = Filesystem(tmp_path)
    bad = patch_body(
        "*** Update File: one.txt",
        "@@ -1,1 +1,1 @@",
        "-one",
        "+ONE",
        "*** Update File: two.txt",
        "@@ -1,1 +1,1 @@",
        "-missing",
        "+TWO",
    )
    with pytest.raises(ValueError, match="context"):
        await apply_patch(fs, bad)
    assert (tmp_path / "one.txt").read_text() == "one\n"
    assert (tmp_path / "two.txt").read_text() == "two\n"
    with pytest.raises(ValueError, match="Revision mismatch"):
        await apply_patch(
            fs,
            patch_body(
                "*** Update File: one.txt",
                "@@ -1,1 +1,1 @@",
                "-one",
                "+ONE",
            ),
            expected_hashes={"one.txt": "0" * 64},
        )


async def test_commit_failure_rolls_back_previous_files(tmp_path: Path, monkeypatch):
    (tmp_path / "one.txt").write_text("one\n")
    (tmp_path / "two.txt").write_text("two\n")
    fs = Filesystem(tmp_path)
    patch = patch_body(
        "*** Update File: one.txt",
        "@@ -1,1 +1,1 @@",
        "-one",
        "+ONE",
        "*** Update File: two.txt",
        "@@ -1,1 +1,1 @@",
        "-two",
        "+TWO",
    )
    original = os.replace
    calls = 0

    def fail_second(source, destination):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected replace failure")
        return original(source, destination)

    monkeypatch.setattr("mypr_mcp.patching.os.replace", fail_second)
    with pytest.raises(OSError, match="injected"):
        await apply_patch(fs, patch)
    assert (tmp_path / "one.txt").read_text() == "one\n"
    assert (tmp_path / "two.txt").read_text() == "two\n"
    assert not await asyncio.to_thread(lambda: list(tmp_path.glob(".*.rollback.*")))


async def test_rollback_preserves_backup_after_external_change(tmp_path: Path, monkeypatch):
    (tmp_path / "one.txt").write_text("one\n")
    (tmp_path / "two.txt").write_text("two\n")
    fs = Filesystem(tmp_path)
    patch = patch_body(
        "*** Update File: one.txt",
        "@@ -1,1 +1,1 @@",
        "-one",
        "+ONE",
        "*** Update File: two.txt",
        "@@ -1,1 +1,1 @@",
        "-two",
        "+TWO",
    )
    original = os.replace
    calls = 0

    def fail_after_external_change(source, destination):
        nonlocal calls
        calls += 1
        if calls == 2:
            (tmp_path / "one.txt").chmod(0o600)
            raise OSError("injected replace failure")
        return original(source, destination)

    monkeypatch.setattr("mypr_mcp.patching.os.replace", fail_after_external_change)
    with pytest.raises(RuntimeError, match="rollback was incomplete"):
        await apply_patch(fs, patch)
    backups = await asyncio.to_thread(lambda: list(tmp_path.glob(".*.mypr-backup.*")))
    assert backups
    for backup in backups:
        backup.unlink()


async def test_move_is_rolled_back_when_a_later_file_fails(tmp_path: Path, monkeypatch):
    (tmp_path / "source.txt").write_text("source\n")
    (tmp_path / "later.txt").write_text("later\n")
    fs = Filesystem(tmp_path)
    patch = patch_body(
        "*** Update File: source.txt",
        "*** Move to: moved.txt",
        "@@ -1,1 +1,1 @@",
        "-source",
        "+moved",
        "*** Update File: later.txt",
        "@@ -1,1 +1,1 @@",
        "-later",
        "+LATER",
    )
    original = os.replace

    def fail_later(source, destination):
        if destination == tmp_path / "later.txt":
            raise OSError("injected later failure")
        return original(source, destination)

    monkeypatch.setattr("mypr_mcp.patching.os.replace", fail_later)
    with pytest.raises(OSError, match="injected later"):
        await apply_patch(fs, patch)
    assert (tmp_path / "source.txt").read_text() == "source\n"
    assert not (tmp_path / "moved.txt").exists()
    assert (tmp_path / "later.txt").read_text() == "later\n"


async def test_parent_child_paths_are_rejected_before_writes(tmp_path: Path):
    fs = Filesystem(tmp_path)
    with pytest.raises(ValueError, match="parent and child"):
        await apply_patch(
            fs,
            patch_body(
                "*** Add File: generated",
                "+file",
                "*** Add File: generated/result.txt",
                "+result",
            ),
        )
    assert not (tmp_path / "generated").exists()


async def test_postcommit_stat_failure_is_rolled_back(tmp_path: Path, monkeypatch):
    target = tmp_path / "value.txt"
    target.write_text("old\n")
    fs = Filesystem(tmp_path)
    original = Path.stat
    failed = False

    def fail_after_replace(path, *args, **kwargs):
        nonlocal failed
        result = original(path, *args, **kwargs)
        if path == target and not failed and path.read_bytes() == b"new\n":
            failed = True
            raise OSError("injected postcommit stat failure")
        return result

    monkeypatch.setattr(Path, "stat", fail_after_replace)
    with pytest.raises(OSError, match="postcommit stat"):
        await apply_patch(
            fs,
            patch_body(
                "*** Update File: value.txt",
                "@@ -1,1 +1,1 @@",
                "-old",
                "+new",
            ),
        )
    assert target.read_text() == "old\n"


async def test_postlink_cleanup_failure_is_rolled_back_and_reported(tmp_path: Path, monkeypatch):
    fs = Filesystem(tmp_path)
    original = Path.unlink
    temporary: Path | None = None

    def fail_temp_unlink(path, *args, **kwargs):
        nonlocal temporary
        if path.name.startswith(".value.txt.") and path.name != "value.txt":
            temporary = path
            raise OSError("injected temporary cleanup failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_temp_unlink)
    with pytest.raises(OSError, match="temporary cleanup"):
        await apply_patch(fs, patch_body("*** Add File: value.txt", "+value"))
    assert not (tmp_path / "value.txt").exists()
    assert temporary is not None
    assert await asyncio.to_thread(temporary.exists)
    await asyncio.to_thread(original, temporary, missing_ok=True)


async def test_noop_update_does_not_replace_file(tmp_path: Path):
    target = tmp_path / "value.txt"
    target.write_text("value\n")
    fs = Filesystem(tmp_path)
    before = await asyncio.to_thread(target.stat)
    result = await apply_patch(
        fs,
        patch_body(
            "*** Update File: value.txt",
            "@@ -1,1 +1,1 @@",
            " value",
        ),
    )
    after = await asyncio.to_thread(target.stat)
    assert result["changes"][0]["changed"] is False
    assert (before.st_dev, before.st_ino, before.st_mtime_ns) == (
        after.st_dev,
        after.st_ino,
        after.st_mtime_ns,
    )


async def test_dry_run_diff_and_end_of_file(tmp_path: Path):
    (tmp_path / "value.txt").write_text("old\n")
    fs = Filesystem(tmp_path)
    result = await apply_patch(
        fs,
        patch_body(
            "*** Update File: value.txt",
            "@@ -1,1 +1,1 @@",
            "-old",
            "+new",
            "*** End of File",
        ),
        dry_run=True,
    )
    assert result["dry_run"]
    assert "-old" in result["diff"]
    assert (tmp_path / "value.txt").read_text() == "old\n"
    await apply_patch(
        fs,
        patch_body(
            "*** Update File: value.txt",
            "@@ -1,1 +1,1 @@",
            "-old",
            "+new",
            "*** End of File",
        ),
    )
    assert (tmp_path / "value.txt").read_bytes() == b"new\n"


async def test_update_preserves_crlf_and_existing_no_final_newline(tmp_path: Path):
    (tmp_path / "windows.txt").write_bytes(b"before\r\nafter\r\n")
    (tmp_path / "plain.txt").write_bytes(b"before")
    fs = Filesystem(tmp_path)
    await apply_patch(
        fs,
        patch_body(
            "*** Update File: windows.txt",
            "@@ -1,2 +1,2 @@",
            "-before",
            "+changed",
            " after",
        ),
    )
    await apply_patch(
        fs,
        patch_body(
            "*** Update File: plain.txt",
            "@@ -1,1 +1,1 @@",
            "-before",
            "+changed",
        ),
    )
    assert (tmp_path / "windows.txt").read_bytes() == b"changed\r\nafter\r\n"
    assert (tmp_path / "plain.txt").read_bytes() == b"changed"


async def test_update_supports_zero_line_insertion_hunk(tmp_path: Path):
    (tmp_path / "empty.txt").write_text("")
    fs = Filesystem(tmp_path)
    await apply_patch(
        fs,
        patch_body(
            "*** Update File: empty.txt",
            "@@ -0,0 +1,1 @@",
            "+inserted",
        ),
    )
    assert (tmp_path / "empty.txt").read_text() == "inserted\n"


async def test_cancellation_waits_for_worker_and_cleans_staging(tmp_path: Path, monkeypatch):
    (tmp_path / "value.txt").write_text("old\n")
    fs = Filesystem(tmp_path)
    started = threading.Event()
    release = threading.Event()
    original = os.replace

    def delayed_replace(source, destination):
        started.set()
        release.wait(timeout=5)
        return original(source, destination)

    monkeypatch.setattr("mypr_mcp.patching.os.replace", delayed_replace)
    task = asyncio.create_task(
        apply_patch(
            fs,
            patch_body(
                "*** Update File: value.txt",
                "@@ -1,1 +1,1 @@",
                "-old",
                "+value",
            ),
        )
    )
    await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (tmp_path / "value.txt").read_text() == "value\n"
    assert not await asyncio.to_thread(lambda: list(tmp_path.glob(".*")))


async def test_alias_and_symlink_are_rejected(tmp_path: Path):
    target = tmp_path / "target.txt"
    target.write_text("old\n")
    link = tmp_path / "link.txt"
    link.symlink_to(target)
    fs = Filesystem(tmp_path)
    with pytest.raises(ValueError, match="symlink"):
        await apply_patch(
            fs,
            patch_body(
                "*** Update File: link.txt",
                "@@ -1,1 +1,1 @@",
                "-old",
                "+new",
            ),
        )
    with pytest.raises(ValueError, match="alias"):
        await apply_patch(
            fs,
            patch_body(
                "*** Update File: target.txt",
                "@@ -1,1 +1,1 @@",
                "-old",
                "+new",
                "*** Update File: target.txt",
                "@@ -1,1 +1,1 @@",
                "-old",
                "+new",
            ),
        )


async def test_worker_cleanup_leaves_no_temporary_files(tmp_path: Path):
    fs = Filesystem(tmp_path)
    await apply_patch(fs, patch_body("*** Add File: value.txt", "+value"))
    assert not await asyncio.to_thread(lambda: list(tmp_path.glob(".*.")))


def test_parser_requires_native_boundaries():
    with pytest.raises(ValueError):
        parse_patch("*** Begin Patch\n*** Add File: x\n+x\n")
