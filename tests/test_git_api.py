from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

import pytest

from mypr_mcp.git_api import Git


class ShellRunner:
    async def run(self, command, *, cwd, check, max_bytes, env=None):
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        return {
            "returncode": process.returncode,
            "stdout": stdout[:max_bytes].decode("utf-8", "replace"),
            "stderr": stderr.decode("utf-8", "replace"),
            "truncated": len(stdout) > max_bytes,
            "warnings": [],
        }


def _git(path: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=path, check=True, stdout=subprocess.PIPE)


@pytest.mark.asyncio
async def test_git_status_diff_and_show(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test")
    (tmp_path / "file.txt").write_text("one\n", encoding="utf-8")
    _git(tmp_path, "add", "file.txt")
    _git(tmp_path, "commit", "-qm", "initial")
    (tmp_path / "file.txt").write_text("two\n", encoding="utf-8")

    api = Git(tmp_path, ShellRunner())
    status = await api.status()
    assert status["branch"]["head"]
    assert status["files"][0]["path"] == "file.txt"
    diff = await api.diff()
    assert "-one" in diff["patch"] and "+two" in diff["patch"]
    shown = await api.show("HEAD", path="file.txt")
    assert shown["text"] == "one\n"


@pytest.mark.asyncio
async def test_git_views_page_and_reject_cross_view_cursors(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test")
    for index in range(5):
        (tmp_path / f"file-{index}.txt").write_text("old\n", encoding="utf-8")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "initial")
    for index in range(5):
        (tmp_path / f"file-{index}.txt").write_text("new\n", encoding="utf-8")

    api = Git(tmp_path, ShellRunner())
    first = await api.status(max_entries=1, max_bytes=1024)
    assert first["has_more"] and first["next_cursor"]
    seen = list(first["files"])
    cursor = first["next_cursor"]
    while cursor:
        page = await api.status(cursor=cursor, max_entries=1, max_bytes=1024)
        seen.extend(page["files"])
        cursor = page["next_cursor"]
    assert len(seen) == 5

    with pytest.raises(ValueError, match="different query"):
        await api.diff(cursor=first["cursor"] or first["next_cursor"])


def test_git_parses_rename_and_unmerged_records():
    rename = Git._status_record("2 R. N... 100644 100644 100644 a b R100 new.txt", "old.txt")
    assert rename["path"] == "new.txt"
    assert rename["original_path"] == "old.txt"
    assert Git._diff_files("R100\0old.txt\0new.txt\0") == [
        {"status": "R100", "path": "new.txt", "original_path": "old.txt"}
    ]


async def test_real_rename_conflict_and_nested_workspace(tmp_path: Path):
    _git(tmp_path, "init", "-q", "-b", "main")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "conflict.txt").write_text("base\n")
    (nested / "old.txt").write_text("rename me\n")
    (nested / "link").symlink_to("old.txt")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "base")
    _git(tmp_path, "checkout", "-qb", "side")
    (nested / "conflict.txt").write_text("side\n")
    _git(tmp_path, "commit", "-qam", "side")
    _git(tmp_path, "checkout", "-q", "main")
    (nested / "conflict.txt").write_text("main\n")
    _git(tmp_path, "commit", "-qam", "main")
    merged = await asyncio.to_thread(
        subprocess.run, ["git", "merge", "side"], cwd=tmp_path, capture_output=True
    )
    assert merged.returncode == 1
    _git(tmp_path, "mv", "nested/old.txt", "nested/new name\n.txt")
    api = Git(nested, ShellRunner())
    status = await api.status()
    assert status["root"] == str(tmp_path)
    conflict = next(item for item in status["files"] if item.get("unmerged"))
    assert conflict["path"] == "nested/conflict.txt"
    rename = next(item for item in status["files"] if item.get("original_path"))
    assert rename["path"] == "nested/new name\n.txt"
    assert rename["original_path"] == "nested/old.txt"
    shown = await api.show("HEAD", path="link")
    assert shown["text"] == "old.txt"
    diff = await api.diff(staged=True, paths=["new name\n.txt"])
    assert diff["files"]
    assert "new name" in diff["patch"]


async def test_git_diff_metadata_and_unicode_patch_are_paged_without_loss(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "test@example.com")
    _git(tmp_path, "config", "user.name", "Test")
    for i in range(40):
        (tmp_path / f"file-{i}.txt").write_text("old\n")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "base")
    for i in range(40):
        (tmp_path / f"file-{i}.txt").write_text("한글é" * 200 + "\n")
    api = Git(tmp_path, ShellRunner())
    page = await api.diff(max_bytes=1024)
    snapshot_id = page["snapshot_id"]
    files, pieces = [], []
    while True:
        files.extend(page["files"])
        pieces.append(page["patch"])
        assert len(page["patch"].encode()) <= 1024
        if not page["has_more"]:
            break
        page = await api.diff(cursor=page["next_cursor"], max_bytes=1024)
    snapshot = api.snapshots.load(snapshot_id)
    assert len(files) == 40
    assert "".join(pieces) == "".join(item.get("text", "") for item in snapshot["items"])
