from __future__ import annotations

import asyncio
import json
import os
import subprocess
import threading
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


def _git(path: Path, *args: str, env=None) -> str:
    result = subprocess.run(
        ["git", *args], cwd=path, env=env, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def _commit(path: Path, message: str, author: str, date: str) -> str:
    _git(path, "config", "user.name", author)
    _git(path, "config", "user.email", f"{author.lower().replace(' ', '.')}@example.test")
    env = {
        **os.environ,
        "GIT_AUTHOR_DATE": date,
        "GIT_COMMITTER_DATE": date,
    }
    _git(path, "add", "-A")
    _git(path, "commit", "-qm", message, env=env)
    return _git(path, "rev-parse", "HEAD")


@pytest.mark.asyncio
async def test_log_filters_and_pages_are_pinned_to_initial_commit(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    weird = "src/name with newline\né.txt"
    target = tmp_path / weird
    target.parent.mkdir()
    target.write_text("first\nsecond\n", encoding="utf-8")
    first = _commit(tmp_path, "first file", "Ada One", "2020-06-01T12:00:00+0000")

    target.write_text("first\n한글\n", encoding="utf-8")
    second = _commit(tmp_path, "second file", "Ada One", "2021-06-01T12:00:00+0000")

    (tmp_path / "other.txt").write_text("other\n", encoding="utf-8")
    head = _commit(tmp_path, "unrelated", "Grace Two", "2022-06-01T12:00:00+0000")

    api = Git(tmp_path, ShellRunner())
    filtered = await api.log(
        path=weird,
        author="Ada One",
        since="2020-01-01",
        until="2020-12-31",
    )
    assert [item["commit"] for item in filtered["commits"]] == [first]
    assert filtered["commits"][0]["author"] == "Ada One"
    assert filtered["commits"][0]["email"] == "ada.one@example.test"
    assert filtered["commits"][0]["subject"] == "first file"

    page = await api.log(path=weird, max_entries=1, max_bytes=1024)
    assert page["ref"] == head
    seen = [item["commit"] for item in page["commits"]]
    cursor = page["next_cursor"]
    current = tmp_path / "later.txt"
    current.write_text("later\n", encoding="utf-8")
    later = _commit(tmp_path, "after first page", "Lin Three", "2023-06-01T12:00:00+0000")
    while cursor:
        page = await api.log(cursor=cursor, max_entries=1, max_bytes=1024)
        seen.extend(item["commit"] for item in page["commits"])
        cursor = page["next_cursor"]
    assert seen == [second, first]
    assert later not in seen

    pinned = await api.log(ref=first)
    assert pinned["ref"] == first
    assert [item["commit"] for item in pinned["commits"]] == [first]


@pytest.mark.asyncio
async def test_blame_ranges_unicode_paths_and_pages_use_saved_snapshot(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    relative = "nested/file\n*名.txt"
    path = tmp_path / relative
    path.parent.mkdir()
    path.write_text("alpha\nbeta\n", encoding="utf-8")
    first = _commit(tmp_path, "initial", "Ada", "2020-06-01T12:00:00+0000")

    path.write_text("alpha\n한글\n", encoding="utf-8")
    second = _commit(tmp_path, "update", "Lin", "2021-06-01T12:00:00+0000")

    api = Git(tmp_path, ShellRunner())
    page = await api.blame(relative, max_entries=1, max_bytes=1024)
    assert page["path"] == relative
    assert page["lines"][0]["line"] == 1
    assert page["lines"][0]["commit"] == first
    cursor = page["next_cursor"]

    path.write_text("new\ncontent\n", encoding="utf-8")
    _commit(tmp_path, "later", "Grace", "2022-06-01T12:00:00+0000")
    second_page = await api.blame(cursor=cursor, max_entries=1, max_bytes=1024)
    assert second_page["ref"] == second
    assert second_page["lines"][0]["line"] == 2
    assert second_page["lines"][0]["text"] == "한글"
    assert second_page["lines"][0]["commit"] == second

    selected = await api.blame(relative, ref=first, start_line=2, end_line=2)
    assert len(selected["lines"]) == 1
    assert selected["lines"][0]["line"] == 2
    assert selected["lines"][0]["text"] == "beta"
    assert selected["ref"] == first

    with pytest.raises(ValueError, match="provided together"):
        await api.blame(relative, start_line=1)
    with pytest.raises(ValueError, match="line range"):
        await api.blame(relative, start_line=3, end_line=2)


@pytest.mark.asyncio
async def test_history_cursors_validate_explicit_filters_and_keep_nondefaults(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    path = tmp_path / "file.txt"
    path.write_text("one\ntwo\n", encoding="utf-8")
    first = _commit(tmp_path, "first", "Ada", "2020-06-01T12:00:00+0000")
    path.write_text("one\nthree\n", encoding="utf-8")
    second = _commit(tmp_path, "second", "Ada", "2021-06-01T12:00:00+0000")

    api = Git(tmp_path, ShellRunner())
    log = await api.log(
        ref=second,
        path="file.txt",
        author="Ada",
        since="2020-01-01",
        until="2022-01-01",
        follow=False,
        max_entries=1,
        max_bytes=1024,
    )
    assert log["has_more"]
    resumed = await api.log(cursor=log["next_cursor"], max_entries=10, max_bytes=4096)
    assert [item["commit"] for item in resumed["commits"]] == [first]
    matching = await api.log(
        ref=second,
        path="./file.txt",
        author="Ada",
        since="2020-01-01",
        until="2022-01-01",
        follow=False,
        cursor=log["next_cursor"],
        max_entries=10,
        max_bytes=4096,
    )
    assert matching["commits"] == resumed["commits"]
    with pytest.raises(ValueError, match="different query"):
        await api.log(author="Grace", cursor=log["next_cursor"])
    with pytest.raises(TypeError, match="follow must be a boolean"):
        await api.log(cursor=log["next_cursor"], follow=0)

    blame = await api.blame("file.txt", ref=second, start_line=1, end_line=2, max_entries=1)
    assert blame["has_more"]
    resumed_blame = await api.blame(cursor=blame["next_cursor"], max_entries=10)
    assert [item["line"] for item in resumed_blame["lines"]] == [2]
    with pytest.raises(ValueError, match="different query"):
        await api.blame(ref=first, cursor=blame["next_cursor"])
    with pytest.raises(ValueError, match="start_line must be a positive integer"):
        await api.blame(cursor=blame["next_cursor"], start_line=True)


@pytest.mark.asyncio
async def test_show_cursor_validates_ref_and_path_but_allows_cursor_only(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    path = tmp_path / "file.txt"
    path.write_text("content\n" * 1000, encoding="utf-8")
    commit = _commit(tmp_path, "initial", "Ada", "2020-06-01T12:00:00+0000")
    api = Git(tmp_path, ShellRunner())

    page = await api.show(commit, path="file.txt", max_bytes=1024)
    assert page["has_more"]
    resumed = await api.show(cursor=page["next_cursor"], max_bytes=4096)
    matching = await api.show(
        commit,
        path="./file.txt",
        cursor=page["next_cursor"],
        max_bytes=4096,
    )
    assert matching["text"] == resumed["text"]
    with pytest.raises(ValueError, match="different query"):
        await api.show("HEAD", cursor=page["next_cursor"])


@pytest.mark.asyncio
async def test_history_pages_enforce_record_budget_and_cursor_kind(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    (tmp_path / "large.txt").write_text("x" * 2000, encoding="utf-8")
    _commit(tmp_path, "large", "Ada", "2020-06-01T12:00:00+0000")
    (tmp_path / "small.txt").write_text("small\n", encoding="utf-8")
    _commit(tmp_path, "small", "Grace", "2021-06-01T12:00:00+0000")
    api = Git(tmp_path, ShellRunner())

    with pytest.raises(ValueError, match="increase the budget"):
        await api.blame("large.txt", max_bytes=1024)

    log = await api.log(max_entries=1)
    with pytest.raises(ValueError, match="different query"):
        await api.blame("large.txt", cursor=log["next_cursor"])


@pytest.mark.asyncio
async def test_history_snapshot_retention_is_bounded_under_concurrent_queries(tmp_path: Path):
    api = Git(tmp_path, ShellRunner())
    snapshots = await asyncio.gather(
        *(
            asyncio.to_thread(
                api._create_history_snapshot,
                {"query": index},
                [],
                kind="log",
                root=str(tmp_path),
                ref="0" * 40,
                truncated=False,
                warnings=[],
            )
            for index in range(40)
        )
    )
    ids = [ident for ident, _ in snapshots]
    files = list(api.history_snapshots.root.glob("*.json"))
    assert len(files) == 32
    assert all(len(ident) == 32 for ident in ids)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["log", "blame"])
async def test_new_history_page_survives_eviction_before_initial_page_build(
    tmp_path: Path, monkeypatch, method: str
):
    _git(tmp_path, "init", "-q")
    (tmp_path / "file.txt").write_text("one\n", encoding="utf-8")
    _commit(tmp_path, "initial", "Ada", "2020-06-01T12:00:00+0000")
    monkeypatch.setattr("mypr_mcp.git_api._MAX_HISTORY_SNAPSHOTS", 1)

    runner = ShellRunner()
    first, second = Git(tmp_path, runner), Git(tmp_path, runner)
    create = first._create_history_snapshot
    created = threading.Event()
    release = threading.Event()

    def pause_after_create(*args, **kwargs):
        snapshot = create(*args, **kwargs)
        created.set()
        if not release.wait(5):
            raise TimeoutError("history snapshot test gate timed out")
        return snapshot

    monkeypatch.setattr(first, "_create_history_snapshot", pause_after_create)

    async def query(api: Git):
        if method == "log":
            return await api.log()
        return await api.blame("file.txt")

    initial = asyncio.create_task(query(first))
    try:
        assert await asyncio.to_thread(created.wait, 5)
        await query(second)
    finally:
        release.set()
        page = await initial

    assert page["commits" if method == "log" else "lines"]
    assert not (first.history_snapshots.root / f"{page['snapshot_id']}.json").exists()


@pytest.mark.asyncio
async def test_commit_info_includes_root_metadata_unicode_files_and_exact_pages(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    (tmp_path / "한글.txt").write_text("one\n", encoding="utf-8")
    first = _commit(tmp_path, "initial body", "Ada", "2020-06-01T12:00:00+0000")
    api = Git(tmp_path, ShellRunner())
    page = await api.commit_info(first, max_bytes=1024)
    assert page["ref"] == first
    assert page["commit"]["root"] is True
    assert page["commit"]["parents"] == []
    assert page["commit"]["body"] == "initial body"
    assert page["files"] == [
        {"status": "A", "path": "한글.txt", "additions": 1, "deletions": 0}
    ]
    assert len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()) <= 1024


@pytest.mark.asyncio
async def test_commit_info_patch_pages_are_bounded_and_reassemble(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    (tmp_path / "large.txt").write_text("한글 line\n" * 3000, encoding="utf-8")
    commit = _commit(tmp_path, "large patch", "Ada", "2020-06-01T12:00:00+0000")
    api = Git(tmp_path, ShellRunner())

    page = await api.commit_info(
        commit, include_files=False, include_patch=True, max_bytes=4096
    )
    parts = [page["patch"]]
    while page["next_cursor"]:
        encoded = json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()
        assert len(encoded) <= 4096
        page = await api.commit_info(cursor=page["next_cursor"], max_bytes=4096)
        parts.append(page["patch"])
    encoded = json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()
    assert len(encoded) <= 4096

    complete = await api.commit_info(
        commit, include_files=False, include_patch=True, max_bytes=16 * 1024 * 1024
    )
    assert "".join(parts) == complete["patch"]


@pytest.mark.asyncio
async def test_commit_info_initial_snapshot_id_is_included_in_budget(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    (tmp_path / "empty.txt").write_text("content\n", encoding="utf-8")
    _commit(tmp_path, "description " + "a" * 1500, "Ada", "2020-06-01T12:00:00+0000")
    api = Git(tmp_path, ShellRunner())
    page = await api.commit_info(include_files=False, include_patch=False)
    budget_without_snapshot_id = len(
        json.dumps(
            {key: value for key, value in page.items() if key != "snapshot_id"},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    )

    with pytest.raises(ValueError, match="metadata"):
        await api.commit_info(
            include_files=False,
            include_patch=False,
            max_bytes=budget_without_snapshot_id,
        )

    page = await api.commit_info()
    exact_budget = len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode())
    fitted = await api.commit_info(max_bytes=exact_budget)
    assert fitted["files"] == page["files"]
    encoded = json.dumps(fitted, ensure_ascii=False, separators=(",", ":")).encode()
    assert len(encoded) == exact_budget


@pytest.mark.asyncio
async def test_log_follow_requires_path_and_preserves_path_history(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    path = tmp_path / "file.txt"
    path.write_text("one\n", encoding="utf-8")
    first = _commit(tmp_path, "first", "Ada", "2020-06-01T12:00:00+0000")
    path.rename(tmp_path / "renamed.txt")
    second = _commit(tmp_path, "rename", "Ada", "2021-06-01T12:00:00+0000")
    api = Git(tmp_path, ShellRunner())
    with pytest.raises(ValueError, match="requires path"):
        await api.log(follow=True)
    result = await api.log(path="renamed.txt", follow=True)
    assert [item["commit"] for item in result["commits"]] == [second, first]


async def test_merge_commit_info_compares_files_stats_and_patch_to_first_parent(tmp_path: Path):
    _git(tmp_path, "init", "-q", "-b", "main")
    (tmp_path / "base.txt").write_text("base\n")
    _commit(tmp_path, "base", "Ada", "2020-06-01T12:00:00+0000")
    _git(tmp_path, "checkout", "-qb", "side")
    (tmp_path / "side.txt").write_text("side\n")
    _commit(tmp_path, "side", "Ada", "2021-06-01T12:00:00+0000")
    _git(tmp_path, "checkout", "-q", "main")
    (tmp_path / "main.txt").write_text("main\n")
    parent = _commit(tmp_path, "main", "Ada", "2022-06-01T12:00:00+0000")
    _git(tmp_path, "merge", "--no-ff", "-qm", "merge", "side")
    result = await Git(tmp_path, ShellRunner()).commit_info(include_patch=True)
    assert result["commit"]["comparison_base"] == parent
    assert result["files"] == [{"status": "A", "path": "side.txt", "additions": 1, "deletions": 0}]
    assert "+side\n" in result["patch"]
    assert "main.txt" not in result["patch"]


async def test_commit_info_rename_statistics_preserve_tabbed_paths(tmp_path: Path):
    _git(tmp_path, "init", "-q")
    old = tmp_path / "a\told\tname.txt"
    old.write_text("unchanged\n")
    (tmp_path / "z.txt").write_text("one\n")
    _commit(tmp_path, "initial", "Ada", "2020-06-01T12:00:00+0000")
    old.rename(tmp_path / "b\tnew\tname.txt")
    (tmp_path / "z.txt").write_text("one\ntwo\nthree\n")
    _commit(tmp_path, "rename and update", "Ada", "2021-06-01T12:00:00+0000")
    result = await Git(tmp_path, ShellRunner()).commit_info()
    files = {item["path"]: item for item in result["files"]}
    assert files["b\tnew\tname.txt"]["old_path"] == "a\told\tname.txt"
    assert files["b\tnew\tname.txt"]["additions"] == 0
    assert files["z.txt"]["additions"] == 2
