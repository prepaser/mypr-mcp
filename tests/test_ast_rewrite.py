from __future__ import annotations

import asyncio
import os
import shutil
import stat
from pathlib import Path

import pytest

from mypr_mcp.ast_rewrite import (
    _apply_matches,
    _NewlineTracker,
    _OutputLimit,
    _PlanStore,
    _write_once,
)
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.patching import _sha256


class LocalShell:
    async def run(  # noqa: ASYNC109
        self, command, *, cwd, timeout, check, max_bytes, input=None  # noqa: ASYNC109
    ):
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            stdin=asyncio.subprocess.PIPE if input is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(timeout):
                stdout, stderr = await process.communicate(
                    None if input is None else input.encode("utf-8")
                )
        except TimeoutError:
            process.kill()
            await process.wait()
            return {"returncode": process.returncode, "stdout": "", "stderr": "", "timed_out": True}
        truncated = len(stdout) + len(stderr) > max_bytes
        remaining = max_bytes
        out = stdout[:remaining]
        remaining -= len(out)
        err = stderr[:remaining]
        return {
            "returncode": process.returncode,
            "stdout": out.decode("utf-8", "ignore"),
            "stderr": err.decode("utf-8", "ignore"),
            "truncated": truncated,
            "error": None,
            "timed_out": False,
        }


def local_fs(path: Path) -> Filesystem:
    return Filesystem(path, LocalShell())


pytestmark = pytest.mark.skipif(
    shutil.which("ast-grep") is None, reason="ast-grep is not installed"
)


async def test_rewrite_preview_is_immutable_and_apply_preserves_crlf(tmp_path: Path):
    source = tmp_path / "sample.py"
    source.write_bytes(b"foo(1)\r\nfoo(2)\r\n")
    fs = local_fs(tmp_path)

    preview = await fs.rewrite_ast(
        "foo($A)", replacement="bar(\n    $A\n)", lang="python", paths="sample.py"
    )

    assert preview["applicable"] is True
    assert preview["complete"] is True
    assert preview["changed_files"] == 1
    assert preview["changes"][0]["old_revision"] == _sha256(b"foo(1)\r\nfoo(2)\r\n")
    assert source.read_bytes() == b"foo(1)\r\nfoo(2)\r\n"
    applied = await fs.apply_rewrite(preview["plan_id"])
    assert applied["applied"] is True
    assert source.read_bytes() == b"bar(\r\n    1\r\n)\r\nbar(\r\n    2\r\n)\r\n"


async def test_rule_rewrite_and_stale_source_rejection(tmp_path: Path):
    source = tmp_path / "sample.py"
    source.write_text("foo('é')\n", encoding="utf-8")
    fs = local_fs(tmp_path)

    preview = await fs.rewrite_ast(
        rule={"pattern": "foo($A)"},
        replacement="bar($A)",
        lang="python",
        paths="sample.py",
    )
    source.write_text("external()\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="changed since preview"):
        await fs.apply_rewrite(preview["plan_id"])
    assert source.read_text(encoding="utf-8") == "external()\n"


async def test_multifile_commit_failure_rolls_back(tmp_path: Path, monkeypatch):
    first = tmp_path / "first.py"
    second = tmp_path / "second.py"
    first.write_text("foo(1)\n", encoding="utf-8")
    second.write_text("foo(2)\n", encoding="utf-8")
    fs = local_fs(tmp_path)
    preview = await fs.rewrite_ast("foo($A)", replacement="bar($A)", lang="python")
    original = os.replace
    calls = 0

    def fail_second(source, destination):
        nonlocal calls
        if destination in (first, second):
            calls += 1
            if calls == 2:
                raise OSError("injected replace failure")
        return original(source, destination)

    monkeypatch.setattr("mypr_mcp.patching.os.replace", fail_second)
    with pytest.raises(OSError, match="injected"):
        await fs.apply_rewrite(preview["plan_id"])
    assert first.read_text(encoding="utf-8") == "foo(1)\n"
    assert second.read_text(encoding="utf-8") == "foo(2)\n"


async def test_no_matches_returns_complete_nonapplicable_result(tmp_path: Path):
    (tmp_path / "sample.py").write_text("other()\n", encoding="utf-8")
    result = await local_fs(tmp_path).rewrite_ast(
        "foo($A)", replacement="bar($A)", lang="python"
    )
    assert result["complete"] is True
    assert result["applicable"] is False
    assert result["reason"] == "no_matches"
    assert result["plan_id"] is None


async def test_large_source_stops_before_reading_full_contents(tmp_path: Path):
    source = tmp_path / "large.py"
    with source.open("wb") as stream:
        stream.write(b"foo()\n")
        stream.truncate(16 * 1024 * 1024 + 1)

    result = await local_fs(tmp_path).rewrite_ast(
        "foo()", replacement="bar()", lang="python", paths="large.py"
    )

    assert result["complete"] is False
    assert result["applicable"] is False
    assert result["reason"] == "byte_limit"
    assert result["plan_id"] is None


def test_overlapping_ast_replacements_are_rejected():
    records = [
        {"replacementOffsets": {"start": 0, "end": 4}, "replacement": "one"},
        {"replacementOffsets": {"start": 2, "end": 6}, "replacement": "two"},
    ]

    with pytest.raises(ValueError, match="overlapping"):
        _apply_matches(b"abcdef", records, "sample.py")


def test_ast_config_write_handles_max_length_target_name(tmp_path: Path):
    target = tmp_path / ("x" * 255)
    _write_once(target, "ruleDirs: []\n")
    assert target.read_text() == "ruleDirs: []\n"


def test_ast_rewrite_offsets_must_use_utf8_boundaries():
    records = [
        {"replacementOffsets": {"start": 1, "end": 2}, "replacement": "x"},
    ]

    with pytest.raises(RuntimeError, match="UTF-8 offsets"):
        _apply_matches("é".encode(), records, "sample.py")


def test_ast_rewrite_output_limit_is_enforced_during_construction():
    records = [
        {"replacementOffsets": {"start": 0, "end": 3}, "replacement": "x" * 8},
    ]

    with pytest.raises(_OutputLimit):
        _apply_matches(b"foo", records, "sample.py", max_bytes=3)


def test_newline_tracker_scans_each_match_gap_once():
    class CountingBytes(bytes):
        def __new__(cls, value):
            result = super().__new__(cls, value)
            result.find_calls = 0
            result.rfind_calls = 0
            return result

        def find(self, *args):
            self.find_calls += 1
            return super().find(*args)

        def rfind(self, *args):
            self.rfind_calls += 1
            return super().rfind(*args)

    data = CountingBytes(b"line\n" * 512 + b"tail")
    tracker = _NewlineTracker(data)
    offsets = list(range(0, len(data) + 1, 17))
    assert all(tracker.style(offset) == "\n" for offset in offsets)
    assert data.find_calls <= 2
    assert data.rfind_calls == 2 * (len(offsets) - 1)

    no_newlines = CountingBytes(b"x" * 4096)
    tracker = _NewlineTracker(no_newlines)
    assert all(tracker.style(offset) == "\n" for offset in offsets)
    assert no_newlines.find_calls == 2
    assert no_newlines.rfind_calls == 2 * (len(offsets) - 1)
    mixed = _NewlineTracker(b"a\rb\nc\r\nd")
    assert [mixed.style(offset) for offset in (0, 2, 4, 6, 7, 8)] == [
        "\r", "\r", "\n", "\r\n", "\r\n", "\r\n",
    ]


def test_plan_store_rejects_fifo_without_waiting_for_a_writer(tmp_path: Path):
    store = _PlanStore(tmp_path / "rewrites")
    ident = "0" * 32
    os.mkfifo(store.root / f"{ident}.json")

    with pytest.raises(RuntimeError, match="invalid rewrite plan file"):
        store.load(ident)


async def test_empty_path_and_glob_lists_are_rejected(tmp_path: Path):
    fs = local_fs(tmp_path)

    with pytest.raises(ValueError, match="paths"):
        await fs.rewrite_ast(
            "foo()", replacement="bar()", lang="python", paths=[]
        )
    with pytest.raises(ValueError, match="glob"):
        await fs.rewrite_ast(
            "foo()", replacement="bar()", lang="python", glob=[]
        )


async def test_copied_plan_is_rejected_in_another_workspace(tmp_path: Path):
    first_workspace = tmp_path / "first"
    second_workspace = tmp_path / "second"
    first_workspace.mkdir()
    second_workspace.mkdir()
    source = first_workspace / "sample.py"
    source.write_text("foo()\n", encoding="utf-8")
    first_fs = local_fs(first_workspace)
    preview = await first_fs.rewrite_ast(
        "foo()", replacement="bar()", lang="python", paths="sample.py"
    )
    original = first_workspace / ".mypr" / "rewrites" / f"{preview['plan_id']}.json"
    copied_root = second_workspace / ".mypr" / "rewrites"
    copied_root.mkdir(parents=True)
    (copied_root / original.name).write_bytes(original.read_bytes())

    with pytest.raises(ValueError, match="different or moved workspace"):
        await local_fs(second_workspace).apply_rewrite(preview["plan_id"])
    assert source.read_text(encoding="utf-8") == "foo()\n"


async def test_diff_truncation_keeps_metadata_for_every_changed_file(tmp_path: Path):
    first = tmp_path / "a.py"
    second = tmp_path / "b.py"
    first.write_bytes(b"foo(1)\n" * 6000)
    second.write_text("foo(2)\n", encoding="utf-8")
    fs = local_fs(tmp_path)
    preview = await fs.rewrite_ast("foo($A)", replacement="bar($A)", lang="python")

    assert preview["diff_truncated"] is True
    assert preview["changed_files"] == 2
    assert {change["path"] for change in preview["changes"]} == {"a.py", "b.py"}
    result = await fs.apply_rewrite(preview["plan_id"])
    assert len(result["changes"]) == 2


@pytest.mark.parametrize("failure", ["fsync", "replace"])
def test_failed_new_plan_install_does_not_evict_existing_preview(
    tmp_path: Path, monkeypatch, failure: str
):
    store = _PlanStore(tmp_path / "rewrites")
    monkeypatch.setattr("mypr_mcp.ast_rewrite._STORE_PLANS", 1)
    existing = store.create({"files": []})
    original = os.replace
    original_fsync = os.fsync

    def fail_install(source, destination):
        if str(destination).endswith(".json"):
            raise OSError("injected plan install failure")
        return original(source, destination)

    def fail_fsync(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("injected plan fsync failure")
        return original_fsync(fd)

    if failure == "replace":
        monkeypatch.setattr("mypr_mcp.ast_rewrite.os.replace", fail_install)
        match = "install failure"
    else:
        monkeypatch.setattr("mypr_mcp.ast_rewrite.os.fsync", fail_fsync)
        match = "fsync failure"
    with pytest.raises(OSError, match=match):
        store.create({"files": []})

    assert store.load(existing) == {"files": []}
