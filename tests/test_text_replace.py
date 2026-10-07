from __future__ import annotations

import asyncio
import base64
import subprocess
import threading
from pathlib import Path

import pytest

import mypr_mcp.patching as patching
import mypr_mcp.text_replace as text_replace
from mypr_mcp.change_plans import ChangePlanError, ChangePlanStore
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.text_replace_worker import replace as worker_replace


class Shell:
    async def run(self, command, **kwargs):
        result = await asyncio.to_thread(
            subprocess.run,
            command,
            input=kwargs.get("input"),
            cwd=kwargs.get("cwd"),
            capture_output=True,
            text=True,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "state": "succeeded" if result.returncode == 0 else "failed",
            "timed_out": False,
            "truncated": False,
            "warnings": [],
        }


@pytest.mark.asyncio
async def test_replace_preview_and_apply_is_cas_checked(tmp_path: Path):
    (tmp_path / "one.txt").write_text("old old\n")
    (tmp_path / "two.txt").write_text("old\n")
    fs = Filesystem(tmp_path, Shell())
    preview = await fs.replace("old", "new", glob="*.txt", fixed=True)
    assert preview["applicable"]
    (tmp_path / "two.txt").write_text("changed\n")
    with pytest.raises(ValueError, match="source changed"):
        await fs.apply_replace(preview["plan_id"])


@pytest.mark.asyncio
async def test_apply_replace_bounds_stale_source_reads(tmp_path: Path, monkeypatch):
    target = tmp_path / "value.txt"
    with target.open("wb") as stream:
        stream.truncate(20 * 1024 * 1024)
    plan_id = ChangePlanStore(tmp_path, "replace").create(
        {
            "history": False,
            "operations": [
                {
                    "input": "value.txt",
                    "display": "value.txt",
                    "old": b"x",
                    "new": b"y",
                    "matches": 1,
                }
            ],
        }
    )
    calls = []
    original = text_replace._read_state

    def instrument(path, display, *args, **kwargs):
        calls.append(kwargs.get("max_bytes", args[0] if args else None))
        return original(path, display, *args, **kwargs)

    monkeypatch.setattr(text_replace, "_read_state", instrument)
    fs = Filesystem(tmp_path)
    with pytest.raises(ChangePlanError, match="source changed"):
        await fs.apply_replace(plan_id)
    assert calls == [1]


@pytest.mark.asyncio
async def test_cancelled_apply_waits_for_commit_before_releasing_lock(tmp_path: Path, monkeypatch):
    path = tmp_path / "one.txt"
    path.write_text("old\n")
    fs = Filesystem(tmp_path)
    plan_id = ChangePlanStore(tmp_path, "replace").create(
        {
            "history": False,
            "operations": [
                {
                    "input": "one.txt",
                    "display": "one.txt",
                    "old": b"old\n",
                    "new": b"new\n",
                    "matches": 1,
                }
            ],
        }
    )
    entered = threading.Event()
    release = threading.Event()
    calls = 0
    original = patching._assert_unchanged

    def gated(expected, current):
        nonlocal calls
        calls += 1
        result = original(expected, current)
        if calls == 2:
            entered.set()
            release.wait(5)
        return result

    monkeypatch.setattr(patching, "_assert_unchanged", gated)
    task = asyncio.create_task(fs.apply_replace(plan_id))
    await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert path.read_text() == "new\n"


@pytest.mark.asyncio
async def test_replace_applies_all_matching_files(tmp_path: Path):
    (tmp_path / "one.txt").write_text("old\n")
    (tmp_path / "two.txt").write_text("old\n")
    fs = Filesystem(tmp_path, Shell())
    preview = await fs.replace("old", "new", glob="*.txt", fixed=True)
    result = await fs.apply_replace(preview["plan_id"])
    assert result["applied"]
    assert (tmp_path / "one.txt").read_text() == "new\n"
    assert (tmp_path / "two.txt").read_text() == "new\n"


@pytest.mark.asyncio
async def test_replace_defaults_to_literal_and_honors_case_insensitive(tmp_path: Path):
    (tmp_path / "value.txt").write_text("A.B a.b\n")
    fs = Filesystem(tmp_path, Shell())
    preview = await fs.replace("a.b", "x", glob="*.txt", ignore_case=True)
    assert preview["changes"][0]["matches"] == 2
    await fs.apply_replace(preview["plan_id"])
    assert (tmp_path / "value.txt").read_text() == "x x\n"


@pytest.mark.asyncio
async def test_replace_regex_uses_worker_matching_and_substitution(tmp_path: Path):
    (tmp_path / "value.txt").write_text("item-12 item-34\n")
    fs = Filesystem(tmp_path, Shell())
    preview = await fs.replace(r"item-(\d+)", r"value-\1", glob="*.txt", fixed=False)
    assert preview["changes"][0]["matches"] == 2
    await fs.apply_replace(preview["plan_id"])
    assert (tmp_path / "value.txt").read_text() == "value-12 value-34\n"


def test_replace_worker_keeps_literal_replacement_text(tmp_path: Path):
    (tmp_path / "value.txt").write_text("old\n")
    result = worker_replace(
        {
            "root": str(tmp_path),
            "paths": ["value.txt"],
            "pattern": "old",
            "replacement": r"\1",
            "fixed": True,
            "ignore_case": False,
        }
    )
    assert result["complete"]
    assert base64.b64decode(result["operations"][0]["new"]) == b"\\1\n"


def test_replace_worker_rejects_lexical_symlink_alias(tmp_path: Path):
    (tmp_path / "target.txt").write_text("old\n")
    (tmp_path / "link").symlink_to(tmp_path / "target.txt")
    result = worker_replace(
        {
            "root": str(tmp_path),
            "paths": ["link/../target.txt"],
            "pattern": "old",
            "replacement": "new",
            "fixed": True,
            "ignore_case": False,
        }
    )
    assert result == {"complete": False, "reason": "path_outside_workspace"}


def test_replace_worker_bounds_source_reads(tmp_path: Path, monkeypatch):
    source = tmp_path / "large.txt"
    source.write_bytes(b"x" * 64)

    def unexpected_read(*_args, **_kwargs):
        raise AssertionError("worker must use its bounded reader")

    monkeypatch.setattr(Path, "read_bytes", unexpected_read)
    result = worker_replace(
        {
            "root": str(tmp_path),
            "paths": [source.name],
            "pattern": "x",
            "replacement": "y",
            "fixed": True,
            "ignore_case": False,
            "max_files": 100,
            "max_bytes": 16,
        }
    )
    assert result == {"complete": False, "reason": "byte_limit"}


def test_replace_worker_bounds_expanded_replacement(tmp_path: Path):
    source = tmp_path / "value.txt"
    source.write_bytes(b"x" * 64)
    result = worker_replace(
        {
            "root": str(tmp_path),
            "paths": [source.name],
            "pattern": "x",
            "replacement": "12345678",
            "fixed": True,
            "ignore_case": False,
            "max_files": 100,
            "max_bytes": 100,
        }
    )
    assert result == {"complete": False, "reason": "byte_limit"}


def test_replace_worker_keeps_noop_matches_when_output_budget_is_zero(tmp_path: Path):
    source = tmp_path / "value.txt"
    source.write_bytes(b"x" * 64)
    result = worker_replace(
        {
            "root": str(tmp_path),
            "paths": [source.name],
            "pattern": "x",
            "replacement": "x",
            "fixed": True,
            "ignore_case": False,
            "max_files": 100,
            "max_bytes": 64,
        }
    )
    assert result == {"complete": True, "operations": []}


def test_replace_worker_bounds_repeated_capture_expansion(tmp_path: Path):
    source = tmp_path / "value.txt"
    source.write_bytes(b"a" * 64)
    result = worker_replace(
        {
            "root": str(tmp_path),
            "paths": [source.name],
            "pattern": "(a+)",
            "replacement": r"\1" * 64,
            "fixed": False,
            "ignore_case": False,
            "max_files": 100,
            "max_bytes": 100,
        }
    )
    assert result == {"complete": False, "reason": "byte_limit"}


def test_replace_worker_validates_regex_replacement_without_matches(tmp_path: Path):
    source = tmp_path / "value.txt"
    source.write_text("nothing to replace")
    result = worker_replace(
        {
            "root": str(tmp_path),
            "paths": [source.name],
            "pattern": "missing",
            "replacement": r"\1",
            "fixed": False,
            "ignore_case": False,
            "max_files": 100,
            "max_bytes": 100,
        }
    )
    assert result == {"complete": False, "reason": "invalid_replacement"}


@pytest.mark.asyncio
async def test_replace_preview_keeps_metadata_after_diff_truncation(tmp_path: Path):
    (tmp_path / "one.txt").write_text("old\n" * 6_000)
    (tmp_path / "two.txt").write_text("old\n" * 6_000)
    fs = Filesystem(tmp_path, Shell())
    preview = await fs.replace("old", "new", paths=["one.txt", "two.txt"], history=False)
    assert preview["diff_truncated"]
    assert {change["path"] for change in preview["changes"]} == {"one.txt", "two.txt"}
