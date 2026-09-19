from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.modules import ModuleManager


class FakeShell:
    async def run(self, command, **kwargs):
        source = kwargs["input"]
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=kwargs["cwd"],
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(source.encode()), kwargs.get("timeout")
            )
        except TimeoutError:
            process.kill()
            stdout, stderr = await process.communicate()
            return {
                "state": "failed",
                "returncode": process.returncode,
                "stdout": stdout.decode(),
                "stderr": stderr.decode(),
                "timed_out": True,
                "truncated": False,
            }
        return {
            "state": "succeeded" if process.returncode == 0 else "failed",
            "returncode": process.returncode,
            "stdout": stdout.decode(),
            "stderr": stderr.decode(),
            "timed_out": False,
            "truncated": False,
        }


def manager(tmp_path: Path) -> ModuleManager:
    root = tmp_path / ".mypr" / "lib" / "ws_lib"
    root.mkdir(parents=True)
    (root / "__init__.py").touch()
    python = tmp_path / ".mypr" / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.symlink_to(sys.executable)
    return ModuleManager(tmp_path, Filesystem(tmp_path), FakeShell())


@pytest.mark.asyncio
async def test_module_write_read_and_stale_revision(tmp_path: Path):
    modules = manager(tmp_path)
    created = await modules.write("demo", "VALUE = 1\n")
    assert created["revision"]
    page = await modules.read("demo")
    assert page["text"] == "VALUE = 1\n"
    with pytest.raises(ValueError, match="Revision mismatch"):
        await modules.write("demo", "VALUE = 2\n", expected_hash="0" * 64)


@pytest.mark.asyncio
async def test_check_rejects_syntax_and_runs_test_code(tmp_path: Path):
    modules = manager(tmp_path)
    result = await modules.check("demo", "VALUE = 3\n", test_code="assert VALUE == 3")
    assert result["valid"] is True
    with pytest.raises(SyntaxError, match="invalid syntax"):
        await modules.check("bad", "if:")


@pytest.mark.asyncio
async def test_check_runs_in_child_without_parent_side_effects(tmp_path: Path):
    modules = manager(tmp_path)
    result = await modules.check(
        "side_effect", "CHECK_CHILD_VALUE = 7\nraise RuntimeError('child failure')\n"
    )
    assert result["valid"] is False
    assert result["state"] == "failed"
    assert "CHECK_CHILD_VALUE" not in globals()


@pytest.mark.asyncio
async def test_check_rejects_truncated_existing_source(tmp_path: Path):
    class TruncatedFilesystem(Filesystem):
        async def read(self, *args, **kwargs):
            return {"text": "VALUE =", "revision": "full", "truncated": True}

    root = tmp_path / ".mypr" / "lib" / "ws_lib"
    root.mkdir(parents=True)
    (root / "__init__.py").touch()
    modules = ModuleManager(tmp_path, TruncatedFilesystem(tmp_path), FakeShell())
    with pytest.raises(ValueError, match="exceeds max_bytes"):
        await modules.check("demo")


@pytest.mark.asyncio
async def test_check_reports_child_timeout(tmp_path: Path):
    modules = manager(tmp_path)
    result = await modules.check("slow", "import time\ntime.sleep(1)\n", timeout=0.01)
    assert result["valid"] is False
    assert result["timed_out"] is True


def test_reload_failure_keeps_old_module_and_old_reference(tmp_path: Path):
    modules = manager(tmp_path)
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "demo.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")
    sys.modules.pop("ws_lib.demo", None)
    old = modules.load("demo")
    old_reference = old
    path.write_text("raise RuntimeError('broken')\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="broken"):
        modules.reload("demo")
    assert sys.modules["ws_lib.demo"] is old_reference
    assert old_reference.VALUE == 1


def test_reload_reads_same_size_same_mtime_source(tmp_path: Path):
    modules = manager(tmp_path)
    path = tmp_path / ".mypr" / "lib" / "ws_lib" / "same.py"
    path.write_text("VALUE = 1\n", encoding="utf-8")
    sys.modules.pop("ws_lib.same", None)
    old = modules.load("same")
    stat = path.stat()
    path.write_text("VALUE = 2\n", encoding="utf-8")
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    fresh = modules.reload("same")
    assert old.VALUE == 1
    assert fresh.VALUE == 2


@pytest.mark.asyncio
async def test_dry_run_existing_module_requires_revision(tmp_path: Path):
    modules = manager(tmp_path)
    await modules.write("demo", "VALUE = 1\n")
    with pytest.raises(FileExistsError, match="expected_hash"):
        await modules.write("demo", "VALUE = 2\n", dry_run=True)
