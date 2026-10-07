from __future__ import annotations

import asyncio
import os
import shlex
import sys
from pathlib import Path

import pytest

from mypr_mcp.bootstrap import _workspace_python_probe, ensure_workspace_python


def _write_valid_venv(path: Path) -> None:
    (path / "bin").mkdir(parents=True)
    (path / "bin" / "python").symlink_to(Path(sys.executable).resolve())
    (path / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")


@pytest.mark.asyncio
async def test_repair_replaces_invalid_venv_and_preserves_old_contents(tmp_path: Path):
    root = tmp_path / ".mypr"
    old_python = root / "venv" / "bin" / "python"
    old_python.parent.mkdir(parents=True)
    old_python.write_text("damaged", encoding="utf-8")
    calls = []

    async def command(*args, **_kwargs):
        calls.append(args)
        _write_valid_venv(Path(args[2]))

    result = await ensure_workspace_python(root, command=command)

    assert result == root / "venv/bin/python"
    assert result.is_symlink()
    assert result.exists()
    assert calls == [("uv", "venv", str(root / "venv"), "--python", sys.executable)]
    backups = list(root.glob(".venv-invalid-*"))
    assert len(backups) == 1
    assert (backups[0] / "bin/python").read_text(encoding="utf-8") == "damaged"
    assert ".venv-invalid-*/" in (root / ".gitignore").read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_repair_restores_invalid_venv_when_creation_fails(tmp_path: Path):
    root = tmp_path / ".mypr"
    old_python = root / "venv" / "bin" / "python"
    old_python.parent.mkdir(parents=True)
    old_python.write_text("damaged", encoding="utf-8")

    async def command(*_args, **_kwargs):
        raise RuntimeError("uv failed")

    with pytest.raises(RuntimeError, match="uv failed"):
        await ensure_workspace_python(root, command=command)

    assert old_python.read_text(encoding="utf-8") == "damaged"
    assert not list(root.glob(".venv-invalid-*"))


@pytest.mark.asyncio
async def test_valid_symlinked_bin_directory_is_reused(tmp_path: Path):
    root = tmp_path / ".mypr"
    shared = root / "shared-venv"
    _write_valid_venv(shared)
    venv = root / "venv"
    venv.mkdir()
    (venv / "pyvenv.cfg").symlink_to(shared / "pyvenv.cfg")
    (venv / "bin").symlink_to(shared / "bin", target_is_directory=True)

    async def command(*_args, **_kwargs):
        raise AssertionError("valid environment should be reused")

    result = await ensure_workspace_python(root, command=command)

    assert result == venv / "bin/python"
    assert result.is_symlink()


@pytest.mark.asyncio
async def test_symlinked_venv_is_repaired(tmp_path: Path):
    root = tmp_path / ".mypr"
    external = tmp_path / "external-venv"
    _write_valid_venv(external)
    root.mkdir()
    (root / "venv").symlink_to(external, target_is_directory=True)
    calls = []

    async def command(*args, **_kwargs):
        calls.append(args)
        _write_valid_venv(Path(args[2]))

    result = await ensure_workspace_python(root, command=command)

    assert result == root / "venv/bin/python"
    assert calls == [("uv", "venv", str(root / "venv"), "--python", sys.executable)]
    assert external.is_dir()


@pytest.mark.asyncio
async def test_external_bin_symlink_is_repaired(tmp_path: Path):
    root = tmp_path / ".mypr"
    external = tmp_path / "external-bin"
    _write_valid_venv(external)
    venv = root / "venv"
    venv.mkdir(parents=True)
    (venv / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
    (venv / "bin").symlink_to(external / "bin", target_is_directory=True)
    calls = []

    async def command(*args, **_kwargs):
        calls.append(args)
        _write_valid_venv(Path(args[2]))

    result = await ensure_workspace_python(root, command=command)

    assert result == root / "venv/bin/python"
    assert calls == [("uv", "venv", str(root / "venv"), "--python", sys.executable)]
    assert external.is_dir()


@pytest.mark.asyncio
async def test_existing_older_python_is_rejected_without_replacement(tmp_path: Path):
    root = tmp_path / ".mypr"
    venv = root / "venv"
    python = venv / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text(
        "#!/bin/sh\nprintf '%s\\n' '3.13.5' 'True'\n", encoding="utf-8"
    )
    python.chmod(0o700)
    (venv / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")
    before = (venv / "pyvenv.cfg").read_bytes()
    calls = []

    async def command(*args, **kwargs):
        calls.append((args, kwargs))

    with pytest.raises(RuntimeError, match=r"3\.13\.5.*3\.14.*preserved"):
        await ensure_workspace_python(root, command=command)

    assert calls == []
    assert (venv / "pyvenv.cfg").read_bytes() == before
    assert venv.is_dir()


@pytest.mark.asyncio
async def test_python_probe_cancellation_reaps_child(tmp_path: Path):
    root = tmp_path / ".mypr"
    python = root / "venv" / "bin" / "python"
    marker = tmp_path / "child.pid"
    python.parent.mkdir(parents=True)
    python.write_text(
        "#!/bin/sh\n"
        f"sleep 30 &\nprintf '%s\\n' \"$!\" > {shlex.quote(str(marker))}\n"
        "wait\n",
        encoding="utf-8",
    )
    python.chmod(0o700)
    (root / "venv" / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")

    task = asyncio.create_task(_workspace_python_probe(root))
    for _ in range(100):
        if marker.is_file():
            break
        await asyncio.sleep(0.01)
    assert marker.is_file()
    child_pid = int(marker.read_text(encoding="utf-8"))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for _ in range(100):
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            break
        await asyncio.sleep(0.01)
    else:
        os.kill(child_pid, 9)
        pytest.fail(f"probe descendant {child_pid} survived cancellation")


@pytest.mark.asyncio
async def test_python_probe_rejects_unbounded_output(tmp_path: Path):
    root = tmp_path / ".mypr"
    python = root / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text(
        "#!/bin/sh\nwhile :; do printf '%04096d' 0; done\n",
        encoding="utf-8",
    )
    python.chmod(0o700)
    (root / "venv" / "pyvenv.cfg").write_text("home = /usr/bin\n", encoding="utf-8")

    assert await asyncio.wait_for(_workspace_python_probe(root), 2) is None
