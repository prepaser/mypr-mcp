from __future__ import annotations

import sys
from pathlib import Path

import pytest

from mypr_mcp.bootstrap import ensure_workspace_python


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
