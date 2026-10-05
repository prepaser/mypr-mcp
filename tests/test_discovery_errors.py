from __future__ import annotations

import os
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.kernel_api import Skills
from mypr_mcp.modules import ModuleManager


class _Shell:
    pass


def _permission_fixture(tmp_path: Path) -> tuple[Path, Path]:
    module_root = tmp_path / ".mypr" / "lib" / "ws_lib"
    module_root.mkdir(parents=True)
    (module_root / "visible.py").write_text("value = 1\n")
    blocked_module = module_root / "blocked"
    blocked_module.mkdir()
    (blocked_module / "hidden.py").write_text("value = 2\n")

    skill_root = tmp_path / ".mypr" / "skills"
    (skill_root / "visible").mkdir(parents=True)
    (skill_root / "visible" / "SKILL.md").write_text("# visible\n")
    blocked_skill = skill_root / "blocked"
    blocked_skill.mkdir()
    (blocked_skill / "SKILL.md").write_text("# hidden\n")
    blocked_module.chmod(0)
    blocked_skill.chmod(0)
    if os.access(blocked_module, os.R_OK | os.X_OK) or os.access(blocked_skill, os.R_OK | os.X_OK):
        blocked_module.chmod(0o700)
        blocked_skill.chmod(0o700)
        pytest.skip("permission checks require a non-root test process")
    return blocked_module, blocked_skill


@pytest.mark.asyncio
async def test_module_page_reports_incomplete_discovery(tmp_path: Path):
    blocked_module, blocked_skill = _permission_fixture(tmp_path)
    try:
        manager = ModuleManager(tmp_path, Filesystem(tmp_path), _Shell())
        result = await manager.list_page(limit=100)
        assert result["complete"] is False
        assert result["has_more"] is False
        assert result["warnings"]
        assert result["items"][0]["name"] == "visible"
    finally:
        blocked_module.chmod(0o700)
        blocked_skill.chmod(0o700)


@pytest.mark.asyncio
async def test_legacy_module_list_does_not_hide_discovery_error(tmp_path: Path):
    blocked_module, blocked_skill = _permission_fixture(tmp_path)
    try:
        manager = ModuleManager(tmp_path, Filesystem(tmp_path), _Shell())
        with pytest.raises(RuntimeError, match="Module discovery incomplete"):
            manager.list()
    finally:
        blocked_module.chmod(0o700)
        blocked_skill.chmod(0o700)


@pytest.mark.asyncio
async def test_skill_list_does_not_hide_discovery_error(tmp_path: Path):
    blocked_module, blocked_skill = _permission_fixture(tmp_path)
    try:
        skills = Skills(tmp_path, Filesystem(tmp_path))
        with pytest.raises(RuntimeError, match="Skill discovery incomplete"):
            await skills.list(scan_limit=100)
    finally:
        blocked_module.chmod(0o700)
        blocked_skill.chmod(0o700)
