from __future__ import annotations

import pytest

from mypr_mcp.doctor import doctor_workspace


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.mark.asyncio
async def test_doctor_reports_layered_config_sources(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    global_path = tmp_path / "global.toml"
    _write(global_path, '[mcp.servers.shared]\ncommand = "server"\n')
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(global_path))

    result = await doctor_workspace(workspace)

    assert result["config"]["valid"]
    assert result["config"]["paths"]["global"] == str(global_path)
    assert result["config"]["sources"]["global"]["revision"]
    assert result["config"]["sources"]["workspace"]["revision"] is None


@pytest.mark.asyncio
async def test_doctor_reports_invalid_global_config(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    global_path = tmp_path / "global.toml"
    _write(global_path, "[mcp.servers.broken\ncommand = 'missing bracket'\n")
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(global_path))

    result = await doctor_workspace(workspace)

    assert not result["config"]["valid"]
    assert str(global_path) in result["config"]["error"]
    assert "configuration is unavailable" in result["warnings"]
    assert not result["ready"]
