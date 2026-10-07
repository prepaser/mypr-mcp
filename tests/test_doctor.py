from __future__ import annotations

import venv

import pytest

from mypr_mcp.doctor import _probe_python, doctor_workspace


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


@pytest.mark.asyncio
async def test_doctor_reports_runtime_unknown_and_available_storage(tmp_path):
    result = await doctor_workspace(tmp_path)

    assert result["runtime"]["status"] == "unknown"
    assert result["runtime"]["available"] is False
    assert result["storage"]["available"] is True
    assert result["storage"]["free_bytes"] > 0


@pytest.mark.asyncio
async def test_doctor_python_probe_suppresses_fd_level_import_output(tmp_path):
    environment = tmp_path / "venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
    sites = list((environment / "lib").glob("python*/site-packages"))
    assert len(sites) == 1
    (sites[0] / "tomlkit.py").write_text(
        "import os\nos.write(1, b'tomlkit native banner\\n')\n"
    )
    metadata = sites[0] / "tomlkit-0.15.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: tomlkit\nVersion: 0.15.0\n"
    )

    result = await _probe_python(environment / "bin" / "python")

    assert result["available"] is True
    assert result["packages"]["tomlkit"]["status"] == "installed"
