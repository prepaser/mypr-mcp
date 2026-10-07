from __future__ import annotations

import venv
from pathlib import Path

import pytest

import mypr_mcp.doctor as doctor_module
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


@pytest.mark.asyncio
async def test_doctor_python_probe_reports_startup_and_shape_failures(tmp_path, monkeypatch):
    async def direct(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(doctor_module.asyncio, "to_thread", direct)
    corrupt = tmp_path / "corrupt-python"
    corrupt.write_bytes(b"not an executable\n")
    corrupt.chmod(0o700)
    startup = await _probe_python(corrupt)
    assert startup["available"] is False
    assert "Exec format error" in startup["error"]

    malformed = tmp_path / "malformed-python"
    malformed.write_text("#!/bin/sh\nprintf '%s\\n' '[1, 2, 3]'\n", encoding="utf-8")
    malformed.chmod(0o700)
    shape = await _probe_python(malformed)
    assert shape["available"] is False
    assert shape["error"] == "invalid probe output shape"


@pytest.mark.asyncio
async def test_doctor_python_probe_reports_incompatible_interpreter(tmp_path, monkeypatch):
    async def direct(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(doctor_module.asyncio, "to_thread", direct)
    old_python = tmp_path / "old-python"
    old_python.write_text(
        "#!/bin/sh\nprintf '%s\\n' '{\"version\":\"3.13.5\",\"packages\":{}}'\n",
        encoding="utf-8",
    )
    old_python.chmod(0o700)
    result = await _probe_python(old_python)

    assert result["available"] is True
    assert result["version"] == "3.13.5"
    assert result["version_compatible"] is False
    assert "Python >= 3.14 is required" in result["error"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["constructor", "load"])
async def test_doctor_contains_configuration_failures_and_continues(
    tmp_path: Path, monkeypatch, failure
):
    async def direct(function, /, *args, **kwargs):
        return function(*args, **kwargs)

    class EmptyStore:
        data_root = tmp_path / "data"
        cache_root = tmp_path / "cache"

        def names(self, kind):
            return ()

        async def close(self):
            return None

    class BrokenConfig:
        global_path = tmp_path / "global.toml"

        def __init__(self, workspace):
            if failure == "constructor":
                raise ValueError("global and workspace configuration paths must differ")

        def load(self):
            raise RuntimeError("configuration changed while being read; retry")

    monkeypatch.setattr(doctor_module.asyncio, "to_thread", direct)
    monkeypatch.setattr(doctor_module, "ConfigStore", BrokenConfig)
    monkeypatch.setattr(doctor_module, "DependencyStore", EmptyStore)

    result = await doctor_workspace(tmp_path / "workspace")

    assert result["config"]["valid"] is False
    assert "configuration is unavailable" in result["warnings"]
    assert result["storage"]["available"] is True
    assert "python" in result and "binaries" in result
