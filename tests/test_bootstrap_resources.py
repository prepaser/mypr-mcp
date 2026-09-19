import json

import pytest

from mypr_mcp import bootstrap


async def test_bootstrap_checks_packages_without_version_bump(tmp_path, monkeypatch):
    python = tmp_path / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    monkeypatch.setattr(bootstrap.importlib.metadata, "version", lambda name: "1.0")
    installed = {"ipykernel": "1.0", "pyyaml": "1.0"}
    commands = []

    async def versions(_python):
        return dict(installed)

    async def command(*args):
        commands.append(args)
        installed.update({name: "1.0" for name in bootstrap.RUNTIME_PACKAGES})

    monkeypatch.setattr(bootstrap, "runtime_versions", versions)
    await bootstrap.ensure_runtime(python, command)
    assert len(commands) == 1
    assert "playwright==1.0" in commands[0]
    await bootstrap.ensure_runtime(python, command)
    assert len(commands) == 1
    installed.pop("httpx2")
    await bootstrap.ensure_runtime(python, command)
    assert len(commands) == 2
    marker = json.loads((python.parent.parent / ".mypr-runtime.json").read_text())
    assert marker == {name: "1.0" for name in bootstrap.RUNTIME_PACKAGES}


async def test_bootstrap_failure_does_not_write_ready_marker(tmp_path, monkeypatch):
    python = tmp_path / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    monkeypatch.setattr(bootstrap.importlib.metadata, "version", lambda name: "1.0")

    async def versions(_python):
        return {}

    async def command(*args):
        raise RuntimeError("install failed")

    monkeypatch.setattr(bootstrap, "runtime_versions", versions)
    with pytest.raises(RuntimeError, match="install failed"):
        await bootstrap.ensure_runtime(python, command)
    assert not (python.parent.parent / ".mypr-runtime.json").exists()
