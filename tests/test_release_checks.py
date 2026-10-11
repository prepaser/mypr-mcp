from __future__ import annotations

import importlib.util
import json
import zipfile
from pathlib import Path

import pytest


def _module():
    path = Path(__file__).parents[1] / "scripts" / "check_release.py"
    spec = importlib.util.spec_from_file_location("check_release", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _project() -> dict:
    return {
        "project": {
            "name": "mypr-mcp",
            "version": "1.5.3",
            "requires-python": ">=3.14",
            "dependencies": ["mcp>=2.2,<3", "httpx2[http2,socks]>=2.12,<3"],
            "scripts": {"mypr-mcp": "mypr_mcp.cli:main"},
        }
    }


def _wheel(path: Path, *, dependencies: tuple[str, ...] = ()) -> Path:
    wheel = path / "mypr_mcp-1.5.3-py3-none-any.whl"
    metadata = "\n".join(
        [
            "Metadata-Version: 2.4",
            "Name: mypr-mcp",
            "Version: 1.5.3",
            "Requires-Python: >=3.14",
            *(f"Requires-Dist: {item}" for item in dependencies),
            "",
        ]
    )
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("mypr_mcp-1.5.3.dist-info/METADATA", metadata)
        archive.writestr(
            "mypr_mcp-1.5.3.dist-info/entry_points.txt",
            "[console_scripts]\nmypr-mcp = mypr_mcp.cli:main\n",
        )
    return wheel


def test_wheel_metadata_matches_project(tmp_path: Path):
    module = _module()
    wheel = _wheel(tmp_path, dependencies=("mcp<3,>=2.2", "httpx2[socks,http2]<3,>=2.12"))

    assert module._metadata_from_wheel(wheel, _project())["version"] == "1.5.3"


def test_wheel_metadata_mismatch_is_reported(tmp_path: Path):
    module = _module()
    wheel = _wheel(tmp_path, dependencies=("mcp<3,>=2.2",))
    project = _project()
    project["project"]["version"] = "1.5.4"

    with pytest.raises(module.PhaseFailure, match="version"):
        module._metadata_from_wheel(wheel, project)


def test_clean_environment_removes_runtime_and_python_selectors(monkeypatch, tmp_path: Path):
    module = _module()
    monkeypatch.setenv("MYPR_SOCKET", "inherited")
    monkeypatch.setenv("PYTHONPATH", "/checkout/src")
    monkeypatch.setenv("VIRTUAL_ENV", "/checkout/.venv")
    monkeypatch.setenv("UV_PROJECT_ENVIRONMENT", "/checkout/.venv")
    monkeypatch.setenv("UV_PUBLISH_TOKEN", "secret")

    env = module._clean_environment(root=tmp_path)

    assert "MYPR_SOCKET" not in env
    assert "PYTHONPATH" not in env
    assert "VIRTUAL_ENV" not in env
    assert "UV_PROJECT_ENVIRONMENT" not in env
    assert "UV_PUBLISH_TOKEN" not in env
    assert env["XDG_RUNTIME_DIR"] == str(tmp_path / "runtime")


def test_run_reports_phase_failure_and_removes_owned_workspace(monkeypatch, tmp_path: Path):
    module = _module()
    owned = tmp_path / "owned"
    report = tmp_path / "report.json"

    def make_temp(*, prefix):
        owned.mkdir()
        return str(owned)

    def fail(*args, **kwargs):
        raise module.PhaseFailure("lock", "x" * 4096)

    monkeypatch.setattr(module.tempfile, "mkdtemp", make_temp)
    monkeypatch.setattr(module, "_run", fail)

    assert module.run(report) == 1
    payload = json.loads(report.read_text())
    assert payload["ok"] is False
    assert payload["error"]["phase"] == "lock"
    assert len(payload["error"]["message"]) <= 1024
    assert payload["phases"][0]["ok"] is False
    assert payload["phases"][0]["duration_ms"] >= 0
    assert not owned.exists()
