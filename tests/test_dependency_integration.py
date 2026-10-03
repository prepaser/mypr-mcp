from __future__ import annotations

import base64
import hashlib
import json
import shutil
import zipfile
from pathlib import Path

from conftest import execute, mcp_session, result_text, stop_manager


def _cssselect_wheel(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    wheel = directory / "cssselect-99.99.0-py3-none-any.whl"
    files = {
        "cssselect/__init__.py": "__version__ = '99.99.0'\n",
        "cssselect-99.99.0.dist-info/METADATA": (
            "Metadata-Version: 2.1\nName: cssselect\nVersion: 99.99.0\n"
        ),
        "cssselect-99.99.0.dist-info/WHEEL": (
            "Wheel-Version: 1.0\nGenerator: mypr-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
    }
    records = []
    with zipfile.ZipFile(wheel, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in files.items():
            data = value.encode()
            archive.writestr(name, data)
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            records.append(f"{name},sha256={digest},{len(data)}")
        records.append("cssselect-99.99.0.dist-info/RECORD,,")
        archive.writestr("cssselect-99.99.0.dist-info/RECORD", "\n".join(records) + "\n")
    return wheel


def _configure_local_index(monkeypatch, tmp_path: Path) -> None:
    _cssselect_wheel(tmp_path / "packages")
    monkeypatch.setenv("UV_FIND_LINKS", str(tmp_path / "packages"))
    monkeypatch.setenv("UV_OFFLINE", "1")


async def test_mcp_dependency_install_policy_and_reset_persistence(
    workspace, monkeypatch, tmp_path
):
    _configure_local_index(monkeypatch, tmp_path)
    async with mcp_session(workspace) as session:
        listed = await execute(
            session,
            "import json\nprint(json.dumps(await ws.dependencies.list(kind='python')))\n",
        )
        inventory = json.loads(result_text(listed).strip())
        cssselect = next(item for item in inventory["items"] if item["name"] == "cssselect")
        assert cssselect["status"] == "missing"
        assert cssselect["scope"] == "workspace"
        assert ".mypr/venv" in cssselect["path"]

        versions = await execute(
            session,
            "import importlib.metadata as metadata\nprint(metadata.version('ipykernel'))\n",
        )
        runtime_version = result_text(versions).strip()

        installed = await execute(
            session,
            "import json\nprint(json.dumps(await ws.dependencies._automatic('cssselect')))\n",
            wait_ms=30_000,
        )
        assert installed["state"] == "succeeded", result_text(installed)
        installed_payload = json.loads(result_text(installed).strip())
        assert installed_payload["items"][0]["status"] == "installed"
        requirements = (workspace / ".mypr" / "requirements.txt").read_text()
        assert "cssselect==99.99.0" in requirements
        assert f"ipykernel=={runtime_version}" in requirements

        reset = await execute(session, "await ws.reset()", wait_ms=15_000)
        assert reset["state"] == "succeeded", reset
        import_after_reset = await execute(
            session,
            "import cssselect\ncssselect.__version__",
        )
        assert import_after_reset["state"] == "succeeded", import_after_reset
        assert "99.99.0" in result_text(import_after_reset)

        disabled = await execute(
            session,
            "await ws.config.set('dependencies.auto_install', False)\n"
            "await ws.config.reload()\n"
            "(await ws.dependencies.list(kind='python'))['auto_install']",
        )
        assert disabled["state"] == "succeeded", disabled
        assert result_text(disabled).strip().endswith("False")

        installed_while_disabled = await execute(
            session,
            "await ws.dependencies._automatic('cssselect')",
        )
        assert installed_while_disabled["state"] == "succeeded", installed_while_disabled

        missing_while_disabled = await execute(
            session,
            "await ws.dependencies._automatic('pillow')",
        )
        assert missing_while_disabled["state"] == "failed"
        assert "dependencies.auto_install is false" in result_text(missing_while_disabled).lower()


async def test_workspace_python_dependency_environments_are_independent(
    workspace, monkeypatch, tmp_path
):
    _configure_local_index(monkeypatch, tmp_path)
    (workspace / ".mypr").mkdir()
    (workspace / ".mypr" / "config.toml").write_text(
        "[dependencies]\nauto_install = false\n"
    )
    other = workspace.parent / "other-workspace"
    other.mkdir()
    try:
        async with mcp_session(workspace) as first:
            disabled = await execute(
                first,
                "(await ws.dependencies.list(kind='python'))['auto_install']",
            )
            assert disabled["state"] == "succeeded", result_text(disabled)
            assert result_text(disabled).strip() == "False"
            installed = await execute(
                first,
                "await ws.dependencies.ensure('cssselect')",
                wait_ms=30_000,
            )
            assert installed["state"] == "succeeded", result_text(installed)

        async with mcp_session(other) as second:
            listed = await execute(
                second,
                "import json\nprint(json.dumps(await ws.dependencies.list(kind='python')))\n",
            )
            inventory = json.loads(result_text(listed).strip())
            cssselect = next(item for item in inventory["items"] if item["name"] == "cssselect")
            assert cssselect["status"] == "missing"
            assert str(workspace / ".mypr" / "venv") != cssselect["path"]
            requirements = other / ".mypr" / "requirements.txt"
            assert not requirements.exists() or "cssselect==99.99.0" not in requirements.read_text()
    finally:
        await stop_manager(other)
        shutil.rmtree(other / ".mypr" / "venv", ignore_errors=True)
