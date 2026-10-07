from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from mypr_mcp import bootstrap
from mypr_mcp.python_dependencies import (
    package_environment,
    python_version_satisfies,
    version_satisfies,
)

_EXPECTED_HOME = os.path.expanduser("~")


@pytest.mark.parametrize(
    ("version", "requirement", "expected"),
    [
        ("7.3.0", "ipykernel>=7.3,<8", True),
        ("7.2.9", "ipykernel>=7.3,<8", False),
        ("1.0.0rc1", "demo>=1.0", False),
        ("1.0.0", "demo>=1.0", True),
        ("not-a-version", "demo>=1.0", False),
        ("1.0.0", "demo[broken", False),
        (None, "demo", False),
        ("1.0.0", None, True),
    ],
)
def test_version_satisfies_uses_packaging_constraints(version, requirement, expected):
    assert version_satisfies(version, requirement) is expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [("3.14.0", True), ("3.14.0rc1", True), ("3.13.5", False), ("invalid", False)],
)
def test_python_version_satisfies_runtime_minimum(version, expected):
    assert python_version_satisfies(version) is expected


def test_catalogue_import_is_stdlib_only_until_version_check():
    source_root = Path(__file__).resolve().parents[1] / "src"
    script = """
import json
import sys
sys.path.insert(0, sys.argv[1])
from mypr_mcp.python_dependencies import version_satisfies
before = 'packaging' in sys.modules
result = version_satisfies('1.0.0', 'demo>=1')
after = 'packaging' in sys.modules
print(json.dumps({'before': before, 'after': after, 'result': result}))
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(source_root)],
        check=True,
        capture_output=True,
        text=True,
        env={"PATH": os.environ["PATH"]},
    )
    assert json.loads(completed.stdout) == {"before": False, "after": True, "result": True}


def test_package_environment_prefers_explicit_environment_and_expands_config_path():
    configured = {
        "dependencies": {
            "uv_cache_dir": "~/mypr-cache",
            "uv_link_mode": "hardlink",
        }
    }

    result = package_environment(configured, {"PATH": "/bin"})

    assert result["UV_CACHE_DIR"] == os.path.join(_EXPECTED_HOME, "mypr-cache")
    assert result["UV_LINK_MODE"] == "hardlink"
    assert result["PATH"] == "/bin"

    explicit = package_environment(
        configured,
        {
            "PATH": "/bin",
            "UV_CACHE_DIR": "/explicit/cache",
            "UV_LINK_MODE": "copy",
        },
    )
    assert explicit["UV_CACHE_DIR"] == "/explicit/cache"
    assert explicit["UV_LINK_MODE"] == "copy"


@pytest.mark.asyncio
async def test_install_core_passes_workspace_uv_settings_to_worker(tmp_path, monkeypatch):
    monkeypatch.delenv("UV_CACHE_DIR", raising=False)
    monkeypatch.delenv("UV_LINK_MODE", raising=False)
    calls = []

    async def command(*args, env=None, **kwargs):
        calls.append((args, env, kwargs))

    await bootstrap.install_core(
        tmp_path / "venv" / "bin" / "python",
        tmp_path / ".mypr",
        ("ipykernel", "tomlkit"),
        {
            "uv_cache_dir": "~/cache",
            "uv_link_mode": "hardlink",
        },
        command=command,
    )

    assert len(calls) == 1
    args, environment, _kwargs = calls[0]
    assert args[0] == sys.executable
    assert args[-1] == "--automatic"
    assert environment["UV_CACHE_DIR"] == os.path.join(_EXPECTED_HOME, "cache")
    assert environment["UV_LINK_MODE"] == "hardlink"
