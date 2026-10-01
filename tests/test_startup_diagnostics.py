from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from mypr_mcp.cli import ensure, run_manager
from mypr_mcp.diagnostics import RPCError
from mypr_mcp.startup import (
    read_startup_failure,
    startup_error,
    write_startup_failure,
)


def test_startup_failure_is_bounded_and_round_trips(tmp_path: Path):
    root = tmp_path / ".mypr"
    path = write_startup_failure(root, ValueError("x" * 50_000), operation="cold_start")
    assert path is not None
    assert path.stat().st_size <= 16 * 1024
    record = read_startup_failure(root)
    assert record is not None
    assert record["operation"] == "cold_start"
    error = startup_error(root)
    assert isinstance(error, RPCError)
    assert error.code == "invalid_request"


async def test_ensure_prevalidates_only_a_cold_manager(workspace: Path, monkeypatch):
    root = workspace / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text("[limits]\nresponse_bytes = 1\n")

    def unexpected_start(*args, **kwargs):
        raise AssertionError("invalid cold config must not start a manager")

    monkeypatch.setattr("mypr_mcp.cli.subprocess.Popen", unexpected_start)
    with pytest.raises(RPCError) as caught:
        await ensure(workspace)
    assert caught.value.code == "invalid_workspace_config"
    record = read_startup_failure(root)
    assert record is not None
    assert record["operation"] == "config_validate"


async def test_manager_wrapper_records_runtime_boot_failure(tmp_path: Path, monkeypatch):
    import mypr_mcp.runtime

    class FailingRuntime:
        def __init__(self, workspace):
            self.root = Path(workspace) / ".mypr"
            self.healthy = False
            self.health_error = "prepare failed"

        async def run(self):
            raise RuntimeError("kernel could not start")

    monkeypatch.setattr(mypr_mcp.runtime, "Runtime", FailingRuntime)
    with pytest.raises(RuntimeError, match="kernel could not start"):
        await run_manager(tmp_path)
    record = read_startup_failure(tmp_path / ".mypr")
    assert record is not None
    assert record["operation"] == "manager_start"
    assert record["error_info"]["details"]["health_error"] == "prepare failed"


async def test_manager_wrapper_clears_failure_after_healthy_start(tmp_path: Path, monkeypatch):
    import mypr_mcp.runtime

    root = tmp_path / ".mypr"
    write_startup_failure(root, RuntimeError("old"))

    class HealthyRuntime:
        def __init__(self, workspace):
            self.root = Path(workspace) / ".mypr"
            self.healthy = False
            self.health_error = None

        async def run(self):
            self.healthy = True

    monkeypatch.setattr(mypr_mcp.runtime, "Runtime", HealthyRuntime)
    await run_manager(tmp_path)
    assert read_startup_failure(root) is None


def test_startup_record_is_json(tmp_path: Path):
    root = tmp_path / ".mypr"
    write_startup_failure(root, RuntimeError("broken"))
    json.loads((root / "startup-error.json").read_text())


def test_startup_record_bounds_escaped_errors_without_breaking_json(tmp_path: Path):
    root = tmp_path / ".mypr"
    path = write_startup_failure(root, RuntimeError("\x00" * 4096))
    assert path.stat().st_size <= 16 * 1024
    record = read_startup_failure(root)
    assert record["error"].startswith("RuntimeError:")
    assert record["error_info"]["truncated"]


def test_cli_status_reports_invalid_config_without_starting_manager(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text("[limits]\nresponse_bytes = 1048577\n")
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).parents[1] / "src"))
    result = subprocess.run(
        [sys.executable, "-m", "mypr_mcp.cli", "status"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["healthy"] is False
    assert payload["manager_available"] is False
    assert payload["startup_error"]["error_info"]["code"] == "invalid_workspace_config"
