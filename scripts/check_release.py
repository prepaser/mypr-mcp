#!/usr/bin/env python3
"""Build and smoke-test the wheel that would be released from this checkout."""

from __future__ import annotations

import argparse
import configparser
import contextlib
import json
import os
import shutil
import signal
import subprocess
import tempfile
import time
import tomllib
import zipfile
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TIMEOUTS = {
    "lock": 30,
    "build": 180,
    "venv": 90,
    "install": 300,
    "smoke": 180,
    "reconnect": 120,
    "restart": 300,
    "saved_output": 120,
    "stop": 45,
}
_ERROR_LIMIT = 1024
_UV_ENV_SELECTORS = {
    "UV_CONFIG_FILE",
    "UV_PROJECT",
    "UV_PYTHON",
    "UV_PYTHON_DOWNLOADS",
    "UV_WORKING_DIR",
    "UV_PUBLISH_TOKEN",
    "UV_PROJECT_ENVIRONMENT",
    "VIRTUAL_ENV",
}
_SMOKE_HELPER = r'''
import asyncio
import json
import os
import re
import sys
import time
from contextlib import asynccontextmanager

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def decode(result):
    structured = getattr(result, "structured_content", None)
    if structured is None:
        structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    text = "".join(block.text for block in result.content if hasattr(block, "text"))
    return json.loads(text)


def output_text(payload):
    return "".join(event.get("text", "") for event in payload.get("output", []))


async def execute(session, code):
    payload = decode(await session.call_tool("execute", {"code": code, "wait_ms": 10000}))
    deadline = time.monotonic() + 30
    events = list(payload.get("output", []))
    while time.monotonic() < deadline:
        terminal = payload.get("state") in {"succeeded", "failed", "cancelled", "lost"}
        if terminal and not payload.get("has_more"):
            payload["output"] = events
            if payload["state"] != "succeeded":
                raise RuntimeError(output_text(payload)[-1024:] or payload["state"])
            return payload
        payload = decode(await session.call_tool(
            "poll",
            {"exec_id": payload["exec_id"], "cursor": payload.get("cursor", 0), "wait_ms": 1000},
        ))
        events.extend(payload.get("output", []))
    raise TimeoutError("workspace cell did not finish")


@asynccontextmanager
async def session():
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "mypr_mcp.cli", "serve"],
        cwd=os.getcwd(),
        env=dict(os.environ),
    )
    async with stdio_client(params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as client:
            await client.initialize()
            yield client


async def main():
    mode = sys.argv[1]
    expected_version = sys.argv[2]
    client_id = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != "-" else None
    job_id = sys.argv[4] if len(sys.argv) > 4 else None
    async with session() as client:
        arguments = {} if client_id is None else {"client_id": client_id}
        initial = decode(await client.call_tool("init", arguments))
        if client_id is not None and initial.get("client_id") != client_id:
            raise RuntimeError("client reconnect returned a different client id")
        status = await execute(client, """
status = await ws.status(detail=False)
print("RELEASE_GATE_VERSION=" + str(status["manager_version"]))
print("RELEASE_GATE_HEALTHY=" + str(status["healthy"]).lower())
""")
        status_text = output_text(status)
        if (
            f"RELEASE_GATE_VERSION={expected_version}" not in status_text
            or "RELEASE_GATE_HEALTHY=true" not in status_text
        ):
            raise RuntimeError("workspace status did not report the installed healthy version")
        if mode == "start":
            payload = await execute(client, """
job = await ws.shell.start(["/bin/sh", "-c", "printf release-gate-output"])
print("RELEASE_GATE_JOB=" + job.id)
await job
print("RELEASE_GATE_DONE")
""")
            match = re.search(r"RELEASE_GATE_JOB=([0-9a-f]{32})", output_text(payload))
            if match is None:
                raise RuntimeError("shell job id was not returned")
            print(json.dumps({"client_id": initial["client_id"], "job_id": match.group(1)}))
            return
        if not isinstance(job_id, str) or len(job_id) != 32:
            raise RuntimeError("saved shell job id is missing")
        if mode == "reconnect":
            payload = await execute(client, f"""
job = await ws.tasks.attach({job_id!r})
print(job.status()["status"])
""")
            if "succeeded" not in output_text(payload):
                raise RuntimeError("same-client reconnect did not find completed shell job")
            print(json.dumps({"client_id": initial["client_id"], "reconnected": True}))
            return
        if mode == "saved-output":
            payload = await execute(client, f"""
job = await ws.tasks.attach({job_id!r})
await job
cursor = None
pages = []
for _ in range(32):
    page = await job.read(cursor=cursor, max_bytes=32768, wait_ms=0)
    pages.append(page)
    cursor = page["cursor"]
    if not page["has_more"]:
        break
else:
    raise RuntimeError("saved shell output exceeded the release-gate page limit")
print(pages[-1]["state"])
print("".join(page["output"] for page in pages))
""")
            text = output_text(payload)
            if "succeeded" not in text or "release-gate-output" not in text:
                raise RuntimeError("saved shell output was not recoverable after restart")
            print(json.dumps({"client_id": initial["client_id"], "saved_output": True}))
            return
        raise RuntimeError(f"unknown smoke mode: {mode}")


asyncio.run(main())
'''


class PhaseFailure(RuntimeError):
    def __init__(self, phase: str, message: str):
        self.phase = phase
        super().__init__(message)


def _bounded(value: object) -> str:
    text = " ".join(str(value).split())
    return text[:_ERROR_LIMIT]


def _clean_environment(*, root: Path | None = None) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if (
            not key.startswith("MYPR_")
            and not key.startswith("PYTHON")
            and key not in _UV_ENV_SELECTORS
        )
    }
    if root is not None:
        env.update(
            {
                "XDG_RUNTIME_DIR": str(root / "runtime"),
                "XDG_CONFIG_HOME": str(root / "config"),
                "XDG_STATE_HOME": str(root / "state"),
                "XDG_DATA_HOME": str(root / "data"),
                "XDG_CACHE_HOME": str(root / "cache"),
                "PYTHONUNBUFFERED": "1",
            }
        )
        env.setdefault("UV_CACHE_DIR", str(root / "uv-cache"))
    return env


def _run(
    command: list[str],
    *,
    phase: str,
    cwd: Path,
    env: dict[str, str],
    timeout: int,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            text=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        if process is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.communicate()
        raise PhaseFailure(phase, f"command timed out after {timeout}s") from exc
    except OSError as exc:
        raise PhaseFailure(phase, str(exc)) from exc
    result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    if check and result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        raise PhaseFailure(phase, detail)
    return result


def _read_project() -> dict[str, Any]:
    try:
        with (PROJECT_ROOT / "pyproject.toml").open("rb") as handle:
            return tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PhaseFailure("metadata", f"cannot read pyproject.toml: {exc}") from exc


def _metadata_from_wheel(wheel: Path, project: dict[str, Any]) -> dict[str, Any]:
    expected = project["project"]
    try:
        with zipfile.ZipFile(wheel) as archive:
            metadata_name = next(
                name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
            )
            entry_name = metadata_name.removesuffix("METADATA") + "entry_points.txt"
            metadata = archive.read(metadata_name).decode("utf-8")
            entries = (
                archive.read(entry_name).decode("utf-8")
                if entry_name in archive.namelist()
                else ""
            )
    except (OSError, KeyError, StopIteration, UnicodeError, zipfile.BadZipFile) as exc:
        raise PhaseFailure("metadata", f"cannot inspect wheel metadata: {exc}") from exc
    fields: dict[str, list[str]] = {}
    for line in metadata.splitlines():
        if line.startswith((" ", "\t")) or ": " not in line:
            continue
        key, value = line.split(": ", 1)
        fields.setdefault(key, []).append(value)
    if fields.get("Name", [""])[0].lower() != expected["name"].lower():
        raise PhaseFailure("metadata", "wheel name does not match pyproject.toml")
    if fields.get("Version", [""])[0] != expected["version"]:
        raise PhaseFailure("metadata", "wheel version does not match pyproject.toml")
    if fields.get("Requires-Python", [""])[0] != expected["requires-python"]:
        raise PhaseFailure("metadata", "wheel Requires-Python does not match pyproject.toml")
    parser = configparser.ConfigParser()
    parser.read_string(entries or "[console_scripts]\n")
    actual_scripts = (
        dict(parser.items("console_scripts")) if parser.has_section("console_scripts") else {}
    )
    expected_scripts = expected.get("scripts", {})
    if actual_scripts != expected_scripts:
        raise PhaseFailure("metadata", "wheel console entry points do not match pyproject.toml")
    return {
        "name": expected["name"],
        "version": expected["version"],
        "requires_python": expected["requires-python"],
        "dependencies": len(fields.get("Requires-Dist", [])),
        "console_scripts": actual_scripts,
    }


def _verify_target_metadata(
    target_python: Path, project: dict[str, Any], *, cwd: Path, env: dict[str, str]
) -> None:
    expected = json.dumps(project["project"], separators=(",", ":"))
    checker = r'''
import importlib.metadata as metadata
import json
import sys
from packaging.requirements import Requirement


def requirement_key(value):
    requirement = Requirement(value)
    specifiers = tuple(sorted(str(item) for item in requirement.specifier))
    marker = str(requirement.marker) if requirement.marker is not None else None
    name = requirement.name.lower().replace("_", "-")
    return name, tuple(sorted(requirement.extras)), specifiers, marker


expected = json.loads(sys.argv[1])
dist = metadata.distribution(expected["name"])
if dist.version != expected["version"]:
    raise SystemExit("installed version does not match pyproject.toml")
if dist.metadata.get("Requires-Python") != expected["requires-python"]:
    raise SystemExit("installed Requires-Python does not match pyproject.toml")
actual = sorted(requirement_key(value) for value in (dist.requires or []))
wanted = sorted(requirement_key(value) for value in expected.get("dependencies", []))
if actual != wanted:
    raise SystemExit("installed dependencies do not match pyproject.toml")
entry_points = {
    entry.name: entry.value
    for entry in dist.entry_points
    if entry.group == "console_scripts"
}
if entry_points != expected.get("scripts", {}):
    raise SystemExit("installed console entry points do not match pyproject.toml")
'''
    try:
        _run(
            [str(target_python), "-c", checker, expected],
            phase="metadata",
            cwd=cwd,
            env=env,
            timeout=30,
        )
    except PhaseFailure as exc:
        raise PhaseFailure("metadata", str(exc)) from exc


def _find_wheel(directory: Path) -> Path:
    wheels = sorted(directory.glob("*.whl"))
    if len(wheels) != 1:
        raise PhaseFailure("build", f"expected one wheel, found {len(wheels)}")
    return wheels[0]


def _write_smoke_helper(directory: Path) -> Path:
    helper = directory / "smoke.py"
    helper.write_text(_SMOKE_HELPER, encoding="utf-8")
    return helper


def _run_smoke(
    target_python: Path,
    helper: Path,
    workspace: Path,
    env: dict[str, str],
    mode: str,
    *args: str,
) -> dict[str, Any]:
    phase = "smoke" if mode == "start" else mode.replace("-", "_")
    result = _run(
        [str(target_python), str(helper), mode, *args],
        phase=phase,
        cwd=workspace,
        env=env,
        timeout=DEFAULT_TIMEOUTS[phase],
    )
    try:
        value = json.loads(result.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        raise PhaseFailure(phase, "smoke helper returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise PhaseFailure(phase, "smoke helper returned a non-object")
    return value


def _stop_owned_manager(target_python: Path, workspace: Path, env: dict[str, str]) -> bool:
    try:
        result = _run(
            [str(target_python), "-m", "mypr_mcp.cli", "stop", "--force"],
            phase="stop",
            cwd=workspace,
            env=env,
            timeout=DEFAULT_TIMEOUTS["stop"],
            check=False,
        )
    except PhaseFailure:
        result = None
    if result is not None and result.returncode == 0:
        return True
    runtime = workspace / ".mypr" / "runtime.json"
    if not runtime.exists():
        return True
    try:
        record = json.loads(runtime.read_text(encoding="utf-8"))
        pid = record.get("pid")
        if type(pid) is not int or pid <= 1:
            return True
        proc_cwd = Path(f"/proc/{pid}/cwd").resolve()
        if not Path(f"/proc/{pid}").exists():
            return True
        if proc_cwd != workspace.resolve():
            return False
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and Path(f"/proc/{pid}").exists():
            time.sleep(0.1)
        if Path(f"/proc/{pid}").exists():
            os.kill(pid, signal.SIGKILL)
        return not Path(f"/proc/{pid}").exists()
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False


def run(output: Path | None = None) -> int:
    started = time.monotonic()
    phases: list[dict[str, Any]] = []
    result: dict[str, Any] = {"ok": False, "version": None, "phases": phases}
    root = Path(tempfile.mkdtemp(prefix="mypr-release-"))
    project: dict[str, Any] | None = None
    project_version: str | None = None
    target_python: Path | None = None
    cleanup_error: str | None = None
    workspace = root / "workspace"
    env = _clean_environment(root=root)
    workspace.mkdir()
    for path in (root / "runtime", root / "config", root / "state", root / "data", root / "cache"):
        path.mkdir()
    try:
        project = _read_project()
        project_version = project["project"]["version"]
        result["version"] = project_version
        uv = shutil.which("uv")
        if uv is None:
            raise PhaseFailure("lock", "uv executable was not found")

        def phase(name: str):
            class _Phase:
                def __enter__(self):
                    self.started = time.monotonic()
                    return self

                def __exit__(self, exc_type, exc, tb):
                    item = {
                        "name": name,
                        "duration_ms": round((time.monotonic() - self.started) * 1000, 1),
                        "ok": exc is None,
                    }
                    phases.append(item)
                    return False

            return _Phase()

        with phase("lock"):
            _run(
                [uv, "lock", "--check", "--offline"],
                phase="lock",
                cwd=PROJECT_ROOT,
                env=env,
                timeout=DEFAULT_TIMEOUTS["lock"],
            )
        dist = root / "dist"
        dist.mkdir()
        with phase("build"):
            _run(
                [uv, "build", "--wheel", "--no-sources", "--out-dir", str(dist)],
                phase="build",
                cwd=PROJECT_ROOT,
                env=env,
                timeout=DEFAULT_TIMEOUTS["build"],
            )
        wheel = _find_wheel(dist)
        with phase("metadata"):
            result["wheel"] = _metadata_from_wheel(wheel, project)
        target_env = root / "venv"
        with phase("venv"):
            _run(
                [uv, "venv", str(target_env), "--python", "3.14", "--no-project"],
                phase="venv",
                cwd=PROJECT_ROOT,
                env=env,
                timeout=DEFAULT_TIMEOUTS["venv"],
            )
            target_python = target_env / "bin" / "python"
            if not target_python.is_file():
                raise PhaseFailure("venv", "target Python executable was not created")
        with phase("install"):
            _run(
                [uv, "pip", "install", "--python", str(target_python), str(wheel)],
                phase="install",
                cwd=PROJECT_ROOT,
                env=env,
                timeout=DEFAULT_TIMEOUTS["install"],
            )
            check = _run(
                [
                    str(target_python),
                    "-c",
                    "import importlib.metadata as m; import mypr_mcp; "
                    "print(m.version('mypr-mcp')); print(mypr_mcp.__version__)",
                ],
                phase="install",
                cwd=root,
                env=env,
                timeout=30,
            )
            versions = check.stdout.splitlines()
            if versions != [project_version, project_version]:
                raise PhaseFailure("install", "installed package version is inconsistent")
            _verify_target_metadata(target_python, project, cwd=root, env=env)
        helper = _write_smoke_helper(root)
        with phase("smoke"):
            first = _run_smoke(target_python, helper, workspace, env, "start", project_version)
        with phase("reconnect"):
            second = _run_smoke(
                target_python,
                helper,
                workspace,
                env,
                "reconnect",
                project_version,
                first["client_id"],
                first["job_id"],
            )
        with phase("restart"):
            _run(
                [str(target_python), "-m", "mypr_mcp.cli", "restart"],
                phase="restart",
                cwd=workspace,
                env=env,
                timeout=DEFAULT_TIMEOUTS["restart"],
            )
        with phase("saved_output"):
            _run_smoke(
                target_python,
                helper,
                workspace,
                env,
                "saved-output",
                project_version,
                second["client_id"],
                first["job_id"],
            )
        result["ok"] = True
    except PhaseFailure as exc:
        result["error"] = {"phase": exc.phase, "message": _bounded(exc)}
    except Exception as exc:
        result["error"] = {"phase": "unknown", "message": _bounded(exc)}
    finally:
        if target_python is not None:
            try:
                if not _stop_owned_manager(target_python, workspace, env):
                    cleanup_error = "owned workspace manager did not stop cleanly"
            except Exception as exc:
                cleanup_error = _bounded(exc)
        result["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
        try:
            shutil.rmtree(root)
        except OSError as exc:
            cleanup_error = cleanup_error or _bounded(exc)
        if cleanup_error:
            if result["ok"]:
                result["ok"] = False
                result["error"] = {"phase": "cleanup", "message": cleanup_error}
            else:
                result["cleanup_error"] = cleanup_error
    if output is not None:
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="also write the JSON result to this path")
    args = parser.parse_args()
    return run(args.output)


if __name__ == "__main__":
    raise SystemExit(main())
