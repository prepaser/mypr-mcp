"""State-preserving manager replacement for a workspace.

The request side of this module is deliberately small: it validates the target,
reserves a ticket, and starts a detached coordinator.  The coordinator owns the
workspace startup lock while replacing the manager, so a failed replacement
cannot race a normal manager startup.
"""

from __future__ import annotations

import asyncio
import fcntl
import inspect
import json
import os
import secrets
import subprocess
import time
from pathlib import Path
from typing import Any

from packaging.version import InvalidVersion, Version

from .protocol import PROTOCOL_VERSION
from .transport import find_runtime, rpc, workspace_id

STATES = {"preparing", "stopping", "starting", "succeeded", "failed"}
TERMINAL_STATES = {"succeeded", "failed"}
TICKET_STALE_SECONDS = 300.0
COORDINATOR_START_GRACE = 10.0
DESCRIPTOR_TIMEOUT = 20.0


class RestartInProgress(RuntimeError):
    """Raised when a workspace already has an active replacement ticket."""

    def __init__(self, ticket: dict[str, Any]):
        self.ticket = ticket
        super().__init__(f"Workspace restart is already in progress ({ticket['id']})")


def _root(workspace: Path) -> Path:
    root = Path(workspace).resolve() / ".mypr"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _ticket_path(workspace: Path, ident: str | None = None) -> Path:
    root = _root(workspace)
    if ident is None:
        return root / "restart.json"
    return root / "restarts" / f"{ident}.json"


def _atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as file:
            os.fchmod(file.fileno(), 0o600)
            json.dump(value, file, ensure_ascii=True, separators=(",", ":"))
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_ticket(workspace: Path, ticket: dict[str, Any]) -> None:
    ticket = dict(ticket)
    ticket["updated_at"] = time.time()
    _atomic_write(_ticket_path(workspace), ticket)
    _atomic_write(_ticket_path(workspace, ticket["id"]), ticket)


async def _acquire_lock(lock) -> None:
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            await asyncio.sleep(0.05)


async def _finish_owned(task):
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


def _valid_ticket(workspace: Path, value: Any, ident: str | None = None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    ticket = dict(value)
    ticket_id = ticket.get("id")
    if (
        not isinstance(ticket_id, str)
        or len(ticket_id) != 32
        or not all(character in "0123456789abcdef" for character in ticket_id)
    ):
        return None
    if ident is not None and ticket_id != ident:
        return None
    if ticket.get("workspace_id") != workspace_id(workspace):
        return None
    if ticket.get("state") not in STATES:
        return None
    target = ticket.get("target")
    if not isinstance(target, dict):
        return None
    for key in ("python", "package_root", "version"):
        if not isinstance(target.get(key), str) or not target[key]:
            return None
    return ticket


def read_ticket(workspace: Path, ident: str | None = None) -> dict[str, Any] | None:
    """Read and validate the current ticket or an archived ticket by ID."""

    workspace = Path(workspace)
    if ident is not None and (
        not isinstance(ident, str)
        or len(ident) != 32
        or any(character not in "0123456789abcdef" for character in ident)
    ):
        return None
    path = _ticket_path(workspace, ident)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError, ValueError:
        return None
    try:
        return _valid_ticket(workspace, raw, ident)
    except OSError, ValueError:
        return None


def _pid_starttime(pid: int) -> str | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError, UnicodeError:
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    fields = raw[closing + 2 :].split()
    return fields[19] if len(fields) > 19 else None


def _process_alive(pid: Any, expected_starttime: Any) -> bool:
    if type(pid) is not int or pid <= 1:
        return False
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError, UnicodeError:
        return False
    closing = raw.rfind(")")
    fields = raw[closing + 2 :].split() if closing >= 0 else []
    if not fields or fields[0] == "Z":
        return False
    if expected_starttime is not None and _pid_starttime(pid) != str(expected_starttime):
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def active_ticket(workspace: Path) -> dict[str, Any] | None:
    """Return an active ticket, ignoring terminal or stale coordinator records."""

    workspace = Path(workspace)
    ticket = read_ticket(workspace)
    if ticket is None or ticket["state"] in TERMINAL_STATES:
        return None
    try:
        age = max(0.0, time.time() - float(ticket.get("updated_at", ticket["created_at"])))
    except KeyError, TypeError, ValueError:
        return None
    pid = ticket.get("coordinator_pid")
    if pid is None and age <= COORDINATOR_START_GRACE:
        return ticket
    if age > TICKET_STALE_SECONDS:
        return None
    if _process_alive(pid, ticket.get("coordinator_starttime")):
        return ticket
    return None


async def recover_ticket(workspace: Path) -> dict[str, Any] | None:
    """Finalize an abandoned coordinator ticket so callers can recover safely."""

    workspace = Path(workspace)
    ticket = read_ticket(workspace)
    if ticket is None or ticket["state"] in TERMINAL_STATES or active_ticket(workspace) is not None:
        return ticket
    ticket["state"] = "failed"
    ticket["error"] = "Restart coordinator is no longer running"
    try:
        await _finalize_origin(workspace, ticket)
    except Exception as exc:
        ticket["error"] += f"; origin finalization failed: {exc}"
    finally:
        _write_ticket(workspace, ticket)
    return ticket


async def wait_ticket(workspace: Path, ident: str, timeout: float = 240.0) -> dict[str, Any]:  # noqa: ASYNC109
    """Wait for one coordinator ticket to reach a terminal state."""

    workspace = Path(workspace)
    if (
        not isinstance(ident, str)
        or len(ident) != 32
        or any(character not in "0123456789abcdef" for character in ident)
    ):
        raise ValueError("invalid restart ticket ID")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout < 0:
        raise ValueError("timeout must be a non-negative number")
    deadline = time.monotonic() + timeout
    while True:
        ticket = read_ticket(workspace, ident)
        if ticket is not None and ticket["state"] in TERMINAL_STATES:
            return ticket
        if ticket is not None and active_ticket(workspace) is None:
            await recover_ticket(workspace)
            continue
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Restart {ident} did not finish within {timeout:g} seconds")
        await asyncio.sleep(0.1)


async def target_descriptor(target: dict[str, Any]) -> dict[str, Any]:
    """Load the protocol descriptor from a candidate installation."""

    python = Path(target.get("python", ""))
    package_root = Path(target.get("package_root", ""))
    if (  # noqa: ASYNC240
        not python.is_absolute()
        or not python.is_file()  # noqa: ASYNC240
        or not os.access(python, os.X_OK)
    ):
        raise ValueError("restart target python must be an executable absolute path")
    if not package_root.is_absolute() or not package_root.is_dir():  # noqa: ASYNC240
        raise ValueError("restart target package_root must be an absolute directory")
    script = """
import importlib
import json
import sys
sys.path.insert(0, sys.argv[1])
module = importlib.import_module('mypr_mcp.protocol')
value = getattr(module, 'descriptor')
if callable(value):
    value = value()
if not isinstance(value, dict):
    raise TypeError('protocol descriptor must be a JSON object')
print(json.dumps(value, separators=(',', ':')))
"""
    process = await asyncio.create_subprocess_exec(
        str(python),
        "-I",
        "-c",
        script,
        str(package_root),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), DESCRIPTOR_TIMEOUT)
    except TimeoutError, asyncio.CancelledError:
        process.kill()
        await process.wait()
        raise
    if process.returncode != 0:
        diagnostic = stderr.decode(errors="replace").strip()[-512:]
        raise RuntimeError(f"Cannot inspect restart target: {diagnostic or 'descriptor failed'}")
    try:
        descriptor = json.loads(stdout.decode())
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError("Restart target returned an invalid protocol descriptor") from exc
    if not isinstance(descriptor, dict):
        raise RuntimeError("Restart target returned an invalid protocol descriptor")
    if not isinstance(descriptor.get("version"), str):
        raise RuntimeError("Restart target descriptor has no package version")
    if type(descriptor.get("protocol_version")) is not int:
        raise RuntimeError("Restart target descriptor has no protocol version")
    return descriptor


def _check_version(target_version: str, current_version: Any) -> None:
    try:
        candidate = Version(target_version)
        current = Version(str(current_version))
    except InvalidVersion as exc:
        raise ValueError("restart target and manager versions must be valid versions") from exc
    if candidate < current:
        raise ValueError(f"Refusing to replace manager {current} with older version {candidate}")


def _coordinator_command(ticket: dict[str, Any], workspace: Path) -> list[str]:
    target = ticket["target"]
    package_root = target["package_root"]
    script = (
        "import sys; sys.path.insert(0, sys.argv[3]); "
        "from mypr_mcp.restart import main; main(sys.argv[1:3])"
    )
    # The target interpreter's installed dependencies (MCP, IPython, and the
    # workspace runtime) are required by the coordinator and new manager.
    return [target["python"], "-c", script, str(workspace), ticket["id"], package_root]


async def request_restart(
    workspace: Path,
    target: dict[str, Any],
    *,
    force: bool = False,
    origin: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Reserve and launch a detached manager replacement coordinator."""

    workspace = Path(workspace).resolve()  # noqa: ASYNC240
    if not isinstance(force, bool):
        raise ValueError("force must be a boolean")
    if not isinstance(target, dict):
        raise ValueError("restart target must be an object")
    existing = active_ticket(workspace)
    if existing is not None:
        raise RestartInProgress(existing)
    descriptor = await target_descriptor(target)
    if (
        descriptor.get("protocol_version") != PROTOCOL_VERSION
        or not isinstance(descriptor.get("capabilities"), list)
        or not all(isinstance(capability, str) for capability in descriptor["capabilities"])
    ):
        raise RuntimeError("Restart target uses an unsupported protocol descriptor")
    target = {
        "python": os.path.abspath(target["python"]),  # noqa: ASYNC240
        "package_root": str(Path(target["package_root"]).resolve()),  # noqa: ASYNC240
        "version": descriptor["version"],
        "protocol": {
            key: descriptor[key]
            for key in ("protocol_version", "capabilities")
            if key in descriptor
        },
    }
    observed = await find_runtime(workspace)
    if observed is None:
        raise RuntimeError("No reachable workspace manager to restart")
    observed_path, observed_state = observed
    root = _root(workspace)
    lock_path = root / "startup.lock"
    lock = await asyncio.to_thread(lock_path.open, "a+")
    try:
        await _acquire_lock(lock)
        existing = active_ticket(workspace)
        if existing is not None:
            raise RestartInProgress(existing)
        abandoned = read_ticket(workspace)
        if abandoned is not None and abandoned["state"] not in TERMINAL_STATES:
            await recover_ticket(workspace)
        found = await find_runtime(workspace)
        if found is None:
            raise RuntimeError("No reachable workspace manager to restart")
        path, state = found
        if (
            str(path) != str(observed_path)
            or state.get("pid") != observed_state.get("pid")
            or state.get("generation") != observed_state.get("generation")
        ):
            raise RuntimeError("Workspace manager changed before restart was reserved")
        _check_version(target["version"], state.get("version"))
        now = time.time()
        ticket = {
            "id": secrets.token_hex(16),
            "state": "preparing",
            "workspace_id": workspace_id(workspace),
            "created_at": now,
            "updated_at": now,
            "coordinator_pid": None,
            "coordinator_starttime": None,
            "old_pid": state.get("pid"),
            "old_generation": state.get("generation"),
            "target": target,
            "origin": dict(origin) if isinstance(origin, dict) else None,
            "force": force,
            "error": None,
            "new_generation": None,
            "new_version": None,
        }
        _write_ticket(workspace, ticket)
        log = (root / f"restart-{ticket['id']}.log").open("ab")
        launch = asyncio.create_task(
            asyncio.to_thread(
                subprocess.Popen,
                _coordinator_command(ticket, workspace),
                cwd=str(workspace),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                start_new_session=True,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
        )
        cancelled = False
        try:
            try:
                process = await asyncio.shield(launch)
            except asyncio.CancelledError:
                cancelled = True
                process = await _finish_owned(launch)
        except Exception:
            ticket["state"] = "failed"
            ticket["error"] = "Unable to start restart coordinator"
            _write_ticket(workspace, ticket)
            raise
        finally:
            log.close()
        ticket["coordinator_pid"] = process.pid
        ticket["coordinator_starttime"] = _pid_starttime(process.pid)
        _write_ticket(workspace, ticket)
        if cancelled:
            raise asyncio.CancelledError
        return ticket
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


async def _invoke_stop(path: Path, workspace: Path, force: bool, restart_id: str | None) -> Any:
    from .cli import stop_runtime

    return await stop_runtime(path, force, workspace=workspace, restart_id=restart_id)


async def _invoke_ensure(workspace: Path) -> Path:
    from .cli import ensure

    return await ensure(workspace, locked=True)


async def _finalize_origin(workspace: Path, ticket: dict[str, Any]) -> None:
    try:
        from .restart_records import finalize_origin
    except ImportError:
        return
    result = finalize_origin(workspace, ticket)
    if inspect.isawaitable(result):
        await result


async def _coordinate(workspace: Path, ident: str) -> None:
    workspace = Path(workspace).resolve()  # noqa: ASYNC240
    root = _root(workspace)
    lock = await asyncio.to_thread((root / "startup.lock").open, "a+")
    try:
        await _acquire_lock(lock)
        ticket = read_ticket(workspace, ident)
        if ticket is None or ticket["state"] in TERMINAL_STATES:
            return
        ticket["coordinator_pid"] = os.getpid()
        ticket["coordinator_starttime"] = _pid_starttime(os.getpid())
        _write_ticket(workspace, ticket)
        try:
            async with asyncio.timeout(225):
                found = await find_runtime(workspace)
                if found is None:
                    raise RuntimeError("Workspace manager disappeared before restart")
                path, state = found
                if ticket.get("old_pid") is not None and state.get("pid") != ticket["old_pid"]:
                    raise RuntimeError("Workspace manager changed before restart")
                if (
                    ticket.get("old_generation") is not None
                    and state.get("generation") != ticket["old_generation"]
                ):
                    raise RuntimeError("Workspace manager generation changed before restart")
                capabilities = state.get("capabilities")
                modern = (
                    state.get("protocol_version") == PROTOCOL_VERSION
                    and isinstance(capabilities, list)
                    and "restart" in capabilities
                )
                if modern:
                    await rpc(
                        path,
                        op="restart_prepare",
                        restart_id=ident,
                        force=ticket["force"],
                        origin=ticket.get("origin"),
                    )
                ticket["state"] = "stopping"
                _write_ticket(workspace, ticket)
                await _invoke_stop(path, workspace, ticket["force"], ident if modern else None)
                ticket["state"] = "starting"
                _write_ticket(workspace, ticket)
                package_root = str(ticket["target"]["package_root"])
                previous_pythonpath = os.environ.get("PYTHONPATH")
                os.environ["PYTHONPATH"] = (
                    package_root
                    if not previous_pythonpath
                    else package_root + os.pathsep + previous_pythonpath
                )
                try:
                    new_path = await _invoke_ensure(workspace)
                finally:
                    if previous_pythonpath is None:
                        os.environ.pop("PYTHONPATH", None)
                    else:
                        os.environ["PYTHONPATH"] = previous_pythonpath
                new_state = await rpc(new_path, op="status")
                if not new_state.get("healthy", True):
                    raise RuntimeError(new_state.get("health_error") or "New manager is unhealthy")
                if new_state.get("workspace_id") not in {None, ticket["workspace_id"]}:
                    raise RuntimeError("New manager workspace identity mismatch")
                ticket["state"] = "succeeded"
                ticket["new_generation"] = new_state.get("generation")
                ticket["new_version"] = new_state.get("version")
                ticket["error"] = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            ticket["state"] = "failed"
            ticket["error"] = (str(exc) or type(exc).__name__)[-2048:]
        if ticket["state"] in TERMINAL_STATES:
            try:
                await _finalize_origin(workspace, ticket)
            except Exception as exc:
                suffix = f"origin finalization failed: {exc}"
                ticket["error"] = f"{ticket['error']}; {suffix}" if ticket["error"] else suffix
        _write_ticket(workspace, ticket)
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(prog="mypr-mcp restart coordinator")
    parser.add_argument("workspace")
    parser.add_argument("ticket_id")
    args = parser.parse_args(argv)
    asyncio.run(_coordinate(Path(args.workspace), args.ticket_id))


__all__ = [
    "RestartInProgress",
    "active_ticket",
    "main",
    "recover_ticket",
    "read_ticket",
    "request_restart",
    "target_descriptor",
    "wait_ticket",
]
