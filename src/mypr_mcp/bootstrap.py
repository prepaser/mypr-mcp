"""Prepare the dependencies needed to start a workspace kernel."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import secrets
import shutil
import signal
import stat
import subprocess
import sys
from pathlib import Path

from .async_utils import wait_owned
from .diagnostics import RPCError
from .file_io import open_regular, read_bytes
from .python_dependencies import CORE_PACKAGES, package_environment

_INVALID_VENV_IGNORE = b".venv-invalid-*/"


def _workspace_python_paths(root: Path) -> tuple[Path, Path, Path]:
    venv = root / "venv"
    return venv, venv / "bin", venv / "bin" / "python"


def _workspace_python_structure(root: Path) -> bool:
    """Check the files that make a workspace virtual environment usable."""

    try:
        root = root.resolve()
        venv, bin_dir, python = _workspace_python_paths(root)
        if venv.is_symlink() or not venv.is_dir():
            return False
        if not bin_dir.is_dir():
            return False
        bin_target = bin_dir.resolve() if bin_dir.is_symlink() else bin_dir
        if not bin_target.is_relative_to(root):
            return False
        config_candidates = [venv / "pyvenv.cfg"]
        if bin_dir.is_symlink():
            config_candidates.append(bin_target.parent / "pyvenv.cfg")
        config_info = next(
            (
                candidate.stat()
                for candidate in config_candidates
                if candidate.is_file()
            ),
            None,
        )
        if (
            config_info is None
            or not stat.S_ISREG(config_info.st_mode)
            or config_info.st_size > 64 * 1024
        ):
            return False
        python_info = python.stat()
        if not stat.S_ISREG(python_info.st_mode) or not os.access(python, os.X_OK):
            return False
        result = subprocess.run(
            [
                str(python), "-I", "-c",
                "import sys; raise SystemExit(sys.prefix == sys.base_prefix)",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
    except (OSError, RuntimeError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


def _remove_workspace_venv(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path)


def _ensure_recovery_ignore(root: Path) -> None:
    path = root / ".gitignore"
    try:
        data = read_bytes(path, max_bytes=16 * 1024 * 1024)
    except FileNotFoundError:
        data = b""
    if _INVALID_VENV_IGNORE in data.splitlines():
        return
    prefix = b"" if not data or data.endswith(b"\n") else b"\n"
    with open_regular(path, "ab") as stream:
        stream.write(prefix + _INVALID_VENV_IGNORE + b"\n")


async def _repair_workspace_python(root: Path, command) -> Path:
    venv, _bin_dir, python = _workspace_python_paths(root)
    backup: Path | None = None
    if venv.exists() or venv.is_symlink():
        backup = root / f".venv-invalid-{secrets.token_hex(8)}"
        os.replace(venv, backup)
    try:
        await command("uv", "venv", str(venv), "--python", sys.executable)
        if not await wait_owned(asyncio.to_thread(_workspace_python_structure, root)):
            raise RuntimeError("uv created an unusable workspace Python environment")
        await wait_owned(asyncio.to_thread(_ensure_recovery_ignore, root))
    except BaseException:
        with contextlib.suppress(OSError):
            _remove_workspace_venv(venv)
        if backup is not None and not venv.exists() and not venv.is_symlink():
            os.replace(backup, venv)
        raise
    return python


async def ensure_workspace_python(root: Path, *, command=None) -> Path:
    """Return a usable workspace interpreter, repairing a broken environment."""

    root = Path(root)
    if command is None:
        command = run_command
    if await wait_owned(asyncio.to_thread(_workspace_python_structure, root)):
        return _workspace_python_paths(root)[2]
    return await wait_owned(_repair_workspace_python(root, command))


async def run_command(*args: str, env=None, timeout_seconds: float = 180) -> None:
    guard = Path(__file__).with_name("process_guard.py")
    launch = asyncio.create_task(
        asyncio.create_subprocess_exec(
            sys.executable,
            str(guard),
            "--parent-pid",
            str(os.getpid()),
            "--tree",
            "--",
            *args,
            stdout=sys.stderr,
            stderr=sys.stderr,
            start_new_session=True,
            env=env,
        )
    )
    process = None
    try:
        async with asyncio.timeout(timeout_seconds):
            process = await asyncio.shield(launch)
            code = await process.wait()
    except BaseException:
        if process is None:
            process = await wait_owned(launch, propagate=False)
        if process.returncode is None:
            await wait_owned(_stop(process), propagate=False)
        raise
    if code:
        raise RuntimeError(f"Command failed: {args[0]}")


async def _stop(process):
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        async with asyncio.timeout(10):
            await process.wait()
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        await process.wait()


async def install_core(python: Path, root: Path, names, config, command=run_command):
    await command(
        sys.executable,
        "-I",
        str(Path(__file__).with_name("package_worker.py")),
        "--python",
        str(python),
        "--root",
        str(root),
        "--spec-json",
        json.dumps(list(names)),
        "--automatic",
        env=package_environment(config),
    )


async def prepare_core(service, *, automatic: bool = True):
    try:
        return await service.ensure(
            CORE_PACKAGES,
            automatic=automatic,
            context={"bootstrap": True},
        )
    except RPCError as exc:
        if exc.code != "dependency_missing":
            raise
        raise RPCError(
            f"{exc} Prepare the workspace kernel with `uvx mypr-mcp prepare` "
            "from the workspace directory, then call init again.",
            code=exc.code,
            operation="kernel_prepare",
            details={
                **exc.details,
                "prepare_command": "uvx mypr-mcp prepare",
                "required": list(CORE_PACKAGES),
            },
        ) from exc


async def prepare_workspace(workspace: Path, *, command=run_command):
    from .config import ConfigStore
    from .dependency_service import DependencyService
    from .file_io import open_regular
    from .transport import manager_running

    workspace = await asyncio.to_thread(workspace.resolve)
    root = workspace / ".mypr"
    await wait_owned(asyncio.to_thread(root.mkdir, parents=True, exist_ok=True))
    config = ConfigStore(workspace).load().values["dependencies"]
    lock = open_regular(root / "startup.lock", "ab")
    service = None
    try:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                await asyncio.sleep(0.05)
        if await asyncio.to_thread(manager_running, workspace):
            raise RPCError(
                "Stop the workspace manager before preparing kernel dependencies.",
                code="manager_running", operation="kernel_prepare",
            )
        python = await ensure_workspace_python(root, command=command)

        async def install(names, _context):
            await install_core(python, root, names, config, command)

        async def no_browser(*_args):
            raise RuntimeError("Kernel preparation does not install browsers")

        service = DependencyService(workspace, python, config, install, no_browser)
        prepared = await prepare_core(service, automatic=False)
        return {"workspace": str(workspace), "python": str(python), **prepared}
    finally:
        if service is not None:
            await wait_owned(service.close(), propagate=False)
        lock.close()
