"""Prepare the dependencies needed to start a workspace kernel."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import signal
import sys
from pathlib import Path

from .async_utils import wait_owned
from .diagnostics import RPCError
from .python_dependencies import CORE_PACKAGES, package_environment


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
        python = root / "venv/bin/python"
        if not python.exists():
            await command("uv", "venv", str(root / "venv"), "--python", sys.executable)

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
