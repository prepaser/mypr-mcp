"""Entry point for the persistent workspace IPython kernel."""

from __future__ import annotations

import ctypes
import os
import signal
import sys
from pathlib import Path

if __package__:
    from .kernel_api import MultiplexStream, create_workspace
else:
    _source = Path(__file__).resolve().parents[1]
    if str(_source) not in sys.path:
        sys.path.insert(0, str(_source))
    from mypr_mcp.kernel_api import MultiplexStream, create_workspace


def _parent_death_signal() -> None:
    value = os.environ.get("MYPR_PARENT_PID")
    if not value:
        raise RuntimeError("MYPR_PARENT_PID is required")
    try:
        parent_pid = int(value)
    except ValueError as exc:
        raise RuntimeError("MYPR_PARENT_PID must be an integer") from exc
    if parent_pid <= 1 or os.getppid() != parent_pid:
        raise RuntimeError("kernel parent changed before startup")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    if os.getppid() != parent_pid:
        raise RuntimeError("kernel parent changed during startup")


def main(argv: list[str] | None = None) -> None:
    _parent_death_signal()
    workspace = Path(os.environ.get("MYPR_WORKSPACE", os.getcwd())).resolve()
    lib = workspace / ".mypr" / "lib"
    lib.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(lib))

    from ipykernel.kernelapp import IPKernelApp

    app = IPKernelApp.instance()
    app.initialize(argv)
    namespace = app.shell.user_ns
    ws = create_workspace(workspace, namespace)
    namespace.update({"ws": ws, "workspace": workspace})
    app.shell.user_ns.setdefault("__name__", "__main__")
    sys.stdout = MultiplexStream(sys.stdout)
    sys.stderr = MultiplexStream(sys.stderr)
    app.start()


if __name__ == "__main__":
    main()
