"""Entry point for the persistent workspace IPython kernel."""

from __future__ import annotations

import ctypes
import os
import signal
import sys
from pathlib import Path

if __package__:
    from .cells import CellExecutor, install_context_displayhook
    from .kernel_api import MultiplexStream, create_workspace, execution_context
else:
    _source = Path(__file__).resolve().parents[1]
    if str(_source) not in sys.path:
        sys.path.insert(0, str(_source))
    from mypr_mcp.cells import CellExecutor, install_context_displayhook
    from mypr_mcp.kernel_api import MultiplexStream, create_workspace, execution_context


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


def _kernel_class():
    from ipykernel.ipkernel import IPythonKernel

    class WorkspaceKernel(IPythonKernel):
        async def execute_request(self, stream, ident, parent):
            metadata = (parent or {}).get("metadata", {})
            request = metadata.get("mypr") if isinstance(metadata, dict) else None
            content = (parent or {}).get("content", {})
            executor = getattr(self, "_mypr_cells", None)
            if isinstance(request, dict) and executor is not None and request.get("exec_id"):
                handle = executor.submit(
                    content.get("code", ""),
                    request,
                    parent,
                    ident,
                    stream,
                    silent=bool(content.get("silent", False)),
                    store_history=bool(content.get("store_history", True)),
                    user_expressions=content.get("user_expressions", {}),
                    allow_stdin=bool(content.get("allow_stdin", False)),
                    stop_on_error=bool(content.get("stop_on_error", False)),
                )
                self.set_parent(ident, parent)
                self.session.send(
                    stream,
                    "execute_reply",
                    {
                        "status": "ok",
                        "execution_count": handle.execution_count,
                        "user_expressions": {},
                        "payload": [],
                    },
                    parent,
                    ident=ident,
                )
                return
            with execution_context(request if isinstance(request, dict) else None):
                await super().execute_request(stream, ident, parent)

    return WorkspaceKernel


def main(argv: list[str] | None = None) -> None:
    _parent_death_signal()
    workspace = Path(os.environ.get("MYPR_WORKSPACE", os.getcwd())).resolve()
    lib = workspace / ".mypr" / "lib"
    lib.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(lib))

    from ipykernel.kernelapp import IPKernelApp

    app = IPKernelApp.instance()
    app.kernel_class = _kernel_class()
    app.initialize(argv)
    namespace = app.shell.user_ns
    ws = create_workspace(workspace, namespace)
    namespace.update({"ws": ws, "workspace": workspace})
    app.shell.user_ns.setdefault("__name__", "__main__")
    install_context_displayhook(app.shell)
    app.kernel._mypr_cells = CellExecutor(app.kernel, app.shell, ws.tasks)
    sys.stdout = MultiplexStream(sys.stdout)
    sys.stderr = MultiplexStream(sys.stderr)
    app.start()


if __name__ == "__main__":
    main()
