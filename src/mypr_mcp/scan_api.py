"""Python-facing manager-backed network scan handles."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any


class ScanTaskMixin:
    """Scan-specific methods mixed into the existing RemoteTask handle."""

    async def summary(self, *, wait_ms: int = 0) -> dict[str, Any]:
        return await self._scan_rpc("scan_summary", id=self.id, wait_ms=wait_ms)

    async def results(
        self,
        cursor: str | None = None,
        *,
        max_entries: int = 100,
        max_bytes: int = 32768,
    ) -> dict[str, Any]:
        return await self._scan_rpc(
            "scan_results",
            id=self.id,
            cursor=cursor,
            max_entries=max_entries,
            max_bytes=max_bytes,
        )

    async def _watch(self) -> None:
        from .diagnostics import safe_error

        try:
            await super()._watch()
            summary = await self.summary(wait_ms=30000)
            if summary.get("state") not in {"succeeded", "failed", "cancelled", "lost"}:
                raise RuntimeError("scan worker exited without a terminal summary")
            self._scan_result = summary
        except Exception as exc:
            self._state = "lost"
            self._error = safe_error(exc)
            raise
        finally:
            self._scan_ready = True
            self._buffer.changed.set()
            for buffer in self._streams.values():
                buffer.changed.set()
            self._manager._completed_handle(self)

    def status(self) -> dict[str, Any]:
        result = super().status()
        result["kind"] = "scan"
        if not getattr(self, "_scan_ready", False) and result["status"] in {
            "succeeded",
            "failed",
            "cancelled",
            "lost",
        }:
            result["status"] = "running"
        return result

    async def _wait(self) -> Any:
        await self._monitor
        return self.result()

    def result(self) -> Any:
        from .kernel_api import NotReady

        if not getattr(self, "_scan_ready", False):
            raise NotReady(f"scan {self.id} is still running")
        if hasattr(self, "_scan_result"):
            return self._scan_result
        return super().result()


def make_scan_task(
    ident: str,
    tasks: Any,
    rpc: Callable[..., Awaitable[Any]],
    remote_task_cls: type | None = None,
) -> Any:
    """Create a RemoteTask-compatible scan handle without an import cycle."""

    if remote_task_cls is None:
        from .kernel_api import RemoteTask

        remote_task_cls = RemoteTask

    class ManagedScanTask(ScanTaskMixin, remote_task_cls):
        def __init__(self, task_id: str, manager: Any):
            self._scan_rpc = rpc
            super().__init__(task_id, manager)

    ManagedScanTask.__name__ = "ScanTask"
    return ManagedScanTask(str(ident), tasks)


class Net:
    """Manager-backed TCP and Nmap scans."""

    def __init__(
        self,
        rpc: Callable[..., Awaitable[Any]],
        tasks: Any,
        task_factory: Callable[[str, Any, Callable[..., Awaitable[Any]]], Any] | None = None,
    ):
        self._rpc = rpc
        self._tasks = tasks
        self._task_factory = task_factory or make_scan_task

    def _task(self, ident: str) -> Any:
        return self._task_factory(str(ident), self._tasks, self._rpc)

    async def scan(  # noqa: ASYNC109
        self,
        targets: str | list[str],
        *,
        ports: Any = "1-1024",
        concurrency: int = 64,
        rate: float = 200,
        timeout: float = 1.0,  # noqa: ASYNC109
    ) -> Any:
        result = await self._rpc(
            "scan_start",
            mode="tcp",
            targets=targets,
            ports=ports,
            concurrency=concurrency,
            rate=rate,
            timeout=timeout,
        )
        ident = result.get("id") if isinstance(result, Mapping) else result
        if not ident:
            raise RuntimeError("scan_start returned no task id")
        return self._task(str(ident))

    async def nmap(self, targets: str | list[str], *, args: list[str] | None = None) -> Any:
        result = await self._rpc("scan_start", mode="nmap", targets=targets, args=args or [])
        ident = result.get("id") if isinstance(result, Mapping) else result
        if not ident:
            raise RuntimeError("scan_start returned no task id")
        return self._task(str(ident))


__all__ = ["Net", "ScanTaskMixin", "make_scan_task"]
