"""Task-scoped cooperative locks for the persistent workspace kernel."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Waiter:
    task: asyncio.Task[Any]
    names: tuple[str, ...]
    event: asyncio.Event = field(default_factory=asyncio.Event)
    identity: dict[str, Any] = field(default_factory=dict)


class WorkspaceLocks:
    """Cooperative locks that live for the lifetime of the Python kernel.

    A lease belongs to the current asyncio task.  The owner callback is sampled
    when a lease is granted, so status remains useful after the request context
    has gone away.
    """

    def __init__(
        self,
        identity: Callable[[], Mapping[str, Any] | None] | None = None,
        *,
        max_items: int = 128,
    ) -> None:
        if not isinstance(max_items, int) or isinstance(max_items, bool) or max_items < 1:
            raise ValueError("max_items must be a positive integer")
        self._identity = identity
        self._max_items = max_items
        self._guard = asyncio.Lock()
        self._held: dict[str, tuple[asyncio.Task[Any], dict[str, Any]]] = {}
        self._waiters: list[_Waiter] = []
        self._watched_tasks: set[asyncio.Task[Any]] = set()

    @staticmethod
    def _names(names: tuple[str, ...]) -> tuple[str, ...]:
        if not names:
            raise ValueError("at least one lock name is required")
        clean: set[str] = set()
        for name in names:
            if not isinstance(name, str) or not name or len(name) > 256:
                raise ValueError("lock names must be non-empty strings of at most 256 characters")
            clean.add(name)
        return tuple(sorted(clean))

    def _owner_info(self) -> dict[str, Any]:
        info: Mapping[str, Any] | None = None
        if self._identity is not None:
            try:
                info = self._identity()
            except Exception:
                info = None
        result = {
            key: str(value)
            for key, value in (info or {}).items()
            if key in {"client_id", "connection_id", "exec_id"} and value is not None
        }
        task = asyncio.current_task()
        if task is not None:
            result["task"] = task.get_name()
        return result

    def _notify(self) -> None:
        for waiter in self._waiters:
            waiter.event.set()

    def _can_grant(self, waiter: _Waiter) -> bool:
        return all(name not in self._held for name in waiter.names)

    def _release_task(self, task: asyncio.Task[Any]) -> None:
        released = [name for name, (owner, _) in self._held.items() if owner is task]
        for name in released:
            self._held.pop(name, None)
        self._waiters[:] = [waiter for waiter in self._waiters if waiter.task is not task]
        if released:
            self._notify()

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        # Done callbacks execute on the same event loop as the lock manager.
        self._watched_tasks.discard(task)
        self._release_task(task)

    @asynccontextmanager
    async def acquire(self, *names: str, timeout: float | None = None):  # noqa: ASYNC109
        names = self._names(names)
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise ValueError("timeout must be a finite non-negative number or None")
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("workspace locks require an asyncio task")
        identity = self._owner_info()
        waiter = _Waiter(task, names, identity=identity)
        acquired = False
        try:

            async def wait_for_lease() -> None:
                while True:
                    async with self._guard:
                        if any(owner is task for owner, _ in self._held.values()):
                            raise RuntimeError("workspace lock acquisition is not re-entrant")
                        if self._can_grant(waiter):
                            self._waiters.remove(waiter)
                            for name in names:
                                self._held[name] = (task, identity)
                            if task not in self._watched_tasks:
                                self._watched_tasks.add(task)
                                task.add_done_callback(self._task_done)
                            return
                        waiter.event.clear()
                    await waiter.event.wait()

            self._waiters.append(waiter)
            if timeout is None:
                await wait_for_lease()
            else:
                async with asyncio.timeout(timeout):
                    await wait_for_lease()
            acquired = True
            yield
        finally:
            # The event loop cannot interleave between these synchronous
            # mutations. Avoid awaiting here so cancellation cannot interrupt
            # cleanup after the body has exited.
            if acquired:
                for name in names:
                    owner = self._held.get(name)
                    if owner is not None and owner[0] is task:
                        self._held.pop(name, None)
                self._notify()
            else:
                with suppress(ValueError):
                    self._waiters.remove(waiter)
                self._notify()

    def list(self) -> list[dict[str, Any]]:
        """Return bounded lock ownership and waiter information."""

        grouped: dict[str, dict[str, Any]] = {}
        for name, (task, owner) in sorted(self._held.items()):
            item = grouped.setdefault(str(id(task)), {"owner": dict(owner), "names": []})
            item["names"].append(name)
        result = list(grouped.values())
        for waiter in self._waiters[: self._max_items]:
            result.append(
                {
                    "waiting": True,
                    "names": list(waiter.names),
                    "owner": dict(waiter.identity),
                }
            )
        if len(self._waiters) > self._max_items:
            result.append({"waiting_truncated": True})
        return result


__all__ = ["WorkspaceLocks"]
