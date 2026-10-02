"""Kernel access to manager-owned dependency preparation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from .diagnostics import RPCError


class Dependencies:
    def __init__(self, rpc: Callable[..., Awaitable[Any]]) -> None:
        self._rpc = rpc

    async def _call(self, method: str, **params: Any) -> Any:
        try:
            return await self._rpc("dependencies", method=method, params=params)
        except RPCError as exc:
            message = str(exc).strip().casefold().removeprefix("valueerror: ").strip()
            if message in {"unknown operation: dependencies", "unknown operation dependencies"}:
                raise RPCError(
                    "The running manager does not support dependency preparation; "
                    "restart it with the current mypr-mcp installation.",
                    code="capability_missing",
                    operation="dependencies",
                    details={"capability": "dependencies", "restart_required": True},
                ) from exc
            raise

    async def list(
        self, kind: str | None = None, limit: int = 50, cursor: str | None = None
    ) -> dict[str, Any]:
        """Inspect a bounded dependency page without installing anything."""
        return await self._call("list", kind=kind, limit=limit, cursor=cursor)

    async def ensure(self, *names: str) -> dict[str, Any]:
        """Prepare registered dependencies, including when automatic installation is off."""
        return await self._call("ensure", names=list(names), automatic=False)

    async def _automatic(self, *names: str, automatic: bool = True) -> dict[str, Any]:
        return await self._call("ensure", names=list(names), automatic=automatic)
