"""Persistent workspace and user configuration API."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

_READ_SCOPES = frozenset(("effective", "global", "workspace"))
_WRITE_SCOPES = frozenset(("global", "workspace"))


class ConfigAPI:
    """Inspect and persist layered configuration through the manager."""

    def __init__(self, rpc: Callable[..., Awaitable[Any]]) -> None:
        self._rpc = rpc

    @staticmethod
    def _path(path: str | None, *, optional: bool = False) -> str | None:
        if path is None and optional:
            return None
        if not isinstance(path, str) or not path.strip():
            raise TypeError("path must be a non-empty string")
        return path

    @staticmethod
    def _scope(scope: str, allowed: frozenset[str]) -> str:
        if not isinstance(scope, str) or scope not in allowed:
            choices = ", ".join(sorted(allowed))
            raise ValueError(f"scope must be one of: {choices}")
        return scope

    async def get(self, path: str | None = None, scope: str = "effective") -> Any:
        """Read a configuration value or section from one configuration scope."""
        path = self._path(path, optional=True)
        scope = self._scope(scope, _READ_SCOPES)
        return await self._rpc("config", method="get", path=path, scope=scope)

    async def set(self, path: str, value: Any, scope: str = "workspace") -> Any:
        """Persist a JSON-compatible value without applying pending changes."""
        path = self._path(path)
        scope = self._scope(scope, _WRITE_SCOPES)
        return await self._rpc("config", method="set", path=path, value=value, scope=scope)

    async def unset(self, path: str, scope: str = "workspace") -> Any:
        """Remove a value from one writable configuration scope."""
        path = self._path(path)
        scope = self._scope(scope, _WRITE_SCOPES)
        return await self._rpc("config", method="unset", path=path, scope=scope)

    async def explain(self, path: str) -> Any:
        """Explain a setting's desired, applied, source, and revision state."""
        path = self._path(path)
        return await self._rpc("config", method="explain", path=path)

    async def reload(self, force: bool = False) -> Any:
        """Apply persisted configuration changes explicitly."""
        if type(force) is not bool:
            raise TypeError("force must be a boolean")
        return await self._rpc("config", method="reload", force=force)


__all__ = ["ConfigAPI"]
