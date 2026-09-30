"""Kernel access to manager-owned storage maintenance."""

from __future__ import annotations

from typing import Any


class StorageAPI:
    def __init__(self, rpc):
        self._rpc = rpc

    async def usage(self) -> dict[str, Any]:
        """Inspect managed disk data and the most recent automatic cleanup."""
        return await self._rpc("storage_usage")

    async def gc(
        self,
        *,
        dry_run: bool = True,
        older_than_days: int | None = None,
        max_bytes: int | None = None,
        revision_keep: int | None = None,
    ) -> dict[str, Any]:
        """Preview cleanup using workspace policy, or explicitly apply it."""
        args = {"dry_run": dry_run}
        if older_than_days is not None:
            args["older_than_days"] = older_than_days
        if max_bytes is not None:
            args["max_bytes"] = max_bytes
        if revision_keep is not None:
            args["revision_keep"] = revision_keep
        return await self._rpc("storage_gc", **args)

    async def gc_apply(self, plan_id: str) -> dict[str, Any]:
        """Apply a cleanup plan after rechecking live work and references."""
        return await self._rpc("storage_gc_apply", plan_id=plan_id)
