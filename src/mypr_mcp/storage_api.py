"""Kernel access to manager-owned storage maintenance."""

from __future__ import annotations

from typing import Any


class StorageAPI:
    def __init__(self, rpc):
        self._rpc = rpc

    async def usage(self) -> dict[str, Any]:
        """Inspect managed data plus protected and total ``.mypr`` usage.

        Legacy ``total_bytes``, ``total_files``, and ``categories`` describe
        GC-managed files.  ``managed``, ``protected``, and ``workspace`` also
        report logical and allocated bytes, unique inodes, and hardlinks.
        """
        return await self._rpc("storage_usage")

    async def gc(
        self,
        *,
        dry_run: bool = True,
        older_than_days: int | None = None,
        max_bytes: int | None = None,
        revision_keep: int | None = None,
    ) -> dict[str, Any]:
        """Preview cleanup using workspace policy, or explicitly apply it.

        The preview includes an exact database history selection when the
        history adapter supports retention maintenance.
        """
        args = {"dry_run": dry_run}
        if older_than_days is not None:
            args["older_than_days"] = older_than_days
        if max_bytes is not None:
            args["max_bytes"] = max_bytes
        if revision_keep is not None:
            args["revision_keep"] = revision_keep
        return await self._rpc("storage_gc", **args)

    async def gc_apply(self, plan_id: str) -> dict[str, Any]:
        """Apply a cleanup plan after rechecking files and database records."""
        return await self._rpc("storage_gc_apply", plan_id=plan_id)
