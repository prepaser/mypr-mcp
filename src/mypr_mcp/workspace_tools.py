"""Kernel proxies for manager-owned workspace queries."""


class Git:
    def __init__(self, rpc):
        self._rpc = rpc

    async def status(self, *, cursor=None, max_entries=200, max_bytes=32768):
        return await self._rpc(
            "git",
            method="status",
            args={
                "cursor": cursor,
                "max_entries": max_entries,
                "max_bytes": max_bytes,
            },
        )

    async def diff(self, *, staged=False, rev=None, paths=None, cursor=None, max_bytes=32768):
        return await self._rpc(
            "git",
            method="diff",
            args={
                "staged": staged,
                "rev": rev,
                "paths": paths,
                "cursor": cursor,
                "max_bytes": max_bytes,
            },
        )

    async def show(self, ref="HEAD", *, path=None, cursor=None, max_bytes=32768):
        return await self._rpc(
            "git",
            method="show",
            args={
                "ref": ref,
                "path": path,
                "cursor": cursor,
                "max_bytes": max_bytes,
            },
        )
