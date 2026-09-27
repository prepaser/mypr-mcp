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

    async def log(
        self,
        ref="HEAD",
        *,
        path=None,
        author=None,
        since=None,
        until=None,
        cursor=None,
        max_entries=50,
        max_bytes=32768,
    ):
        return await self._rpc(
            "git",
            method="log",
            args={
                "ref": ref,
                "path": path,
                "author": author,
                "since": since,
                "until": until,
                "cursor": cursor,
                "max_entries": max_entries,
                "max_bytes": max_bytes,
            },
        )

    async def blame(
        self,
        path=None,
        ref="HEAD",
        *,
        start_line=None,
        end_line=None,
        cursor=None,
        max_entries=100,
        max_bytes=32768,
    ):
        return await self._rpc(
            "git",
            method="blame",
            args={
                "path": path,
                "ref": ref,
                "start_line": start_line,
                "end_line": end_line,
                "cursor": cursor,
                "max_entries": max_entries,
                "max_bytes": max_bytes,
            },
        )
