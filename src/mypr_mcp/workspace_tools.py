"""Kernel proxies for manager-owned workspace queries."""

from .pages import Pages

__all__ = ["Git", "Pages"]


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
        follow=False,
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
                "follow": follow,
                "cursor": cursor,
                "max_entries": max_entries,
                "max_bytes": max_bytes,
            },
        )

    async def commit_info(
        self,
        ref="HEAD",
        *,
        include_files=True,
        include_patch=False,
        cursor=None,
        max_bytes=32768,
    ):
        return await self._rpc(
            "git",
            method="commit_info",
            args={
                "ref": ref,
                "include_files": include_files,
                "include_patch": include_patch,
                "cursor": cursor,
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
