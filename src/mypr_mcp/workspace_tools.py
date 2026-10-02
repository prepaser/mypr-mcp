"""Kernel proxies for manager-owned workspace queries."""

from .git_api import _UNSET
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

    async def diff(
        self,
        *,
        staged=_UNSET,
        rev=_UNSET,
        paths=_UNSET,
        cursor=None,
        max_bytes=32768,
    ):
        args = {"cursor": cursor, "max_bytes": max_bytes}
        if staged is not _UNSET:
            args["staged"] = staged
        if rev is not _UNSET:
            args["rev"] = rev
        if paths is not _UNSET:
            args["paths"] = paths
        return await self._rpc(
            "git",
            method="diff",
            args=args,
        )

    async def show(self, ref=_UNSET, *, path=_UNSET, cursor=None, max_bytes=32768):
        args = {"cursor": cursor, "max_bytes": max_bytes}
        if ref is not _UNSET:
            args["ref"] = ref
        if path is not _UNSET:
            args["path"] = path
        return await self._rpc(
            "git",
            method="show",
            args=args,
        )

    async def log(
        self,
        ref=_UNSET,
        *,
        path=_UNSET,
        author=_UNSET,
        since=_UNSET,
        until=_UNSET,
        follow=_UNSET,
        cursor=None,
        max_entries=50,
        max_bytes=32768,
    ):
        args = {"cursor": cursor, "max_entries": max_entries, "max_bytes": max_bytes}
        for name, value in (
            ("ref", ref),
            ("path", path),
            ("author", author),
            ("since", since),
            ("until", until),
            ("follow", follow),
        ):
            if value is not _UNSET:
                args[name] = value
        return await self._rpc(
            "git",
            method="log",
            args=args,
        )

    async def commit_info(
        self,
        ref=_UNSET,
        *,
        include_files=_UNSET,
        include_patch=_UNSET,
        cursor=None,
        max_bytes=32768,
    ):
        args = {"cursor": cursor, "max_bytes": max_bytes}
        for name, value in (
            ("ref", ref),
            ("include_files", include_files),
            ("include_patch", include_patch),
        ):
            if value is not _UNSET:
                args[name] = value
        return await self._rpc(
            "git",
            method="commit_info",
            args=args,
        )

    async def blame(
        self,
        path=_UNSET,
        ref=_UNSET,
        *,
        start_line=_UNSET,
        end_line=_UNSET,
        cursor=None,
        max_entries=100,
        max_bytes=32768,
    ):
        args = {"cursor": cursor, "max_entries": max_entries, "max_bytes": max_bytes}
        for name, value in (
            ("path", path),
            ("ref", ref),
            ("start_line", start_line),
            ("end_line", end_line),
        ):
            if value is not _UNSET:
                args[name] = value
        return await self._rpc(
            "git",
            method="blame",
            args=args,
        )
