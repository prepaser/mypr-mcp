"""Small, explicit adapters for consuming workspace cursors."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from typing import Any

_MISSING = object()


class PageLimitReached(RuntimeError):
    """Raised when page iteration needs a continuation after its limit."""

    def __init__(
        self,
        method: str | Callable[..., Awaitable[Any]],
        pages_read: int,
        *,
        next_cursor: Any,
        next_kwargs: Mapping[str, Any],
    ) -> None:
        self.method = method
        self.pages_read = pages_read
        self.next_cursor = next_cursor
        self.next_kwargs = dict(next_kwargs)
        name = method if isinstance(method, str) else getattr(method, "__qualname__", repr(method))
        if not isinstance(name, str):
            name = repr(name)
        self.code = "page_limit_reached"
        self.details = {
            "method": name,
            "pages_read": pages_read,
            "next_cursor": next_cursor,
        }
        super().__init__(f"{name} reached max_pages after {pages_read} pages")


class Pages:
    """Expose bounded async iteration without hiding query failures."""

    def __init__(self, workspace: Any):
        self._workspace = workspace

    async def iter(
        self,
        method: str | Callable[..., Awaitable[Any]],
        *args: Any,
        max_pages: int = 100,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        if type(max_pages) is not int or not 1 <= max_pages <= 10_000:
            raise ValueError("max_pages must be between 1 and 10000")
        call, name = self._resolve(method)
        options = dict(kwargs)
        search_method = self._search_method(method, name)
        if (
            search_method
            and options.get("cursor") is not None
            and options.get("page_cursor") is not None
        ):
            raise ValueError("cursor and page_cursor cannot both be provided")
        if search_method and (
            options.get("cursor") is not None or options.get("page_cursor") is not None
        ):
            cursor = options.get("cursor")
            if cursor is None:
                cursor = options["page_cursor"]
            args = ()
            options = self._search_continuation(options, cursor)
        previous_marker: Any = options.get("cursor", _MISSING)
        seen: set[str] = set()
        revision: Any = _MISSING
        for page_number in range(1, max_pages + 1):
            result = await call(*args, **options)
            if not isinstance(result, Mapping):
                raise TypeError(
                    f"paged method returned {type(result).__name__}, expected a mapping"
                )
            page = dict(result)
            current_revision = self._revision(page)
            if current_revision is not None:
                if revision is _MISSING:
                    revision = current_revision
                elif current_revision != revision:
                    raise RuntimeError("source revision changed while iterating pages")
            yield page
            marker = self._next_marker(page)
            if marker is None:
                if page.get("has_more"):
                    raise RuntimeError(f"{name} reported more pages without a continuation cursor")
                return
            encoded = self._marker_key(marker)
            if encoded in seen or marker == previous_marker:
                raise RuntimeError(f"{name} returned a non-progressing or cyclic cursor")
            seen.add(encoded)
            previous_marker = marker
            if search_method:
                args = ()
                options = self._search_continuation(options, marker)
            else:
                self._advance(name, options, marker, method)
            if page_number == max_pages:
                raise PageLimitReached(
                    method,
                    page_number,
                    next_cursor=marker,
                    next_kwargs=options,
                )

    def _resolve(
        self, method: str | Callable[..., Awaitable[Any]]
    ) -> tuple[Callable[..., Any], str]:
        if callable(method):
            return method, getattr(method, "__qualname__", repr(method))
        if not isinstance(method, str) or not method:
            raise TypeError("method must be a dotted workspace method or callable")
        value: Any = self._workspace
        for part in method.split("."):
            if not part or part.startswith("_"):
                raise ValueError("method contains an invalid component")
            try:
                value = getattr(value, part)
            except AttributeError as exc:
                raise AttributeError(f"unknown workspace method: {method}") from exc
        if not callable(value):
            raise TypeError(f"workspace method is not callable: {method}")
        return value, method

    def _search_method(
        self,
        method: str | Callable[..., Awaitable[Any]],
        name: str,
    ) -> bool:
        parts = name.split(".")
        owner = parts[-2] if len(parts) > 1 else ""
        method_name = parts[-1]
        if callable(method):
            bound_owner = getattr(method, "__self__", None)
            bound_name = getattr(method, "__name__", None)
            for candidate in ("fs", "filesystem"):
                if getattr(self._workspace, candidate, None) is bound_owner:
                    owner = candidate
                    method_name = bound_name or method_name
                    break
        return owner in {"fs", "filesystem"} and method_name in {
            "search",
            "search_docs",
            "search_ast",
        }

    @staticmethod
    def _search_continuation(options: Mapping[str, Any], marker: Any) -> dict[str, Any]:
        continuation: dict[str, Any] = {"cursor": marker}
        for key in ("max_bytes", "max_matches"):
            if key in options:
                continuation[key] = options[key]
        if options.get("mode") is not None:
            continuation["mode"] = options["mode"]
        return continuation

    @staticmethod
    def _marker_key(marker: Any) -> str:
        try:
            return json.dumps(marker, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            return repr(marker)

    @staticmethod
    def _next_marker(page: Mapping[str, Any]) -> Any:
        if "next_cursor" in page:
            return page.get("next_cursor")
        if "nextCursor" in page:
            return page.get("nextCursor")
        return None

    @staticmethod
    def _revision(page: Mapping[str, Any]) -> Any:
        for key in ("revision", "sha256", "source_revision"):
            if key in page and page[key] is not None:
                return page[key]
        return None

    def _advance(
        self,
        name: str,
        options: dict[str, Any],
        marker: Any,
        method: str | Callable[..., Awaitable[Any]],
    ) -> None:
        parts = name.split(".")
        owner = parts[-2] if len(parts) > 1 else ""
        method_name = parts[-1]
        if callable(method):
            bound_owner = getattr(method, "__self__", None)
            bound_name = getattr(method, "__name__", None)
            for candidate in ("messages", "fs", "filesystem", "skills", "modules"):
                if getattr(self._workspace, candidate, None) is bound_owner:
                    owner = candidate
                    method_name = bound_name or method_name
                    break
        if owner == "messages" and method_name == "read":
            options.pop("cursor", None)
            options["after"] = marker
            return
        if owner in {"fs", "filesystem"} and method_name == "read":
            if not isinstance(marker, Mapping):
                raise RuntimeError("filesystem.read returned an invalid continuation cursor")
            if "line" not in marker or "byte" not in marker:
                raise RuntimeError("filesystem.read cursor must contain line and byte")
            options.pop("cursor", None)
            options["start_line"] = marker["line"]
            options["start_byte"] = marker["byte"]
            return
        if (
            owner in {"skills", "modules", "fs", "filesystem"}
            and method_name == "read_revision"
        ) or (
            owner in {"fs", "filesystem"} and method_name == "read_bytes"
        ):
            if type(marker) is not int or marker < 0:
                raise RuntimeError("revision cursor must be a non-negative byte offset")
            options["start_byte"] = marker
            options.pop("cursor", None)
            return
        options["cursor"] = marker
        options.pop("after", None)
