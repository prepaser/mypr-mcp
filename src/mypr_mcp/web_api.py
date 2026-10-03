"""Kernel-facing asynchronous web search helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from .diagnostics import RPCError

_PROVIDERS = frozenset({"kagi", "brave", "tavily"})
_MIN_BYTES = 4 * 1024
_MAX_BYTES = 1024 * 1024
_MAX_QUERY_BYTES = 4096
_MAX_URLS = 20


class WebAPI:
    """Access manager-owned web search and extraction services."""

    def __init__(
        self,
        rpc: Callable[..., Awaitable[Any]],
        client_context: Any,
    ) -> None:
        self._rpc = rpc
        self._client_context = client_context

    def _require_client(self) -> None:
        if self._client_context.get() is None:
            raise RPCError("ws.web requires an initialized client")

    async def _call(self, method: str, **params: Any) -> Any:
        self._require_client()
        try:
            return await self._rpc("web", method=method, params=params)
        except RPCError as exc:
            message = str(exc).strip().casefold()
            if message.removeprefix("valueerror: ").strip() in {
                "unknown operation: web",
                "unknown operation web",
            }:
                raise RPCError(
                    "The running workspace manager does not support web; "
                    "restart it with the current mypr-mcp installation.",
                    code="capability_missing",
                    operation="web",
                    details={"capability": "web", "restart_required": True},
                ) from exc
            raise

    @staticmethod
    def _provider(provider: str | None) -> str | None:
        if provider is None:
            return None
        if not isinstance(provider, str) or provider not in _PROVIDERS:
            choices = ", ".join(sorted(_PROVIDERS))
            raise ValueError(f"provider must be one of: {choices}")
        return provider

    @staticmethod
    def _query(query: str) -> str:
        if not isinstance(query, str) or not query.strip():
            raise TypeError("query must be a non-empty string")
        if len(query.encode("utf-8")) > _MAX_QUERY_BYTES:
            raise ValueError(f"query must be at most {_MAX_QUERY_BYTES} bytes")
        return query

    @staticmethod
    def _limit(limit: int, *, maximum: int = 20) -> int:
        if type(limit) is not int or limit < 1 or limit > maximum:
            raise ValueError(f"limit must be an integer between 1 and {maximum}")
        return limit

    @staticmethod
    def _max_bytes(max_bytes: int) -> int:
        if type(max_bytes) is not int or not _MIN_BYTES <= max_bytes <= _MAX_BYTES:
            raise ValueError(f"max_bytes must be between {_MIN_BYTES} and {_MAX_BYTES}")
        return max_bytes

    @staticmethod
    def _options(options: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if options is None:
            return None
        if not isinstance(options, Mapping):
            raise TypeError("options must be a mapping or None")
        if any(not isinstance(key, str) or not key for key in options):
            raise TypeError("options keys must be non-empty strings")
        return dict(options)

    @staticmethod
    def _urls(urls: str | Sequence[str]) -> list[str]:
        if isinstance(urls, str):
            values = [urls]
        elif isinstance(urls, Sequence) and not isinstance(urls, (bytes, bytearray)):
            values = list(urls)
        else:
            raise TypeError("urls must be a URL string or a sequence of strings")
        if not values or len(values) > _MAX_URLS:
            raise ValueError(f"urls must contain between 1 and {_MAX_URLS} items")
        if any(not isinstance(url, str) or not url.strip() for url in values):
            raise TypeError("urls must contain non-empty strings")
        return values

    async def providers(self) -> Any:
        """List configured providers and their supported operations."""
        return await self._call("providers")

    async def search(
        self,
        query: str,
        provider: str | None = None,
        limit: int = 10,
        options: Mapping[str, Any] | None = None,
        max_bytes: int = 32768,
    ) -> Any:
        """Search the web and return a bounded provider-normalized result page."""
        self._query(query)
        self._provider(provider)
        self._limit(limit, maximum=1024)
        self._max_bytes(max_bytes)
        return await self._call(
            "search",
            query=query,
            provider=provider,
            limit=limit,
            options=self._options(options),
            max_bytes=max_bytes,
        )

    async def context(
        self,
        query: str,
        provider: str | None = None,
        limit: int = 10,
        max_tokens: int = 4096,
        options: Mapping[str, Any] | None = None,
        max_bytes: int = 32768,
    ) -> Any:
        """Return bounded source excerpts suitable for answering a web query."""
        self._query(query)
        self._provider(provider)
        self._limit(limit, maximum=50)
        if type(max_tokens) is not int or max_tokens < 1024 or max_tokens > 32768:
            raise ValueError("max_tokens must be an integer between 1024 and 32768")
        self._max_bytes(max_bytes)
        return await self._call(
            "context",
            query=query,
            provider=provider,
            limit=limit,
            max_tokens=max_tokens,
            options=self._options(options),
            max_bytes=max_bytes,
        )

    async def extract(
        self,
        urls: str | Sequence[str],
        provider: str | None = None,
        options: Mapping[str, Any] | None = None,
        max_bytes: int = 32768,
    ) -> Any:
        """Extract bounded page content from one or more URLs."""
        self._provider(provider)
        self._max_bytes(max_bytes)
        return await self._call(
            "extract",
            urls=self._urls(urls),
            provider=provider,
            options=self._options(options),
            max_bytes=max_bytes,
        )

    async def page(self, cursor: str, max_bytes: int = 32768) -> Any:
        """Read a client-owned web result snapshot without another network request."""
        if not isinstance(cursor, str) or not cursor:
            raise TypeError("cursor must be a non-empty string")
        self._max_bytes(max_bytes)
        return await self._call("page", cursor=cursor, max_bytes=max_bytes)


__all__ = ["WebAPI"]
