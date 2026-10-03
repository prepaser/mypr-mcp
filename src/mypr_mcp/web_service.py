"""Manager-owned search requests and short-lived result pages."""

from __future__ import annotations

import asyncio
import copy
import os
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from .async_utils import wait_owned
from .config import validate_web_config
from .diagnostics import RPCError
from .web_snapshots import WebSnapshots, page_limit
from .web_transport import WebTransport, provider_capabilities, validate_request

_PARAMS = {
    "providers": set(),
    "search": {"query", "provider", "limit", "options", "max_bytes"},
    "context": {"query", "provider", "limit", "max_tokens", "options", "max_bytes"},
    "extract": {"urls", "provider", "options", "max_bytes"},
    "page": {"cursor", "max_bytes"},
}


class WebService:
    def __init__(
        self,
        config: Mapping[str, Any] | None = None,
        record: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None,
        *,
        transport_factory: Callable[..., Any] = WebTransport,
    ):
        self.config = validate_web_config(config)
        self.record = record
        self.snapshots = WebSnapshots()
        self._transport_factory = transport_factory
        self.transport = transport_factory(self.config)
        self._slots = asyncio.Semaphore(self.config["max_concurrency"])
        self._requests: dict[asyncio.Task[Any], str] = {}
        self._closed = False
        self._applying = False
        self._lock = asyncio.Lock()
        self._close_task = None
        self._last_log_succeeded = None

    @property
    def applied_config(self):
        return copy.deepcopy(self.config)

    @property
    def active_count(self):
        return len(self._requests)

    def status(self):
        return {
            "active_count": self.active_count,
            "closed": self._closed,
            "last_log_succeeded": self._last_log_succeeded,
            **self.providers(),
        }

    def providers(self):
        result = []
        for name in ("kagi", "brave", "tavily"):
            definition = self.config["providers"].get(name, {})
            enabled = bool(definition) and definition.get("enabled") is not False
            source = definition.get("api_key_env") if enabled else None
            result.append(
                {
                    "provider": name,
                    "configured": enabled,
                    "api_key_env": source,
                    "key_available": bool(source and os.environ.get(source)),
                    **provider_capabilities(name),
                }
            )
        return {"default_provider": self.config["default_provider"] or None, "providers": result}

    def _provider(self, requested):
        if requested is not None and not isinstance(requested, str):
            raise TypeError("provider must be a string or None")
        name = requested if requested is not None else self.config["default_provider"]
        enabled = [
            name
            for name, value in self.config["providers"].items()
            if value.get("enabled") is not False
        ]
        if not name:
            if requested is not None or len(enabled) != 1:
                raise RPCError(
                    "Select a provider or configure web.default_provider",
                    code="provider_required",
                    operation="web",
                )
            name = enabled[0]
        if name not in enabled:
            raise RPCError(
                "Web provider is not configured or is disabled",
                code="provider_unavailable",
                operation="web",
            )
        return name

    async def dispatch(self, method: str, client_id: str, params=None):
        if self._closed or self._applying:
            raise RPCError("Web service is closing or reconfiguring", code="service_unavailable")
        if not isinstance(client_id, str) or not client_id:
            raise ValueError("Web requires a client identity")
        if not isinstance(method, str) or method not in _PARAMS:
            raise ValueError("unknown web method")
        params = {} if params is None else params
        if not isinstance(params, Mapping) or set(params) - _PARAMS[method]:
            raise ValueError("invalid web parameters")
        params = dict(params)
        if method == "providers":
            return self.providers()
        max_bytes = page_limit(params.pop("max_bytes", 32768))
        if method == "page":
            return self.snapshots.page(client_id, params.get("cursor"), max_bytes)
        provider = self._provider(params.pop("provider", None))
        validate_request(method, provider, params)
        task = asyncio.current_task()
        self._requests[task] = client_id
        started = time.monotonic()
        result = None
        status = "cancelled"
        try:
            async with asyncio.timeout(self.config["timeout_seconds"]):
                async with self._slots:
                    result = await self.transport.run(method, provider, params)
            page = self.snapshots.create(client_id, result, max_bytes)
            status = "succeeded"
            return page
        except TimeoutError as exc:
            status = "timeout"
            raise RPCError(
                "Web request timed out",
                code="timeout",
                operation="web",
                details={"provider": provider},
            ) from exc
        except Exception as exc:
            status = exc.code if isinstance(exc, RPCError) else "failed"
            raise
        finally:
            try:
                if self.record is not None:
                    event = {
                        "client_id": client_id,
                        "provider": provider,
                        "state": status,
                        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                    }
                    if result is not None:
                        event["result_count"] = len(result["results"])
                        event["failed_count"] = len(result.get("failed_results", []))
                        for key in ("request_id", "usage"):
                            if key in result:
                                event[key] = result[key]
                    try:
                        await self.record(method, event)
                    except Exception:
                        self._last_log_succeeded = False
                    else:
                        self._last_log_succeeded = True
            finally:
                self._requests.pop(task, None)

    async def apply_config(self, desired, *, force=False):
        if type(force) is not bool:
            raise TypeError("force must be a boolean")
        desired = validate_web_config(desired)
        return await wait_owned(self._apply_config(desired))

    async def _apply_config(self, desired):
        async with self._lock:
            if self._closed:
                raise RPCError("Web service is closed", code="service_unavailable")
            result = {
                "applied": [],
                "deferred": [],
                "errors": {},
                "applied_config": self.applied_config,
            }
            if desired == self.config:
                return result
            if self.active_count:
                result["deferred"] = ["web"]
                return result
            self._applying = True
            try:
                new_transport = self._transport_factory(desired)
                old_transport, self.transport = self.transport, new_transport
                self.config = desired
                self._slots = asyncio.Semaphore(desired["max_concurrency"])
                await old_transport.close()
                result.update(applied=["web"], applied_config=self.applied_config)
                return result
            finally:
                self._applying = False

    async def drop_client(self, client_id):
        self.snapshots.clear(client_id)
        tasks = [task for task, owner in tuple(self._requests.items()) if owner == client_id]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.snapshots.clear(client_id)

    async def reset(self):
        async with self._lock:
            self._applying = True
            try:
                tasks = tuple(self._requests)
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await self.transport.close()
                self.transport = self._transport_factory(self.config)
                self.snapshots.clear()
            finally:
                self._applying = False

    async def close(self):
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close())
        await wait_owned(self._close_task)

    async def _close(self):
        async with self._lock:
            tasks = tuple(self._requests)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.transport.close()
            self.snapshots.clear()
