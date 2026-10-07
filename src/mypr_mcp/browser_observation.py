"""Bounded per-client Playwright event observation."""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qsl, unquote_plus, urlencode, urlsplit, urlunsplit

_MAX_EVENTS = 1000
_MAX_BYTES = 4 * 1024 * 1024
_MAX_EVENT_BYTES = 24 * 1024
_MAX_BODY_BYTES = 256 * 1024
_DEFAULT_BODY_TIMEOUT = 5.0
_MAX_REQUESTS = 256
_MAX_PAGES = 64
_MAX_WORKSPACE_EVENTS = 8192
_MAX_WORKSPACE_BYTES = 64 * 1024 * 1024
_MAX_WORKSPACE_CLIENTS = 128
_INACTIVE_TTL = 30 * 60
_DEFAULT_PAGE_SIZE = 100
_SENSITIVE_HEADER = re.compile(
    r"(?:auth|cookie|token|secret|api.?key|session|credential|password|passwd|pwd|^pass$)", re.I
)
_EVENT_TYPES = frozenset({"console", "pageerror", "request", "response", "requestfailed"})


def _safe_url_fallback(raw: str) -> str:
    before_fragment, has_fragment, fragment = raw.partition("#")
    try:
        decoded_fragment = unquote_plus(fragment)
    except (UnicodeError, ValueError):
        decoded_fragment = fragment
    if has_fragment and _SENSITIVE_HEADER.search(decoded_fragment):
        fragment = "[redacted]"

    before_query, has_query, query = before_fragment.partition("?")
    if has_query:
        fields = []
        for field in query.split("&"):
            key, has_value, _item = field.partition("=")
            try:
                decoded_key = unquote_plus(key)
            except (UnicodeError, ValueError):
                decoded_key = key
            if _SENSITIVE_HEADER.search(decoded_key):
                fields.append(f"{key}=[redacted]" if has_value else "[redacted]")
            else:
                fields.append(field)
        query = "&".join(fields)

    result = before_query
    if has_query:
        result += f"?{query}"
    if has_fragment:
        result += f"#{fragment}"

    marker = result.find("://")
    if marker >= 0:
        authority_start = marker + 3
    elif result.startswith("//"):
        marker = 0
        authority_start = 2
    else:
        marker = -1
    if marker >= 0:
        authority_end = len(result)
        for delimiter in "/?#":
            position = result.find(delimiter, authority_start)
            if position >= 0:
                authority_end = min(authority_end, position)
        authority = result[authority_start:authority_end]
        if "@" in authority:
            host = authority.rsplit("@", 1)[1]
            result = f"{result[:authority_start]}[redacted]@{host}{result[authority_end:]}"

    return _text(result, 8192)


def _safe_url(value: Any) -> str:
    raw = str(value)
    try:
        parts = urlsplit(raw)
        host = parts.hostname or ""
        if ":" in host:
            host = f"[{host}]"
        if parts.port is not None:
            host += f":{parts.port}"
        if parts.username is not None or parts.password is not None:
            host = f"[redacted]@{host}"
        query = urlencode(
            [
                (key, "[redacted]" if _SENSITIVE_HEADER.search(key) else item)
                for key, item in parse_qsl(parts.query, keep_blank_values=True)
            ]
        )
        fragment = (
            "[redacted]"
            if _SENSITIVE_HEADER.search(unquote_plus(parts.fragment))
            else parts.fragment
        )
        return _text(urlunsplit((parts.scheme, host, parts.path, query, fragment)), 8192)
    except (ValueError, UnicodeError):
        return _safe_url_fallback(raw)


def _text(value: Any, limit: int = 16_384) -> str:
    result = str(value)
    encoded = result.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return result
    return encoded[:limit].decode("utf-8", errors="ignore") + "…"


def _headers(value: Any, include_sensitive: bool) -> dict[str, str]:
    try:
        source = value() if callable(value) else value
    except Exception:
        return {}
    if not isinstance(source, Mapping):
        return {}
    result = {}
    size = 0
    for name, item in list(source.items())[:64]:
        key = _text(name, 128)
        value = (
            _text(item, 1024)
            if include_sensitive or not _SENSITIVE_HEADER.search(str(name))
            else "[redacted]"
        )
        size += len(key.encode()) + len(value.encode())
        if size > 12 * 1024:
            break
        result[key] = value
    return result


async def _async_headers(value: Any, include_sensitive: bool) -> dict[str, str]:
    try:
        source = getattr(value, "all_headers", None)
        source = source() if callable(source) else source
        if inspect.isawaitable(source):
            source = await source
        if source is None:
            source = getattr(value, "headers", {})
            source = source() if callable(source) else source
            if inspect.isawaitable(source):
                source = await source
    except Exception:
        return {}
    return _headers(source, include_sensitive)


def _encoded_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _fit_event(event: dict[str, Any], budget: int) -> dict[str, Any]:
    event["truncated"] = True
    for key in ("text", "url", "error", "status_text"):
        value = event.get(key)
        if not isinstance(value, str):
            continue
        while value and _encoded_size(event) > budget:
            original = value
            value = _text(value, max(1, len(value.encode()) // 2))
            if value == original:
                value = value[:-1]
            event[key] = value
    while _encoded_size(event) > budget and "location" in event:
        event.pop("location")
    if _encoded_size(event) > budget:
        return {"cursor": event.get("cursor"), "type": event.get("type"), "truncated": True}
    return event


@dataclass(slots=True)
class _RequestRecord:
    request: Any
    page_id: int
    object_id: int
    response: Any = None
    failed: str | None = None


@dataclass(slots=True)
class _ClientBuffer:
    owner: str = ""
    retained: bool = True
    events: deque[tuple[dict[str, Any], int]] = field(default_factory=deque)
    requests: OrderedDict[str, _RequestRecord] = field(default_factory=OrderedDict)
    pages: dict[int, BrowserObservation] = field(default_factory=dict)
    bytes_used: int = 0
    next_event: int = 1
    next_request: int = 1
    next_page: int = 1
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    changed: asyncio.Event = field(default_factory=asyncio.Event)
    on_access: Callable[[_ClientBuffer], None] | None = None

    def append(self, event: dict[str, Any]) -> None:
        event["cursor"] = self.next_event
        self.next_event += 1
        size = _encoded_size(event)
        if size > _MAX_EVENT_BYTES:
            for key, value in tuple(event.items()):
                if isinstance(value, str):
                    event[key] = _text(value, 2048)
            size = _encoded_size(event)
        if size > _MAX_EVENT_BYTES:
            event = {"cursor": event["cursor"], "type": "event_truncated"}
            size = _encoded_size(event)
        self.events.append((event, size))
        self.bytes_used += size
        self.changed.set()
        while len(self.events) > _MAX_EVENTS or self.bytes_used > _MAX_BYTES:
            old, old_size = self.events.popleft()
            self.bytes_used -= old_size
            request_id = old.get("request_id")
            if request_id is not None:
                self.drop_request(request_id)
        if self.on_access is not None:
            self.on_access(self)

    def add_request(self, request_id: str, record: _RequestRecord) -> None:
        self.requests[request_id] = record
        while len(self.requests) > _MAX_REQUESTS:
            self.drop_request(next(iter(self.requests)))

    def drop_request(self, request_id: str) -> None:
        record = self.requests.pop(request_id, None)
        if record is None:
            return
        observation = next(
            (item for item in self.pages.values() if item.page_id == record.page_id), None
        )
        if observation is not None:
            observation._requests.pop(record.object_id, None)


class BrowserObservation:
    """A page listener exposing its client's bounded event history."""

    def __init__(
        self,
        owner: str,
        page: Any,
        context: Any,
        buffer: _ClientBuffer,
        page_id: int,
        identity: Callable[[], str],
    ):
        self.owner = owner
        self._identity = identity
        self.page = page
        self.context = context
        self._buffer = buffer
        self.page_id = page_id
        self._closed = False
        self._listeners: list[tuple[str, Any]] = []
        self._requests: dict[int, str] = {}
        self._attach()

    def _attach(self) -> None:
        events = {
            "console": self._on_console,
            "pageerror": self._on_pageerror,
            "request": self._on_request,
            "response": self._on_response,
            "requestfailed": self._on_request_failed,
            "close": self._on_page_close,
        }
        try:
            for name, callback in events.items():
                self.page.on(name, callback)
                self._listeners.append((name, callback))
        except BaseException:
            self._remove_listeners()
            raise

    def _append(self, event: dict[str, Any]) -> None:
        if self._closed:
            return
        event["page_id"] = self.page_id
        if "url" not in event:
            try:
                event["url"] = _safe_url(self.page.url)
            except Exception:
                pass
        self._buffer.append(event)

    def _on_console(self, message: Any) -> None:
        try:
            message_type = message.type
        except Exception:
            message_type = "log"
        try:
            text = message.text
        except Exception:
            text = ""
        try:
            location = message.location
            location = location() if callable(location) else location
        except Exception:
            location = None
        event = {"type": "console", "level": _text(message_type, 64), "text": _text(text)}
        if isinstance(location, Mapping):
            event["location"] = {
                str(key): (_safe_url(value) if str(key).casefold() == "url" else _text(value, 2048))
                for key, value in list(location.items())[:16]
            }
        self._append(event)

    def _on_pageerror(self, error: Any) -> None:
        self._append({"type": "pageerror", "text": _text(error)})

    def _request_id(self, request: Any) -> str:
        key = id(request)
        existing = self._requests.get(key)
        if existing is not None:
            return existing
        request_id = f"r{self._buffer.next_request}"
        self._buffer.next_request += 1
        self._requests[key] = request_id
        self._buffer.add_request(request_id, _RequestRecord(request, self.page_id, key))
        return request_id

    @staticmethod
    def _request_fields(request: Any) -> dict[str, Any]:
        def get(name: str, default: Any = None) -> Any:
            try:
                value = getattr(request, name)
                return value() if callable(value) else value
            except Exception:
                return default

        return {
            "method": _text(get("method", ""), 64),
            "url": _safe_url(get("url", "")),
            "resource_type": _text(get("resource_type", ""), 128),
        }

    def _on_request(self, request: Any) -> None:
        request_id = self._request_id(request)
        self._append({"type": "request", "request_id": request_id, **self._request_fields(request)})

    def _on_response(self, response: Any) -> None:
        try:
            request = response.request
            request = request() if callable(request) else request
        except Exception:
            request = None
        request_id = self._request_id(request) if request is not None else None
        record = self._buffer.requests.get(request_id) if request_id else None
        if record is not None:
            record.response = response

        def get(name: str, default: Any = None) -> Any:
            try:
                value = getattr(response, name)
                return value() if callable(value) else value
            except Exception:
                return default

        event = {
            "type": "response",
            "status": get("status"),
            "status_text": _text(get("status_text", ""), 256),
            "url": _safe_url(get("url", "")),
        }
        if request_id is not None:
            event["request_id"] = request_id
        self._append(event)

    def _on_request_failed(self, request: Any) -> None:
        request_id = self._request_id(request)
        try:
            failure = request.failure
            failure = failure() if callable(failure) else failure
        except Exception:
            failure = None
        detail = _text(failure or "request failed", 2048)
        record = self._buffer.requests.get(request_id)
        if record is not None:
            record.failed = detail
        self._append(
            {
                "type": "requestfailed",
                "request_id": request_id,
                **self._request_fields(request),
                "error": detail,
            }
        )

    def _on_page_close(self, *_args: Any) -> None:
        self._detach()

    def _check_owner(self) -> None:
        if self._identity() != self.owner:
            raise RuntimeError("browser observation belongs to a different client")
        if not self._buffer.retained:
            raise KeyError("browser observation is no longer retained for this client")
        if self._buffer.on_access is not None:
            self._buffer.on_access(self._buffer)

    def _remove_listeners(self) -> None:
        remove = getattr(self.page, "remove_listener", None) or getattr(
            self.page, "removeListener", None
        )
        if callable(remove):
            for name, callback in self._listeners:
                try:
                    remove(name, callback)
                except Exception:
                    pass
        self._listeners.clear()

    def close(self) -> dict[str, Any]:
        self._check_owner()
        return self._detach()

    def _detach(self) -> dict[str, Any]:
        if not self._closed:
            self._closed = True
            self._remove_listeners()
            self._buffer.pages.pop(id(self.page), None)
            self._requests.clear()
            self.page = None
            self.context = None
            self._buffer.changed.set()
        return {"closed": True, "page_id": self.page_id}

    @staticmethod
    def _values(value: Any, name: str, *, strings: bool = True) -> set[Any] | None:
        if value is None:
            return None
        if isinstance(value, str) and strings:
            value = (value,)
        elif isinstance(value, int) and not isinstance(value, bool) and not strings:
            value = (value,)
        elif not isinstance(value, (list, tuple, set, frozenset)):
            kind = "strings" if strings else "integers"
            raise TypeError(f"{name} must be a {kind} collection or None")
        result = set(value)
        if not result:
            raise ValueError(f"{name} must not be empty")
        if strings and any(not isinstance(item, str) or not item for item in result):
            raise TypeError(f"{name} must contain non-empty strings")
        if not strings and any(
            isinstance(item, bool) or not isinstance(item, int) for item in result
        ):
            raise TypeError(f"{name} must contain integers")
        return result

    def _matches(
        self,
        event: dict[str, Any],
        event_types: set[str] | None,
        url_contains: str | None,
        methods: set[str] | None,
        statuses: set[int] | None,
    ) -> bool:
        if event_types is not None and event.get("type") not in event_types:
            return False
        if url_contains is not None and url_contains not in str(event.get("url", "")):
            return False
        if methods is not None and str(event.get("method", "")).upper() not in methods:
            return False
        if statuses is not None and event.get("status") not in statuses:
            return False
        return True

    @staticmethod
    def _page(
        events: list[dict[str, Any]],
        next_cursor: int,
        has_more: bool,
        dropped: bool,
        closed: bool,
        truncated: bool,
    ) -> dict[str, Any]:
        return {
            "events": events,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "dropped": dropped,
            "closed": closed,
            "truncated": truncated,
        }

    async def read(
        self,
        cursor: int | None = None,
        *,
        limit: int = _DEFAULT_PAGE_SIZE,
        max_bytes: int = 32 * 1024,
        types: Any = None,
        url_contains: str | None = None,
        methods: Any = None,
        status: Any = None,
        wait_ms: int = 0,
    ) -> dict[str, Any]:
        self._check_owner()
        if type(limit) is not int or not 1 <= limit <= _MAX_EVENTS:
            raise ValueError(f"limit must be an integer from 1 to {_MAX_EVENTS}")
        if type(max_bytes) is not int or not 1024 <= max_bytes <= 32 * 1024:
            raise ValueError("max_bytes must be an integer from 1024 to 32768")
        if cursor is not None and (type(cursor) is not int or cursor < 0):
            raise ValueError("cursor must be a non-negative integer or None")
        if type(wait_ms) is not int or not 0 <= wait_ms <= 30_000:
            raise ValueError("wait_ms must be an integer from 0 to 30000")
        event_types = self._values(types, "types")
        if event_types is not None:
            unknown = event_types - _EVENT_TYPES
            if unknown:
                raise ValueError(f"unknown event types: {sorted(unknown)!r}")
        if url_contains is not None and (
            not isinstance(url_contains, str) or len(url_contains) > 1024
        ):
            raise ValueError("url_contains must be a string of at most 1024 characters or None")
        methods_value = self._values(methods, "methods")
        methods_set = {value.upper() for value in methods_value} if methods_value else None
        statuses = self._values(status, "status", strings=False)
        deadline = asyncio.get_running_loop().time() + wait_ms / 1000
        start = cursor
        while True:
            async with self._buffer.lock:
                events = list(self._buffer.events)
                oldest = events[0][0]["cursor"] if events else self._buffer.next_event
                latest = events[-1][0]["cursor"] if events else self._buffer.next_event - 1
                if start is None:
                    start = max(0, oldest - 1)
                dropped = start < oldest - 1
                page_events = [event for event, _ in events if event.get("page_id") == self.page_id]
                matching = [
                    event
                    for event in page_events
                    if event["cursor"] > start
                    and self._matches(event, event_types, url_contains, methods_set, statuses)
                ]
                selected: list[dict[str, Any]] = []
                truncated = False
                blocked = False
                for source in matching[:limit]:
                    candidate = deepcopy(source)
                    page = self._page(
                        selected + [candidate],
                        candidate["cursor"],
                        True,
                        dropped,
                        self._closed,
                        truncated,
                    )
                    if _encoded_size(page) > max_bytes:
                        base = self._page(
                            selected,
                            candidate["cursor"],
                            True,
                            dropped,
                            self._closed,
                            True,
                        )
                        budget = max_bytes - _encoded_size(base) + 2
                        if budget >= 256:
                            candidate = _fit_event(candidate, budget)
                            truncated = True
                            page = self._page(
                                selected + [candidate],
                                candidate["cursor"],
                                True,
                                dropped,
                                self._closed,
                                truncated,
                            )
                    if _encoded_size(page) > max_bytes:
                        blocked = True
                        break
                    selected.append(candidate)

                next_cursor = selected[-1]["cursor"] if selected else start
                remaining = [event for event in matching if event["cursor"] > next_cursor]
                has_more = bool(remaining) or blocked
                if not has_more and (not selected or len(selected) < limit):
                    next_cursor = latest
                result = self._page(
                    selected,
                    next_cursor,
                    has_more,
                    dropped,
                    self._closed,
                    truncated,
                )
                if _encoded_size(result) > max_bytes:
                    raise RuntimeError("browser observation page exceeded its byte limit")
                should_wait = (
                    wait_ms
                    and not result["events"]
                    and not result["has_more"]
                    and not self._closed
                )
                if should_wait:
                    self._buffer.changed.clear()
            if not should_wait:
                return result
            remaining_time = deadline - asyncio.get_running_loop().time()
            if remaining_time <= 0:
                return result
            try:
                await asyncio.wait_for(self._buffer.changed.wait(), remaining_time)
            except TimeoutError:
                return result

    async def request(
        self,
        request_id: str,
        *,
        body: bool = False,
        include_sensitive_headers: bool = False,
        body_timeout: float = _DEFAULT_BODY_TIMEOUT,
    ) -> dict[str, Any]:
        self._check_owner()
        if type(body) is not bool or type(include_sensitive_headers) is not bool:
            raise TypeError("body and include_sensitive_headers must be booleans")
        if (
            isinstance(body_timeout, bool)
            or not isinstance(body_timeout, (int, float))
            or not 0 < float(body_timeout) <= 60
        ):
            raise ValueError("body_timeout must be a finite number from 0 to 60 seconds")
        if not isinstance(request_id, str):
            raise TypeError("request_id must be a string")
        record = self._buffer.requests.get(request_id)
        if record is None:
            raise KeyError(f"request {request_id!r} is no longer retained")
        result: dict[str, Any] = {
            "id": request_id,
            "request": {
                **self._request_fields(record.request),
                "headers": await _async_headers(record.request, include_sensitive_headers),
            },
            "failed": record.failed,
        }
        response = record.response
        if response is not None:

            def get(name: str, default: Any = None) -> Any:
                try:
                    value = getattr(response, name)
                    return value() if callable(value) else value
                except Exception:
                    return default

            result["response"] = {
                "status": get("status"),
                "status_text": _text(get("status_text", ""), 256),
                "headers": await _async_headers(response, include_sensitive_headers),
            }
            if body:
                headers = result["response"]["headers"]
                content_length = next(
                    (
                        value
                        for name, value in headers.items()
                        if name.casefold() == "content-length"
                    ),
                    None,
                )
                content_encoding = next(
                    (
                        value
                        for name, value in headers.items()
                        if name.casefold() == "content-encoding"
                    ),
                    "identity",
                )
                if content_length is None:
                    result["response"]["body_error"] = (
                        "Content-Length is unavailable; Playwright buffers the full body, "
                        "so retrieval was skipped"
                    )
                elif content_encoding.casefold() not in {"", "identity"}:
                    result["response"]["body_error"] = (
                        "compressed response bodies are skipped because Content-Length does "
                        "not bound Playwright's decoded body"
                    )
                else:
                    try:
                        declared_size = int(content_length)
                    except ValueError:
                        declared_size = _MAX_BODY_BYTES + 1
                    if declared_size < 0 or declared_size > _MAX_BODY_BYTES:
                        result["response"]["body_error"] = (
                            f"response body exceeds the {_MAX_BODY_BYTES}-byte limit"
                        )
                    else:
                        body_task: asyncio.Task[Any] | None = None
                        try:
                            body_task = asyncio.create_task(response.body())
                            async with asyncio.timeout(float(body_timeout)):
                                value = await asyncio.shield(body_task)
                        except TimeoutError:
                            if body_task is not None and not body_task.done():
                                body_task.cancel()
                                with suppress(Exception, asyncio.CancelledError):
                                    await asyncio.wait_for(asyncio.shield(body_task), 1.0)
                            result["response"]["body_error"] = (
                                f"response body timed out after {float(body_timeout):g} seconds"
                            )
                        except asyncio.CancelledError:
                            if body_task is not None and not body_task.done():
                                body_task.cancel()
                                with suppress(Exception, asyncio.CancelledError):
                                    await asyncio.wait_for(asyncio.shield(body_task), 1.0)
                            raise
                        except Exception as exc:
                            result["response"]["body_error"] = _text(exc, 2048)
                        else:
                            if len(value) > _MAX_BODY_BYTES:
                                result["response"]["body_error"] = (
                                    f"response body exceeded the {_MAX_BODY_BYTES}-byte limit"
                                )
                            else:
                                result["response"]["body"] = value.decode("utf-8", errors="replace")
        return result


class BrowserObservations:
    def __init__(self, identity: Callable[[], str]) -> None:
        self._identity = identity
        self._clients: dict[str, _ClientBuffer] = {}
        self._lru: OrderedDict[str, None] = OrderedDict()
        self._accessed: dict[str, float] = {}
        self._total_events = 0
        self._total_bytes = 0

    async def observe(self, owner: str, page: Any, context: Any) -> BrowserObservation:
        self._expire()
        buffer = self._clients.setdefault(owner, _ClientBuffer())
        if not buffer.owner:
            buffer.owner = owner
            buffer.on_access = self._touch_buffer
        page_key = id(page)
        existing = buffer.pages.get(page_key)
        if existing is not None and not existing._closed:
            self._touch_buffer(buffer)
            return existing
        if len(buffer.pages) >= _MAX_PAGES:
            raise RuntimeError(f"this client already observes {_MAX_PAGES} pages")
        if getattr(page, "is_closed", lambda: False)():
            raise RuntimeError("cannot observe a closed page")
        page_id = buffer.next_page
        buffer.next_page += 1
        observation = BrowserObservation(owner, page, context, buffer, page_id, self._identity)
        buffer.pages[page_key] = observation
        self._touch_buffer(buffer)
        self._enforce_workspace_budget()
        return observation

    def close_context(self, context: Any) -> None:
        for buffer in self._clients.values():
            for observation in tuple(buffer.pages.values()):
                if observation.context is context:
                    observation._detach()

    def clear(self) -> None:
        for buffer in self._clients.values():
            buffer.retained = False
            for observation in tuple(buffer.pages.values()):
                observation._detach()
        self._clients.clear()
        self._lru.clear()
        self._accessed.clear()
        self._total_events = 0
        self._total_bytes = 0

    def close_all(self) -> None:
        self.clear()

    def gc(self) -> int:
        before = len(self._clients)
        self._expire()
        self._enforce_workspace_budget()
        return before - len(self._clients)

    def _touch_buffer(self, buffer: _ClientBuffer) -> None:
        owner = buffer.owner
        if not owner or self._clients.get(owner) is not buffer:
            return
        self._lru[owner] = None
        self._lru.move_to_end(owner)
        self._accessed[owner] = asyncio.get_running_loop().time()
        self._sync_totals()
        self._enforce_workspace_budget()

    def _sync_totals(self) -> None:
        self._total_events = sum(len(buffer.events) for buffer in self._clients.values())
        self._total_bytes = sum(buffer.bytes_used for buffer in self._clients.values())

    def _remove_owner(self, owner: str) -> None:
        buffer = self._clients.pop(owner, None)
        if buffer is None:
            return
        buffer.retained = False
        self._lru.pop(owner, None)
        self._accessed.pop(owner, None)
        for observation in tuple(buffer.pages.values()):
            observation._detach()
        self._sync_totals()

    def _expire(self) -> None:
        if _INACTIVE_TTL < 0:
            return
        deadline = asyncio.get_running_loop().time() - _INACTIVE_TTL
        for owner in tuple(self._lru):
            buffer = self._clients.get(owner)
            if buffer is None:
                self._lru.pop(owner, None)
                self._accessed.pop(owner, None)
                continue
            if buffer.pages:
                continue
            if self._accessed.get(owner, 0) > deadline:
                break
            self._remove_owner(owner)

    def _enforce_workspace_budget(self) -> None:
        self._expire()
        while (
            len(self._clients) > _MAX_WORKSPACE_CLIENTS
            or self._total_events > _MAX_WORKSPACE_EVENTS
            or self._total_bytes > _MAX_WORKSPACE_BYTES
        ):
            inactive = next(
                (
                    owner
                    for owner in self._lru
                    if owner in self._clients and not self._clients[owner].pages
                ),
                None,
            )
            if inactive is not None:
                self._remove_owner(inactive)
                continue
            oldest = next(iter(self._lru), None)
            if oldest is None:
                break
            buffer = self._clients.get(oldest)
            if buffer is None:
                self._lru.pop(oldest, None)
                continue
            if buffer.events:
                old, old_size = buffer.events.popleft()
                buffer.bytes_used -= old_size
                request_id = old.get("request_id")
                if request_id is not None:
                    buffer.drop_request(request_id)
                self._sync_totals()
            else:
                self._lru.move_to_end(oldest)
                if all(not item.events for item in self._clients.values()):
                    break


__all__ = ["BrowserObservation", "BrowserObservations"]
