"""Provider-neutral asynchronous transport for web search services."""

from __future__ import annotations

import asyncio
import datetime as _datetime
import json
import math
import os
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx2

from .config import validate_web_config
from .diagnostics import RPCError, safe_text
from .http_transport import RetryableTransport, close_client, retryable_transports

MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_RESULT_URL_BYTES = 8 * 1024
MAX_RESULT_TITLE_BYTES = 8 * 1024
MAX_RESULT_METADATA_BYTES = 8 * 1024
_PROVIDERS = frozenset({"kagi", "brave", "tavily"})
_URL_SCHEMES = frozenset({"http", "https"})


class WebTransport:
    """Own provider HTTP clients and turn provider responses into one shape.

    ``transport`` is primarily useful for deterministic tests.  Production
    clients always use verified TLS and never follow redirects.
    """

    def __init__(
        self,
        config: Mapping[str, Any] | None = None,
        *,
        transport: Any | None = None,
        client_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.config = validate_web_config(config)
        self._transport = transport
        self._client_factory = client_factory
        self._clients: dict[str, Any] = {}
        self._transports: dict[int, tuple[RetryableTransport, ...]] = {}
        self._client_lock = asyncio.Lock()
        self._slots = asyncio.Semaphore(self.config["max_concurrency"])
        self._closed = False

    @property
    def timeout_seconds(self) -> float:
        return self.config["timeout_seconds"]

    async def run(
        self,
        operation: str,
        provider: str | None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        request = self.validate_request(operation, provider, params)
        selected = request["provider"]
        values = request["params"]
        key = self._api_key(selected)
        async with self._slots:
            if self._closed:
                raise _web_error("web transport is closed", "service_closed")
            response = await self._request(selected, key, operation, values)
        return self._normalize(operation, selected, values, response)

    def validate_request(
        self,
        operation: str,
        provider: str | None,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        selected = self._select_provider(provider)
        return validate_request(operation, selected, params)

    async def close(self) -> None:
        self._closed = True
        clients = tuple(self._clients.items())
        results = await asyncio.gather(
            *(self._close_client(client) for _, client in clients), return_exceptions=True
        )
        failures: list[BaseException] = []
        for (provider, client), result in zip(clients, results, strict=True):
            if isinstance(result, BaseException):
                failures.append(result)
            elif self._clients.get(provider) is client:
                self._clients.pop(provider, None)
                self._transports.pop(id(client), None)
        if failures:
            raise failures[0]

    async def _close_client(self, client: Any) -> None:
        transports = self._transports.get(id(client))
        await close_client(client, transports)
        self._transports.pop(id(client), None)

    def _select_provider(self, provider: str | None) -> str:
        if provider is not None and (not isinstance(provider, str) or provider not in _PROVIDERS):
            raise _web_error("unknown web provider", "unknown_provider")
        if provider is not None:
            selected = provider
        else:
            selected = self.config["default_provider"]
            if not selected:
                enabled = [
                    name
                    for name, value in self.config["providers"].items()
                    if isinstance(value, Mapping) and value.get("enabled", True) is not False
                ]
                if len(enabled) != 1:
                    raise _web_error(
                        "provider must be specified when no unique default is configured",
                        "provider_required",
                    )
                selected = enabled[0]
        config = self.config["providers"].get(selected)
        if not isinstance(config, Mapping) or config.get("enabled", True) is False:
            raise _web_error(
                "web provider is not configured", "provider_not_configured", provider=selected
            )
        return selected

    def _api_key(self, provider: str) -> str:
        value = self.config["providers"].get(provider)
        name = value.get("api_key_env", "") if isinstance(value, Mapping) else ""
        key = os.environ.get(name) if name else None
        if not key:
            raise _web_error(
                "web provider credentials are missing", "credentials_missing", provider=provider
            )
        if len(key) > 8192 or any(ord(char) < 0x20 or ord(char) > 0x7E for char in key):
            raise _web_error(
                "web provider credentials are invalid", "credentials_invalid", provider=provider
            )
        return key

    async def _client(self, provider: str):
        client = self._clients.get(provider)
        if client is not None:
            return client
        async with self._client_lock:
            client = self._clients.get(provider)
            if client is not None:
                return client
            kwargs = {
                "timeout": self.config["timeout_seconds"],
                "follow_redirects": False,
                "verify": True,
                "trust_env": True,
            }
            if self._transport is not None:
                kwargs["transport"] = self._transport
            if self._client_factory is None:
                client = httpx2.AsyncClient(**kwargs)
            else:
                client = self._client_factory(provider, **kwargs)
            transports = retryable_transports(client)
            self._clients[provider] = client
            self._transports[id(client)] = transports
            return client

    async def _request(
        self, provider: str, key: str, operation: str, params: Mapping[str, Any]
    ) -> dict[str, Any]:
        client = await self._client(provider)
        headers = {"Accept": "application/json"}
        if provider == "brave":
            headers["X-Subscription-Token"] = key
        else:
            headers["Authorization"] = f"Bearer {key}"
        try:
            if provider == "brave":
                if operation == "search":
                    query = _brave_search_params(params)
                    request = client.stream(
                        "GET",
                        "https://api.search.brave.com/res/v1/web/search",
                        params=query,
                        headers=headers,
                    )
                else:
                    body = _brave_context_body(params)
                    request = client.stream(
                        "POST",
                        "https://api.search.brave.com/res/v1/llm/context",
                        json=body,
                        headers=headers,
                    )
            else:
                endpoint = _endpoint(provider, operation)
                body = _body(provider, operation, params)
                request = client.stream("POST", endpoint, json=body, headers=headers)
            async with request as response:
                body = await _read_response(response, provider)
                return _decode_response(response, body, provider)
        except RPCError:
            raise
        except TimeoutError:
            raise _web_error(
                "web provider request timed out", "timeout", provider=provider
            ) from None
        except httpx2.TimeoutException:
            raise _web_error(
                "web provider request timed out", "timeout", provider=provider
            ) from None
        except httpx2.HTTPError:
            raise _web_error(
                "web provider request failed", "network_error", provider=provider
            ) from None
        except OSError:
            raise _web_error(
                "web provider request failed", "network_error", provider=provider
            ) from None

    def _normalize(
        self, operation: str, provider: str, params: Mapping[str, Any], response: Mapping[str, Any]
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "operation": operation,
            "provider": provider,
            "fetched_at": _datetime.datetime.now(_datetime.UTC).isoformat().replace("+00:00", "Z"),
        }
        if operation == "search":
            result["query"] = params["query"]
            results, failures = _normalize_search(provider, response)
            result["results"] = results
            if failures:
                result["failed_results"] = failures
            if provider == "tavily" and params["options"].get("include_raw_content") == "text":
                for item in result["results"]:
                    if "content" in item:
                        item["format"] = "text"
        elif operation == "context":
            result["query"] = params["query"]
            results, failures = _normalize_context(response)
            result["results"] = results
            if failures:
                result["failed_results"] = failures
        else:
            successes, failures = _normalize_extract(provider, response, params)
            result["results"] = successes
            if failures:
                result["failed_results"] = failures
        metadata = response.get("meta") if isinstance(response.get("meta"), Mapping) else {}
        if isinstance(metadata.get("trace"), str):
            result["request_id"] = safe_text(metadata["trace"], 256)
        query_info = response.get("query")
        if provider == "brave" and isinstance(query_info, Mapping):
            more = query_info.get("more_results_available")
            if type(more) is bool:
                result["provider_has_more"] = more
        for name in ("request_id", "usage"):
            if name in response:
                result[name] = _safe_value(response[name])
            elif name in metadata:
                result[name] = _safe_value(metadata[name])
        return result


def validate_request(
    operation: str, provider: str, params: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Validate a request without reading credentials or making a request."""

    if not isinstance(operation, str) or operation not in {"search", "context", "extract"}:
        raise _web_error("unsupported web operation", "unsupported_operation")
    if provider not in _PROVIDERS:
        raise _web_error("unknown web provider", "unknown_provider")
    if params is None:
        values = {}
    elif isinstance(params, Mapping):
        values = dict(params)
    else:
        raise _web_error("web parameters must be an object", "invalid_request")
    if operation == "search":
        normalized = _validate_search(provider, values)
    elif operation == "context":
        normalized = _validate_context(provider, values)
    else:
        normalized = _validate_extract(provider, values)
    return {"operation": operation, "provider": provider, "params": normalized}


def _endpoint(provider: str, operation: str) -> str:
    if provider == "kagi":
        return (
            "https://kagi.com/api/v1/search"
            if operation == "search"
            else "https://kagi.com/api/v1/extract"
        )
    return (
        "https://api.tavily.com/search"
        if operation == "search"
        else "https://api.tavily.com/extract"
    )


def _body(provider: str, operation: str, params: Mapping[str, Any]) -> dict[str, Any]:
    if provider == "kagi":
        if operation == "search":
            body = {
                "query": params["query"],
                "workflow": "search",
                "format": "json",
                "limit": params["limit"],
            }
            body.update(params["options"])
            return body
        body = {"pages": [{"url": url} for url in params["urls"]], "format": "json"}
        body.update(params["options"])
        return body
    if operation == "search":
        body = {
            "query": params["query"],
            "max_results": params["limit"],
            "search_depth": "basic",
            "include_answer": False,
            "include_raw_content": False,
            "auto_parameters": False,
            "include_usage": True,
        }
        body.update(params["options"])
        return body
    body = {
        "urls": params["urls"],
        "format": "markdown",
        "extract_depth": "basic",
        "include_usage": True,
    }
    body.update(params["options"])
    return body


def _brave_search_params(params: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        "q": params["query"],
        "count": params["limit"],
        "result_filter": "web",
        "text_decorations": False,
    }
    result.update(params["options"])
    return result


def _brave_context_body(params: Mapping[str, Any]) -> dict[str, Any]:
    body = {
        "q": params["query"],
        "count": params["limit"],
        "maximum_number_of_urls": params["limit"],
        "maximum_number_of_tokens": params["max_tokens"],
    }
    body.update(params["options"])
    return body


async def _read_response(response: Any, provider: str) -> bytes:
    try:
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > MAX_RESPONSE_BYTES:
                raise _web_error(
                    "web provider response is too large", "response_too_large", provider=provider
                )
            chunks.append(chunk)
        return b"".join(chunks)
    except RPCError:
        raise
    except httpx2.HTTPError, OSError:
        raise _web_error(
            "web provider response could not be read", "network_error", provider=provider
        ) from None


def _decode_response(response: Any, body: bytes, provider: str) -> dict[str, Any]:
    status = int(response.status_code)
    if not 200 <= status < 300:
        if status in {401, 403}:
            code = "authentication_error"
        elif status in {402, 432, 433}:
            code = "quota_exceeded"
        elif status == 408:
            code = "timeout"
        elif status == 429:
            code = "rate_limited"
        elif status >= 500:
            code = "provider_error"
        else:
            code = "http_error"
        details: dict[str, Any] = {"provider": provider, "status": status}
        retry_after = response.headers.get("Retry-After")
        if retry_after and len(retry_after) <= 32:
            details["retry_after"] = retry_after
        raise _web_error("web provider returned an error", code, **details)
    try:
        result = json.loads(body, parse_constant=_invalid_json_constant)
        _validate_json_tree(result)
    except UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError:
        raise _web_error(
            "web provider returned invalid JSON", "invalid_response", provider=provider
        ) from None
    if not isinstance(result, Mapping):
        raise _web_error(
            "web provider returned an invalid response", "invalid_response", provider=provider
        )
    return dict(result)


def _invalid_json_constant(value: str) -> None:
    raise ValueError(value)


def _validate_json_tree(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite JSON number")
    if isinstance(value, str):
        if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
            raise ValueError("invalid Unicode surrogate")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            _validate_json_tree(key)
            _validate_json_tree(item)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _validate_json_tree(item)


def _validate_search(provider: str, params: dict[str, Any]) -> dict[str, Any]:
    query = _query(provider, params)
    limit = _limit(params.get("limit", 10), 1024 if provider == "kagi" else 20, "limit")
    options = _options(params)
    if provider == "kagi":
        _validate_options(options, _KAGI_SEARCH_OPTIONS, provider)
        _validate_kagi_options(options)
        if "format" in options and options["format"] != "json":
            raise _web_error("Kagi search format is fixed to json", "invalid_request")
    elif provider == "brave":
        _validate_options(options, _BRAVE_SEARCH_OPTIONS, provider)
        _validate_brave_options(options)
    else:
        _validate_options(options, _TAVILY_SEARCH_OPTIONS, provider)
        _validate_tavily_options(options)
    return {"query": query, "limit": limit, "options": options}


def _validate_context(provider: str, params: dict[str, Any]) -> dict[str, Any]:
    if provider != "brave":
        raise _web_error(
            "context is supported only by Brave", "unsupported_operation", provider=provider
        )
    query = _query(provider, params)
    limit = _limit(params.get("limit", 10), 50, "limit")
    max_tokens = _limit(params.get("max_tokens", 4096), 32768, "max_tokens", minimum=1024)
    options = _options(params)
    _validate_options(options, _BRAVE_CONTEXT_OPTIONS, provider)
    _validate_brave_options(options)
    return {"query": query, "limit": limit, "max_tokens": max_tokens, "options": options}


def _validate_extract(provider: str, params: dict[str, Any]) -> dict[str, Any]:
    if provider not in {"kagi", "tavily"}:
        raise _web_error(
            "extract is not supported by this provider", "unsupported_operation", provider=provider
        )
    raw = params.get("urls")
    if isinstance(raw, str):
        urls = [raw]
    elif isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray)):
        urls = list(raw)
    else:
        raise _web_error("urls must be a URL or a list of URLs", "invalid_request")
    maximum = 10 if provider == "kagi" else 20
    if not 1 <= len(urls) <= maximum:
        raise _web_error(f"extract accepts 1..{maximum} URLs", "invalid_request")
    normalized: list[str] = []
    for url in urls:
        if not isinstance(url, str) or not url or len(url) > 8192:
            raise _web_error("extract URL is invalid", "invalid_request")
        try:
            parsed = httpx2.URL(url)
            scheme = parsed.scheme.lower()
        except Exception:
            raise _web_error("extract URL is invalid", "invalid_request") from None
        if (
            not parsed.host
            or parsed.userinfo
            or scheme not in _URL_SCHEMES
            or (provider == "kagi" and scheme != "https")
        ):
            raise _web_error("extract URL scheme is not supported", "invalid_request")
        normalized.append(url)
    options = _options(params)
    allowed = _KAGI_EXTRACT_OPTIONS if provider == "kagi" else _TAVILY_EXTRACT_OPTIONS
    _validate_options(options, allowed, provider)
    if provider == "tavily":
        _validate_tavily_options(options, extraction=True)
    elif "timeout" in options and options["timeout"] <= 0:
        raise _web_error("Kagi timeout must be positive", "invalid_request")
    return {"urls": normalized, "options": options}


def _query(provider: str, params: Mapping[str, Any]) -> str:
    query = params.get("query")
    if not isinstance(query, str) or not query.strip():
        raise _web_error("query must be a non-empty string", "invalid_request")
    if provider == "kagi" and len(query.encode("utf-8")) > 4096:
        raise _web_error("Kagi query must be at most 4096 bytes", "invalid_request")
    if provider == "brave" and (len(query) > 600 or len(query.split()) > 75):
        raise _web_error(
            "Brave query must be at most 600 characters and 75 words", "invalid_request"
        )
    if len(query.encode("utf-8")) > 4096:
        raise _web_error("query must be at most 4096 bytes", "invalid_request")
    return query


def _limit(value: Any, maximum: int, name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise _web_error(f"{name} must be between {minimum} and {maximum}", "invalid_request")
    return value


def _options(params: Mapping[str, Any]) -> dict[str, Any]:
    options = params.get("options", {})
    if options is None:
        return {}
    if not isinstance(options, Mapping):
        raise _web_error("options must be an object", "invalid_request")
    return dict(options)


def _validate_options(
    options: Mapping[str, Any], allowed: Mapping[str, str], provider: str
) -> None:
    unknown = set(options) - set(allowed)
    if unknown:
        raise _web_error(
            f"unsupported {provider} option: {sorted(map(str, unknown))[0]}",
            "invalid_request",
            provider=provider,
        )
    for name, kind in allowed.items():
        if name not in options:
            continue
        value = options[name]
        valid = {
            "str": isinstance(value, str) and bool(value) and len(value) <= 1024,
            "bool": type(value) is bool,
            "int": isinstance(value, int) and not isinstance(value, bool),
            "number": type(value) is int or (type(value) is float and math.isfinite(value)),
            "mapping": isinstance(value, Mapping),
            "raw_content": type(value) is bool or value in ("markdown", "text"),
            "str_or_list": (isinstance(value, str) and 0 < len(value) <= 8192)
            or (
                isinstance(value, (list, tuple))
                and 1 <= len(value) <= 64
                and all(isinstance(item, str) and 0 < len(item) <= 8192 for item in value)
            ),
            "strlist": isinstance(value, Sequence)
            and not isinstance(value, (str, bytes, bytearray))
            and len(value) <= 64
            and all(isinstance(item, str) and 0 < len(item) <= 1024 for item in value),
        }.get(kind, False)
        if not valid:
            raise _web_error(
                f"invalid {provider} option: {name}", "invalid_request", provider=provider
            )


def _validate_brave_options(options: Mapping[str, Any]) -> None:
    if "offset" in options and not 0 <= options["offset"] <= 9:
        raise _web_error("Brave offset must be between 0 and 9", "invalid_request")
    freshness = options.get("freshness")
    if freshness is not None:
        valid_range = _date_range(freshness) if isinstance(freshness, str) else False
        if freshness not in {"pd", "pw", "pm", "py"} and not valid_range:
            raise _web_error("invalid Brave freshness", "invalid_request")
    safesearch = options.get("safesearch")
    if safesearch is not None and safesearch not in {"off", "moderate", "strict"}:
        raise _web_error("invalid Brave safesearch", "invalid_request")
    country = options.get("country")
    if country is not None and not re.fullmatch(r"[A-Za-z]{2}", country):
        raise _web_error("Brave country must be a two-letter code", "invalid_request")
    mode = options.get("context_threshold_mode")
    if mode is not None and mode not in {"strict", "balanced", "lenient", "disabled"}:
        raise _web_error("invalid Brave context_threshold_mode", "invalid_request")
    for name, minimum, maximum in (
        ("maximum_number_of_tokens_per_url", 512, 8192),
        ("maximum_number_of_snippets", 1, 256),
        ("maximum_number_of_snippets_per_url", 1, 100),
    ):
        if name in options:
            _limit(options[name], maximum, name, minimum=minimum)


def _validate_kagi_options(options: Mapping[str, Any]) -> None:
    page = options.get("page")
    if page is not None and not 1 <= page <= 10:
        raise _web_error("Kagi page must be between 1 and 10", "invalid_request")
    if "timeout" in options and not 0 < options["timeout"] <= 120:
        raise _web_error("Kagi timeout must be positive and at most 120", "invalid_request")
    lens = options.get("lens", {})
    lens_fields = {
        "sites_included": "strlist",
        "sites_excluded": "strlist",
        "keywords_included": "strlist",
        "keywords_excluded": "strlist",
        "file_type": "str",
        "time_after": "str",
        "time_before": "str",
        "time_relative": "str",
        "search_region": "str",
    }
    _validate_options(lens, lens_fields, "Kagi lens")
    _date_bounds(lens, "time_after", "time_before")
    if "time_relative" in lens and lens["time_relative"] not in {"day", "week", "month"}:
        raise _web_error("invalid Kagi lens time_relative", "invalid_request")
    filters = options.get("filters", {})
    _validate_options(filters, {"region": "str", "after": "str", "before": "str"}, "Kagi filters")
    _date_bounds(filters, "after", "before")
    for settings, field in ((lens, "search_region"), (filters, "region")):
        if (
            field in settings
            and settings[field] != "no_region"
            and not re.fullmatch(r"[A-Za-z]{2}", settings[field])
        ):
            raise _web_error("invalid Kagi region", "invalid_request")
    extract = options.get("extract")
    if extract is not None:
        _validate_options(extract, {"count": "int", "timeout": "number"}, "Kagi extract")
        if "count" in extract:
            _limit(extract["count"], 10, "extract.count")
        if "timeout" in extract and not 0 < extract["timeout"] <= 120:
            raise _web_error("invalid Kagi extract.timeout", "invalid_request")
    personalizations = options.get("personalizations")
    if personalizations is not None:
        if set(personalizations) - {"domains", "regexes"}:
            raise _web_error("invalid Kagi personalizations", "invalid_request")
        for group in ("domains", "regexes"):
            rules = personalizations.get(group, [])
            if not isinstance(rules, (list, tuple)) or len(rules) > 1000:
                raise _web_error("invalid Kagi personalization rules", "invalid_request")
            for rule in rules:
                if not isinstance(rule, Mapping):
                    raise _web_error("invalid Kagi personalization rule", "invalid_request")
                fields = (
                    {"domain": "str", "kind": "str"}
                    if group == "domains"
                    else {"regex": "str", "replacement": "str"}
                )
                _validate_options(rule, fields, "Kagi personalizations")
                if group == "domains" and (
                    not rule.get("domain")
                    or rule.get("kind") not in {"block", "lower", "raise", "pin"}
                ):
                    raise _web_error("invalid Kagi domain rule", "invalid_request")
                if group == "regexes" and (
                    not rule.get("regex") or len(rule["regex"].encode()) > 1000
                ):
                    raise _web_error("invalid Kagi regex rule", "invalid_request")


def _validate_tavily_options(options: Mapping[str, Any], *, extraction=False) -> None:
    if "chunks_per_source" in options:
        _limit(options["chunks_per_source"], 5 if extraction else 3, "chunks_per_source")
        if extraction and "query" not in options:
            raise _web_error("Tavily chunks_per_source requires query", "invalid_request")
    if options.get("filter_by_language") and "language" not in options:
        raise _web_error("Tavily filter_by_language requires language", "invalid_request")
    if options.get("safe_search") and options.get("search_depth") in {"fast", "ultra-fast"}:
        raise _web_error("Tavily safe_search requires basic or advanced depth", "invalid_request")
    if "extract_depth" in options and options["extract_depth"] not in {"basic", "advanced"}:
        raise _web_error("invalid Tavily extract_depth", "invalid_request")
    if "timeout" in options and not 1 <= options["timeout"] <= 60:
        raise _web_error("Tavily timeout must be between 1 and 60", "invalid_request")
    if "search_depth" in options and options["search_depth"] not in {
        "basic",
        "advanced",
        "fast",
        "ultra-fast",
    }:
        raise _web_error("invalid Tavily search_depth", "invalid_request")
    if "topic" in options and options["topic"] not in {"general", "news", "finance"}:
        raise _web_error("invalid Tavily topic", "invalid_request")
    for name in ("start_date", "end_date"):
        if name in options and not _date_value(options[name]):
            raise _web_error(f"invalid Tavily {name}", "invalid_request")
    if (
        "start_date" in options
        and "end_date" in options
        and options["start_date"] > options["end_date"]
    ):
        raise _web_error("Tavily start_date must not follow end_date", "invalid_request")
    if "time_range" in options and options["time_range"] not in {
        "day",
        "week",
        "month",
        "year",
    }:
        raise _web_error("invalid Tavily time_range", "invalid_request")
    if "format" in options and options["format"] not in {"markdown", "text"}:
        raise _web_error("invalid Tavily extract format", "invalid_request")


def _date_bounds(options, after, before):
    if any(key in options and not _date_value(options[key]) for key in (after, before)):
        raise _web_error("invalid date filter", "invalid_request")
    if after in options and before in options and options[after] > options[before]:
        raise _web_error("date filter bounds are reversed", "invalid_request")


def _date_value(value: Any) -> bool:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return False
    try:
        _datetime.date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _date_range(value: str) -> bool:
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})to(\d{4}-\d{2}-\d{2})", value)
    if not match or not _date_value(match[1]) or not _date_value(match[2]):
        return False
    return match[1] <= match[2]


def _normalize_search(
    provider: str, response: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if provider == "kagi":
        data = response.get("data")
        if not isinstance(data, Mapping):
            raise _web_error(
                "web provider returned invalid search results",
                "invalid_response",
                provider=provider,
            )
        values = data.get("search", [])
    elif provider == "brave":
        data = response.get("web")
        if isinstance(data, Mapping):
            values = data.get("results")
        else:
            values = [] if isinstance(response.get("query"), Mapping) else None
    else:
        values = response.get("results")
    if not isinstance(values, list):
        raise _web_error(
            "web provider returned invalid search results", "invalid_response", provider=provider
        )
    normalized = []
    failures = []
    for item in values:
        if not isinstance(item, Mapping) or not _string(item.get("url")):
            raise _web_error(
                "web provider returned invalid search results",
                "invalid_response",
                provider=provider,
            )
        url = _string(item["url"])
        if len(url.encode("utf-8")) > MAX_RESULT_URL_BYTES:
            prefix, _ = _bounded_text(url, 256)
            failures.append(
                {
                    "url": prefix,
                    "url_truncated": True,
                    "error": f"search result URL exceeds {MAX_RESULT_URL_BYTES} bytes",
                }
            )
            continue
        normalized.append(_search_item(item))
    return normalized, failures


def _search_item(item: Mapping[str, Any]) -> dict[str, Any]:
    title, title_truncated = _bounded_text(_string(item.get("title")), MAX_RESULT_TITLE_BYTES)
    result: dict[str, Any] = {
        "title": title,
        "url": _string(item.get("url")),
        "snippet": _string(item.get("snippet") or item.get("description") or item.get("content")),
    }
    content = item.get("raw_content") or item.get("markdown")
    if isinstance(item.get("extract"), Mapping):
        extracted = item["extract"]
        content = extracted.get("markdown") or extracted.get("content") or content
    elif isinstance(item.get("extract"), str):
        content = item["extract"]
    if isinstance(content, str) and content:
        result["content"] = content
        result["format"] = "markdown"
    if isinstance(item.get("published_date"), str):
        result["published_at"] = item["published_date"]
    metadata = {}
    for key in ("score", "rank", "profile", "source", "time", "age", "page_age"):
        if key in item:
            metadata[key] = _safe_value(item[key])
    if isinstance(item.get("extra_snippets"), list):
        extra = [value for value in item["extra_snippets"] if isinstance(value, str)]
        if extra:
            result["snippet"] = "\n".join(filter(None, [result["snippet"], *extra]))
    if title_truncated:
        metadata["title_truncated"] = True
    if metadata:
        result["metadata"] = _bounded_metadata(metadata)
    return result


def _bounded_text(value: str, limit: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value, False
    return encoded[:limit].decode("utf-8", "ignore"), True


def _url_failure(url: str, error: str) -> dict[str, Any]:
    prefix, _ = _bounded_text(url, 256)
    return {"url": prefix, "url_truncated": True, "error": error}


def _bounded_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    result = {}
    truncated = False
    for key, value in metadata.items():
        candidate = {**result, key: value, "metadata_truncated": True}
        if len(json.dumps(candidate, separators=(",", ":")).encode()) <= MAX_RESULT_METADATA_BYTES:
            result[key] = value
        else:
            truncated = True
    if truncated:
        result["metadata_truncated"] = True
    return result


def _normalize_context(
    response: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grounding = response.get("grounding")
    sources = response.get("sources", {})
    if not isinstance(grounding, Mapping) or not isinstance(sources, Mapping):
        raise _web_error(
            "web provider returned invalid context", "invalid_response", provider="brave"
        )
    results = []
    failures = []
    for kind in ("generic", "poi", "map"):
        values = grounding.get(kind, [])
        if kind == "poi" and values is None:
            continue
        if kind == "poi" and isinstance(values, Mapping):
            values = [values]
        if not isinstance(values, list):
            raise _web_error(
                "web provider returned invalid context", "invalid_response", provider="brave"
            )
        for item in values:
            if not isinstance(item, Mapping) or not _string(item.get("url")):
                raise _web_error(
                    "web provider returned invalid context", "invalid_response", provider="brave"
                )
            url = _string(item["url"])
            if len(url.encode("utf-8")) > MAX_RESULT_URL_BYTES:
                failures.append(
                    _url_failure(url, f"context result URL exceeds {MAX_RESULT_URL_BYTES} bytes")
                )
                continue
            snippets = item.get("snippets", [])
            if not isinstance(snippets, list) or any(
                not isinstance(value, str) for value in snippets
            ):
                raise _web_error(
                    "web provider returned invalid excerpts", "invalid_response", provider="brave"
                )
            source = sources.get(item["url"], {})
            if not isinstance(source, Mapping):
                raise _web_error(
                    "web provider returned invalid source", "invalid_response", provider="brave"
                )
            metadata = {"kind": kind}
            if source:
                metadata["source"] = _safe_value(source)
            title, title_truncated = _bounded_text(
                _string(item.get("title") or source.get("title")), MAX_RESULT_TITLE_BYTES
            )
            if title_truncated:
                metadata["title_truncated"] = True
            results.append(
                {
                    "title": title,
                    "url": url,
                    "content": "\n".join(snippets),
                    "format": "text",
                    "metadata": _bounded_metadata(metadata),
                }
            )
    return results, failures


def _normalize_extract(
    provider: str, response: Mapping[str, Any], params: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    values = response.get("data") if provider == "kagi" else response.get("results")
    if isinstance(values, Mapping):
        values = values.get("pages", [])
    if not isinstance(values, list):
        raise _web_error(
            "web provider returned invalid extraction results",
            "invalid_response",
            provider=provider,
        )
    successes = []
    failures = []
    for item in values:
        if not isinstance(item, Mapping):
            raise _web_error(
                "web provider returned invalid extraction results",
                "invalid_response",
                provider=provider,
            )
        url = _string(item.get("url"))
        content = item.get("markdown" if provider == "kagi" else "raw_content")
        error = _string(item.get("error"))
        if not url or (content is not None and not isinstance(content, str)):
            raise _web_error(
                "web provider returned invalid page content", "invalid_response", provider=provider
            )
        if len(url.encode("utf-8")) > MAX_RESULT_URL_BYTES:
            failures.append(
                _url_failure(url, f"extraction result URL exceeds {MAX_RESULT_URL_BYTES} bytes")
            )
            continue
        if error or content is None:
            failures.append({"url": url, "error": error or "content unavailable"})
            continue
        output_format = "markdown"
        if provider == "tavily":
            output_format = params.get("options", {}).get("format", "markdown")
        successes.append({"url": url, "content": content, "format": output_format})
    failed = response.get("failed_results")
    if failed is not None and not isinstance(failed, list):
        raise _web_error(
            "web provider returned invalid extraction results",
            "invalid_response",
            provider=provider,
        )
    if isinstance(failed, list):
        for item in failed:
            if not isinstance(item, Mapping):
                raise _web_error(
                    "web provider returned invalid extraction results",
                    "invalid_response",
                    provider=provider,
                )
            url = _string(item.get("url"))
            error = _string(item.get("error"))
            failures.append(
                _url_failure(url, error)
                if len(url.encode("utf-8")) > MAX_RESULT_URL_BYTES
                else {"url": url, "error": error}
            )
    errors = response.get("errors")
    if isinstance(errors, Mapping):
        errors = [errors]
    if errors is not None and not isinstance(errors, list):
        raise _web_error(
            "web provider returned invalid extraction errors",
            "invalid_response",
            provider=provider,
        )
    if isinstance(errors, list):
        for error in errors:
            if not isinstance(error, Mapping):
                raise _web_error(
                    "web provider returned invalid extraction errors",
                    "invalid_response",
                    provider=provider,
                )
            failures.append({"error": _string(error.get("message") or error.get("error"))})
    failures = [item for item in failures if any(item.values())]
    for item in failures:
        error, shortened = _bounded_text(_string(item.get("error")), MAX_RESULT_TITLE_BYTES)
        item["error"] = error
        if shortened:
            item["error_truncated"] = True
    return successes, failures


def _string(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _safe_value(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return safe_text(value, 4096)
    if isinstance(value, Mapping):
        return {
            safe_text(key, 128): _safe_value(item, depth + 1)
            for key, item in list(value.items())[:32]
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_safe_value(item, depth + 1) for item in list(value)[:32]]
    return safe_text(value, 256)


def _web_error(message: str, code: str, **details: Any) -> RPCError:
    return RPCError(message, code=code, operation="web", details=details)


_KAGI_SEARCH_OPTIONS = {
    "lens": "mapping",
    "lens_id": "str",
    "filters": "mapping",
    "page": "int",
    "personalizations": "mapping",
    "safe_search": "bool",
    "extract": "mapping",
    "timeout": "number",
}
_KAGI_EXTRACT_OPTIONS = {"timeout": "number"}
_BRAVE_SEARCH_OPTIONS = {
    "offset": "int",
    "freshness": "str",
    "country": "str",
    "search_lang": "str",
    "ui_lang": "str",
    "safesearch": "str",
    "goggles": "str_or_list",
    "extra_snippets": "bool",
    "spellcheck": "bool",
    "text_decorations": "bool",
}
_BRAVE_CONTEXT_OPTIONS = {
    "freshness": "str",
    "country": "str",
    "search_lang": "str",
    "safesearch": "str",
    "goggles": "str_or_list",
    "context_threshold_mode": "str",
    "enable_local": "bool",
    "enable_source_metadata": "bool",
    "maximum_number_of_tokens_per_url": "int",
    "maximum_number_of_snippets": "int",
    "maximum_number_of_snippets_per_url": "int",
}
_TAVILY_SEARCH_OPTIONS = {
    "search_depth": "str",
    "topic": "str",
    "time_range": "str",
    "start_date": "str",
    "end_date": "str",
    "include_domains": "strlist",
    "exclude_domains": "strlist",
    "country": "str",
    "include_raw_content": "raw_content",
    "include_usage": "bool",
    "auto_parameters": "bool",
    "chunks_per_source": "int",
    "include_published_date": "bool",
    "filter_by_published_date": "bool",
    "safe_search": "bool",
    "language": "str",
    "filter_by_language": "bool",
    "exact_match": "bool",
}
_TAVILY_EXTRACT_OPTIONS = {
    "format": "str",
    "extract_depth": "str",
    "query": "str",
    "chunks_per_source": "int",
    "timeout": "number",
    "include_usage": "bool",
}


def _capability_options(provider: str, operation: str) -> dict[str, Any]:
    tables = {
        ("kagi", "search"): _KAGI_SEARCH_OPTIONS,
        ("kagi", "extract"): _KAGI_EXTRACT_OPTIONS,
        ("brave", "search"): _BRAVE_SEARCH_OPTIONS,
        ("brave", "context"): _BRAVE_CONTEXT_OPTIONS,
        ("tavily", "search"): _TAVILY_SEARCH_OPTIONS,
        ("tavily", "extract"): _TAVILY_EXTRACT_OPTIONS,
    }
    return {name: kind for name, kind in tables[(provider, operation)].items()}


def provider_capabilities(provider: str) -> dict[str, Any]:
    if provider not in _PROVIDERS:
        raise _web_error("unknown web provider", "unknown_provider")
    options: dict[str, Any] = {"search": _capability_options(provider, "search")}
    operations = ["search"]
    limits: dict[str, Any] = {
        "search_limit": {"minimum": 1, "maximum": 1024 if provider == "kagi" else 20}
    }
    if provider == "brave":
        operations.append("context")
        options["context"] = _capability_options(provider, "context")
        limits.update(
            context_limit={"minimum": 1, "maximum": 50},
            context_max_tokens={"minimum": 1024, "maximum": 32768},
        )
    else:
        operations.append("extract")
        options["extract"] = _capability_options(provider, "extract")
        limits["extract_urls"] = {
            "minimum": 1,
            "maximum": 10 if provider == "kagi" else 20,
        }
    return {"provider": provider, "operations": operations, "options": options, "limits": limits}


__all__ = [
    "MAX_RESPONSE_BYTES",
    "WebTransport",
    "provider_capabilities",
    "validate_request",
    "validate_web_config",
]
