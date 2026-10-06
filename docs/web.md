# Web search and extraction

`ws.web` provides bounded asynchronous access to configured Kagi, Brave, and Tavily APIs. Requests run in the workspace manager, so provider credentials, connection pools, timeouts, concurrency, and response limits are shared by clients of the workspace. The Python kernel receives normalized results and never receives API key values.

## Configuration

Set keys in the manager environment and refer to them by name in global `~/.config/mypr/config.toml` or workspace `.mypr/config.toml`:

```toml
[web]
default_provider = "kagi"
timeout_seconds = 30
max_concurrency = 4

[web.providers.kagi]
api_key_env = "KAGI_API_KEY"

[web.providers.brave]
api_key_env = "BRAVE_SEARCH_API_KEY"

[web.providers.tavily]
api_key_env = "TAVILY_API_KEY"
```

Provider entries are complete definitions by name across the global and workspace layers. A workspace entry replaces the matching global entry. An entry with only `enabled = false` disables an inherited provider. No provider is enabled by default. `default_provider` can be empty; in that case the API uses the only enabled provider and reports an ambiguity error when more than one is available.

Apply saved configuration explicitly with `await ws.config.reload()`. A manager reads environment variables from its own process. Changing the environment of an attached client does not update an existing manager; restart the manager after changing a key value. Configuration inspection returns the environment-variable name and availability metadata, never the secret.

## Search

```python
await ws.web.providers()

page = await ws.web.search(
    "Python structured concurrency",
    provider="brave",
    limit=10,
    options={"freshness": "pw"},
)
for item in page["results"]:
    print(item["title"], item["url"], item.get("snippet", ""))
```

`providers()` does not call a provider. It reports enabled state, key availability, supported operations, provider-specific option names, and configured limits. `search()` accepts a non-empty query, a provider name, 1–1,024 results, an options mapping, and a 4 KiB–1 MiB response page budget. Each provider may enforce a lower documented limit. The default result page is 32 KiB.

The provider selection order is the explicit `provider` argument, `web.default_provider`, and the only enabled provider. A missing or ambiguous selection is an error. mypr never silently falls back to another provider, sends the same query to multiple providers, or retries a request whose outcome is unknown. Callers that intentionally want comparison can use `asyncio.gather()` and retain each result's provider name.

Results use a common envelope:

```python
{
    "operation": "search",
    "provider": "brave",
    "query": "Python structured concurrency",
    "fetched_at": "2026-10-03T00:00:00Z",
    "results": [
        {"title": "...", "url": "https://example.org", "snippet": "..."}
    ],
    "has_more": False,
    "next_cursor": None,
    "snapshot_id": "...",
}
```

Publication dates use `published_at` when available; source scores and rankings remain in each result's `metadata`. Request IDs and usage are included only when provided. mypr requests Tavily usage by default; use `options={"include_usage": False}` to omit it. Scores from different providers are not comparable. A normal empty `results` list means no result was returned; it does not represent authentication, quota, rate-limit, timeout, or malformed-response failures. Page envelopes also include `result_count` and `failed_count`; these describe the complete saved snapshot rather than only the current page.

Search and context titles are bounded before they enter a snapshot. Search, context, and extraction results whose URL exceeds the field limit are retained in `failed_results` with a truncated URL prefix and `url_truncated` instead of making the complete page unreadable. Context titles shortened to the limit report `metadata.title_truncated`; shortened extraction errors report `error_truncated`. Oversized metadata fields are omitted and `metadata.metadata_truncated` is set. Inspect `failed_results` when comparing the saved response with the provider count.

## Context and extraction

```python
context = await ws.web.context(
    "Python structured concurrency",
    provider="brave",
    limit=10,
    max_tokens=4096,
)

pages = await ws.web.extract(
    ["https://example.org/one", "https://example.org/two"],
    provider="tavily",
)
```

`context()` requests source excerpts from a provider that supports contextual search. Brave accepts 1–50 sources and `max_tokens` from 1,024–32,768; mypr sends these as `maximum_number_of_urls` and `maximum_number_of_tokens`. `max_tokens` is separate from the output byte budget. Brave's contextual search returns source URLs and excerpts. Providers that do not support the requested operation report an explicit unsupported-operation error.

`extract()` accepts one URL or up to 20 HTTP(S) URLs. Provider-specific maximums are enforced by the manager before the request. Kagi and Tavily support provider extraction. A response may contain `failed_results` for individual URLs while other URLs succeed; inspect it before treating the batch as complete. Extraction is bounded and does not implicitly fetch a URL through the workspace's `ws.http` client.

Use `ws.http.read_html()` when the workflow needs direct HTTP behavior such as cookies, custom headers, a named client, or local HTML parsing. Use a Playwright page and `ws.http.extract_html(await page.content(), url=...)` when JavaScript rendering is required. These paths do not consume provider search or extraction requests.

## Paging and output limits

Web calls save a client-owned immutable response snapshot in manager memory. A returned `next_cursor` can be passed to `await ws.web.page(cursor, max_bytes=...)` to read the same response without making another network request:

```python
page = await ws.web.search("asyncio", provider="kagi")
while page["next_cursor"]:
    page = await ws.web.page(page["next_cursor"])
```

`page()` accepts only a cursor produced for the current logical client and snapshot. `page_cursor` identifies the current rendered page and can be passed back with a different `max_bytes` to replay that page; `next_cursor` advances through the same snapshot. Cursors expire after five minutes, are invalid after reset, and cannot be used by another client. The manager keeps at most 32 snapshots and 16 MiB of serialized snapshot data per workspace; old snapshots are evicted first. The limits apply globally to the workspace, while access remains client-scoped.

Pages default to 32 KiB and accept 4 KiB–1 MiB. Long `snippet` and `content` fields are split into fragments with `result_index`, `text_field`, `text_offset`, and `text_has_more`; source metadata is repeated on every fragment. A fragment contains the named text field only; reconstruct each field by `result_index`, `text_field`, and the Unicode character offset `text_offset`. `text_has_more` applies to that field, while `has_more` applies to the entire snapshot. Normal pages set `truncated` to `False`: local paging does not silently omit data. Use `has_more` and `next_cursor` for continuation; provider-side omission or truncation remains provider metadata when supplied. `failed_results` is bounded and paged with the same snapshot. The complete snapshot reports result and failure totals when available.

If the initial page cannot fit its metadata into the requested budget, the call raises `output_limit` after saving the snapshot. Read `error_info.details.page_cursor` with a larger `max_bytes` instead of repeating the paid provider request. An oversized complete snapshot still fails with an explicit result-size error and is not cached.

When Brave supplies it, `provider_has_more` reports whether another provider search page is available. This is separate from `has_more`, which describes the cached response. Provider-side pagination is separate from local snapshot paging. A local `page()` call never issues another request. To request another provider result page, call `search()` again with the provider's documented page option, such as Brave's page offset, and preserve the provider and query in the caller's state.

## Provider options

Options are provider-specific and are validated before a paid or rate-limited request is made. Unsupported names and invalid values are errors.

- Kagi uses a POST request to the v1 search API with `query`, `workflow="search"`, `format="json"`, and `limit`. Options include a validated `lens` or `lens_id`, `filters`, `page` (1–10), personalizations, safe search, and optional inline extraction. The account's blocked, promoted, and snippet preferences are applied by Kagi. Kagi API documentation is at [kagi.com/api/docs](https://kagi.com/api/docs).
- Brave web search supports freshness (`pd`, `pw`, `pm`, `py`, or a date range), language, country, search-page offset, and related web-search options. Brave contextual search provides bounded source excerpts. See the [web search API](https://api-dashboard.search.brave.com/documentation/services/web-search) and [LLM context API](https://api-dashboard.search.brave.com/documentation/services/llm-context).
- Tavily search supports search depth, topic, time range, date bounds, domain inclusion and exclusion, and optional raw content. Generated answers are disabled by mypr. `basic` is the default depth; deeper modes are opt-in. Search and extraction are separate operations. See the [search endpoint](https://docs.tavily.com/documentation/api-reference/endpoint/search) and [extract endpoint](https://docs.tavily.com/documentation/api-reference/endpoint/extract).

Examples of provider-specific options:

```python
await ws.web.search(
    "asyncio TaskGroup",
    provider="kagi",
    options={"lens": {"sites_included": ["docs.python.org"]}, "extract": {"count": 2}},
)

await ws.web.context(
    "asyncio cancellation",
    provider="brave",
    options={"context_threshold_mode": "strict", "maximum_number_of_tokens_per_url": 1024},
)

await ws.web.extract(
    "https://docs.python.org/3/library/asyncio-task.html",
    provider="tavily",
    options={"extract_depth": "advanced", "query": "TaskGroup cancellation", "chunks_per_source": 5},
)
```

Tavily extraction accepts `chunks_per_source` only with a query. Search supports `language`, `filter_by_language`, and `exact_match`; strict language filtering requires `language`. Provider extraction timeouts are remote processing budgets; `web.timeout_seconds` bounds the whole operation, including local queueing and network time.

Do not use provider scores as a cross-provider ranking. Do not enable raw content when a short source snippet is sufficient; it increases output and may increase provider usage.

## Errors and lifecycle

The manager distinguishes missing credentials, invalid credentials, unsupported operations, provider rate limits, quota exhaustion, request timeouts, network failures, invalid provider responses, and empty results. It honors provider response limits and applies one bounded request deadline. It does not perform automatic provider fallback or automatic retries. Check `error_info.code`, `operation`, and bounded `details` instead of parsing error text.

Provider HTTP clients close during manager shutdown and reset. In-progress calls are included in the manager's active work and are cleaned up during a forced lifecycle operation. Web calls require the current MCP connection to be initialized and connected; a detached background task or stale connection cannot continue a provider request after disconnect. A running manager that predates web support returns a `capability_missing` error; restart it with the current installation. Resetting the Python kernel clears the kernel's `ws` object and local references, while manager snapshots and provider configuration follow the manager lifecycle described above.

Failed connection-pool cleanup is reported and retained for retry; successfully closed transports are skipped. Configuration reload can apply new web settings while reporting an error closing the previous pool. Inspect both `applied` and `errors` in the reload result. A later reload, reset, or shutdown retries the remaining cleanup.

`(await ws.status())["web"]` reports active requests and cached provider readiness. `last_log_succeeded` is `None` before logging, then records whether the latest operation metadata was written. A logging failure does not discard a fetched response or repeat a provider request. `await ws.doctor()` also reports web configuration and key-source availability without contacting the providers.
