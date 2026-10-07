# Browser observation

`ws.browser` exposes native async Playwright objects. Observation and accessibility snapshots add bounded inspection without replacing Playwright's page, locator, or event APIs.

Managed browser installation uses Playwright's registry rules. An absolute `PLAYWRIGHT_BROWSERS_PATH` selects that directory; a relative value is resolved from `INIT_CWD` when set, or from the workspace directory used to start the browser service. `PLAYWRIGHT_BROWSERS_PATH=0` uses the installed Playwright driver's `.local-browsers` directory. When the variable is unset on Linux, the registry is below `$XDG_CACHE_HOME/ms-playwright`, defaulting to `~/.cache/ms-playwright`. These paths are also used for installation locks and cache fingerprints.

HAR recordings use a separate temporary path for each context and are published to `record_har_path` when the context closes. Archive parsing and resource publication run outside the kernel event loop, and `close()`/`aclose()` wait for the owned finalization task. This applies to plain HAR files, ZIP archives, and attached resources. Contexts can use the same final path without consuming each other's temporary recording; the last successfully closed context replaces the final file.

While a context is closing, `context()` rejects reuse of its name. If HAR publication fails, the capture remains associated with the context; resolve the filesystem error and retry `await ws.browser.close(name)`. Close contexts explicitly to flush their recordings, as described in [Playwright's context lifecycle](https://playwright.dev/python/docs/api/class-browsercontext#browser-context-close).

`await ws.browser.aclose()` closes all managed browser resources during workspace shutdown. If a resource cleanup fails, the next call retries only the resources that remain; a successful call is idempotent.

## Page events

```python
observation = await ws.browser.observe(page)
page.on("console", lambda message: print(message.text))
page_result = await observation.read()
details = await observation.request("r3", body=True)
observation.close()
```

`read(cursor=None, limit=100, max_bytes=32768, types=None, url_contains=None, methods=None, status=None, wait_ms=0)` returns this page's console messages, page errors, requests, responses, and failed requests, plus `next_cursor`, `has_more`, `dropped`, and `closed`. Filters use AND semantics. `wait_ms` waits for a matching event for at most 30 seconds; a filter does not cause unrelated events to be returned, but the inspected cursor still advances over them. The cursor is a monotonically increasing client cursor, so a page observer may see gaps caused by events from other observed pages. Each client has one shared ring capped at 1,000 events and 4 MiB, can retain details for at most 256 requests, and can observe at most 64 pages. The workspace also caps retained observation history at 8,192 events and 64 MiB across clients. Old events and request details are evicted when the limits are reached; inactive clients are removed after 30 minutes, while clients with live page observers remain available for reconnects. If every owner still has a live observer, old event history is trimmed before live handles are removed. `dropped` reports that the requested cursor predates retained history.

Request events carry an ID. `request(id, body=False, include_sensitive_headers=False, body_timeout=5)` returns request and response metadata while redacting authorization, cookie, token, secret, session, password, and API-key headers by default. URL user information and sensitive query values, including `password`, `passwd`, `pwd`, and `pass`, are also redacted. Response bodies are opt-in and capped at 256 KiB. Playwright buffers a response body before returning it; to avoid an unknown-size allocation, mypr only reads a body when `Content-Length` is present, no larger than the cap, and `Content-Encoding` is absent or `identity`, then checks the actual size again. Missing, invalid, oversized, compressed, or timed-out reads produce `body_error` instead. The precheck relies on the server's declared length; if a server understates it, Playwright may allocate more than the cap before mypr can reject the returned body. Request and response bodies are never collected automatically. Console location URLs receive the same sensitive URL masking as request and response locations.

Observers attach only to pages whose context belongs to the current client, or to an explicitly shared context or connection. Calling `observation.close()` removes listeners but retains bounded event history and request detail references until eviction or reset. Closing a page or its context removes that page's listeners. Reset removes all observers and their in-memory event history.

## Accessibility snapshots

```python
first = await ws.browser.snapshot(page, selector="main", depth=5)
matches = await ws.browser.find(first["snapshot_id"], "Continue")
next_page = await ws.browser.snapshot(page, cursor=first["next_cursor"])
later = await ws.browser.snapshot(page)
changes = await ws.browser.diff(first["snapshot_id"], later["snapshot_id"])
```

`snapshot(page, selector=None, cursor=None, depth=None, mode=None, boxes=None, limit=32768)` captures the native Playwright ARIA snapshot once and returns URL, title, capture time, snapshot ID, and a UTF-8 bounded text page. If the metadata and a minimal text page cannot fit within `limit`, it raises `ValueError`; increase the limit and retry. A returned cursor pages the same immutable capture; it does not query a changed DOM again. Explicit `depth`, `mode`, or `boxes` options are accepted only when supported by the installed Playwright version. Captures are kept in memory per client, up to 32 snapshots and 16 MiB, with a workspace cap of 256 snapshots and 64 MiB. Least recently used captures are evicted at the workspace cap, and inactive captures expire after 30 minutes; reconnecting clients retain their captures until eviction or expiry.

`find(snapshot_id, text, regex=False, cursor=None, limit=100)` returns matching lines and 1-based line and column positions. Searches run in a short-lived, parent-supervised worker with a two-second deadline and a 768 MiB address-space cap so a pathological expression cannot stall the shared Python kernel. `diff(before_id, after_id, cursor=None, limit=32768)` returns a unified text diff in bounded pages and uses the same worker limits; diffs above 50,000 lines are rejected. Search cursors are tied to their snapshot and query; diff cursors are tied to both snapshot IDs. Expired or mismatched cursors raise an error.
