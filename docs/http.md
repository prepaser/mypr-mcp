# HTTP clients and HTML extraction

`ws.http` keeps named `httpx2.AsyncClient` instances in the workspace kernel. A client name preserves cookies and connection settings across calls for the same mypr client. Set `shared=True` only when callers should deliberately use the same client and cookie jar. The native client remains available through `ws.http.client()`; requests made through it do not use the bounded response body limit.

`await ws.http.download(url, path, overwrite=False)` streams a successful response into a temporary file and atomically publishes the target, returning its `Path`. Errors before publication leave the target unchanged. If publication succeeds but temporary-file cleanup fails, the target is returned and `ws.http.last_warnings` contains a `download_cleanup_failed` warning. Warnings belong to the current logical client and describe its most recently completed download; concurrent downloads keep separate warning lists. At most four warnings per client and 32 client records are retained.

Install the optional extraction packages into the workspace Python environment before using HTML extraction:

```python
ws.local["html_install"] = await ws.packages.add("trafilatura", "cssselect")
await ws.local["html_install"]
```

The MCP server's environment is separate from the workspace kernel. Installing packages with `uvx` does not make them available to `ws.http`. HTML extraction reports an install hint when these packages are missing and never installs them automatically.

`await ws.http.extract_html(html, url=None, selector=None, max_bytes=32768, cursor=None, include_structure=False)` extracts the page title, main text, and links from an HTML string without making a network request. `selector` is an optional CSS selector; when supplied, text and links are extracted from matching elements while the document title and metadata still come from the full page. Set `include_structure=True` to add bounded headings and canonical, description, and language metadata. Relative links resolve against the page URL and the document's first valid `<base href>`. The HTML is parsed as static input; scripts are not run. If Trafilatura finds no article text, extraction falls back to cleaned visible text and adds a warning; non-content elements such as navigation, headers, footers, and scripts are removed first.

`await ws.http.read_html(url, name="default", shared=False, max_input_bytes=16777216, max_bytes=32768, selector=None, cursor=None, include_structure=False, **request_options)` fetches the response through the named HTTP client before extracting it. The client's cookie jar, default headers, authentication, and other settings therefore apply. The response must be successful; redirects use the final response URL as provenance. `max_input_bytes` limits the decoded response body to 16 MiB by default. Connection and response deadlines come from the named HTTP client's timeout configuration.

Both methods return `title`, `url`, `source_hash`, `text`, and `links`, along with `snapshot_id`, `page_cursor`, `next_cursor`, `has_more`, and `truncated`. `url_truncated` indicates that a long provenance URL was shortened to fit the result bound. Continue a large result by passing `next_cursor` as `cursor` and omitting the original HTML or URL. A cursor replays the same immutable in-memory result, so later page changes cannot mix into it. `truncated` describes the current output page; `complete` and `stop_reason` describe whether extraction reached its configured text and link limits. `source_hash` is SHA-256 over the UTF-8 HTML passed to the extractor.

Pages default to 32 KiB and accept `max_bytes` from 4 KiB to 1 MiB. HTML input is limited to 16 MiB, the main-text result to 8 MiB, extracted links to 10,000 entries or 1 MiB of link data, and structure to 256 headings or 64 KiB. `structure_truncated` is separate from text-page truncation. At most two worker processes run at once; each has a 20-second wall-clock deadline and Linux CPU, memory, file-size, and descriptor limits. Result snapshots are client-isolated, kept only in kernel memory for five minutes, and bounded to 32 results and 16 MiB of serialized data. Resetting the Python kernel clears them.

```python
page = await ws.http.read_html("https://example.org/article", name="research")
while page["next_cursor"]:
    page = await ws.http.read_html(cursor=page["next_cursor"])
```
