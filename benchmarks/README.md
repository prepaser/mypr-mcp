# Performance benchmark

Run the benchmark from an installed checkout with its project virtual environment:

```sh
.venv/bin/python benchmarks/perf.py run --runs 5 --output /tmp/mypr-perf.json
```

The runner starts one MCP client and a fresh manager in a temporary workspace under `.mypr/`, uses the checkout's virtual environment, and stops that manager before removing the workspace. It measures empty and scalar cells, paged stream output, Korean output, and an inline image. A gated cell also measures a `poll` request for output already available while execution remains active. Startup and initialization are excluded; one unreported cell warms the connection before the cases run.

Each case reports MCP call counts, serialized response bytes, end-to-end case latency, and observed request latency with median and p95 summaries. When the server returns `timing_ms` metadata, the report also summarizes its bridge and manager timing fields. The optional `runtime_performance` entry captures the manager's rolling `ws.performance()` view. Image bytes are counted after decoding the MCP image payload.

Compare saved reports with:

```sh
.venv/bin/python benchmarks/perf.py compare /tmp/before.json /tmp/after.json
```

These are local observations, not pass/fail thresholds. Host load, dependency state, and filesystem conditions affect results; compare runs on the same machine under similar conditions.
