# Performance benchmark

Run the benchmark from an installed checkout with its project virtual environment:

```sh
.venv/bin/python benchmarks/perf.py run --runs 5 --output /tmp/mypr-perf.json
```

The runner starts one MCP client and a fresh manager in a temporary workspace under `.mypr/`, uses the checkout's prepared virtual environment, and stops only that manager before removing the workspace. The report records the source checkout, installed distribution, Python path, and source path selection so results cannot be mistaken for a wheel run. It measures frontend startup through `session.initialize()`, the `init` call, empty and scalar cells, paged stream output, Korean output, an inline image, and a compact shell case that retains 256 KiB without printing it through MCP. A gated cell also measures a `poll` request for output already available while execution remains active. One unreported cell warms the connection before the measured cases run.

Each case reports MCP call counts, serialized response bytes, end-to-end case latency, and observed request latency with median and p95 summaries. The report also compares per-case call counts and records partial failures with a phase and bounded error. A failed case is excluded from comparisons, and failed samples are never included in a success-only median. When the server returns `timing_ms` metadata, the report also summarizes its bridge and manager timing fields. The optional `runtime_performance` entry captures the manager's rolling `ws.performance()` view. Image bytes are counted after decoding the MCP image payload.

The runner writes a JSON report even when startup, initialization, a case, or cleanup fails, and exits with status 1 for a failed report. Samples are checkpointed after each completed case, so a partial report remains useful. A failed case stops the remaining cases and skips the probes; a stateful operation is never retried after an uncertain timeout. Per-case deadlines prevent a stalled manager from blocking the complete report. The benchmark removes inherited `MYPR_*` variables and isolates XDG config, state, data, and runtime directories before starting its manager; it keeps the conventional UV cache and does not prepare dependencies or modify the checkout's lock or package metadata.

A cancelled run propagates cancellation after checkpointing a report with `status: "cancelled"`, `incomplete: true`, and the phase where cancellation occurred.

Compare saved reports with:

```sh
.venv/bin/python benchmarks/perf.py compare /tmp/before.json /tmp/after.json
```

These are local observations, not pass/fail thresholds. Host load, dependency state, and filesystem conditions affect results; compare runs on the same machine under similar conditions.
