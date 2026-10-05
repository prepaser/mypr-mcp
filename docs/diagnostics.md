# Local Diagnostics

`ws.net.sockets()` lists local TCP, UDP, and Unix sockets. It accepts exact filters for `protocol`, `local_address`, `local_port`, `remote_address`, `remote_port`, `state`, and `pid`. The first call captures a short-lived snapshot; follow `next_cursor` to read later pages from that same capture.

```python
page = await ws.net.sockets(protocol="tcp", local_port=8000, limit=20)
while page["next_cursor"]:
    page = await ws.net.sockets(cursor=page["next_cursor"])
```

The default page contains 20 records and accepts limits from 1 to 50. Each result is capped at 32 KiB. A snapshot holds at most 1,000 records and expires after 60 seconds. `truncated` reports that the source had more records than the snapshot retained; `omitted_by_source` gives the omitted count. An expired or evicted cursor raises `ValueError`. Socket ownership can be unavailable for processes hidden by OS permissions, so rows may have `pid=None` and the result may include warnings.

`ws.system.process(pid, children=False, open_files=False, sockets=False)` returns one process with its creation time, parent, executable, working directory, name, username, and status. Optional flags include immediate children, open files, and sockets. File and socket lists are limited to 128 records each. A process that exits during inspection returns `status="gone"`; if its PID is reused while being inspected, details are discarded and `status="reused"` is returned. Permission and other per-field failures preserve available fields and add warnings.

```python
detail = await ws.system.process(1234, children=True, open_files=True)
detail["process"]
detail["warnings"]
```

Both methods run their psutil collectors in the existing isolated system worker, so a slow operating-system query does not block Python execution. The process detail response uses the standard 32 KiB result cap. These methods inspect the workstation under the current user's OS permissions; they do not elevate privileges or terminate processes.

`await ws.doctor()` is the readiness check for optional workstation features. It reports the workspace Python interpreter and packages, registered dependency tools and models, `rg`/`rga`/AST backends, configured LSP commands, browser engine, OCR executable and language data, external MCP configuration, runtime worker health, and available storage. Each check is independent, so a missing optional dependency does not hide unrelated results. `runtime` reports live manager health and execution counts when called through `ws.doctor()`; standalone `uvx mypr-mcp doctor` reports that health as unknown without starting or attaching to a kernel. `storage` reports filesystem total, used, and free bytes, plus managed usage when a workspace connection is available. The other checks can run before manager startup with `uvx mypr-mcp doctor`; neither form installs packages or dependency artifacts or changes configuration. Use `await ws.dependencies.ensure("name")` when a missing registered dependency should be prepared explicitly.

Malformed persisted metadata is reported through bounded warnings or structured errors. Shell, message, and history queries preserve readable fields and identify omitted data or unknown outcomes. Execution polling and task attachment reject invalid saved metadata instead of trusting it. Inspect these diagnostics before retrying a state-changing operation or treating a result as complete.
