# mypr-mcp

`mypr-mcp` provides a persistent, workspace-scoped Python layer between an LLM agent and the workstation. Each workspace has one Python kernel. Every MCP connection opened for that workspace shares the same variables, imports, functions, and background-task handles.

It provides a development toolkit for LLM agents: file operations, code and document search, web search, Git, shell commands, HTTP, browser automation, network scanning, and system diagnostics through one Python API. Agents can combine these tools in scripts and extend the environment with reusable Python functions, skills, and external MCP integrations.

The runtime is intended for Linux, Python 3.14, and [uv](https://docs.astral.sh/uv/). Commands run with the current OS user's permissions; mypr-mcp does not provide a sandbox.

Licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0-only).

## Run

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then configure your MCP client to launch:

```sh
uvx mypr-mcp serve
```

The client communicates with the server over stdio. The server's working directory becomes the workspace, so configure the client to launch it from the directory where the agent should work. `uvx` downloads and runs the package automatically.

For clients using `mcpServers` JSON configuration:

```json
{
  "mcpServers": {
    "mypr": {
      "command": "uvx",
      "args": ["mypr-mcp", "serve"]
    }
  }
}
```

Make sure `uvx` is on the client's `PATH`, or use its absolute executable path.

### Codex

With `uv` installed, add the following table to the target workspace's `.codex/config.toml`. Codex loads project configuration for trusted projects. Use `~/.codex/config.toml` instead for a user-wide registration. See the [official Codex MCP documentation](https://developers.openai.com/codex/mcp).

```toml
[mcp_servers.mypr]
command = "uvx"
args = ["mypr-mcp", "serve"]
startup_timeout_sec = 180
tool_timeout_sec = 60
required = true
```

Start Codex in the target workspace. The MCP process uses its launch directory as the workspace. `uvx` must be on Codex's `PATH`; use its absolute executable path if needed.

The startup allowance covers the first package download and workspace venv creation. `required = true` makes Codex wait for this server and report a startup failure if it cannot initialize. The tool timeout covers individual `init`/`execute`/`poll` calls, not the lifetime of background jobs.

Alternatively, register the launch command through the CLI:

```sh
codex mcp add mypr -- uvx mypr-mcp serve
```

This creates a user-wide entry. Add the timeout and `required` settings above to its `[mcp_servers.mypr]` table in `~/.codex/config.toml`; do not add the same table twice. A user-wide entry can serve different projects using each launch directory; use project-scoped configuration for project-specific settings.

Restart the Codex client after changing its configuration. Check registration with `codex mcp get mypr` or `codex mcp list`, then use `/mcp` in the Codex CLI to inspect the live connection. After connecting, the agent must call `init` once to bind the connection to a logical client identity before it can run Python code.

Codex's `[mcp_servers.mypr]` launches this Python layer. External MCP servers called from Python can be set as global defaults in `~/.config/mypr/config.toml`, overridden in the workspace's `.mypr/config.toml`, or registered dynamically with `await ws.mcp.configure(...)`. Start with [config.example.toml](config.example.toml); the [configuration reference](docs/config.md) covers every field, its default and allowed values, and how to apply changes.

## Workspace runtime

The first start creates `.mypr/` in the launch workspace, its Python environment, and a manager process. The manager and kernel continue running after an MCP client disconnects, so another client for the same workspace reconnects to the same in-memory environment.

Symlink and bind-mount aliases of the same directory connect to the same workspace runtime. Discovery uses the directory's filesystem device and inode, not its path string. The identity is visible as `workspace_id` in runtime status and `.mypr/runtime.json`; copying a workspace creates a separate identity.

Clients must run as the same OS user and be able to reach the runtime socket. When using separate mount namespaces or containers, share the runtime directory (`$XDG_RUNTIME_DIR/mypr`, or `/tmp/mypr-<uid>/mypr` when unset). The recorded socket also allows discovery when clients use different runtime-directory settings, provided that socket remains accessible. An unreachable existing manager is reported instead of launching a duplicate.

Stop the manager before moving a workspace, then start it at the new location. A same-filesystem rename preserves directory identity, but Python objects, venv entry points, and external MCP configurations can contain old absolute paths; these are not rewritten automatically. If the original directory is moved or replaced while running, new attachments and cells are rejected with a restart instruction. Status and stop remain available through the discovered socket.

## MCP tools

Three tools are exposed to the agent:

- `init(client_id=None)` binds this MCP connection to a logical client. With no argument, it allocates a new readable ID such as `calm-otter`; with an argument, it creates or resumes that ID. The returned ID is bound to the connection, so later calls do not repeat it.

- `execute(code, wait_ms=None, request_id=None, max_bytes=None)` submits a Python cell and returns its execution state and output. The connection must be initialized first. An omitted `wait_ms` uses `limits.execute_wait_ms` (1,000 ms by default); an explicit value, including `0`, overrides it for that call. `wait_ms` only controls how long the MCP call waits; it does not set a Python timeout.
- `poll(exec_id, cursor=None, wait_ms=None, max_bytes=None)` reads a submitted cell's state and output. Use the returned cursor to read later output. Polling an existing execution is allowed before `init`, since the execution ID identifies the target. An omitted `wait_ms` uses `limits.poll_wait_ms` (1,000 ms by default); an explicit value, including `0`, overrides it for that call. `max_bytes` controls one response page and accepts 1 KiB–1 MiB; when omitted, the workspace's configured response limit is used.

Tool responses provide readable text in `content` and the complete machine-readable payload in `structuredContent`. The text includes the current output page in full, with execution state, cursor, errors, warnings, and inbox previews. Adjacent fragments of the same stream are combined for display; the structured events and their cursors are unchanged. Initialization and runtime changes also include the running manager's API instructions.

Clients must parse `structuredContent` (the Python MCP SDK exposes `result.structured_content`), rather than treating text blocks as JSON. For older servers, a client may fall back to parsing their JSON text when structured content is absent. The new text format intentionally no longer mirrors the complete JSON payload.

When an operation fails, inspect both the bounded `error` string and the optional `error_info` object. `error_info.code` identifies categories such as `conflict`, `dependency_missing`, `timeout`, `invalid_cursor`, `outcome_unknown`, and `python_exception`; `operation` and bounded `details` provide machine-readable context. Branch on the code and inspect the execution or transaction ID before retrying a state-changing operation.

`execute` waits for completion, inbox activity, or its wait deadline so short cells normally need only one call. `poll` returns immediately when the requested output is available or the execution is terminal; otherwise it waits for output, completion, inbox activity, or its wait deadline. `wait_ms` bounds this notification wait, not total request latency or Python execution time. The configured defaults are `limits.execute_wait_ms` and `limits.poll_wait_ms`, each an integer from 0 through 30,000 milliseconds. Continue polling running cells, and read remaining pages while `has_more` is true even after execution finishes.

The `init`, `execute`, and `poll` responses include small previews of the current client's unacknowledged inbox and expired timers. Message previews contain up to five messages and fit within a 4 KiB JSON budget; timer previews contain up to five alerts in the same budget. Reading a preview or receiving a timer alert does not acknowledge it. Use `ws.messages.read()` and `ws.timers.list()` for full records. Before `init`, `poll` omits the inbox and timer previews.

Call `init()` before the first `execute`:

```text
init()                       -> binds calm-otter and returns runtime guidance
execute(code="...", ...)      -> uses calm-otter automatically
poll(exec_id="...", ...)      -> reads the execution
```

Repeating `init()` or specifying the currently bound ID returns that same ID. Trying to bind an initialized connection to a different ID is rejected. Only one live MCP connection may use a logical client ID at a time; after disconnecting, another connection can call `init(client_id="calm-otter")` to resume it. A connection that has not called `init` can still be counted and inspected by operational status, but it cannot submit Python code.

Submitted cells run as independent asyncio tasks in the shared kernel. They may complete in a different order from submission, including cells from the same client. An `await` that is still pending yields to other cells; synchronous code, synchronous IPython magics, and CPU-heavy work continue to occupy the event loop. If one cell depends on another, wait for the prerequisite to complete before submitting the dependent cell.

Use a background handle when work should remain detached from the submitting cell:

```python
job = await ws.shell.start("make -j2")
```

The cell returns after the process is started. In a later cell:

```python
job.status()
job.output()
job.result()
await job.cancel()
```

`result()` raises `NotReady` until completion. `await job` explicitly waits for completion of that job; for `persist_result=True`, it also waits for the persistence attempt and terminal publication to settle while returning the original in-memory value. Use `await job.wait_saved()` when the caller needs confirmed saved JSON; it returns `None` on success and raises `ResultUnavailable` when saving is unavailable, unserializable, unknown, or later removed by retention. Calling it for a live job without `persist_result=True` raises `ValueError`. Cancelling the wait does not cancel the job or its reporter. Other async cells continue to run. Use `ws.tasks.list()` and `ws.tasks.get(task_id)` to find handles created in another session. A disconnected MCP client does not cancel its submitted cells or background jobs.

## Python workspace API

The kernel injects `ws`, a `Workspace` instance, into every Python cell. Imports and globals are shared; use `ws.local` for client-specific scratch state.

```python
print(ws.help())
print(ws.help("fs"))
await ws.fs.write("notes.txt", "Hello from Python.\n")
```

The [Python workspace API guide](docs/workspace-api.md) covers entry points, examples, dependency preparation, and result paging. Detailed guides cover [configuration](docs/config.md), [code navigation](docs/code.md), [media and documents](docs/media.md), and [workspace storage](docs/storage.md).

## Reset and lifecycle

`await ws.reset()` resets the shared Python memory for every agent connected to the workspace. By default it is rejected while any other cell or managed job is active. Pass `force=True` to cancel that work and reset. Lifecycle and MCP management methods require an actual boolean for `force`; strings and integers are rejected. The reset cell's completion is reported by `execute`/`poll`. Saved files, skills, modules, package environment, and run records remain. The kernel generation changes and old Python job handles expire. Historical MCP execution IDs remain readable. Reusing a request ID within the same client returns the original execution instead of repeating side effects, including after a kernel reset or reconnecting with the same ID. Resuming that ID after a manager restart also retains request deduplication. A newly allocated ID starts a separate request-ID scope; use history to check earlier executions before retrying an uncertain operation.

`reset` only resets shared kernel memory. It does not update the installed package or replace the manager process. Globals, imports, functions, `ws.local`, and in-memory handles are lost; files, skills, modules, the package environment, messages, history, and saved task output remain.

Use `await ws.restart()` when a running workspace must apply the installation that made the request. This replaces the manager and kernel through a detached workspace coordinator, then reconnects planned MCP clients. It is explicit: package resolution and downloads belong to the command that launched the client (for example, `uvx mypr-mcp@latest`); restart does not resolve a new version from the network by itself. The default refuses while another cell, shell, scan, package job, or Python task is active. Use `await ws.restart(force=True)` to cancel active work first. The restart call's execution result is recorded and can be retrieved with `poll`; its Python code is never replayed.

The equivalent CLI command is:

```sh
cd /absolute/path/to/workspace
uvx mypr-mcp@latest restart
# Use --force only when cancelling active work is intended.
uvx mypr-mcp@latest restart --force
```

The new manager starts only after the old one exits and passes its health check. The coordinator records its ID, phase, old and new generation, target installation, and failure details under `.mypr/`. A failed start is reported without automatic rollback or an unbounded restart loop; inspect `.mypr/manager.log`, `uvx` diagnostics, and the restart record before retrying. External browser processes and pre-existing tabs remain owned by their launcher. Managed resources close during a normal replacement. In-memory Python state is always lost, while files, skills, modules, the package environment, messages, history, saved output, and completed scan records persist.

The CLI also provides operational controls. Run them from the workspace:

```sh
cd /absolute/path/to/workspace
uvx mypr-mcp status
uvx mypr-mcp doctor
uvx mypr-mcp logs
uvx mypr-mcp logs --limit 50 --follow
uvx mypr-mcp reset
uvx mypr-mcp restart
uvx mypr-mcp stop
```

`logs` prints JSONL lifecycle and output events. Without `--follow` it prints the most recent 20 events; `--limit` changes the page size (1–200). `--follow` waits for new records until interrupted. Use `ws.history.logs(client_id=...)` inside Python to filter by caller. The commands are local administration commands, not MCP tools. `reset` and `stop` reject active work unless `--force` is supplied. CLI `stop` reports success only after the original manager exits, so it is safe to reconnect immediately. Shutdown taking more than 30 seconds reports an error. Status includes the manager `pid`, `protocol_version`, manager and client installation versions, capabilities, `update_pending`, and a `health_error` when an essential runtime worker fails. Compatible package versions reuse the existing manager and kernel, so installing a newer `uvx` package alone does not clear Python memory. A protocol mismatch or an unknown legacy manager is reported through initialization with an actionable restart instruction rather than being hidden as a generic MCP handshake failure. The legacy 0.9.0 runtime has no `ws.restart()`; run the CLI `restart` once from the new installation to perform the first explicit replacement.

Automatic reconnection requires the updated MCP frontend; older frontend processes must be reopened once after installation.

Manager attachment has a 30-second handshake deadline, and rebinding a logical client ID has a separate 30-second deadline. Cold startup waits up to 180 seconds for readiness, including status-response waits; cleanup of an owned process follows a failed start.

Planned restart keeps the MCP stdio connection alive. After replacement, the connection receives a new connection ID and generation and rebinds its existing logical client ID. Other clients reconnect the same way. No submitted cell is replayed; use its recorded execution ID with `poll`. An ordinary crash, unplanned stop, or unreachable runtime does not trigger this reconnect path: unfinished executions become `lost`, and Python memory must be recreated.

Shell/package commands and local stdio MCP servers run through a supervisor. The supervisor's Python interpreter ignores Python-specific environment settings, while the command receives its configured environment unchanged. The supervisor closes its own standard streams after spawning the command, so it does not hold a terminated command's stdin/stdout pipes open. If the manager dies, their process groups receive SIGTERM, followed by SIGKILL after two seconds if needed. Same-group descendants remain supervised even after the original command exits. Processes that deliberately start a separate session are outside this boundary. HTTP MCP shutdown closes the local connection.

Python memory and running handles survive normal client disconnects, but they cannot be restored after a manager or kernel crash. In that case unfinished executions are marked `lost`; mypr-mcp never silently re-runs code or external MCP calls. The saved `.mypr/runs/` records and files remain available for inspection. The logical client ID and its history remain available, so a new connection can call `init(client_id="calm-otter")` after the runtime is healthy; in-memory variables and local state must be recreated after a crash.

## Output limits

Execution output is retained up to 16 MiB per run by default. Text events are paged with a default 32 KiB UTF-8 JSON budget, and `poll` uses cursors to retrieve later events. This is an output-page budget, not a limit on the entire MCP response: metadata, the readable text representation, inbox previews, and inline images add to it. Excess retained output is consumed and marked as truncated. Non-text display data is stored under `.mypr/artifacts/`.

Inline PNG/JPEG images share a 2 MiB source-byte budget per response; base64 encoding increases their wire size. Images that exceed this budget are omitted from inline content, with their artifact paths and reasons retained in the response. Their files remain available for later inspection. Image reads are bounded even if a file grows after its metadata is checked. Malformed display items and unavailable image files produce bounded `warnings` without changing successful Python execution to failure. Valid text, execution state, cursors, and inbox data remain available. Excess warnings are indicated by `warnings_truncated`. Shell and package jobs report output persistence failures through `warnings` in `job.status()` and `job.output(cursor=0)`. Python tasks drain retained output before publishing completion. Each result or lifecycle reporting RPC has a 10-second response deadline, independent of the Python task duration. After an output write cannot be confirmed, the reporter stops sending further output deltas and still attempts result and terminal reporting. An unconfirmed output write marks saved output as truncated and reports `task_output_persistence_unknown` in task status and history; the in-memory output remains readable. Their exit status still describes the command itself, and output continues to be drained after a storage failure. Historical polling preserves readable journal entries and replaces damaged lines with warning events, keeping event cursors stable. Cold polling after output retention expires returns `output_evicted=true`, `truncated=true`, and an `output_expired` warning; it does not present deleted output as complete empty output. `journal_truncated` identifies an incomplete final line; `journal_corrupt` identifies other invalid records. The original journal is left unchanged. The `error` field is a bounded summary (at most 1 KiB, smaller for small response budgets); `error_truncated` marks shortened summaries. Detailed traceback output is paged subject to the normal output limit.

### Completed-work retention

Configure retention as global defaults or workspace overrides. `completed_records` and `cache_bytes` apply through `await ws.config.reload()`; `completed_tasks` requires a manager and kernel restart:

```toml
[limits]
completed_tasks = 128
completed_records = 128
cache_bytes = 33554432
response_bytes = 32768
execute_wait_ms = 1000
poll_wait_ms = 1000
```

`response_bytes` controls the default `execute`/`poll` response page and must be between 1 KiB and 1 MiB. A per-call `max_bytes` can lower or raise the page budget within that same range.

`execute_wait_ms` and `poll_wait_ms` control the default notification wait for the corresponding MCP tools. Each accepts an integer from 0 through 30,000 milliseconds. A per-call `wait_ms` overrides the configured value, including with `0` for an immediate response.

The kernel retains the most recently completed `completed_tasks` handles, in addition to all active handles. Older handles disappear from `ws.tasks.list()` and `ws.tasks.get()`; use `await ws.tasks.attach(id)` or `ws.history` for saved records. A handle saved in your own variable or `ws.local` remains usable. These limits release internal cache references, not arbitrary objects retained by Python code.

Manager record and shell-output caches each retain at most `completed_records` completed entries and share the serialized-byte budget equally. Counts must be positive and `cache_bytes` must be at least 1024. Active work is never evicted. If shell metadata cannot be saved, the most recent affected completed job is retained outside these cache limits so its result and warning remain inspectable. This fallback lasts only while the manager is alive and retains at most one job's output, subject to the normal per-job output limit. Execution output, Python-task journals, and shell/package journals remain on disk under `.mypr/runs/` and `.mypr/jobs/`, so historical polling and delayed job monitors survive cache eviction. Request deduplication uses SQLite and survives eviction and restart, including empty request IDs. Query snapshots remain under `.mypr/searches/` and `.mypr/git/` until the storage retention policy removes expired, unreferenced data. Cache eviction alone does not delete snapshots, journals, saved files, or messages.

Execution admission, output publication, and terminal responses wait for their required records to be stored. Manager history and message I/O runs off the event loop in order; reset, restart, and shutdown settle pending records before closing storage. A storage failure is reported rather than returning an unrecorded execution success.

Execution journals have rebuildable `.idx` byte-offset indexes. Historical polling seeks directly to the requested event cursor instead of loading the entire output file for each page. Older journals are indexed once on first read, off the manager's event loop; missing or stale indexes are rebuilt.

Runtime files are created under `.mypr/`. The generated `.mypr/.gitignore` excludes the virtual environment, run records, artifacts, locks, logs, and the SQLite history, and other runtime metadata; reusable modules, skills, configuration, and `requirements.txt` remain available for version control as desired.

When upgrading, launching a newer compatible package is enough to reconnect to an existing manager; running managers are not silently replaced. Use the explicit `restart` command or `ws.restart()` when the new code must be loaded. Existing execution files are imported into history, and logical client IDs remain attached to those records.

## Development and publishing

For development, run `uv sync` in the checkout, then launch its `.venv/bin/mypr-mcp` executable from the target workspace.

To publish a release, update the package version, remove previous distributions, and rebuild:

Set the version in `pyproject.toml` and refresh `uv.lock`. Runtime version reporting reads the installed distribution metadata; source-only development kernels read `pyproject.toml`.

```sh
uv lock
rm -f dist/mypr_mcp-*.whl dist/mypr_mcp-*.tar.gz
uv build --no-sources
uv publish
```

`uv publish` uploads the distributions in `dist/` to PyPI; individual filenames are unnecessary when it contains only the intended release. Authenticate with `UV_PUBLISH_TOKEN` or configure Trusted Publishing for CI. See the [uv publishing guide](https://docs.astral.sh/uv/guides/package/).
