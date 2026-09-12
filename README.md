# mypr-mcp

`mypr-mcp` provides a persistent, workspace-scoped Python layer between an
LLM agent and the workstation. Each workspace has one Python kernel. Every
MCP connection opened for that workspace shares the same variables, imports,
functions, and background-task handles.

The runtime is intended for Linux, Python 3.14, and [uv](https://docs.astral.sh/uv/).
Commands run with the current OS user's permissions; mypr-mcp does not provide
a sandbox.

## Install

From this repository:

```sh
uv sync
```

Start the MCP server with an absolute workspace path:

```sh
uv run mypr-mcp serve --workspace /absolute/path/to/workspace
```

Each server process gets a random logical client ID by default and a fresh
connection ID for every connection. Pass `--client-id` to keep the same
logical identity across reconnects, and `--client-name` to make it easier to
recognize in status and history output:

```sh
uv run mypr-mcp serve --workspace /absolute/path/to/workspace \
  --client-id conversation-a --client-name "Review agent"
```

Client IDs identify callers for attribution and coordination. They are not an
authentication mechanism or a security boundary. If several conversations
share a logical ID, use a unique ID for each conversation when per-conversation
ownership and filtering are needed.

The first start creates `/absolute/path/to/workspace/.mypr/`, its Python
environment, and a manager process. The manager and kernel continue running
after an MCP client disconnects, so another client for the same workspace
reconnects to the same in-memory environment.

For an MCP host that starts commands from JSON, use the repository directory
in `--directory` and pass the workspace separately:

```json
{
  "mcpServers": {
    "mypr": {
      "command": "uv",
      "args": [
        "--directory", "/absolute/path/to/mypr-mcp",
        "run", "mypr-mcp", "serve",
        "--workspace", "/absolute/path/to/workspace"
      ]
    }
  }
}
```

Symlink and bind-mount aliases of the same directory connect to the same
workspace runtime. Discovery uses the directory's filesystem device and inode,
not its path string. The identity is visible as `workspace_id` in runtime status
and `.mypr/runtime.json`; copying a workspace creates a separate identity.

Clients must run as the same OS user and be able to reach the runtime socket.
When using separate mount namespaces or containers, share the runtime directory
(`$XDG_RUNTIME_DIR/mypr`, or `/tmp/mypr-<uid>/mypr` when unset). The recorded socket
also allows discovery when clients use different runtime-directory settings,
provided that socket remains accessible. An unreachable existing manager is
reported instead of launching a duplicate.

Stop the manager before moving a workspace, then start it at the new location.
A same-filesystem rename preserves directory identity, but Python objects, venv
entry points, and external MCP configurations can contain old absolute paths;
these are not rewritten automatically. If the original directory is moved or
replaced while running, new attachments and cells are rejected with a restart
instruction. Status and stop remain available through the discovered socket.

## MCP tools

Only two tools are exposed to the agent:

- `execute(code, wait_ms=1000, request_id=None)` submits a Python cell and
  returns its execution state and output. `wait_ms` only controls how long the
  MCP call waits; it does not set a Python timeout.
- `poll(exec_id, cursor=None, wait_ms=1000)` reads a submitted cell's state and
  output. Use the returned cursor to read later output.

Cells execute one at a time in FIFO order. Keep cells short. A normal cell
that waits for a long operation keeps the shared kernel occupied, so start
long work through a background handle instead:

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

`result()` raises `NotReady` until completion. `await job` explicitly waits
for completion. Use `ws.tasks.list()` and `ws.tasks.get(task_id)` to find
handles created in another session. A disconnected MCP client does not cancel
its submitted cells or background jobs.

## Python workspace API

The kernel injects `ws`, a `Workspace` instance. Ordinary file and data work
uses standard Python and `pathlib`.

### Shell and async tasks

`await ws.shell.start(command, *, cwd=None, env=None)` starts a command in its
own process group and returns a handle. `command` may be a string or a list of
arguments. The default working directory is the kernel's current directory (initially the workspace). Shell output is
captured by the handle.

`ws.tasks.start(awaitable, *, task_id=None, visible=True)` starts an awaitable
in the persistent kernel event loop and returns a handle. This is useful for
non-blocking async I/O:

```python
job = ws.tasks.start(ws.mcp.call_tool("reports", "fetch", {"id": "42"}))
```

### Configured MCP services

External MCP servers are configured in `.mypr/config.toml`:

```toml
[mcp.servers.reports]
command = "uv"
args = ["--directory", "/absolute/path/to/reports", "run", "reports-mcp"]
cwd = "/absolute/path/to/workspace"

[mcp.servers.reports.env_from]
REPORTS_TOKEN = "REPORTS_TOKEN"
```

The stdio service uses `command`, optional `args`, optional `cwd`, and
optional `env_from`. `command` can also be an argument list. HTTP services use
`url`, optional `headers_from`, and are connected lazily. Environment mappings
name variables in the manager's environment; their secret values are not
written to the config.

The API is:

```python
await ws.mcp.list_servers()
await ws.mcp.list_tools("reports")
await ws.mcp.call_tool("reports", "fetch", {"id": "42"})
await ws.mcp.list_resources("reports")
await ws.mcp.read_resource("reports", "reports://latest")
await ws.mcp.list_prompts("reports")
await ws.mcp.get_prompt("reports", "summary", {"id": "42"})
```

Results are JSON-compatible MCP models. Start a call with
`ws.tasks.start(...)` when it should run while the kernel accepts later cells.
Calls whose completion or external side effect is uncertain are not retried
automatically. Authentication variable names refer to the manager's environment;
changing their values still requires restarting the manager.

### Change MCP capabilities without resetting Python

Add or replace a server from the kernel:

```python
await ws.mcp.configure("reports", {
    "command": "/absolute/path/to/server-venv/bin/python",
    "args": ["/absolute/path/to/reports_server.py"],
})
await ws.mcp.list_tools("reports")
```

`configure` persists a complete replacement of that server's configuration to
`.mypr/config.toml`. To change selected fields, read the active configuration first:

```python
ws.local["server_config"] = await ws.mcp.get_config("reports")
ws.local["server_config"]["args"] = ["/absolute/path/to/new_server.py"]
await ws.mcp.configure("reports", ws.local["server_config"])

await ws.mcp.restart("reports")  # Reload edited server code using the active config.
await ws.mcp.reload()            # Apply direct edits to config.toml's MCP servers.
await ws.mcp.remove("reports")  # Disconnect and remove the saved server entry.
```

The shared kernel, variables, `ws.local`, and unrelated server connections stay
alive. New or changed configurations connect lazily on the next call; `restart`
establishes a fresh initialized connection. It does not reread the config file.
This lets an agent write or improve a local MCP server, reconnect it, and use
its updated tools in the same Python session.

Changes affecting active or queued calls are rejected by default. Use
`force=True` on these management methods to close the affected connections and
fail their pending calls. Other servers remain usable. Closing a connection does
not undo completed external side effects.

Configuration is validated before changes are applied. Writes are atomic and
preserve unrelated TOML sections and comments. If the file changed since the last
load, `configure` and `remove` ask for `reload()` rather than overwriting those
edits. `reload()` applies only added, changed, or removed MCP entries; unchanged
connections stay open. A valid configuration does not guarantee that its server
can start or authenticate: connection failures are reported when connecting.
Management changes and their outcomes are recorded in workspace history. Once
accepted, a management operation completes even if its caller disconnects or
stops waiting; inspect configuration and history to confirm the outcome.

### Client-local state and history

The shared Python namespace is deliberately common to every client. Use the
client identity and local mapping for values that belong to the current agent:

```python
ws.client.id
ws.client.name
ws.local["review_job"] = ws.tasks.start(do_review())
```

`ws.local` is persisted in the running kernel and is namespaced by logical
client ID. `ws.client.id` and `ws.client.name` identify the caller that
submitted the current cell; `connection_id` identifies this particular MCP
connection. They prevent accidental name reuse only when code follows the
`ws.local` convention; ordinary globals remain shared.

Use `await ws.status()` for the current manager, kernel, active execution,
queue, and connected-client information. Use `ws.history.list(...)` and
`ws.history.get(exec_id)` to inspect execution records from Python. History
records include the logical client and connection IDs, timestamps, state, and
output metadata. A task inherits its creator's identity even while another
client executes. IDs are organizational labels, not access-control boundaries.

```python
state = await ws.status()
state["connection_count"], state["client_count"]
await ws.history.list(client_id=ws.client.id, limit=20)
await ws.history.get(exec_id)  # Also accepts a background task ID.
await ws.history.logs(client_id=ws.client.id, limit=20)
```

`connections` contains names, connection and activity timestamps, the active
execution, and owned task IDs. Open IPC connections determine liveness, so a
killed client is removed without cancelling its workspace jobs.

History is stored in `.mypr/history.sqlite3`. Lists return `items` and
`next_cursor`; logs return ascending events and a cursor for subsequent reads.
Execution details include a paged output view (continue with MCP `poll`).
Background task details retain up to 64 KiB of output and mark truncation;
live handles retain their normal output buffers. Logs stream cell, shell, and
Python task output while work is running.

### Skills and reusable Python

Workspace skills live at `.mypr/skills/<name>/SKILL.md`. Discover and read
them with:

```python
ws.skills.list()
ws.skills.read("review")
```

Skill instructions are read by the agent. Skill files and helper scripts can
be created or edited with `pathlib` in the shared workspace.

For reusable Python, put modules under `.mypr/lib/ws_lib/`. That directory is
already on the kernel's import path:

```python
from pathlib import Path

path = ws.workspace / ".mypr" / "lib" / "ws_lib" / "helpers.py"
path.write_text("def answer(value):\n    return value * 2\n", encoding="utf-8")

import ws_lib.helpers as helpers
import importlib
importlib.reload(helpers)
```

Reload is explicit. Existing references to functions or objects from the old
module remain unchanged.

### Packages and inspection

`await ws.packages.add(*specs)` installs package requirements into the
workspace's `.mypr/venv` and returns a background handle. For example:

```python
job = await ws.packages.add("httpx", "rich>=13")
await job
```

The installation uses `uv pip` and writes the resulting freeze to
`.mypr/requirements.txt`. Packages already imported by the current kernel may
need a kernel reset before an upgrade is visible.

`ws.inspect()` returns the workspace path, kernel generation, visible variable
names and types, task summaries, and discovered skills. `await ws.status()`
returns manager health, generation, connections, active execution, and queued
executions.

## Reset and lifecycle

`await ws.reset()` resets the shared Python memory for every agent connected to
the workspace. It terminates the reset cell; its completion is reported by the
`execute`/`poll` result. Saved files, skills, modules, package environment,
and run records remain. Pass `force=True` to cancel active Python tasks and
reset. The kernel generation changes and old Python job handles expire. Historical MCP
execution IDs remain readable. Request IDs are scoped to the logical client:
reusing the same client ID and request ID returns the original execution instead
of repeating side effects, including after reset or manager restart.

The CLI also provides operational controls:

```sh
uv run mypr-mcp status --workspace /absolute/path/to/workspace
uv run mypr-mcp logs --workspace /absolute/path/to/workspace
uv run mypr-mcp logs --workspace /absolute/path/to/workspace \
  --client-id conversation-a --limit 50 --follow
uv run mypr-mcp reset --workspace /absolute/path/to/workspace
uv run mypr-mcp stop --workspace /absolute/path/to/workspace
```

`logs` prints JSONL lifecycle and output events. Without `--follow` it prints
the most recent 20 events; `--limit` changes the page size (1–200), and `--client-id`
filters by logical caller. `--follow` waits for new records until interrupted.
The commands are local administration commands, not MCP tools. `reset` and
`stop` reject active work unless `--force` is supplied.

Python memory and running handles survive normal client disconnects, but they
cannot be restored after a manager or kernel crash. In that case unfinished
executions are marked `lost`; mypr-mcp never silently re-runs code or external
MCP calls. The saved `.mypr/runs/` records and files remain available for
inspection.

## Output limits

Execution output is retained up to 16 MiB per run by default. Text events are paged with a default 32 KiB budget, and `poll` uses cursors
to retrieve later events. PNG/JPEG displays up to 2 MiB are also returned as
MCP images. Excess output is consumed and marked as truncated. Non-text display
data is stored under `.mypr/artifacts/`.

Runtime files are created under `.mypr/`. The generated `.mypr/.gitignore`
excludes the virtual environment, run records, artifacts, locks, logs, and
the SQLite history, and other runtime metadata; reusable modules, skills, configuration, and
`requirements.txt` remain available for version control as desired.

When upgrading, explicitly stop an older manager before reconnecting with the
new version. Running managers are not silently replaced. Existing execution files
are imported into history; legacy client IDs remain attached to those records.
