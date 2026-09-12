# mypr-mcp

`mypr-mcp` puts a persistent, workspace-scoped Python environment between an
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

Use a canonical absolute workspace path. Symlinked paths resolve to the same
workspace runtime.

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
automatically. Editing MCP configuration takes effect after `await ws.reset()`;
changing authentication environment variables requires restarting the manager.

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
execution IDs remain readable; reusing a request ID returns its original execution
instead of repeating side effects, including after reset or manager restart.

The CLI also provides operational controls:

```sh
uv run mypr-mcp status --workspace /absolute/path/to/workspace
uv run mypr-mcp reset --workspace /absolute/path/to/workspace
uv run mypr-mcp stop --workspace /absolute/path/to/workspace
```

These are local administration commands, not MCP tools. `reset` and `stop`
reject active work unless `--force` is supplied.

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
other runtime metadata; reusable modules, skills, configuration, and
`requirements.txt` remain available for version control as desired.
