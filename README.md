# mypr-mcp

`mypr-mcp` provides a persistent, workspace-scoped Python layer between an
LLM agent and the workstation. Each workspace has one Python kernel. Every
MCP connection opened for that workspace shares the same variables, imports,
functions, and background-task handles.

The runtime is intended for Linux, Python 3.14, and [uv](https://docs.astral.sh/uv/).
Commands run with the current OS user's permissions; mypr-mcp does not provide
a sandbox.

## Run

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then
configure your MCP client to launch:

```sh
uvx mypr-mcp serve
```

The client communicates with the server over stdio. The server's working
directory becomes the workspace, so configure the client to launch it from
the directory where the agent should work. `uvx` downloads and runs the
package automatically.

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
To pin a version, use `mypr-mcp@0.7.0` as the first argument.

### Codex

With `uv` installed, add the following table to the target workspace's
`.codex/config.toml`. Codex loads project configuration for
trusted projects. Use `~/.codex/config.toml` instead for a user-wide registration.
See the [official Codex MCP documentation](https://developers.openai.com/codex/mcp).

```toml
[mcp_servers.mypr]
command = "uvx"
args = ["mypr-mcp", "serve"]
startup_timeout_sec = 180
tool_timeout_sec = 60
required = true
```

Start Codex in the target workspace. The MCP process uses its launch directory
as the workspace. `uvx` must be on Codex's `PATH`; use its absolute
executable path if needed.

The startup allowance covers the first package download and workspace venv creation. `required = true`
makes Codex wait for this server and report a startup failure if it cannot
initialize. The tool timeout covers individual `init`/`execute`/`poll` calls, not the
lifetime of background jobs.

Alternatively, register the launch command through the CLI:

```sh
codex mcp add mypr -- uvx mypr-mcp serve
```

This creates a user-wide entry. Add the timeout and `required` settings above
to its `[mcp_servers.mypr]` table in `~/.codex/config.toml`; do not add the same
table twice. A user-wide entry can serve different projects using each launch
directory; use project-scoped configuration for project-specific settings.

Restart the Codex client after changing its configuration. Check registration
with `codex mcp get mypr` or `codex mcp list`, then use `/mcp` in the Codex CLI
to inspect the live connection. After connecting, the agent must call `init`
once to bind the connection to a logical client identity before it can run
Python code.

Codex's `[mcp_servers.mypr]` launches this Python layer. External MCP servers
called from Python belong in the workspace's `.mypr/config.toml`, or can be
registered dynamically with `await ws.mcp.configure(...)`.

## Workspace runtime

The first start creates `.mypr/` in the launch workspace, its Python
environment, and a manager process. The manager and kernel continue running
after an MCP client disconnects, so another client for the same workspace
reconnects to the same in-memory environment.

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

Three tools are exposed to the agent:

- `init(client_id=None)` binds this MCP connection to a logical client. With no
  argument, it allocates a new readable ID such as `calm-otter`; with an
  argument, it creates or resumes that ID. The returned ID is bound to the
  connection, so later calls do not repeat it.

- `execute(code, wait_ms=1000, request_id=None)` submits a Python cell and
  returns its execution state and output. The connection must be initialized
  first. `wait_ms` only controls how long the MCP call waits; it does not set a
  Python timeout.
- `poll(exec_id, cursor=None, wait_ms=1000)` reads a submitted cell's state and
  output. Use the returned cursor to read later output. Polling an existing
  execution is allowed before `init`, since the execution ID identifies the
  target.

The `init`, `execute`, and `poll` responses include a small preview of the
current client's unacknowledged inbox. Previews contain up to five messages and
fit within a 4 KiB JSON budget; reading a preview does not acknowledge its messages.
Use `ws.messages.read()` to page through full messages. Before `init`, `poll`
omits the inbox.

Call `init()` before the first `execute`:

```text
init()                         -> {"client_id": "calm-otter"}
execute(code="...", ...)      -> uses calm-otter automatically
poll(exec_id="...", ...)      -> reads the execution
```

Repeating `init()` or specifying the currently bound ID returns that same ID.
Trying to bind an
initialized connection to a different ID is rejected. Only one live MCP
connection may use a logical client ID at a time; after disconnecting, another
connection can call `init(client_id="calm-otter")` to resume it. A connection
that has not called `init` can still be counted and inspected by operational
status, but it cannot submit Python code.

Submitted cells run as independent asyncio tasks in the shared kernel. They
may complete in a different order from submission, including cells from the
same client. An `await` that is still pending yields to other cells; synchronous
code, synchronous IPython magics, and CPU-heavy work continue to occupy the
event loop. If one cell depends on another, wait for the prerequisite to complete
before submitting the dependent cell.

Use a background handle when work should remain detached from the submitting
cell:

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
for completion of that job; other async cells continue to run. Use
`ws.tasks.list()` and `ws.tasks.get(task_id)` to find handles created in
another session. A disconnected MCP client does not cancel its submitted
cells or background jobs.

## Python workspace API

The kernel injects `ws`, a `Workspace` instance, into every Python cell.
Methods shown with `await` are asynchronous; property access, inspection,
skill reads, and task-handle inspection are synchronous.

| Entry point | Purpose |
| --- | --- |
| `ws.workspace`, `ws.root` | `Path` objects for the workspace and its `.mypr/` directory |
| `ws.client`, `ws.local` | Current caller identity and its in-memory scratch dictionary |
| `ws.shell`, `ws.tasks` | Start and inspect background work |
| `ws.mcp` | Call and reconfigure external MCP servers |
| `ws.messages` | Send and receive persistent client messages |
| `ws.skills`, `ws.packages` | Read skills and install kernel packages |
| `ws.history` | Query saved execution and task records |
| `ws.inspect()`, `await ws.status()` | Inspect Python state and runtime health |
| `await ws.reset()` | Reset shared Python memory; see [Reset and lifecycle](#reset-and-lifecycle) |

Use ordinary Python for file and data work. `ws.workspace` stays anchored to
the workspace even if code changes the kernel's current directory:

```python
(ws.workspace / "notes.txt").write_text("Hello from Python.\n", encoding="utf-8")
```

All clients share imports, globals, and filesystem changes. Store caller-specific
values in `ws.local`; they persist across cells from the same logical client.
Kernel resets and crashes clear all in-memory local dictionaries.

### Shell and async tasks

`await ws.shell.start(command, *, cwd=None, env=None)` starts a command in its
own process group and returns a handle immediately. A string is interpreted by
`/bin/sh`; a list quotes each argument literally. Standard input is closed, and
stdout and stderr are captured together in the handle's output.

The default `cwd` is the kernel's current directory. The default environment is
the kernel's environment; an explicit `env` replaces it rather than merging it.
To override one variable while retaining the others:

```python
import os

ws.local["job"] = await ws.shell.start(
    ["python", "--version"],
    cwd=ws.workspace,
    env={**os.environ, "PYTHONUNBUFFERED": "1"},
)
```

`ws.tasks.start(awaitable, *, task_id=None, visible=True)` schedules a detached
awaitable in the kernel's event loop and returns a handle without `await`:

```python
import asyncio

ws.local["job"] = ws.tasks.start(asyncio.sleep(2, result="done"))
```

Cell, remote-job, and Python-task handles share one ID namespace. Custom task
IDs cannot replace existing handles or use generated ID forms: 32 lowercase
hexadecimal characters, `task-…-<number>`, or the `remote-watch:` prefix.
Automatic task IDs use a monotonically increasing counter without retaining
old IDs in memory. Custom IDs remain reserved until kernel reset, including
IDs used with `visible=False`.

Async tasks must yield to the event loop. Blocking or CPU-heavy work should run
in a separate process, for example through `ws.shell.start(...)`.
Use `ws.tasks.start()` for detached work that needs managed status and captured
output. Raw `asyncio.create_task()` children are not managed; output emitted
after their submitting cell finishes is not retained by that cell.

Inspect the handle in a later cell:

```python
ws.local["job"].status()
ws.local["job"].output()
ws.tasks.list(client_id=ws.client.id)
ws.tasks.get(ws.local["job"].id)
```

| Handle operation | Result |
| --- | --- |
| `job.status()` | Dictionary with `id`, `status`, owner IDs, and timestamps |
| `job.output()` | Captured text so far |
| `job.output(cursor=0)` | Dictionary with `output`, the next character `cursor`, and `truncated` |
| `job.result()` | Completed result; raises `NotReady` while still running |
| `await job` | Waits for completion and returns the result |
| `await job.cancel()` | Returns `False` if already terminal, otherwise requests cancellation and returns `True` |

Async jobs return the awaitable's value and propagate its exception. Successful
shell jobs return `{"returncode": 0}`; a failed shell job raises `RPCError` when
its result is retrieved. Cancelled jobs raise `asyncio.CancelledError`.
Waiting with `await job` suspends the current cell until completion while
other runnable cells continue.

Every submitted cell also has a task handle. Retrieve it with
`ws.tasks.get(exec_id)`. Its status has `kind="cell"`, and `result()` returns
the cell's actual last-expression value. `await` on a cell handle waits for
that cell only; a cell cannot await its own handle.

Task IDs are shared across the kernel; omit `task_id` to generate one.
An explicit ID can be reused after reset. Each kernel generation keeps a separate
history record: `await ws.history.get(task_id)` returns the latest record, while
`await ws.history.get(record["history_id"])` retrieves a specific generation.
The `python:<generation>:...` history-ID namespace is reserved.
`ws.tasks.list()` includes completed visible tasks, while `ws.tasks.active()`
returns active handles. `visible=False` omits an async task from discovery,
history reporting, and the reset guard; keep the default for managed work.

### MCP services

External MCP servers are configured in `.mypr/config.toml`:

```toml
[mcp.servers.reports]
command = "reports-mcp"
cwd = "/absolute/path/to/workspace"

[mcp.servers.reports.env_from]
REPORTS_TOKEN = "REPORTS_TOKEN"
```

The stdio service uses `command`, optional `args`, optional `cwd`, and
optional `env_from`. `command` can also be an argument list. HTTP services use
`url`, optional `headers_from`, and are connected lazily. Environment mappings
name variables in the manager's environment; their secret values are not
written to the config. A stdio server's default `cwd` is the workspace;
a relative `cwd` is resolved against it.

For an HTTP server:

```toml
[mcp.servers.remote]
url = "https://example.com/mcp"

[mcp.servers.remote.headers_from]
Authorization = "REMOTE_AUTHORIZATION"
```

`REMOTE_AUTHORIZATION` must contain the complete header value, including any
required scheme such as `Bearer `.

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

Remote results are dictionaries serialized from MCP models. Tool responses may
contain `content`, `structuredContent`, and `isError`; check `isError` before
using a tool result. A tool-reported error can be returned normally, while a
transport or bridge failure raises `RPCError`.

Tool, resource, and prompt listing methods require a server name and accept
`cursor=` for pagination. Pass the response's `nextCursor` to the next call
when it is non-null. Server discovery uses `servers` and `next_cursor` instead;
for additional pages use
`await ws.mcp.request("list_servers", cursor=next_cursor, limit=50)`.
Server-list cursors must be non-negative integers or decimal strings, and
limits must be integers from 1 to 1000. Invalid values are rejected.

Calls on the same external MCP connection may run concurrently, with each
response kept with its requesting cell. Start a call with `ws.tasks.start(...)`
when it should remain detached while the kernel accepts later cells. Calls
whose completion or external side effect is uncertain are not retried
automatically. Authentication variable names refer to the manager's environment;
changing their values still requires restarting the manager.

Add or replace a server directly from the kernel without resetting Python:

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

The first `init()` call assigns a readable logical client ID such as
`calm-otter` or `swift-fox`. It randomly combines one of 256 adjectives with one
of 256 animal names, giving 65,536 possible IDs per workspace. Automatically
issued IDs are reserved in `.mypr/history.sqlite3` and never reused while that
database is retained, even after disconnects or manager restarts. Explicit IDs
can create a new client or resume an existing one; IDs in history remain
reserved. If every automatic combination has been used, allocation fails with
an explicit exhaustion error.

Connection IDs remain random UUIDs. Read the bound identity through
`ws.client.id` and this particular connection through `ws.client.connection_id`.
Before `init`, the connection has no logical client ID. These IDs identify
callers for attribution and coordination; they are not authentication or
security boundaries.

The shared Python namespace is deliberately common to every client. Use the
client identity and local mapping for values that belong to the current agent:

```python
ws.client.id
ws.local["review_job"] = ws.tasks.start(do_review())
```

`ws.local` is persisted in the running kernel and is namespaced by logical
client ID. `ws.client.id` identifies the caller that submitted the current
cell; `connection_id` identifies this particular MCP connection. They prevent
accidental name reuse only when code follows the `ws.local` convention;
ordinary globals remain shared. Reconnecting with the same ID resumes the same
local dictionary while the kernel remains alive.

Use `await ws.status()` for the current manager, kernel, active executions,
queue, and connected-client information. `active` is an array of execution IDs;
each connection also reports its active execution IDs. `connection_count`
includes connections waiting to call `init`; `client_count` includes only
initialized logical clients. Use `await ws.history.list(...)` and
`await ws.history.get(exec_id)` to inspect execution records from Python. History
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

History methods accept `limit=1..200` (default 20). Omit `client_id` to include
all callers. Continue `list()` with its `next_cursor` until it is null. For logs,
reuse the returned cursor with the same filter:

```python
ws.local["logs"] = await ws.history.logs(client_id=ws.client.id)
```

In a later cell:

```python
ws.local["logs"] = await ws.history.logs(
    client_id=ws.client.id, cursor=ws.local["logs"]["cursor"],
)
ws.local["logs"]["events"]
```

Without a cursor, `logs()` returns recent events; `cursor=0` starts at the
beginning. Log cursors, history-list cursors, task-output cursors, and MCP
`poll` cursors belong to different APIs and must not be interchanged.

`connections` contains connection and activity timestamps, active execution
IDs, and owned task IDs. Open IPC connections determine liveness, so a
killed client is removed without cancelling its workspace jobs.

History is stored in `.mypr/history.sqlite3`. Lists return `items` and
`next_cursor`; logs return ascending events and a cursor for subsequent reads.
Execution details include a paged output view (continue with MCP `poll`).
Background task details retain up to 64 KiB of output and mark truncation;
live handles retain their normal output buffers. Logs stream cell, shell, and
Python task output while work is running.

### Client messages

Every logical client has a persistent inbox in the workspace SQLite history.
Messages remain available when the recipient is offline, across Python resets,
and after a manager restart. The recipient must already be registered in the
workspace, but does not need to remain connected.

```python
await ws.messages.send("bright-fox", "The review is complete.")
ws.local["inbox"] = await ws.messages.read()
# Handle the messages before acknowledging them.
await ws.messages.ack([message["id"] for message in ws.local["inbox"]["messages"]])
```

`send(to, text)` uses the current logical client as the sender and accepts
non-empty text up to 16 KiB in UTF-8. It returns `id`, `from`, `to`, `text`, and
`created_at`; text whose JSON escaping would exceed a 32 KiB page is rejected.
`read(limit=20, after=None, wait_ms=0)` returns
unacknowledged messages in ID order with `messages`, `next_cursor`, and
`has_more`. The limit is 1–100, the serialized page is at most 32 KiB, and
`wait_ms` is limited to 30 seconds. Use the returned `next_cursor` as `after`
to continue paging. A non-zero `wait_ms` waits only when no messages match the cursor.

`ack(ids)` explicitly acknowledges messages belonging to the current client.
Acknowledgement is idempotent, and reading or previewing a message never marks
it as read. It returns the number newly acknowledged; unknown or foreign IDs
reject the entire batch. These methods require an initialized client because messages are
scoped to its logical ID.

The MCP responses from `init`, `execute`, and `poll` include up to five short
inbox previews (within a 4 KiB budget), plus the total unacknowledged count.
The `inbox` field contains `unacked`, `messages`, and `has_more`; each preview has
`id`, `from`, `text`, and `truncated`. Use `read()` for full text when truncated.
Messages can end an `execute` or `poll` wait early without cancelling the cell:
check its state and continue polling if needed. Polling another client's execution
still returns your own inbox. Before `init`, `poll` omits the inbox.
They do not acknowledge the previews automatically. An agent that is not
calling an MCP tool is not woken when a message arrives; use `read(wait_ms=...)`
from a running Python task when a bounded wait is useful.

### Skills and reusable Python

Workspace skills live at `.mypr/skills/<name>/SKILL.md`. Discover and read
them with:

```python
ws.skills.list()
ws.skills.read("review")
```

`list()` returns metadata from YAML front matter plus each file's `path`,
defaulting `name` to the directory name. Invalid YAML is reported on that
item through an `error` field while the other skills remain available. Skills
whose files resolve outside `.mypr/skills` through a symlink are skipped;
links that stay inside the skills root are supported. `read(name)` returns the
full Markdown.
Both read from disk, so edits are visible on the next call without a reload.

Create or edit a skill with ordinary file operations:

```python
ws.local["skill"] = ws.skills.root / "review" / "SKILL.md"
ws.local["skill"].parent.mkdir(parents=True, exist_ok=True)
ws.local["skill"].write_text(
    "---\nname: review\ndescription: Review workspace changes.\n---\n\n"
    "Read the diff, check affected callers, and report concrete issues.\n",
    encoding="utf-8",
)
ws.skills.read("review")
```

Skill text is interpreted by the agent; reading it does not execute its
instructions or scripts.

For reusable Python, put modules under `.mypr/lib/ws_lib/`. Its parent,
`.mypr/lib/`, is on the kernel's import path:

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
ws.local["install"] = await ws.packages.add("httpx", "rich>=13")
ws.local["install"].status()
```

Retrieve the result in a later cell with `await ws.local["install"]` before
importing the new packages. Installation changes the kernel environment, not
the environments of separately launched MCP servers.

The installation uses `uv pip` and writes the resulting freeze to
`.mypr/requirements.txt`. Packages already imported by the current kernel may
need a kernel reset before an upgrade is visible.

`ws.inspect()` returns the workspace path, kernel generation, visible variable
names and types, task summaries, and discovered skills. `await ws.status()`
returns manager health, generation, connections, active execution IDs, and
queued executions.

## Reset and lifecycle

`await ws.reset()` resets the shared Python memory for every agent connected to
the workspace. By default it is rejected while any other cell or managed job
is active. Pass `force=True` to cancel that work and reset. The reset cell's
completion is reported by `execute`/`poll`. Saved files, skills, modules,
package environment, and run records remain. The kernel generation changes
and old Python job handles expire. Historical MCP
execution IDs remain readable. Reusing a request ID within the same client
returns the original execution instead of repeating side effects, including
after a kernel reset or reconnecting with the same ID. Resuming that ID after
a manager restart also retains request deduplication. A newly allocated ID
starts a separate request-ID scope; use history to check earlier executions
before retrying an uncertain operation.

The CLI also provides operational controls. Run them from the workspace:

```sh
cd /absolute/path/to/workspace
uvx mypr-mcp status
uvx mypr-mcp logs
uvx mypr-mcp logs --limit 50 --follow
uvx mypr-mcp reset
uvx mypr-mcp stop
```

`logs` prints JSONL lifecycle and output events. Without `--follow` it prints
the most recent 20 events; `--limit` changes the page size (1–200).
`--follow` waits for new records until interrupted. Use `ws.history.logs(client_id=...)`
inside Python to filter by caller.
The commands are local administration commands, not MCP tools. `reset` and
`stop` reject active work unless `--force` is supplied.
CLI `stop` reports success only after the original manager exits, so it is safe
to reconnect immediately. Shutdown taking more than 30 seconds reports an error.
Status includes the manager `pid` and a `health_error` when an essential runtime
worker fails. Such a failure marks unfinished executions lost and rejects new
executions until an explicit reset; Python code is never automatically replayed.

Shell/package commands and local stdio MCP servers run through a supervisor.
The supervisor's Python interpreter ignores Python-specific environment
settings, while the command receives its configured environment unchanged.
The supervisor closes its own standard streams after spawning the command,
so it does not hold a terminated command's stdin/stdout pipes open.
If the manager dies, their process groups receive SIGTERM, followed by SIGKILL
after two seconds if needed. Same-group descendants remain supervised even
after the original command exits. Processes that deliberately start a separate
session are outside this boundary. HTTP MCP shutdown closes the local connection.

Python memory and running handles survive normal client disconnects, but they
cannot be restored after a manager or kernel crash. In that case unfinished
executions are marked `lost`; mypr-mcp never silently re-runs code or external
MCP calls. The saved `.mypr/runs/` records and files remain available for
inspection. The logical client ID and its history remain available, so a new
connection can call `init(client_id="calm-otter")` after the runtime is healthy;
in-memory variables and local state must be recreated after a crash.

## Output limits

Execution output is retained up to 16 MiB per run by default. Text events are paged with a default 32 KiB budget, and `poll` uses cursors
to retrieve later events. PNG/JPEG displays up to 2 MiB are also returned as
MCP images. Excess output is consumed and marked as truncated. Non-text display
data is stored under `.mypr/artifacts/`.
Malformed display items and unavailable image files produce bounded `warnings`
without changing successful Python execution to failure. Valid text, execution
state, cursors, and inbox data remain available. Excess warnings are indicated
by `warnings_truncated`.
Shell and package jobs report output persistence failures through `warnings` in
`job.status()` and `job.output(cursor=0)`. Their exit status still describes the
command itself, and output continues to be drained after a storage failure.
Historical polling preserves readable journal entries and replaces damaged lines
with warning events, keeping event cursors stable. `journal_truncated` identifies
an incomplete final line; `journal_corrupt` identifies other invalid records.
The original journal is left unchanged.
The `error` field is a bounded summary (at most 1 KiB, smaller for small response
budgets); `error_truncated` marks shortened summaries. Detailed traceback output
is paged subject to the normal output limit.

### Completed-work retention

Configure retention in `.mypr/config.toml`; changes apply on manager restart:

```toml
[limits]
completed_tasks = 128
completed_records = 128
cache_bytes = 33554432
```

The kernel retains the most recently completed `completed_tasks` handles, in
addition to all active handles. Older handles disappear from `ws.tasks.list()`
and `ws.tasks.get()`; use `ws.history` for saved records. A handle saved in your
own variable or `ws.local` remains usable. These limits release internal cache
references, not arbitrary objects retained by Python code.

Manager record and shell-output caches each retain at most `completed_records`
completed entries and share the serialized-byte budget equally. Counts must be
positive and `cache_bytes` must be at least 1024. Active work is never evicted.
If shell metadata cannot be saved, the most recent affected completed job is
retained outside these cache limits so its result and warning remain inspectable.
This fallback lasts only while the manager is alive and retains at most one job's
output, subject to the normal per-job output limit.
Execution output and shell/package journals remain on disk under `.mypr/runs/`
and `.mypr/jobs/`, so historical polling and delayed job monitors survive cache
eviction. Request deduplication uses SQLite and survives eviction and restart,
including empty request IDs. Retention does not delete saved files or messages.
Execution journals have rebuildable `.idx` byte-offset indexes. Historical
polling seeks directly to the requested event cursor instead of loading the
entire output file for each page. Older journals are indexed once on first
read, off the manager's event loop; missing or stale indexes are rebuilt.

Runtime files are created under `.mypr/`. The generated `.mypr/.gitignore`
excludes the virtual environment, run records, artifacts, locks, logs, and
the SQLite history, and other runtime metadata; reusable modules, skills, configuration, and
`requirements.txt` remain available for version control as desired.

When upgrading, explicitly stop an older manager before reconnecting with the
new version. Running managers are not silently replaced. Existing execution files
are imported into history; legacy client IDs remain attached to those records.

## Development and publishing

For development, run `uv sync` in the checkout, then launch its
`.venv/bin/mypr-mcp` executable from the target workspace.

To publish a release, update the package version, remove previous distributions,
and rebuild:

Set the version in `pyproject.toml`. Runtime version reporting reads the installed
distribution metadata; source-only development kernels read `pyproject.toml`.

```sh
rm -f dist/mypr_mcp-*.whl dist/mypr_mcp-*.tar.gz
uv build --no-sources
uv publish
```

`uv publish` uploads the distributions in `dist/` to PyPI; individual filenames
are unnecessary when it contains only the intended release. Authenticate with `UV_PUBLISH_TOKEN`
or configure Trusted Publishing for CI. See the
[uv publishing guide](https://docs.astral.sh/uv/guides/package/).
