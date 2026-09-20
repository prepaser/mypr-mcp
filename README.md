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
To pin a version, use `mypr-mcp@<version>` as the first argument.

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
| `ws.fs` | Read, search, create, and patch files with bounded results |
| `ws.shell`, `ws.tasks` | Start and inspect background work |
| `ws.mcp` | Call and reconfigure external MCP servers |
| `ws.messages` | Send and receive persistent client messages |
| `ws.skills`, `ws.modules` | Validate, save, and reuse workspace capabilities |
| `ws.git` | Read structured Git status, diffs, and committed files |
| `ws.http` | Use named, persistent HTTPX2 clients and bounded requests |
| `ws.browser` | Use native Playwright browser contexts and pages |
| `ws.net` | Resolve hosts, inspect TCP/TLS endpoints, and run scans |
| `ws.system` | Inspect workstation hardware, limits, and current resource usage |
| `ws.locks` | Coordinate shared work with task-scoped logical locks |
| `ws.packages` | Install kernel packages |
| `ws.history` | Query saved execution and task records |
| `ws.inspect()`, `await ws.status()` | Inspect Python state and runtime health |
| `await ws.reset()` | Reset shared Python memory; see [Reset and lifecycle](#reset-and-lifecycle) |
| `await ws.restart()` | Replace the manager and kernel with this installation; see [Reset and lifecycle](#reset-and-lifecycle) |

Use the async helpers for everyday file work. `ws.workspace` and `ws.fs` paths
stay anchored to the workspace even if code changes the kernel's current directory:

```python
await ws.fs.write("notes.txt", "Hello from Python.\n")
```

All clients share imports, globals, and filesystem changes. Store caller-specific
values in `ws.local`; they persist across cells from the same logical client.
Kernel resets and crashes clear all in-memory local dictionaries.

### Files and search

```python
ws.local["page"] = await ws.fs.read("src/app.py", start_line=20, end_line=80)
await ws.fs.search("TODO|FIXME", paths="src", glob="*.py", context=2)
await ws.fs.search(glob="*.py")
await ws.fs.patch(
    "src/app.py",
    [{"old": "timeout = 10", "new": "timeout = 30"}],
    expected_hash=ws.local["page"]["revision"],
    dry_run=True,
)
```

`read(path, *, start_line=1, end_line=None, start_byte=None, max_bytes=32768)`
returns UTF-8 `text`, a SHA-256 `revision`, file `size`, line information, and
`truncated`. Lines are one-based and `end_line` is inclusive. If the output limit
cuts a line, continue using `next_cursor["line"]` as `start_line` and
`next_cursor["byte"]` as `start_byte`. Compare revisions when reading multiple
pages of a file that may change.

`search(pattern=None, *, paths=None, glob=None, fixed=False, ignore_case=False,
hidden=False, no_ignore=False, context=0, max_matches=100, max_bytes=32768, cursor=None)` uses
`rg` from the workstation's PATH. Install ripgrep to enable it. With a pattern,
it returns `matches` containing file paths, line numbers, byte-based columns,
text, and match/context kind. Without a pattern, it returns `files`. `paths` and
`glob` accept a string or a list. Ignore files and hidden-file rules apply by
default. `truncated` reports incomplete results; `text_truncated` marks shortened
match text. Search runs as a managed shell job and its captured scan output is
bounded separately from the returned results. The returned `id` identifies its
job; use `await ws.tasks.attach(result["id"])` to inspect it.
The first request completes the query and saves a bounded snapshot. Continue with
`await ws.fs.search(cursor=page["next_cursor"])` while `has_more` is true.
`max_matches` and `max_bytes` apply to each page. Continuations read the saved
snapshot without rerunning the search, even after a reset or restart. The scan
is limited to 16 MiB; `scan_truncated` reports an incomplete retained query.
`truncated` also covers remaining pages and shortened match text. A budget too
small for a path and its metadata raises an error; increase `max_bytes`.

`await ws.fs.tree(path=".", depth=3, max_entries=200, hidden=False)` returns
a deterministic directory view with `entries` and `truncated`. Symlinks are
listed without traversing their targets. `await ws.fs.stat(path,
follow_symlinks=False)` returns file metadata, including kind, size, modification
time, mode, and symlink target; it does not hash file contents.

`write(path, text, *, expected_hash=None, overwrite=False, encoding="utf-8",
create_parents=False)` creates a file. Replacing an existing file requires its
current revision or explicit `overwrite=True`. `patch(path, edits, *,
expected_hash=None, dry_run=False, encoding="utf-8", max_diff_bytes=32768)` applies
an ordered list of exact `{"old": ..., "new": ...}` replacements. Each target
must occur once by default; use `count=N` for the first N matches or
`count="all"` for all matches. A missing or ambiguous target fails before writing.
Results include old/new revisions and a bounded unified diff; `dry_run=True`
leaves the file unchanged.

Use `apply_patch(patch, *, expected_hashes=None, dry_run=False,
max_diff_bytes=32768)` for a patch spanning multiple files:

```python
await ws.fs.apply_patch("""*** Begin Patch
*** Add File: notes/new.txt
+Created from Python.
*** Update File: notes/old.txt
*** Move to: notes/renamed.txt
@@
-Before
+After
*** Delete File: notes/obsolete.txt
*** End Patch
""", dry_run=True)
```

The format uses `*** Add File`, `*** Update File`, `*** Delete File`, optional
`*** Move to`, and `@@` context hunks inside `*** Begin Patch` / `*** End Patch`.
Matching is exact; ambiguous context is rejected. `expected_hashes` maps file
paths to revisions, with `None` requiring that a path does not exist. All targets
and hunks are validated before mutation, and affected paths share the same locks
as `write()` and `patch()`. Files are staged before application; ordinary commit
failures trigger rollback. This is not a filesystem-wide atomic transaction:
external writers and process or machine crashes can interrupt recovery.
Symlink paths, duplicate targets, and hard-link aliases within one patch are
rejected. `*** End of File` anchors the final hunk to the end of the file.

`await ws.fs.image("plot.png")` loads a PNG/JPEG for inline MCP image output.
Return it as the cell's last expression or pass it to IPython's `display()`.
`max_bytes` defaults to 2 MiB, matching the inline image limit; the source file
is left unchanged.

Writes use atomic replacement and preserve existing file permissions.
Single-file `write()` and `patch()` follow symlinks while preserving the link
itself. Edits through these helpers serialize
per resolved path, including across clients. Revision checks reject stale content;
they do not lock out edits by external programs. Absolute paths are accepted under
the current user's permissions. Ordinary Python remains available for other file
and data operations.

### Git

```python
await ws.git.status()
await ws.git.diff(staged=True, paths=["src"])
await ws.git.show("HEAD", path="README.md")
```

`status(*, cursor=None, max_entries=200, max_bytes=32768)` returns branch and
file information, including index/worktree changes, conflicts, and renames.
`diff(*, staged=False, rev=None, paths=None, cursor=None, max_bytes=32768)`
returns file metadata and patch text. `show(ref="HEAD", *, path=None,
cursor=None, max_bytes=32768)` reads a commit or a file at that revision.
Input paths are relative to the workspace; returned file paths are relative to
the reported repository `root`. `max_bytes` must be at least 1024. Collect both
`files` and `patch` across diff pages; a page may contain only file metadata.

These are read-only commands with paging, color, external diff programs, and
textconv disabled. Follow `next_cursor` with the same method while `has_more`
is true. Pages come from a saved snapshot, so later changes to the worktree do
not alter an existing query. Snapshots survive kernel reset and manager restart.

### HTTP

`ws.http` keeps named native `httpx2.AsyncClient` instances alive in the Python
kernel. Clients are private to the current logical client by default; pass
`shared=True` when every client should use the same cookie jar and connection
pool. The default HTTP timeout is 30 seconds. Client options are fixed after creation,
so close a named client before
changing its configuration:

```python
response = await ws.http.get("https://example.com/api", name="api")
response.status_code, response.json()

client = ws.http.client("upload", base_url="https://example.com", timeout=10)
response = await client.post("/files", content=b"data")
await ws.http.close("upload")
```

`get()`, `post()`, `put()`, `patch()`, `delete()`, `head()`, and `options()`
return native responses after consuming the body. They enforce a 16 MiB decoded
body limit by default; set `max_bytes=None` only when the caller can safely
handle an unbounded response. `stream()` yields the native streaming response
for incremental processing. `download()` writes atomically (relative paths use the workspace),
refuses to overwrite by default, and limits the decoded response to 256 MiB;
pass `overwrite=True` or another `max_bytes` when appropriate. Cancellation
removes incomplete downloads. The raw client returned by `client()` is an
escape hatch for full HTTPX2 behavior and does not apply the convenience
request limit.

### Browser automation

`ws.browser` returns native async Playwright objects, so pages, locators,
frames, requests, tracing, and other Playwright APIs remain available:

```python
context = await ws.browser.context(
    "shop", browser="chromium", launch_options={"headless": True}
)
page = await context.new_page()
await page.goto("https://example.com")
await page.get_by_role("button", name="Continue").click()
await ws.browser.screenshot(page, "artifacts/shop.png")
```

The first managed context automatically installs the requested Playwright
browser engine when it is missing. Installation runs as a managed workspace
job and reuses the configured Playwright browser cache. Set
`PLAYWRIGHT_BROWSERS_PATH` before starting the manager to select that cache.
Managed browsers, contexts, and pages belong to the workspace runtime and are
closed during reset. Use `ws.browser.close(...)` to release them earlier.

Use `save_state()` and `load_state()` to persist authentication explicitly;
saved state includes IndexedDB by default and is kept under `.mypr/browser`.
State is not saved automatically when a context closes:

```python
await ws.browser.save_state(context, name="login")
state = await ws.browser.load_state("login")
restored = await ws.browser.context("restored", storage_state=state)
```

`shared=True` gives all logical clients the same named context or connection;
otherwise the name is private to the current client. Use `connect(endpoint,
protocol="playwright"|"cdp", name="remote")` for an externally managed
browser, then pass `connection="remote"` to `context()`. Closing or resetting
mypr-mcp disconnects from external browsers and leaves their processes and
pre-existing tabs running. `screenshot()` saves an artifact and returns it as
inline image content, subject to the normal 2 MiB image limit. HAR and video
paths supplied through Playwright context options are resolved below the
workspace.

### Network diagnostics and scans

`ws.net.resolve(host, port=None)` returns deduplicated IPv4/IPv6 addresses.
`connect(host, port, timeout=3)` reports `open`, `closed`, `timeout`, or
`unreachable` without raising for ordinary connection failures. `tls()` uses
certificate and hostname verification by default and reports the negotiated
TLS version, cipher, peer certificate, and SHA-256 fingerprint. Set
`verify=False` only for diagnostics; `cert_pem` or `fingerprint` can pin the
peer certificate.

TCP scans default to ports 1–1024, 64 concurrent connections, 200 probes per second,
and a one-second connection timeout. TCP scans and Nmap runs are managed background jobs:

```python
scan = await ws.net.scan(
    ["127.0.0.1"], ports="22,80,443", concurrency=64, rate=200, timeout=1
)
await scan.summary(wait_ms=30000)
page = await scan.results(max_entries=100, max_bytes=32768)
rows = page["results"]
while page["has_more"]:
    page = await scan.results(cursor=page["next_cursor"])
    rows.extend(page["results"])

nmap = await ws.net.nmap("127.0.0.1", args=["-sV"])
summary = await nmap
```

Awaiting a scan returns its terminal summary, including failed or cancelled states;
check `state` and `error`. `result()` returns that summary once it is ready.

Scan handles support the normal task methods (`status()`, `read()`,
`expect()`, `output()`, `result()`, `cancel()`, and `await handle`) as well as
`summary()` and paged `results()`. `await ws.tasks.attach(scan_id)` reconnects
to a retained scan after a reset or manager restart. TCP results and parsed
Nmap results are retained under `.mypr/scans`; each result store is capped at
16 MiB and each page defaults to 100 entries and 32 KiB. Nmap owns its XML
output channel, so output flags such as `-oX`, `-oA`, and `-oN` are rejected;
install Nmap and arrange privileges explicitly when a scan requires them.

HTTP clients, managed browser resources, and active scans are attached to the
workspace runtime. A reset closes clients and managed browser resources and
cancels active scans; saved browser state, completed scan records, and files
remain available afterward.

### Shell and async tasks

```python
await ws.shell.run(["git", "status", "--short"])
await ws.shell.run("make -j2", timeout=120, check=True)
await ws.shell.run(["sort"], input="bravo\nalpha\n")
```

`run(command, *, cwd=None, env=None, input=None, timeout=None, check=False,
max_bytes=32768, pty=False, rows=24, cols=80)` waits asynchronously for completion
and returns `id`, `state`,
`returncode`, separate `stdout`/`stderr`, `timed_out`, and `truncated`. The byte
budget is shared between stdout and stderr, with stdout first. The full retained
combined output remains in `ws.tasks.get(result["id"]).output()`. A nonzero exit
is returned normally; `check=True` raises `ShellError` with the result in
`exception.result`. A timeout cancels the process group and returns
`timed_out=True`; cancelling the awaiting cell also cancels the command.

`await ws.shell.start(command, *, cwd=None, env=None, input=None, stdin=False,
pty=False, rows=24, cols=80)` starts a command in its
own process group and returns a handle immediately. A string is interpreted by
`/bin/sh`; a list executes argv directly. Standard input is closed by default.
`input` supplies UTF-8 text and then closes stdin; `stdin=True` keeps the pipe
open for later `await job.write(text)` calls. Use `await job.write(eof=True)` to
close it. stdout and stderr are captured together in the handle's output.

Set `pty=True` for programs that need a terminal:

```python
ws.local["term"] = await ws.shell.start(
    ["bash", "--noprofile", "--norc", "-i"], pty=True, rows=30, cols=100,
)
await ws.local["term"].write("pwd\n")
await ws.local["term"].resize(40, 120)
await ws.local["term"].write("\x03")  # Ctrl-C
ws.local["term"].output()
await ws.local["term"].cancel()
```

PTY mode provides a controlling terminal and combines stdout/stderr into stdout.
Terminal echo, ANSI sequences, and CRLF line endings are preserved. `write()` is
available without `stdin=True`. `write(eof=True)` sends the terminal's EOF control
character instead of closing its master descriptor; programs in raw mode decide
how to interpret it. Use `cancel()` to terminate the job. `resize(rows, cols)`
updates the terminal size and notifies its foreground process group.

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
| `await job.read(cursor=None, stream=None, max_bytes=32768, wait_ms=0)` | Bounded output page and opaque continuation cursor |
| `await job.expect(pattern, cursor=None, stream=None, regex=False, timeout=30, max_scan_bytes=65536)` | Wait for text or a regex across output chunks |
| `job.result()` | Completed result; raises `NotReady` while still running |
| `await job` | Waits for completion and returns the result |
| `await job.cancel()` | Returns `False` if already terminal, otherwise requests cancellation and returns `True` |

Async jobs return the awaitable's value and propagate its exception. Successful
shell jobs return `{"returncode": 0}`; a failed shell job raises `RPCError` when
its result is retrieved. Cancelled jobs raise `asyncio.CancelledError`.
Waiting with `await job` suspends the current cell until completion while
other runnable cells continue.

`read()` supports stdout/stderr selection and waits up to 30 seconds for output
or completion. Its opaque cursor belongs to that job and stream; it is separate
from the character cursor used by `output(cursor=...)`. Check `truncated` and
`warnings` for output loss. `expect()` distinguishes a match, EOF, timeout, and
scan limit. A match advances its cursor just past the matched text; an unmatched
result keeps the starting cursor for retry. Cancelling a read or expect wait
does not cancel the job.

`await ws.tasks.attach(task_id_or_history_id)` returns an existing live handle
or reconnects to retained shell/package output. Historical Python and cell
handles expose saved output and status but cannot restore Python values;
`result()` raises `ResultUnavailable`. Use generation-qualified `history_id`
for a specific historical Python task. Legacy records may only contain a
limited output prefix.

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

### Coordinating shared work

```python
async with ws.locks.acquire("module:review", "file:report", timeout=10):
    await ws.fs.write("report.txt", "Done.\n", overwrite=True)
ws.locks.list()
```

Locks belong to the current asyncio task. Names are normalized and acquired
together; reacquiring while the same task owns locks raises an error. Leaving
the context, completing or cancelling the task, or resetting the kernel releases
them. Disconnecting a client does not end its running tasks or release their locks.
`list()` shows owners and waiters. These are cooperative locks: filesystem writes
and other clients must explicitly use the same names to participate.

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

`send(to, text, data=None, reply_to=None)` uses the current logical client as the sender and accepts
non-empty text up to 16 KiB in UTF-8. It returns `id`, `from`, `to`, `text`, and
`created_at`; text whose JSON escaping would exceed a 32 KiB page is rejected.
`data` accepts bounded JSON-serializable values. `reply(message_id, text, data=None)`
sends to the original sender and records `reply_to`; only the original recipient
can reply, including after acknowledging the original message.
`read(limit=20, after=None, wait_ms=0, sender=None, reply_to=None)` returns
unacknowledged messages in ID order with `messages`, `next_cursor`, and
`has_more`. The limit is 1–100, the serialized page is at most 32 KiB, and
`wait_ms` is limited to 30 seconds. Use the returned `next_cursor` as `after`
to continue paging. A non-zero `wait_ms` waits only when no messages match both
the cursor and the optional sender/reply filters. Unrelated messages do not end
a filtered wait.

`ack(ids)` explicitly acknowledges messages belonging to the current client.
Acknowledgement is idempotent, and reading or previewing a message never marks
it as read. It returns the number newly acknowledged; unknown or foreign IDs
reject the entire batch. These methods require an initialized client because messages are
scoped to its logical ID.

The MCP responses from `init`, `execute`, and `poll` include up to five short
inbox previews (within a 4 KiB budget), plus the total unacknowledged count.
The `inbox` field contains `unacked`, `messages`, and `has_more`; each preview has
`id`, `from`, `text`, `reply_to`, and `truncated`. Structured `data` is omitted
from previews; use `read()` for full content.
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

Validate and edit a skill through the revision-aware helpers:

```python
skill = await ws.skills.write(
    "review",
    "---\nname: review\ndescription: Review workspace changes.\n---\n\n"
    "Read the diff, check affected callers, and report concrete issues.\n",
)
skill["revision"]
await ws.skills.validate("review")
```

Skill text is interpreted by the agent; reading it does not execute its
instructions or scripts. `validate()` accepts legacy Markdown without front
matter with a warning, rejects malformed YAML and invalid metadata types, and
reports missing or escaping local Markdown links. Existing skills require
`expected_hash=page["revision"]` when updated. Pass `dry_run=True` to preview a
bounded diff without writing.

For reusable Python, use `ws.modules` for files under `.mypr/lib/ws_lib/`:

```python
await ws.modules.write("helpers", "def answer(value):\n    return value * 2\n")
await ws.modules.check("helpers", test_code="assert answer(2) == 4")
helpers = ws.modules.load("helpers")
helpers.answer(2)
await ws.modules.write(
    "helpers", "def answer(value):\n    return value * 3\n",
    expected_hash=(await ws.modules.read("helpers"))["revision"],
)
helpers = ws.modules.reload("helpers")
```

`modules.list()` is synchronous; `read()`, `check()`, and `write()` are async.
Module names are public dotted Python names and cannot escape `ws_lib`. `check()`
compiles the candidate and runs optional test code in the workspace Python
environment with its own timeout. `write()` validates syntax, uses an atomic
CAS write, and never activates the module. `load()` and `reload()` execute a
fresh module and bind it only after successful execution; failed reloads leave
the old module binding intact. References already held elsewhere keep pointing
to the previous module after a successful reload. Imports can have external side
effects; a failed reload does not undo those side effects.

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

### Workstation resources

`ws.system` collects a bounded snapshot of the workstation visible to the
kernel. Collection runs in short-lived guarded helper processes, so vendor
utilities or a slow filesystem do not block other Python cells. The result of
each method is a JSON-compatible envelope with `collected_at`,
`duration_seconds`, `scope`, `sources`, `warnings`, and `truncated` fields.

```python
info = await ws.system.info()
usage = await ws.system.usage(interval=0.5)
processes = await ws.system.processes(
    sort="rss", limit=10, interval=0.5, cmdline=True
)
gpus = await ws.system.gpus(processes=True)
workspace_disk = await ws.system.disks(path=".")
```

The `info` envelope contains `os`, `python`, `cpu`, `memory`, `storage`, and
`limits`. `usage` contains interval-based `cpu`, `memory`, `swap`, per-interface
`network`, per-device `disk_io`, `disks`, and `gpus` data. `processes` returns a
bounded `processes` list with PID, creation time, status, CPU percentage, RSS,
and I/O counters; `sort` accepts `cpu`, `rss`, `read`, or `write`, and `limit`
is capped at 200. The `pids` and `user` filters narrow that list, while
`cmdline` is false by default. `gpus` reports available vendor metrics and
optional GPU processes. `disks(path=...)` reports filesystem capacity for the
workspace-relative path (or an absolute path).

All sizes are bytes and all rates are bytes per second. Whole-machine CPU usage
is reported from 0 to 100%; process CPU usage uses one logical CPU as 100%, so
a multi-threaded process can exceed 100%. Unsupported or inaccessible values
are `None`, with the reason recorded in `warnings` or `sources`, rather than
being reported as zero. A missing driver, inaccessible process, or unavailable
vendor utility does not discard other sections. NVIDIA metrics use
`nvidia-smi`; AMD uses `amd-smi`; Intel uses `xpu-smi` and, where available,
`intel_gpu_top`, with Linux DRM/sysfs fallback. The API does not install
operating-system tools automatically. Vendor utilization uses the driver's
measurement window; DRM client counters use the requested interval. DRM engine
activity and client memory are separate from whole-device utilization and VRAM.
Shared DRM descriptors are counted once, with their owning PIDs listed. Vendor
process PIDs can refer to a different PID namespace from the Python kernel.

The snapshot uses the kernel's PID namespace and filesystem view. `limits` separately reports CPU affinity and cgroup v2 quota or memory
headroom when discoverable; these are execution limits and available headroom,
not a reservation of those resources. Responses are limited to 32 KiB; `truncated` and `omitted` identify details
removed to fit that budget. A timeout
stops the guarded helper, and cleanup can make the completed call slightly
longer than the requested deadline.

The default measurement interval is 0.5 seconds and must be between 0.1 and
10 seconds. The default timeout is 5 seconds and must exceed the interval.
Repeated snapshots are best scheduled as ordinary async cells or background
tasks; the API does not create a resident monitor or retain historical samples.

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

`reset` only resets shared kernel memory. It does not update the installed
package or replace the manager process. Globals, imports, functions, `ws.local`,
and in-memory handles are lost; files, skills, modules, the package environment,
messages, history, and saved task output remain.

Use `await ws.restart()` when a running workspace must apply the installation
that made the request. This replaces the manager and kernel through a detached
workspace coordinator, then reconnects planned MCP clients. It is explicit:
package resolution and downloads belong to the command that launched the client
(for example, `uvx mypr-mcp@latest`); restart does not resolve a new version from
the network by itself. The default refuses while another cell, shell, scan,
package job, or Python task is active. Use `await ws.restart(force=True)` to
cancel active work first. The restart call's execution result is recorded and
can be retrieved with `poll`; its Python code is never replayed.

The equivalent CLI command is:

```sh
cd /absolute/path/to/workspace
uvx mypr-mcp@latest restart
# Use --force only when cancelling active work is intended.
uvx mypr-mcp@latest restart --force
```

The new manager starts only after the old one exits and passes its health check.
The coordinator records its ID, phase, old and new generation, target
installation, and failure details under `.mypr/`. A failed start is reported
without automatic rollback or an unbounded restart loop; inspect
`.mypr/manager.log`, `uvx` diagnostics, and the restart record before retrying.
External browser processes and pre-existing tabs remain owned by their launcher.
Managed resources close during a normal replacement. In-memory Python state is
always lost, while files, skills, modules, the package environment, messages,
history, saved output, and completed scan records persist.

The CLI also provides operational controls. Run them from the workspace:

```sh
cd /absolute/path/to/workspace
uvx mypr-mcp status
uvx mypr-mcp logs
uvx mypr-mcp logs --limit 50 --follow
uvx mypr-mcp reset
uvx mypr-mcp restart
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
Status includes the manager `pid`, `protocol_version`, manager and client
installation versions, capabilities, `update_pending`, and a `health_error`
when an essential runtime worker fails. Compatible package versions reuse the
existing manager and kernel, so installing a newer `uvx` package alone does not
clear Python memory. A protocol mismatch or an unknown legacy manager is
reported through initialization with an actionable restart instruction rather
than being hidden as a generic MCP handshake failure. The legacy 0.9.0 runtime
has no `ws.restart()`; run the CLI `restart` once from the new installation to
perform the first explicit replacement.

Automatic reconnection requires the updated MCP frontend; older frontend processes
must be reopened once after installation.

Planned restart keeps the MCP stdio connection alive. After replacement, the
connection receives a new connection ID and generation and rebinds its existing
logical client ID. Other clients reconnect the same way. No submitted cell is
replayed; use its recorded execution ID with `poll`. An ordinary crash,
unplanned stop, or unreachable runtime does not trigger this reconnect path:
unfinished executions become `lost`, and Python memory must be recreated.

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
and `ws.tasks.get()`; use `await ws.tasks.attach(id)` or `ws.history` for saved records. A handle saved in your
own variable or `ws.local` remains usable. These limits release internal cache
references, not arbitrary objects retained by Python code.

Manager record and shell-output caches each retain at most `completed_records`
completed entries and share the serialized-byte budget equally. Counts must be
positive and `cache_bytes` must be at least 1024. Active work is never evicted.
If shell metadata cannot be saved, the most recent affected completed job is
retained outside these cache limits so its result and warning remain inspectable.
This fallback lasts only while the manager is alive and retains at most one job's
output, subject to the normal per-job output limit.
Execution output, Python-task journals, and shell/package journals remain on disk under `.mypr/runs/`
and `.mypr/jobs/`, so historical polling and delayed job monitors survive cache
eviction. Request deduplication uses SQLite and survives eviction and restart,
including empty request IDs. Query snapshots remain under `.mypr/searches/` and
`.mypr/git/`. Cache retention does not delete these snapshots, journals, saved
files, or messages.
Execution journals have rebuildable `.idx` byte-offset indexes. Historical
polling seeks directly to the requested event cursor instead of loading the
entire output file for each page. Older journals are indexed once on first
read, off the manager's event loop; missing or stale indexes are rebuilt.

Runtime files are created under `.mypr/`. The generated `.mypr/.gitignore`
excludes the virtual environment, run records, artifacts, locks, logs, and
the SQLite history, and other runtime metadata; reusable modules, skills, configuration, and
`requirements.txt` remain available for version control as desired.

When upgrading, launching a newer compatible package is enough to reconnect to
an existing manager; running managers are not silently replaced. Use the
explicit `restart` command or `ws.restart()` when the new code must be loaded.
Existing execution files are imported into history, and logical client IDs remain
attached to those records.

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
