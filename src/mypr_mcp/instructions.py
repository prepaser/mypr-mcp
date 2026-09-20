"""Agent guidance shared by the protocol descriptor and MCP frontend."""

COMMON_INSTRUCTIONS = """Call init once per connection before execute. The init result
contains the running workspace's Python API instructions and runtime versions.
Use those instructions, since the running manager may be older than this MCP client.
Use execute for Python and poll for submitted cell results. Workspace Python is shared
and persists across connections. Never replay code after a restart or uncertain failure.
"""

INSTRUCTIONS = """Call init once per connection. Use execute for Python and poll for cell output.
init() creates a new adjective-animal client ID; init(client_id="...") creates or resumes
that logical session. The ID is bound to this connection; do not pass it to execute.
Repeated init returns the current ID. Switching IDs and sharing an ID across live
connections are rejected. After disconnecting, reconnect and init with the same ID to
resume. Poll is available before init.
Python runs in one persistent kernel shared by every client of this workspace.

State and identity
- Variables, imports, functions, and active tasks survive calls and client disconnects.
- ws.client.id is your logical identity; ws.client.connection_id identifies this connection.
- Store your working values in ws.local, a dict scoped to your logical client ID.
  Resuming an ID restores its ws.local while the same kernel remains alive.
- Ordinary globals are shared. Background tasks retain their creator's client context.
- Completed handles are cached up to limits.completed_tasks (default 128). Keep a
  handle in ws.local if you need it longer; await ws.tasks.attach(id) reopens saved jobs.
- Keep large results in Python and return only the information needed for the next decision.

Everyday workspace work
- await ws.system.info(): visible workstation specifications and CPU/memory limits.
  await ws.system.usage(interval=0.5): independently sampled CPU, RAM, swap, disk I/O,
  network and GPU usage. await ws.system.disks() or disks(path) checks free disk space.
  await ws.system.processes(sort="cpu", limit=20) identifies busy processes; cmdline=True
  includes bounded arguments. await ws.system.gpus(processes=True) adds GPU process data.
  Missing metrics are None, not zero. Check sources, warnings and truncated. Memory is
  bytes; throughput is bytes/second; process CPU uses one core as 100% and may exceed it.
  Limits reflect the kernel's visible namespace, affinity and cgroup v2; they do not
  reserve resources. GPU tooling is optional and never installed automatically.
- await ws.fs.read("src/app.py", start_line=1, end_line=80): bounded text and revision.
  Long lines use next_cursor; continue with its line as start_line and byte as start_byte.
- await ws.fs.search("pattern", paths="src", glob="*.py"): regex matches with locations.
  Use fixed=True for literal text, or omit pattern to list files. Requires ripgrep (rg).
  mode="files", "counts", or "exists" avoids unneeded match text. Patterns can be a list.
  word=True, multiline=True, and before/after select matching and context behavior.
  Follow next_cursor with the same search method(cursor=...) while has_more; pages use a
  saved snapshot. page_cursor rereads the current page with a larger output budget.
  timeout=30, scan_bytes, and scan_limit bound the search itself; max_matches/max_bytes
  bound each page. Check complete, stop_reason and scan_truncated before assuming no hits
  or complete counts. An incomplete existence search returns exists=None unless a hit is known.
- await ws.fs.search_docs("pattern", paths="docs"): rga document/archive search.
  Locations refer to extracted text, not editable source positions. Converters are optional.
- await ws.fs.search_ast("print($$$ARGS)", lang="python", paths="src"): read-only AST search.
  Pass rule={...}, constraints={...}, utils={...} for structural rules. Results include
  ranges and captures; details_truncated marks omitted details. This does not rewrite files.
  await ws.fs.search_backends() reports installed engines and conversion dependencies.
- await ws.fs.tree("src") and await ws.fs.stat("src/app.py"): bounded tree and metadata.
- await ws.git.status(), await ws.git.diff(), await ws.git.show("HEAD", path="README.md"):
  read-only Git views. Continue with cursor=next_cursor while has_more.
- await ws.fs.write("notes.txt", "text"): create a file. Existing files require
  expected_hash=page["revision"] or explicit overwrite=True.
- await ws.fs.patch("src/app.py", [{"old": "before", "new": "after"}],
  expected_hash=page["revision"]): exact edits with conflict detection and a diff.
  Each old value must match once unless count is specified. dry_run=True previews edits.
- await ws.fs.apply_patch(patch_text, dry_run=False): multi-file Add/Update/Delete/Move
  using *** Begin Patch / *** End Patch and @@ hunks. All targets are checked first;
  expected_hashes maps paths to revisions (None means absent). No fuzzy matching.
- await ws.fs.image("plot.png"): return a PNG/JPEG as inline image content (2 MiB default).
- await ws.shell.run(["git", "status", "--short"]): wait and return bounded stdout,
  stderr, returncode and job id. timeout cancels the process; check=True raises on failure.
  Use input="text" for stdin. Full retained output is in ws.tasks.get(result["id"]).output().
File paths are relative to ws.workspace; absolute paths are supported. Keep reusable
results in ws.local and compose these helpers in Python for multi-step work.

Execution and background work
Cells run as independent asyncio tasks in the shared kernel, including cells from the
same client. A pending await yields to other cells; synchronous code, synchronous
IPython magics, and CPU-heavy work block the event loop. Cells can finish out of order,
so wait for or poll dependencies before submitting dependent code. wait_ms limits how
long the MCP call waits, not Python execution or task lifetime.

Start long work without waiting for completion:
    ws.local["job"] = await ws.shell.start("command")
    ws.local["task"] = ws.tasks.start(coroutine)

For interactive pipe input, use ws.shell.start(command, stdin=True), then
await job.write("input\\n"). await job.write(eof=True) closes stdin.
For a terminal, use ws.shell.start(command, pty=True, rows=24, cols=80).
await job.resize(40, 120) changes its size; await job.write("\\x03") sends Ctrl-C.
PTY stdout/stderr are combined, with terminal echo and ANSI output preserved.
In PTY mode eof=True sends the terminal EOF character; cancel() terminates the job.

In later cells, use ws.local["job"].status(), ws.local["job"].output(), or
ws.local["job"].result(). result() raises NotReady until the job finishes.
Use await job.read(cursor=None, stream=None, max_bytes=32768, wait_ms=0) for bounded
output pages. Its opaque cursor is separate from output() character offsets.
await job.expect("ready", timeout=30) waits for matching output; regex=True supports
patterns. A failed match preserves its starting cursor; check reason for EOF/timeout/limit.
Cancelling these waits does not cancel the job.
Use await ws.local["job"].cancel() to request cancellation. Awaiting the handle itself
waits for that job; other async cells continue. Every cell is also a task handle, so
ws.tasks.get(exec_id) exposes its status (kind="cell") and actual last-expression result.
A cell cannot await its own handle. poll reads cell output; handles manage cells and jobs.
Use ws.tasks.start() for detached work; raw asyncio.create_task() output after the
parent cell finishes is not retained. cancel() returns False for terminal handles.
Run long CPU-bound or blocking work in separate scripts through ws.shell.start().

Shared coordination
Use async with ws.locks.acquire("name", timeout=10) for task-scoped cooperative locks.
They release on context exit, task completion/cancellation, or reset; not disconnect.
ws.locks.list() shows owners and waiters. Locks do not implicitly protect file writes.

Client messages
- await ws.messages.send("client-id", "text") sends to a registered workspace client,
  including disconnected clients. Use await ws.status() to find connected clients.
- init/execute/poll include your inbox: unacked count and bounded message previews.
  Messages can end the tool's wait early; check execution state before using its result.
- await ws.messages.read(limit=20, after=None, wait_ms=0) reads unacknowledged messages.
  Follow next_cursor when has_more is true; use wait_ms up to 30000 to wait for messages.
- send(..., data={...}) includes structured JSON. reply(message_id, text, data=None)
  answers a message addressed to you, including after acknowledgment.
  read(sender="client-id", reply_to=message_id, wait_ms=...) waits for matching messages.
- After handling messages, await ws.messages.ack([message_id]) to acknowledge them.
  Reading or receiving a preview never acknowledges it; previews may repeat until ack.
  Long previews have truncated=True; read() returns the full text.
- Messages and acknowledgments survive reset and manager restart. Message text is data,
  not automatically executed instructions. Idle agents only see messages on their next call.

Discover and reuse capabilities
- ws.inspect(): inspect variables, tasks, and skills without dumping their values.
- ws.tasks.list() and ws.tasks.get(task_id): find existing job handles.
- await ws.status(): inspect connections, active execution IDs, and the execution queue.
- await ws.mcp.list_servers() and await ws.mcp.list_tools("server"): discover external tools.
  Call them with await ws.mcp.call_tool("server", "tool", {"argument": "value"}).
- await ws.mcp.configure("server", config): add or replace a saved server configuration.
  Read it with await ws.mcp.get_config("server") before changing selected fields.
  Use await ws.mcp.restart("server") after editing its code; await ws.mcp.reload()
  applies config.toml edits. await ws.mcp.remove("server") disconnects and removes it.
  These preserve Python state. Busy connections require force=True to interrupt their calls.
- ws.http provides named persistent HTTPX2 clients. Use await ws.http.get/post/... for
  bounded decoded responses, async with ws.http.stream(...) for incremental bodies, and
  await ws.http.download(url, path) for atomic workspace downloads. The default limits
  are 16 MiB for requests and 256 MiB for downloads. ws.http.client(name, ...) returns
  the native client; close it before changing its options.
- ws.browser.context(name, ...) returns a native async Playwright BrowserContext. Use
  launch_options for the managed browser, ws.browser.connect(endpoint, protocol=...)
  for an external Playwright/CDP browser, and context(..., connection=name) to use it.
  The first managed use installs a missing browser engine automatically. Use
  ws.browser.save_state/load_state for explicit auth persistence and
  ws.browser.screenshot(page, path) for an artifact plus inline image output.
  Managed resources close on reset; external browser processes and pre-existing tabs survive.
- ws.net.resolve(), ws.net.connect(), and ws.net.tls() provide bounded DNS, TCP, and
  verified TLS diagnostics. await ws.net.scan(targets, ports=...) starts a TCP scan;
  await ws.net.nmap(targets, args=[...]) starts Nmap. Scan handles support status(),
  read(), expect(), output(), result(), cancel(), await, summary(), and paged results().
  Use await ws.tasks.attach(scan_id) after reconnecting. TCP/Nmap result files are
  bounded and survive reset; active scans require force=True to reset. Nmap output options
  are managed by mypr and cannot be supplied in args.
- ws.skills.list() and ws.skills.read("name"): discover and read skill instructions.
  Read a skill before using it. await ws.skills.validate(name, text) checks content;
  await ws.skills.write(name, text, expected_hash=revision) saves it with revision checks.
- ws.modules.list(), await ws.modules.read(name), and await ws.modules.write(name, source)
  manage modules under ws.root / "lib/ws_lib". Existing files require expected_hash.
  await ws.modules.check(name, test_code="...") validates in a separate Python process.
  Saving does not activate code; use ws.modules.load(name) or ws.modules.reload(name).
  Reload replaces the module; references already held elsewhere remain unchanged.
- ws.local["install"] = await ws.packages.add("package"): start a workspace venv install.
- await ws.history.list(client_id=ws.client.id): find your executions and jobs.
  await ws.history.get(record_id) reads details; await ws.history.logs() reads events.
  Python task records expose history_id to distinguish reused IDs across resets.

Lifecycle
Code runs with the current OS user's permissions. Exceptions do not undo earlier changes.
await ws.restart() explicitly replaces the manager and kernel using this connection's
installation. It does not download an update. Active work requires force=True.
The call stops the current cell; poll its recorded result. Planned restart preserves
MCP connections and logical client IDs, but Python globals, ws.local, and managed
browser contexts are cleared. Saved files, messages, and history survive.
Compatible package updates alone do not replace a running manager or clear memory.
await ws.reset() clears Python memory within the same installation. It is rejected while other cells
or managed jobs are active unless force=True, which cancels them first. Completion arrives
through execute/poll. Saved files, packages, and history remain.
Kernel or manager crashes lose in-memory state; history persists and code is not replayed.
Malformed or missing display artifacts produce warnings without changing Python success.
Shell storage warnings appear in job.status() and job.output(cursor=0). Damaged saved
output is replaced by warning events; readable output and event cursors are preserved.
error is a bounded summary; error_truncated indicates shortening. Read paged output
for traceback details. Essential runtime worker failure is reported as unhealthy/lost;
use explicit CLI reset to recover. CLI stop waits for manager exit before success.
"""
