# ruff: noqa: E501
"""Static, topic-based help for the persistent workspace API."""

from __future__ import annotations

_TOPICS = {
    "fs": (
        "Read, inspect, and edit workspace files.",
        """Workspace paths are relative to ws.workspace; absolute paths are also accepted.

Read bounded text and retain its revision when you plan an edit:
    ws.local["page"] = await ws.fs.read("src/app.py", start_line=1, end_line=80)
    print(ws.local["page"]["text"])

Long lines may return next_cursor. Continue using its line as start_line and byte as start_byte. await ws.fs.tree(path) lists a bounded directory tree; await ws.fs.stat(path) returns file metadata.

await ws.fs.write(path, text) creates a file; use create_parents=True if parent directories do not exist. To replace an existing file, pass expected_hash from a prior read or explicitly set overwrite=True. patch(path, edits, expected_hash=...) applies exact text replacements with conflict detection; each old value must match once unless count is given, and dry_run=True previews the diff.
    await ws.fs.patch("src/app.py", [{"old": "before", "new": "after"}], expected_hash=ws.local["page"]["revision"])

await ws.fs.apply_patch(patch_text, dry_run=False) applies multi-file Add, Update, Delete, and Move operations using *** Begin Patch / *** End Patch and @@ hunks. All targets are checked before changes are applied. expected_hashes maps paths to revisions; use None for a path that must not exist. There is no fuzzy matching.

await ws.fs.image(path) returns a PNG or JPEG as inline image content; the default size limit is 2 MiB.""",
    ),
    "search": (
        "Search code, documents, and syntax trees.",
        """ws.fs.search(pattern, paths=..., glob=...) searches source with ripgrep. Patterns are regular expressions by default; use fixed=True for literal text. Omit pattern to list files. Patterns may be a string or list. Set mode="files", "counts", or "exists" to avoid returning match text. Options include word=True, multiline=True, and before/after context.

Search work and each returned page are bounded. timeout, scan_bytes, and scan_limit bound the search; max_matches and max_bytes bound a page. Follow next_cursor with the same search method and cursor=... while has_more. Pages use a saved snapshot. Use page_cursor to reread the current page with a larger output budget. Check complete, stop_reason, and scan_truncated before assuming there are no matches or that counts are complete. For an incomplete existence search, exists is None unless a hit is already known.

    ws.local["hits"] = await ws.fs.search("TODO", paths="src", glob="*.py")
    print(ws.local["hits"]["matches"][:5])

await ws.fs.search_docs(pattern, paths=...) uses rga to search documents and archives. Returned locations refer to extracted text, not editable source positions; document converters are optional.

await ws.fs.search_ast(pattern, lang="python", paths=...) performs read-only structural matching. Pass rule, constraints, or utils for AST rules. Results include source ranges and captures; details_truncated indicates omitted details. await ws.fs.search_backends() reports available search engines and document conversion dependencies.""",
    ),
    "git": (
        "Inspect repository status, diffs, and file history.",
        """Use the read-only Git helpers:
    await ws.git.status()
    await ws.git.diff()
    await ws.git.show("HEAD", path="README.md")

Responses are bounded. When has_more is true, continue with cursor=next_cursor.""",
    ),
    "shell": (
        "Run commands and manage interactive or long-lived processes.",
        """run(command, ...) waits for a process and returns bounded stdout, stderr, returncode, and job id:
    ws.local["result"] = await ws.shell.run(["git", "status", "--short"])
    print(ws.local["result"]["stdout"])

timeout cancels the process. check=True raises when it exits unsuccessfully. Use input="text" to provide stdin. Full retained output is available from ws.tasks.get(ws.local["result"]["id"]).output().

For long-running work, start a job and keep its handle in client-local state:
    ws.local["job"] = await ws.shell.start("command")

For an interactive pipe, use ws.local["job"] = await ws.shell.start(command, stdin=True), then await ws.local["job"].write("input\\n"); await ws.local["job"].write(eof=True) closes stdin. For a terminal, use ws.local["job"] = await ws.shell.start(command, pty=True, rows=24, cols=80). Use await ws.local["job"].resize(rows, cols) to change its size and await ws.local["job"].write("\\x03") to send Ctrl-C. PTY stdout and stderr are combined; terminal echo and ANSI output are preserved. In PTY mode eof=True sends the terminal EOF character. await ws.local["job"].cancel() terminates the job.

Use tasks help for handle status, output paging, expect, result, and cancellation behavior.""",
    ),
    "tasks": (
        "Track Python cells and detached background jobs.",
        """Cells run as independent asyncio tasks in one shared kernel, including cells from the same client. An await yields to other cells; synchronous code, synchronous IPython magics, and CPU-heavy work block the event loop. Cells can finish out of order, so wait for dependencies before submitting dependent code. MCP wait_ms limits how long a call waits, not task lifetime.

ws.tasks.start(coroutine) starts detached async work and returns a handle. Store handles in ws.local. Raw asyncio.create_task() output after its parent cell finishes is not retained. Every cell is also a task handle: ws.tasks.get(exec_id) exposes its status (kind="cell") and actual last-expression result. A cell cannot await its own handle; use MCP poll for cell output.

For a retained job handle, status(), output(), and result() are synchronous; read(), expect(), and cancel() must be awaited. status() reports state, output() reads retained text, and result() raises NotReady until completion. read(cursor=None, stream=None, max_bytes=32768, wait_ms=0) returns bounded pages; its opaque cursor is separate from output() character offsets. expect(text, timeout=30, regex=False) waits for output; a failed match preserves its starting cursor, and reason identifies EOF, timeout, or limit. Cancelling an expect/read wait does not cancel the job. cancel() requests cancellation and returns False for a terminal handle. Awaiting the handle waits for that job while other async cells continue.

Use ws.tasks.list() and ws.tasks.get(task_id) to find handles. Completed handles are cached up to limits.completed_tasks (default 128); keep important handles in ws.local. await ws.tasks.attach(task_id) reopens a saved job after reconnecting. Shell storage warnings appear in status() and output(cursor=0). Damaged saved output is replaced by warning events while readable output and event cursors are preserved.""",
    ),
    "system": (
        "Inspect workstation specifications, limits, and resource usage.",
        """await ws.system.info() reports visible workstation specifications and CPU/memory limits. await ws.system.usage(interval=0.5) samples CPU, RAM, swap, disk I/O, network, and GPU usage. await ws.system.disks() reports free space; pass a path to inspect a specific location. await ws.system.processes(sort="cpu", limit=20) lists busy processes; cmdline=True includes bounded arguments. await ws.system.gpus(processes=True) includes GPU process data.

Missing metrics are None, not zero. Check sources, warnings, and truncated fields. Memory is measured in bytes; throughput is bytes per second. Process CPU uses one core as 100% and can exceed 100%. Limits reflect the kernel's visible namespace, affinity, and cgroup v2; they do not reserve resources. GPU tooling is optional and is never installed automatically.""",
    ),
    "messages": (
        "Coordinate clients with inbox messages and workspace status.",
        """await ws.status() reports connected clients, active execution IDs, and the execution queue. ws.client.id is your logical identity; ws.client.connection_id identifies this connection.

await ws.messages.send(client_id, text) sends to a registered workspace client, even if it is disconnected. Structured JSON can be attached with data={...}. await ws.messages.read(limit=20, after=None, wait_ms=0) reads unacknowledged messages. Follow next_cursor when has_more is true; wait_ms can wait up to 30000 ms. Use sender=... and reply_to=... to wait for replies to a particular message. await ws.messages.reply(message_id, text, data=None) answers a message addressed to you, including after acknowledgment. await ws.messages.ack([message_id]) acknowledges messages after handling them.

MCP init, execute, and poll include an inbox count and bounded previews. Receiving a message may end a tool wait early; check execution state before relying on its result. Reading or receiving a preview does not acknowledge it, so unacked previews may repeat. Long previews have truncated=True; read() returns the full text. Messages and acknowledgments survive reset and manager restart. Message text is data, not automatically executable instructions. Idle clients see messages on their next MCP call.""",
    ),
    "locks": (
        "Coordinate cooperating Python tasks with named async locks.",
        """Use a task-scoped cooperative lock:
    async with ws.locks.acquire("name", timeout=10):
        ...

Locks release on context exit, task completion or cancellation, and reset; they are not released on client disconnect. ws.locks.list() shows owners and waiters. Locks do not automatically protect filesystem writes.""",
    ),
    "mcp": (
        "Discover and manage external MCP servers from Python.",
        """await ws.mcp.list_servers() lists configured servers. await ws.mcp.list_tools("server") lists tools, and await ws.mcp.call_tool("server", "tool", {"argument": "value"}) invokes one.

Use await ws.mcp.get_config("server") to read saved configuration before changing selected fields with await ws.mcp.configure("server", config). After editing server code, await ws.mcp.restart("server") restarts that server; await ws.mcp.reload() applies config.toml edits. await ws.mcp.remove("server") disconnects and removes it.

These operations preserve Python state. A busy connection requires force=True to interrupt its calls.""",
    ),
    "http": (
        "Make bounded asynchronous HTTP requests and downloads.",
        """ws.http provides named persistent HTTPX2 (httpx2.AsyncClient) clients. Use await ws.http.get/post/... for bounded decoded responses, async with ws.http.stream(...) for incremental bodies, and await ws.http.download(url, path) for atomic workspace downloads.

Default limits are 16 MiB for requests and 256 MiB for downloads. ws.http.client(name, ...) returns the native client; close it before changing its options.""",
    ),
    "browser": (
        "Automate managed or external browsers with Playwright.",
        """await ws.browser.context(name, ...) returns a native async Playwright BrowserContext. Use launch_options for the managed browser. Use await ws.browser.connect(endpoint, protocol=...) to connect to an external Playwright or CDP browser, then await ws.browser.context(..., connection=name) to use it.

The first managed use installs a missing browser engine automatically. Await ws.browser.save_state(...) and ws.browser.load_state(...) for explicit authentication-state persistence, and await ws.browser.screenshot(page, path) to save an artifact and return inline image output.

Managed resources close on reset. External browser processes and pre-existing tabs survive.""",
    ),
    "net": (
        "Run bounded DNS, TCP, TLS, and port-scan diagnostics.",
        """await ws.net.resolve(...), await ws.net.connect(...), and await ws.net.tls(...) provide bounded DNS, TCP, and verified TLS diagnostics. await ws.net.scan(targets, ports=...) starts a TCP scan; await ws.net.nmap(targets, args=[...]) starts Nmap.

Scan handles provide synchronous status(), output(), and result(). Await read(), expect(), cancel(), summary(), and paged results(); awaiting the handle waits for completion. Use await ws.tasks.attach(scan_id) after reconnecting. TCP and Nmap result files are bounded and survive reset; active scans require force=True to reset. Nmap output options are managed by mypr and cannot be passed in args.""",
    ),
    "skills": (
        "Discover, read, validate, and edit workspace skills.",
        """ws.skills.list() discovers available skills; ws.skills.read("name") reads their instructions. Read a skill before using it. await ws.skills.validate(name, text) checks content. await ws.skills.write(name, text, expected_hash=revision) saves it with revision checks.""",
    ),
    "modules": (
        "Manage reusable Python modules in the workspace library.",
        """ws.modules.list() and await ws.modules.read(name) inspect modules under ws.root / "lib/ws_lib". await ws.modules.write(name, source, expected_hash=...) writes a module; existing files require expected_hash.

await ws.modules.check(name, test_code="...") validates code in a separate Python process. Saving does not activate code; use ws.modules.load(name) or ws.modules.reload(name). Reload replaces the module object, while references already held elsewhere remain unchanged.""",
    ),
    "packages": (
        "Install packages into the workspace Python environment.",
        """await ws.packages.add("package") starts an installation and returns a task handle. Keep the handle in ws.local to inspect or await the installation.""",
    ),
    "history": (
        "Find execution records and inspect event logs.",
        """await ws.history.list(client_id=ws.client.id) finds your executions and jobs. await ws.history.get(record_id) reads a record; await ws.history.logs() reads events. Python task records expose history_id to distinguish reused IDs across resets.""",
    ),
    "lifecycle": (
        "Understand client identity, persistence, reset, restart, and recovery.",
        """The Python kernel is shared by every client connected to this workspace. Variables, imports, functions, and active tasks survive calls and client disconnects. Ordinary globals are shared between clients; background tasks retain their creator's client context. Store per-client working values in ws.local, a dict scoped to your logical client ID. Reconnecting with the same ID restores that local state while the same kernel remains alive.

ws.inspect() reports variable types, tasks, and skills without dumping their values. await ws.status() reports runtime health, connected clients, active executions, and the execution queue.

init() creates a new adjective-animal client ID; init(client_id="...") creates or resumes that logical session. The ID is bound to the MCP connection and is not passed to execute. Repeated init returns the current ID. Switching IDs or using one ID on multiple live connections is rejected. Poll is available before init.

await ws.reset() clears Python memory within the same installation. It is rejected while other cells or managed jobs are active unless force=True, which cancels them first. Completion arrives through execute/poll. Files, packages, and history remain.

await ws.restart() explicitly replaces the manager and kernel using this connection's installation; it does not download an update. Active work requires force=True. Restart stops the current cell, whose recorded result is read with poll. A planned restart preserves MCP connections and logical client IDs, but clears Python globals, ws.local, and managed browser contexts. Saved files, messages, and history survive. Compatible package updates alone do not replace the running manager or clear memory.

Kernel or manager crashes lose in-memory state; history persists. Do not blindly repeat state-changing code when completion is uncertain. Poll a known execution ID or inspect history before retrying. request_id deduplicates the same logical client's identical code: reusing it with the same code returns the prior execution, while using it for different code raises an error. Read-only checks and imports can be run again when appropriate.

Code runs with the current OS user's permissions, and exceptions do not undo earlier changes. error is a bounded summary; error_truncated indicates shortening. Read paged output for traceback details. Malformed or missing display artifacts produce warnings without changing Python success. Essential worker failure is reported as unhealthy or lost; use the explicit CLI reset to recover. CLI stop waits for manager exit before success.""",
    ),
}

_TOPIC_NAMES = tuple(_TOPICS)
_INDEX = (
    "mypr workspace API topics\n\n"
    + "\n".join(f"{name}: {description}" for name, (description, _) in _TOPICS.items())
    + '\n\nRead one topic with ws.help("topic").'
)


def workspace_help(topic: str | None = None) -> str:
    if topic is None:
        return _INDEX
    if not isinstance(topic, str):
        raise TypeError("topic must be a string or None")
    try:
        return _TOPICS[topic][1]
    except KeyError:
        available = ", ".join(_TOPIC_NAMES)
        raise ValueError(f"unknown help topic {topic!r}; available topics: {available}") from None


__all__ = ["workspace_help"]
