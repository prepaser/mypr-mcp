# ruff: noqa: E501
"""Static, topic-based help for the persistent workspace API."""

from __future__ import annotations

import inspect

_TOPICS = {
    "fs": (
        "Read, inspect, and edit workspace files.",
        """Workspace paths are relative to ws.workspace; absolute paths are also accepted.

Read bounded text and retain its revision when you plan an edit:
    ws.local["page"] = await ws.fs.read("src/app.py", start_line=1, end_line=80)
    print(ws.local["page"]["text"])

Long lines may return next_cursor. Continue using its line as start_line and byte as start_byte. await ws.fs.tree(path) lists a bounded directory tree; await ws.fs.stat(path) returns file metadata.

await ws.fs.write(path, text, history=True) creates a file; use create_parents=True if parent directories do not exist. To replace an existing file, pass expected_hash from a prior read or explicitly set overwrite=True. Set history=False only to opt out of the recovery entry. patch(path, edits, expected_hash=..., history=True) applies exact text replacements with conflict detection; each old value must match once unless count is given, and dry_run=True previews the diff.
    await ws.fs.patch("src/app.py", [{"old": "before", "new": "after"}], expected_hash=ws.local["page"]["revision"])

await ws.fs.apply_patch(patch_text, dry_run=False) applies multi-file Add, Update, Delete, and Move operations using *** Begin Patch / *** End Patch and @@ hunks. All targets are checked before changes are applied. expected_hashes maps paths to revisions; use None for a path that must not exist. There is no fuzzy matching.

All mutating filesystem helpers share revision checks, per-path locks, and the workspace change history. Use await ws.fs.read_bytes() and write_bytes() for binary data. delete() and move() require the source revision; copy() accepts an optional source revision. All three reject destination overwrite. history() lists changes made through ws.fs; read_revision() returns an empty `absent` result for a deleted file; restore() reverts one entry only when the current revision still matches its precondition.

For repeated text changes, await ws.fs.replace(...) creates a bounded preview and await ws.fs.apply_replace(plan_id) applies it after rechecking every source revision. Literal matching is the default; set fixed=False for Python regular-expression matching and ignore_case=True for case-insensitive matching.

await ws.fs.rewrite_ast(..., history=True) previews structural replacements without changing files. Keep the returned plan_id and inspect the diff before await ws.fs.apply_rewrite(plan_id). Applying a plan checks every original file hash first; incomplete scans cannot be applied. Successful applies record recovery history unless history=False was requested when creating the plan.

await ws.fs.image(path) returns a PNG or JPEG as inline image content; the default file size limit is 2 MiB. Pass resize=(width, height) to fit an image into a box, or crop=(left, top, right, bottom) in source pixel coordinates after EXIF orientation is applied. Transformations require Pillow in the workspace Python environment, run in a bounded subprocess, and never overwrite the original. await ws.fs.image_info(path) reports oriented source dimensions and revision. Inline images share a 2 MiB source-byte budget per MCP response; omitted images retain their artifact paths and a warning.""",
    ),
    "docs": (
        "Inspect, extract text from, and render PDF pages.",
        """PDF helpers prepare PyMuPDF in the workspace Python environment when it is missing. Image transformations also prepare "pillow". Automatic preparation is enabled by default; use await ws.dependencies.ensure("pymupdf", "pillow") for an explicit install, including when dependencies.auto_install is false. Installing packages only in the MCP client's environment does not install them in the workspace kernel.

await ws.docs.info(path, page=1) returns PDF metadata, revision, and optional page geometry. await ws.docs.read(path, start_page=1, max_pages=5, max_chars=20000) extracts bounded page text. Page numbers are one-based. Inspect truncation and continuation fields; text extraction does not perform OCR. Use await ws.docs.ocr(path, ...) explicitly for scanned PDF pages or PNG/JPEG images. OCR accepts resume_cursor for continuing a bounded cached page result; it cannot be combined with the ordinary result cursor. Cached words remain readable after a source edit, while the next unprocessed page rechecks the source revision before OCR. await ws.docs.extract(path, ...) reads DOCX paragraphs/tables, PPTX slides, or XLSX cells. await ws.docs.backends() checks optional packages and OCR language data. These operations run outside the kernel, preserve source files, and return bounded pages tied to the source revision.

await ws.docs.render_page(path, page=1, dpi=120) returns an inline page image. clip=(left, top, right, bottom) selects a region in PDF points. Source path, page, revision, geometry, and rendering details accompany the result. Workers bound input size, output size, pixel count, and execution time. Read ws.help("docs.render_page") for the current method signature.""",
    ),
    "search": (
        "Search code, documents, and syntax trees.",
        """ws.fs.search(pattern, paths=..., glob=...) searches source with ripgrep. Patterns are regular expressions by default; use fixed=True for literal text. Omit pattern to list files. Patterns may be a string or list. Set mode="files", "counts", or "exists" to avoid returning match text. Options include word=True, multiline=True, and before/after context.

Search work and each returned page are bounded. timeout, scan_bytes, and scan_limit bound the search; max_matches and max_bytes bound a page. Follow next_cursor with the same search method and cursor=... while has_more. Pages use a saved snapshot. Use page_cursor to reread the current page with a larger output budget. Check complete, stop_reason, and scan_truncated before assuming there are no matches or that counts are complete. For an incomplete existence search, exists is None unless a hit is already known.

    ws.local["hits"] = await ws.fs.search("TODO", paths="src", glob="*.py")
    print(ws.local["hits"]["matches"][:5])

await ws.fs.search_docs(pattern, paths=...) uses rga to search documents and archives. Missing registered search tools and selected converters are prepared automatically by default. Returned locations refer to extracted text, not editable source positions; document converters remain optional.

await ws.fs.search_ast(pattern, lang="python", paths=...) performs read-only structural matching. Missing ast-grep is prepared automatically by default. Pass rule, constraints, or utils for AST rules. Results include source ranges and captures; details_truncated indicates omitted details. await ws.fs.search_backends() reports available search engines and document conversion dependencies.""",
    ),
    "code": (
        "Use optional language servers for definitions, references, hover, and diagnostics.",
        """await ws.code.configure("clangd", ["clangd", "-j=2"], ["c", "cpp"]) starts an explicitly selected stdio language server. Language servers remain a manual dependency; mypr does not download them. Servers are shared within this workspace and close during reset.

await ws.code.definition("clangd", "src/main.c", line=10, character=5), references(...), and hover(...) query saved source. Public line and character values are one-based Unicode code-point positions; mypr converts the server's negotiated position encoding. Source changes are synchronized before queries. Inspect result truncation and coordinate metadata; diagnostics report the synchronized document version.

await ws.code.diagnostics("clangd", "src/main.c", wait_ms=1500) reads diagnostics for the synchronized document; pending or stale results must not be treated as a clean bill of health. ws.code.status() lists configured servers. await ws.code.close("clangd") stops one. await ws.code.document_symbols(server, path) returns the document outline. workspace_symbols(server, query) searches project symbols; calls(server, path, line=..., character=..., direction="incoming") returns one level of callers, or use direction="outgoing" for callees. Server capability checks distinguish unsupported features from empty results. Structure queries default to max_bytes=32768; inspect truncated before treating a result as complete. Call hierarchy uses one overall server timeout.

Code navigation settings can be persisted with configure(..., persist=True),
then reused after reset or restart. rename(...) and actions(...) create
revision-checked change previews; prepare_action(...) resolves one selected
action and apply_edit(...) applies only supported WorkspaceEdit file changes.
Actions that require command execution are reported but never run by mypr.
workspace_diagnostics(...) requests project-wide diagnostics when the server
advertises that capability. Use ws.help("code.configure") and the live method
signatures for current defaults and result fields.""",
    ),
    "git": (
        "Inspect repository status, diffs, and file history.",
        """Use the read-only Git helpers:
    await ws.git.status()
    await ws.git.diff()
    await ws.git.show("HEAD", path="README.md")

await ws.git.log(...) returns structured commit history and await ws.git.blame(path, ...) returns line attribution. Both default to HEAD and pin the resolved commit for subsequent pages. Use follow=True to follow a path across renames. await ws.git.commit_info(ref, include_files=True) returns commit metadata, parents, body, changed files, and statistics; patch text remains opt-in. Responses are bounded. When has_more is true, continue with cursor=next_cursor.""",
    ),
    "shell": (
        "Run commands and manage interactive or long-lived processes.",
        """run(command, ...) waits for a process and returns bounded stdout, stderr, returncode, and job id:
    ws.local["result"] = await ws.shell.run(["git", "status", "--short"])
    print(ws.local["result"]["stdout"])

timeout cancels the process. check=True raises when it exits unsuccessfully. Use input="text" to provide stdin. Full retained output is available from ws.tasks.get(ws.local["result"]["id"]).output().

env overlays the calling kernel's environment by default. Use env={"NO_COLOR": "1", "VARIABLE_TO_REMOVE": None} to set or remove variables without losing PATH. inherit_env=False starts with an empty environment and applies only env. Both run and start use these rules.

For long-running work, start a job and keep its handle in client-local state:
    ws.local["job"] = await ws.shell.start("command")

For an interactive pipe, use ws.local["job"] = await ws.shell.start(command, stdin=True), then await ws.local["job"].write("input\\n"); await ws.local["job"].write(eof=True) closes stdin. For a terminal, use ws.local["job"] = await ws.shell.start(command, pty=True, rows=24, cols=80). Use await ws.local["job"].resize(rows, cols) to change its size and await ws.local["job"].write("\\x03") to send Ctrl-C. PTY stdout and stderr are combined; terminal echo and ANSI output are preserved. In PTY mode eof=True sends the terminal EOF character. await ws.local["job"].cancel() terminates the job.

Use tasks help for handle status, output paging, expect, result, and cancellation behavior.""",
    ),
    "tasks": (
        "Track Python cells and detached background jobs.",
        """Cells run as independent asyncio tasks in one shared kernel, including cells from the same client. An await yields to other cells; synchronous code, synchronous IPython magics, and CPU-heavy work block the event loop. Cells can finish out of order, so wait for dependencies before submitting dependent code. MCP wait_ms limits notification waiting, not total request latency or task lifetime. Poll returns available output immediately and wakes for new output or completion.

ws.tasks.start(coroutine) starts detached async work and returns a handle. Store handles in ws.local. Raw asyncio.create_task() output after its parent cell finishes is not retained. Every cell is also a task handle: ws.tasks.get(exec_id) exposes its status (kind="cell") and actual last-expression result. A cell cannot await its own handle; use MCP poll for cell output.

For a retained job handle, status(), output(), and result() are synchronous; read(), expect(), cancel(), and wait_saved() must be awaited. status() reports state and optional warnings, plus result_persisted when persistence was requested; output() reads retained text, and result() raises NotReady until computation completes; it remains a synchronous computation result even when persistence was requested. read(cursor=None, stream=None, max_bytes=32768, wait_ms=0) returns bounded pages; its opaque cursor is separate from output() character offsets. expect(text, timeout=30, regex=False) waits for output; a failed match preserves its starting cursor, and reason identifies EOF, timeout, or limit. Cancelling an expect/read/wait_saved wait does not cancel the job or its reporter. cancel() requests cancellation and returns False for a terminal handle. Awaiting a persist_result=True handle waits for computation and the persistence attempt to settle while returning the original in-memory value; other async cells continue.

Use ws.tasks.list() and ws.tasks.get(task_id) to find handles. Completed handles are cached up to limits.completed_tasks (default 128); keep important handles in ws.local. Set persist_result=True when starting a visible Python task to retain a strict JSON result up to 256 KiB after handle eviction or reconnect; await job.wait_saved() returns None after confirmed persistence and raises ResultUnavailable for an unavailable, unserializable, unknown, or garbage-collected saved result. Calling it for a live job that was not started with persist_result=True raises ValueError. Persistence warnings do not change task success. Python tasks drain their retained output before terminal publication; task_output_persistence_unknown warns when an output append was not confirmed, without replaying an uncertain append. await ws.tasks.attach(task_id) reopens a saved job after reconnecting. Shell and cell handles do not provide a persistent result contract. Shell storage warnings appear in status() and output(cursor=0). Damaged saved output is replaced by warning events while readable output and event cursors are preserved.""",
    ),
    "system": (
        "Inspect workstation specifications, limits, and resource usage.",
        """await ws.system.info() reports visible workstation specifications and CPU/memory limits. await ws.system.usage(interval=0.5) samples CPU, RAM, swap, disk I/O, network, and GPU usage. await ws.system.disks() reports free space; pass a path to inspect a specific location. await ws.system.processes(sort="cpu", limit=20) lists busy processes; cmdline=True includes bounded arguments. await ws.system.gpus(processes=True) includes GPU process data.

await ws.system.process(pid, children=True, open_files=True, sockets=True) inspects one process and its optional relationships. Process identity includes its creation time; disappearance or permission failures are reported explicitly.

Missing metrics are None, not zero. Check sources, warnings, and truncated fields. Memory is measured in bytes; throughput is bytes per second. Process CPU uses one core as 100% and can exceed 100%. Limits reflect the kernel's visible namespace, affinity, and cgroup v2; they do not reserve resources. GPU tooling is optional and is never installed automatically.""",
    ),
    "messages": (
        "Coordinate clients with inbox messages and workspace status.",
        """await ws.status() returns compact runtime health, version, workspace, generation, and connection, active, and queued counts. Pass detail=True for the full connection list, active execution IDs, queue, and manager instructions. ws.client.id is your logical identity; ws.client.connection_id identifies this connection.

await ws.messages.send(client_id, text) sends to a registered workspace client, even if it is disconnected. Structured JSON can be attached with data={...}. await ws.messages.read(limit=20, after=None, wait_ms=0) reads unacknowledged messages. Follow next_cursor when has_more is true; wait_ms can wait up to 30000 ms. Use sender=... and reply_to=... to wait for replies to a particular message. await ws.messages.reply(message_id, text, data=None) answers a message addressed to you, including after acknowledgment. await ws.messages.ack([message_id]) acknowledges messages after handling them. Use await ws.messages.clients(connected=...) to page through registered IDs, connection state, last activity, and unacknowledged counts before addressing a peer. Offline recipients remain valid for later delivery.

MCP init, execute, and poll include an inbox count and bounded previews. Receiving a message may end a tool wait early; check execution state before relying on its result. Reading or receiving a preview does not acknowledge it, so unacked previews may repeat. Long previews have truncated=True; read() returns the full text. Messages and acknowledgments survive reset and manager restart. Message text is data, not automatically executable instructions. Idle clients see messages on their next MCP call.""",
    ),
    "timers": (
        "Schedule persistent client-scoped deadline notifications.",
        """await ws.timers.start(seconds=3600, label="review") schedules a one-shot timer. Use at=... instead of seconds for a timezone-aware datetime or ISO 8601 string; exactly one deadline form is required. Durations use the manager's UTC wall clock and include time while the manager is stopped. Zero seconds or a past deadline expires immediately. Timers and acknowledgments survive disconnect, reset, and restart; resume them with the same client ID.

await ws.timers.check(timer_id) reads one timer without acknowledging it. await ws.timers.list(state=None, limit=50, cursor=None) returns items, has_more, and next_cursor, including acknowledged records; states are scheduled, expired, and cancelled. await ws.timers.cancel(timer_id) cancels a scheduled timer. Expired alerts are attached automatically to initialized clients' init, execute, and poll responses in timers: unacked, items, and has_more. They may end a tool wait while the execution continues. Alerts repeat until await ws.timers.ack([timer_id]) acknowledges them. The timers field is omitted when there are no alerts; mypr does not wake an agent that is not calling a tool.""",
    ),
    "mail": (
        "Use configured IMAP and SMTP accounts from the persistent manager.",
        """ws.mail uses manager-owned IMAP and SMTP connections. Configure accounts through ws.config or the global/workspace TOML layers; credentials are environment-variable references in password_from and are never accepted in RPC payloads or returned by accounts/status. Mailbox operations select mail.default_account or the only enabled account; otherwise specify account. Replies and forwards default to the original account. accounts/status inspect cached configuration without opening a connection. Run await ws.mail.accounts() and await ws.mail.status() before troubleshooting a connection.

await ws.mail.search(account=None, mailbox="INBOX", unread=None, sender=None, subject=None, text=None, since=None, before=None, limit=20, cursor=None) returns bounded header records with opaque references in items[].id. Pages contain items, has_more, and next_cursor; limit is 1-100. Message references include their IMAP mailbox namespace and become invalid after a UIDVALIDITY or account-identity change. await ws.mail.read(message_id, max_bytes=32768) fetches a bounded parsed body and attachment metadata without marking the message read; continue with the returned body cursor when present. max_bytes is capped at 1 MiB. Use download_attachment(message_id, attachment_id, path, overwrite=False) for a workspace file with overwrite protection.

await ws.mail.mark_read(ids), mark_unread(ids), and move(ids, mailbox) change server flags or mailbox placement. Notification acknowledgment is separate from message read state. Search and mutations use the manager's network service, so a temporary server failure is reported with a structured error instead of being mistaken for an empty mailbox.

Draft and send are separate. await ws.mail.draft(account=..., to=..., cc=..., bcc=..., subject=..., text=..., html=..., attachments=...) validates and persists an immutable MIME draft. Use reply_to=message_id or forward=message_id, never both; revise a draft by creating a new one. await ws.mail.send(draft_id, request_id=...) queues delivery and returns a send record. request_id deduplicates retries for one client. accepted means the SMTP server accepted the message, not final delivery; unknown means acceptance could not be confirmed and blocks resubmission of that draft. Confirm delivery outside mypr before creating another draft. Omitted request_id uses a draft-bound key; a definitively failed send can be retried with a new key. Attachments must resolve inside the workspace; MIME and attachments are capped at 25 MiB. BCC recipients are envelope-only. A configured sent_mailbox is an opt-in append copy and its failure does not undo SMTP acceptance.

await ws.mail.watch(account=None, mailbox="INBOX") creates a per-client subscription. Watch state and cursors survive reset, disconnect, and manager restart; one manager connection is shared by clients. New arrivals appear automatically as bounded mail previews on later init, execute, and poll calls, and can wake their wait. Use notifications(limit=20, cursor=None) for cached pages and ack(notification_ids) after handling them. A notification preview or acknowledgment never marks its message read. await ws.mail.unwatch(watch_id) removes a subscription; await ws.mail.watches() reports sync and error state. The mail field is absent when there are no cached notifications, pending or uncertain sends, or reportable watch issues, so its absence does not prove that a mailbox is empty.""",
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

await ws.mcp.list_resources("server") and await ws.mcp.read_resource("server", uri) expose resources. await ws.mcp.list_resource_templates("server", cursor=None) discovers parameterized URIs and preserves the server's nextCursor; pass it back as cursor. await ws.mcp.list_prompts("server") lists prompts; await ws.mcp.get_prompt("server", name, arguments={...}) returns a prompt's messages. Read the relevant server descriptions before supplying arguments. ws.help("mcp.call_tool") and other method names show live signatures.

These operations preserve Python state. A busy connection requires force=True to interrupt its calls. Initial connection and protocol initialization have a 30-second deadline, including lazy first use; startup failure reaches queued calls, and a later call can retry after cleanup. This is not a timeout on initialized tool calls.""",
    ),
    "config": (
        "Inspect and persist global and workspace configuration.",
        """await ws.config.get() reads the desired effective configuration. Use scope="global" or scope="workspace" to inspect one raw layer, or pass a dotted TOML path such as "mcp.servers.reports" or a quoted key path such as 'mcp.servers."reports".env_from'. Known sections include mcp, lsp, mail, limits, and storage.

await ws.config.set(path, value, scope="workspace") and await ws.config.unset(path, scope="workspace") persist one layer without applying the change. The global scope is also writable when the manager permits it. Server entries and mail accounts are replaced as complete values; an explicit enabled=false workspace entry disables an inherited global server, while unset reveals the global value again.

await ws.config.explain(path) reports desired and applied values, source, revisions, pending state, and whether a restart is required. await ws.config.reload(force=False) explicitly applies persisted changes and reports applied, deferred, errors, revision, and restart_required fields. Settings are never implicitly applied by set or unset.""",
    ),
    "http": (
        "Make bounded asynchronous HTTP requests and downloads.",
        """ws.http provides named persistent HTTPX2 (httpx2.AsyncClient) clients. Use await ws.http.get/post/... for bounded decoded responses, async with ws.http.stream(...) for incremental bodies, and await ws.http.download(url, path) for atomic workspace downloads. A completed download returns its Path even if temporary-file cleanup fails; ws.http.last_warnings reports warnings from the current logical client's most recently completed download.

await ws.http.extract_html(html, url=..., selector=...) extracts readable content from existing HTML. await ws.http.read_html(url, ...) fetches through a named HTTP client before extraction. Both return bounded text and link results with source metadata. Pass include_structure=True when heading hierarchy and document metadata are needed. Parsing prepares trafilatura and, when selector is used, cssselect in the workspace environment as needed; it does not execute JavaScript. Pass browser page.content() to extract a rendered document.

Default limits are 16 MiB for requests and 256 MiB for downloads. ws.http.client(name, ...)
returns the native client; close it before changing its options.""",
    ),
    "browser": (
        "Automate managed or external browsers with Playwright.",
        """await ws.browser.context(name, ...) returns a native async Playwright BrowserContext. The Playwright package and a managed browser engine are prepared automatically by default; use await ws.dependencies.ensure("browser:chromium") for an explicit install. Use launch_options for the managed browser. Use await ws.browser.connect(endpoint, protocol=...) to connect to an external Playwright or CDP browser, then await ws.browser.context(..., connection=name) to use it.

The first managed use installs a missing browser engine automatically. Await ws.browser.save_state(...) and ws.browser.load_state(...) for explicit authentication-state persistence, and await ws.browser.screenshot(page, path) to save an artifact and return inline image output.

await ws.browser.observe(page) starts a client-owned bounded event collector; use await observation.read(...) and await observation.request(...) on the returned handle; observation.close() is synchronous. Response bodies are opt-in, bounded, and accept body_timeout; sensitive headers and structured URLs are masked by default. observation.read(types=..., url_contains=..., methods=..., status=...) applies AND filters; wait_ms waits for a matching event for at most 30 seconds. await ws.browser.snapshot(page, ...) captures a bounded accessibility snapshot. Follow its cursor to read the same capture; await ws.browser.find(snapshot_id, text, ...) searches a saved capture and await ws.browser.diff(before_id, after_id, ...) compares two captures. These helpers reuse native Playwright and do not replace locators.

Managed resources close on reset. External browser processes and pre-existing tabs survive.""",
    ),
    "net": (
        "Run bounded DNS, TCP, TLS, and port-scan diagnostics.",
        """await ws.net.resolve(...), await ws.net.connect(...), and await ws.net.tls(...) provide bounded DNS, TCP, and verified TLS diagnostics. resolve(timeout=...) raises TimeoutError after bounded worker cleanup when the deadline is exceeded. await ws.net.scan(targets, ports=..., max_probes=..., max_duration=...) starts a bounded TCP scan; await ws.net.nmap(targets, args=[...], max_duration=...) starts Nmap. Scan results report attempted probes and the stop reason when a limit ends collection.

await ws.net.sockets(...) inspects local TCP, UDP, and Unix sockets with address, port, state, and PID filters. This is local listener/connection inspection, separate from remote scanning.

Scan handles provide synchronous status(), output(), and result(). Await read(), expect(), cancel(), summary(), and paged results(); awaiting the handle waits for completion. Use await ws.tasks.attach(scan_id) after reconnecting. TCP and Nmap result files are bounded and survive reset; active scans require force=True to reset. Nmap output options are managed by mypr and cannot be passed in args.""",
    ),
    "skills": (
        "Discover, read, validate, and edit workspace skills.",
        """ws.skills.list() discovers available skills, including nested names such as "group/review"; ws.skills.read("name") reads their instructions. Listed name values are readable relative paths; a different YAML name is preserved as declared_name. Read a skill before using it. await ws.skills.validate(name, text) checks content. await ws.skills.write(name, text, expected_hash=revision) saves it with revision checks.

await ws.skills.history(name, limit=20, cursor=None) lists saved revisions newest first. await ws.skills.read_revision(name, revision, start_byte=0, max_bytes=32768) reads a revision; recorded=False identifies a verified recovery blob without an index entry. await ws.skills.restore(name, revision, expected_hash=current_revision) validates and restores the file. History is stored under .mypr/revisions and survives reset. Edits made outside these helpers are not continuously tracked.""",
    ),
    "modules": (
        "Manage reusable Python modules in the workspace library.",
        """ws.modules.list() and await ws.modules.read(name) inspect modules under ws.root / "lib/ws_lib". await ws.modules.write(name, source, expected_hash=...) writes a module; existing files require expected_hash.

await ws.modules.check(name, test_code="...") validates code in a separate Python process and returns the checked source hash. Saving does not activate code; use ws.modules.load(name, expected_hash=...) or ws.modules.reload(name, expected_hash=...) to activate exactly the bytes that were checked. A mismatched hash refuses activation. Reload replaces the module object, while references already held elsewhere remain unchanged.

await ws.modules.history(name, limit=20, cursor=None) lists saved revisions newest first. await ws.modules.read_revision(name, revision, start_byte=0, max_bytes=32768) reads saved source; recorded=False identifies a verified recovery blob without an index entry. await ws.modules.restore(name, revision, expected_hash=current_revision) restores source after validation and revision checks. Restore does not activate the file or change existing Python references. Direct edits are captured only when a later helper write observes them.""",
    ),
    "packages": (
        "Install packages into the workspace Python environment.",
        """await ws.packages.add("package") starts an explicit installation and returns a task handle. Keep the handle in ws.local to inspect or await the installation. Package changes are serialized per workspace; the frozen manifest is replaced atomically after installation succeeds. Failures before replacement leave the previous manifest intact. If the replacement cannot be confirmed durable, the job succeeds with a manifest_durability_unknown warning in its status and output. Installation can change the environment before a later freeze or manifest failure. Reset is not required for unrelated imports, but already-imported modules may need a reset before an upgrade is visible.

Built-in features automatically prepare their registered Python packages when dependencies.auto_install is true. Use await ws.dependencies.ensure("pymupdf") for an explicit preparation; use ws.packages.add(...) for arbitrary user packages. Automatic preparation never installs an arbitrary import and preserves existing compatible package versions.""",
    ),
    "dependencies": (
        "Inspect and prepare registered tools, packages, browser engines, and OCR models.",
        """await ws.dependencies.list(kind=None, limit=50, cursor=None) returns items, has_more, next_cursor, and auto_install without installing anything. kind may be binary, python, model, browser, or None.

await ws.dependencies.ensure("rg", "ast-grep", "pymupdf", "tessdata:eng") prepares registered dependencies and waits for completion. Explicit ensure bypasses dependencies.auto_install for missing items. Supported binaries are rg 15.2.0, ast-grep 0.45.3, rga 0.10.10, and pandoc 3.12; browser names are browser:chromium, browser:firefox, and browser:webkit. Supported automatic Python packages are pillow, pymupdf, trafilatura, cssselect, python-docx, python-pptx, and openpyxl. OCR models use the pinned tessdata_fast 4.1.0 catalog.

Binary tools and OCR models are shared under the XDG data directory, with downloads under the XDG cache directory. Python packages belong to the workspace .mypr environment. Built-in feature calls prepare only the dependencies they declare; cached-page reads and list/doctor/status operations do not install anything. Tesseract, FFmpeg, Poppler, Git, LSP servers, and external services remain manual dependencies. Explicit TESSDATA_PREFIX is preserved.""",
    ),
    "history": (
        "Find execution records and inspect event logs.",
        """await ws.history.list(client_id=ws.client.id) finds your executions and jobs. await ws.history.get(record_id) reads a record; await ws.history.logs() reads events. Python task records expose history_id to distinguish reused IDs across resets.""",
    ),
    "pages": (
        "Consume paged workspace results without repeating queries.",
        """ws.pages.iter(method, *args, max_pages=100, **kwargs) is an async iterator for APIs that return next_cursor/has_more pages. It calls the supplied bound workspace method with the first arguments, then forwards each cursor using that method's cursor parameter. When max_pages is reached with more data available, PageLimitReached exposes method, pages_read, next_cursor, and next_kwargs; resume with ws.pages.iter(error.method, *args, **error.next_kwargs). An explicit loop break is normal. A stalled cursor or an expired snapshot raises instead of silently restarting the query.""",
    ),
    "performance": (
        "Inspect recent manager, storage, kernel, and bridge timings.",
        """await ws.performance() returns rolling timing summaries for manager, storage, kernel, and bridge work. Each label keeps its latest 256 samples and a total_count observed since manager start, with p50, p95, and max in milliseconds. Bridge timing for the most recent completed request appears on the next call; concurrent or disconnected calls may not all be reported. Execution results include timing_ms in MCP _meta (result.meta in the Python SDK), outside printed content and structuredContent; request errors may lack timings. Manager and RPC times include requested notification waits and are not overhead-only measurements. Nested spans overlap, so do not sum them to infer unmeasured overhead; kernel round-trip includes IPC and output persistence, not only Python execution. Time outside the MCP server, including host scheduling, external transport, and model execution, is not measured. Samples remain in memory until manager restart.""",
    ),
    "storage": (
        "Inspect and clean retained workspace data.",
        """await ws.storage.usage() reports disk usage by retained output, journals, snapshots, artifacts, revisions, and other managed data using a metadata-only scan that does not hash or read file contents. await ws.storage.gc(dry_run=True) builds a deletion plan without changing files; await ws.storage.gc_apply(plan_id) applies that exact plan after rechecking it. Automatic cleanup runs periodically and removes data older than 30 days. When managed data exceeds the soft 1 GiB target, it also selects the oldest eligible recent data until the target is reached. The `[storage]` config fields are `enabled`, `retention_days`, `max_bytes`, `revision_keep`, and `gc_interval_seconds`. Active work, current files, the latest `revision_keep` revisions per resource (50 by default), and shared blobs referenced by retained records are protected. Check `await ws.status()` for the automatic pass's `storage_maintenance` fields.""",
    ),
    "doctor": (
        "Diagnose workspace readiness and optional dependencies.",
        """await ws.doctor() checks the workspace Python environment, packages,
registered dependency tools and models, search backends, LSP configuration,
browser engine, OCR data, MCP settings, runtime health, and available storage.
Each check reports ready, missing, invalid, or unknown with a bounded reason and
suggested action. The same diagnostics are available without starting a manager
through `mypr-mcp doctor`. The command does not install packages or dependency
artifacts, start servers, or rewrite configuration.""",
    ),
    "lifecycle": (
        "Understand client identity, persistence, reset, restart, and recovery.",
        """The Python kernel is shared by every client connected to this workspace. Variables, imports, functions, and active tasks survive calls and client disconnects. Ordinary globals are shared between clients; background tasks retain their creator's client context. Store per-client working values in ws.local, a dict scoped to your logical client ID. Reconnecting with the same ID restores that local state while the same kernel remains alive.

ws.inspect() reports variable types, tasks, and skills without dumping their values. await ws.status() returns compact health and counts; pass detail=True for connection records, active execution IDs, queued IDs, and manager instructions. await ws.performance() reports rolling manager, storage, kernel, and bridge timing summaries.

init() creates a new adjective-animal client ID; init(client_id="...") creates or resumes that logical session. The ID is bound to the MCP connection and is not passed to execute. Repeated init returns the current ID. Switching IDs or using one ID on multiple live connections is rejected. Poll is available before init.

await ws.reset() clears Python memory within the same installation. It is rejected while other cells or managed jobs are active unless force=True, which cancels them first. Completion arrives through execute/poll. Files, packages, and history remain.

await ws.restart() explicitly replaces the manager and kernel using this connection's installation; it does not download an update. Active work requires force=True. Restart stops the current cell, whose recorded result is read with poll. A planned restart preserves MCP connections and logical client IDs, but clears Python globals, ws.local, and managed browser contexts. Saved files, messages, and history survive. Compatible package updates alone do not replace the running manager or clear memory.

Kernel or manager crashes lose in-memory state; history persists. Do not blindly repeat state-changing code when completion is uncertain. Poll a known execution ID or inspect history before retrying. request_id deduplicates the same logical client's identical code: reusing it with the same code returns the prior execution, while using it for different code raises an error. Read-only checks and imports can be run again when appropriate.

Code runs with the current OS user's permissions, and exceptions do not undo earlier changes. error is a bounded summary; error_truncated indicates shortening. Read paged output for traceback details. Malformed or missing display artifacts produce warnings without changing Python success. Essential worker failure is reported as unhealthy or lost; use the explicit CLI reset to recover. CLI stop waits for manager exit before success.""",
    ),
}

_TOPIC_NAMES = tuple(_TOPICS)
_TOPIC_ALIASES = {"filesystem_history": "fs", "task_results": "tasks"}
_INDEX = (
    "mypr workspace API topics\n\n"
    + "\n".join(f"{name}: {description}" for name, (description, _) in _TOPICS.items())
    + "\n\nAliases: filesystem_history -> fs; task_results -> tasks."
    + '\n\nRead a topic with ws.help("topic") or inspect a method with ws.help("shell.run").'
)


_METHOD_NOTES = {
    "shell.run": "Returns a dict with id, state, returncode, stdout, stderr, timed_out, and truncated. timeout cancels the process; check=True raises on unsuccessful exit. Retained output is accessible through ws.tasks.get(id).",
    "shell.start": "Returns a job handle. Await it to wait for completion; status/output/result are synchronous and read/write/expect/cancel are async. env overlays inherited variables by default; None removes a variable. inherit_env=False replaces the environment.",
    "fs.read": "Returns text, revision (SHA-256), and range/paging metadata. Follow next_cursor for bounded or long-line reads. Keep revision for expected_hash when editing.",
    "fs.write": "Returns path and revision metadata. Existing files require expected_hash or overwrite=True. A mismatched revision rejects the write.",
    "fs.patch": "Returns revision and bounded diff metadata. Each old string must match exactly; dry_run previews without writing.",
    "fs.apply_patch": "Returns per-file changes and a bounded diff. expected_hashes validates target revisions; dry_run previews without writing.",
    "fs.search": "Returns a bounded result page. Continue with cursor or page_cursor and page budgets, omitting the original pattern and query options. Follow next_cursor while has_more; inspect complete and stop_reason before treating absence or counts as definitive.",
    "fs.search_docs": "Returns document search matches in extracted text, not editable file coordinates. Continue with cursor or page_cursor and page budgets, omitting query options. Follow next_cursor and inspect complete/stop_reason.",
    "fs.search_ast": "Returns structural matches with source ranges and captures. Continue with cursor or page_cursor and page budgets, omitting the pattern, lang, and other query options. Follow next_cursor; inspect details_truncated and complete.",
    "mcp.call_tool": "Returns the external server's MCP result; inspect isError and structuredContent/content. Arguments follow that tool's inputSchema from list_tools.",
    "mcp.read_resource": "Returns the external server's resource contents. URIs come from list_resources or list_resource_templates.",
    "mcp.get_prompt": "Returns the external server's prompt messages. Inspect list_prompts for supported arguments.",
    "status": "Returns compact runtime health, versions, generation, and counts. detail=True includes connection records, active/queued execution IDs, and manager instructions.",
    "performance": "Returns manager_dispatch, bridge, storage, and kernel timing summaries. See ws.help('performance') for sample limits and measurement semantics.",
    "modules.load": "If expected_hash is supplied, only the exact bytes returned by check() may be activated; a mismatch leaves the current binding untouched.",
    "modules.reload": "If expected_hash is supplied, activation is refused when the checked source is stale. Existing references held elsewhere are not automatically updated.",
    "fs.replace": "Builds a bounded multi-file replacement preview. It never mutates files; retain plan_id and call fs.apply_replace after reviewing it.",
    "fs.apply_replace": "Rechecks every source revision before applying a replacement plan and records the change in filesystem history.",
    "fs.restore": "Restores one tracked filesystem revision only when the current revision matches the supplied precondition.",
    "code.apply_edit": "Applies a prepared, revision-checked LSP edit plan. Command-only actions are never executed.",
    "storage.gc": "Returns a dry-run cleanup plan by default; call storage.gc_apply(plan_id) to apply the exact plan.",
    "storage.gc_apply": "Applies a previously generated cleanup plan after validating its workspace identity and protected references.",
    "storage.usage": "Reports category file counts and byte totals from a metadata-only scan without hashing or reading file contents.",
    "config.get": "Reads effective, global, or workspace configuration. A dotted TOML path narrows the returned value.",
    "config.set": "Persists one global or workspace value without applying it; call config.reload() explicitly.",
    "config.unset": "Removes one value from a writable configuration layer without applying the change.",
    "config.explain": "Reports desired/applied values, source, revisions, pending state, and restart requirements for a path.",
    "config.reload": "Explicitly applies persisted configuration and reports applied, deferred, failed, and restart-required changes.",
    "doctor": "Returns readiness checks for runtime, packages, tools, LSP, browser, OCR, MCP configuration, and storage without installing or changing anything.",
    "dependencies.list": "Returns a paged dependency inventory without installing anything.",
    "dependencies.ensure": "Prepares registered dependencies and waits for completion; explicit ensure bypasses dependencies.auto_install.",
}
_WORKSPACE_METHODS = {"help", "inspect", "status", "performance", "doctor", "reset", "restart"}


def _method(workspace, path):
    parts = path.removeprefix("ws.").split(".")
    if parts:
        parts[0] = _TOPIC_ALIASES.get(parts[0], parts[0])
    if len(parts) == 1 and parts[0] in _WORKSPACE_METHODS:
        owner, name = workspace, parts[0]
    elif len(parts) == 2 and parts[0] in _TOPICS:
        owner, name = vars(workspace).get(parts[0]), parts[1]
    else:
        raise ValueError(f"unknown API method {path!r}")
    if owner is None or not name or name.startswith("_"):
        raise ValueError(f"unknown API method {path!r}")
    member = inspect.getattr_static(owner, name, None)
    if isinstance(member, (staticmethod, classmethod)):
        member = member.__get__(owner, type(owner))
    elif inspect.isfunction(member):
        if name not in vars(owner):
            member = member.__get__(owner, type(owner))
    elif not inspect.ismethod(member):
        raise ValueError(f"unknown API method {path!r}")
    return member, ".".join(parts)


def method_help(workspace, path):
    member, name = _method(workspace, path)
    prefix = "async " if inspect.iscoroutinefunction(member) else ""
    lines = [f"{prefix}ws.{name}{inspect.signature(member, eval_str=False)}"]
    if doc := inspect.getdoc(member):
        lines.extend(("", doc))
    if note := _METHOD_NOTES.get(name):
        lines.extend(("", note))
    if "." in name:
        lines.extend(("", f'Related guidance: ws.help("{name.split(".")[0]}")'))
    return "\n".join(lines)


def workspace_help(topic: str | None = None, *, workspace=None) -> str:
    if topic is None:
        return _INDEX
    if not isinstance(topic, str):
        raise TypeError("topic must be a string or None")
    parts = topic.removeprefix("ws.").split(".")
    canonical_topic = _TOPIC_ALIASES.get(parts[0], parts[0])
    canonical_path = ".".join([canonical_topic, *parts[1:]])
    if workspace is not None and ("." in topic or canonical_topic in _WORKSPACE_METHODS - {"performance"}):
        return method_help(workspace, canonical_path)
    try:
        text = _TOPICS[canonical_topic][1]
    except KeyError:
        available = ", ".join(_TOPIC_NAMES)
        raise ValueError(f"unknown help topic {topic!r}; available topics: {available}") from None
    if workspace is not None and (owner := vars(workspace).get(canonical_topic)) is not None:
        methods = [
            name
            for name, member in inspect.getmembers_static(owner)
            if not name.startswith("_")
            and (inspect.isroutine(member) or isinstance(member, (staticmethod, classmethod)))
        ]
        if methods:
            text += "\n\nMethods: " + ", ".join(f"{canonical_topic}.{name}" for name in methods)
            text += f'\nInspect one with ws.help("{canonical_topic}.{methods[0]}").'
    return text


__all__ = ["workspace_help"]
