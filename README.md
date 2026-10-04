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

The kernel injects `ws`, a `Workspace` instance, into every Python cell. Methods shown with `await` are asynchronous; property access, help, inspection, skill reads, and task-handle inspection are synchronous.

`init` returns the running manager's core instructions and capabilities. When `help` is available, query detailed guidance from that kernel as needed:

```python
print(ws.help())          # Topic index
print(ws.help("fs"))      # File reads and revision-checked edits
print(ws.help("search"))  # Text, document, and AST search
print(ws.help("shell.run"))  # Live method signature, defaults, and return guidance
```

`ws.help(topic=None)` returns a string without I/O. Topics cover workspace APIs and execution lifecycle; each API topic lists its public methods. The capability aliases `filesystem_history` and `task_results` resolve to the `fs` and `tasks` topics. Pass a method path such as `"shell.run"` or `"ws.mcp.read_resource"` to inspect the running implementation's signature, defaults, return annotation, and documentation. Private attributes and arbitrary attribute traversal are rejected. Follow the running manager's instructions when it differs from the installed MCP client.

| Entry point | Purpose |
| --- | --- |
| `ws.workspace`, `ws.root` | `Path` objects for the workspace and its `.mypr/` directory |
| `ws.client`, `ws.local` | Current caller identity and its in-memory scratch dictionary |
| `ws.fs` | Read, search, patch files, and preview/apply structural rewrites |
| `ws.code` | Query language servers for navigation, symbols, calls, and diagnostics |
| `ws.docs` | Read PDFs and Office documents, render pages, and explicitly run OCR |
| `ws.shell`, `ws.tasks` | Start and inspect background work |
| `ws.mcp` | Call and reconfigure external MCP servers |
| `ws.config` | Inspect and persist global and workspace configuration |
| `ws.messages` | Send and receive persistent client messages |
| `ws.timers` | Schedule persistent client-scoped deadline notifications |
| `ws.mail` | Search configured mailboxes, draft and send mail, and watch for arrivals |
| `ws.skills`, `ws.modules` | Validate, save, and reuse workspace capabilities |
| `ws.git` | Read structured Git status, diffs, history, blame, and committed files |
| `ws.http` | Use persistent HTTPX2 clients and extract readable HTML content |
| `ws.web` | Search the web and extract bounded content through Kagi, Brave, or Tavily |
| `ws.browser` | Use native Playwright, bounded event observation, and saved snapshots |
| `ws.net` | Inspect local sockets, resolve hosts, query TCP/TLS endpoints, and run scans |
| `ws.system` | Inspect hardware, limits, resource usage, and process relationships |
| `ws.dependencies` | Inspect and prepare registered tools, packages, browser engines, and OCR models |
| `ws.locks` | Coordinate shared work with task-scoped logical locks |
| `ws.packages` | Install kernel packages |
| `ws.history` | Query saved execution and task records |
| `ws.storage` | Inspect retained workspace data and run planned cleanup |
| `ws.pages` | Iterate bounded cursor-based API results |
| `await ws.doctor()` | Check runtime, package, tool, and configuration readiness |
| `await ws.performance()` | Read recent timing summaries |
| `ws.help()`, `ws.help("topic")` | Read the topic index or API guidance from the running kernel |
| `await ws.inspect()`, `await ws.status()` | Inspect Python state and runtime health |
| `await ws.reset()` | Reset shared Python memory; see [Reset and lifecycle](#reset-and-lifecycle) |
| `await ws.restart()` | Replace the manager and kernel with this installation; see [Reset and lifecycle](#reset-and-lifecycle) |

For language-server setup and coordinate semantics, see [code navigation](docs/code.md).

### Dependencies

mypr prepares registered dependencies when a built-in feature needs them. `dependencies.auto_install` defaults to `true` and can be overridden globally or per workspace. Set it to `false` to require explicit preparation of optional dependencies; `await ws.dependencies.ensure("name")` still installs a registered dependency when requested. `await ws.dependencies.list(kind=None, limit=50, cursor=None)` only inspects the inventory and returns `items`, `has_more`, `next_cursor`, and `auto_install`.

Binary tools and OCR models are shared across workspaces under `$XDG_DATA_HOME/mypr` (`~/.local/share/mypr` by default), with download temporary files under `$XDG_CACHE_HOME/mypr` (`~/.cache/mypr` by default). When a registered binary or OCR model is missing, mypr resolves the latest official stable release available for the platform and downloads it. Downloads are checked against SHA-256 metadata from the official release when available. Existing usable installations are reused and are not upgraded automatically. OCR models use the latest `tessdata_fast` release. Python packages are installed only into the current workspace's `.mypr/venv`, while uv's download cache remains global by default. Existing Python packages are preserved if they import successfully and satisfy their registered version requirements.

Built-in automatic Python packages include `ipykernel`, `tomlkit`, `httpx2`, `h2`, `socksio`, `playwright`, `psutil`, `pyyaml`, `pillow`, `pymupdf`, `trafilatura`, `cssselect` when a selector is used, `python-docx`, `python-pptx`, and `openpyxl`. The core packages `ipykernel` and `tomlkit` are always prepared at kernel startup, regardless of `auto_install`. Other registered packages are prepared when the first feature that declares them runs and automatic installation is enabled. Use `ws.packages.add(...)` for arbitrary packages; importing an arbitrary missing module never triggers an install. Playwright uses its existing SDK and browser cache through the same policy. Set `dependencies.uv_cache_dir` to an absolute path, `~`, or a `~/`-prefixed path and `dependencies.uv_link_mode` to `clone`, `hardlink`, or `copy` to control later installs; `UV_CACHE_DIR` and `UV_LINK_MODE` take precedence. Existing environments are not moved or reinstalled. Tesseract, FFmpeg, Poppler, Git, LSP servers, and external services remain manual dependencies. `doctor`, dependency listing, status, and cached-page reads never install anything.

`mypr-mcp prepare` prepares the core workspace packages in advance or for recovery while the workspace manager is stopped. Normal kernel startup also prepares these packages regardless of `auto_install`. It does not accept a workspace selector.

Dependency status counts running and queued package probes as active work. Reset and shutdown cancel these probes and collect their subprocesses before releasing the service.

Most workspace queries return bounded pages. `ws.pages.iter(method, *args, max_pages=100, **kwargs)` is an async iterator for consuming them without resubmitting the query:

```python
async for page in ws.pages.iter(ws.fs.search, "TODO", paths="src"):
    for match in page["matches"]:
        print(match["path"], match["line"])
```

The iterator forwards each API's `next_cursor` as `cursor` and detects an expired snapshot or a non-advancing cursor. Continuation values also replace cursor arguments supplied positionally, including `after` for message reads. If `max_pages` is reached while more data remains, it raises `PageLimitReached` with `method`, `pages_read`, `next_cursor`, and `next_kwargs`; resume with `ws.pages.iter(error.method, *args, **error.next_kwargs)`. An explicit loop break is normal. For search methods, the pattern and query options are submitted only for the first page; continuation uses the saved cursor and page budgets. It does not silently rerun a query against changed files.

See [media and document extraction](docs/media.md) for images, PDF pages, OCR, Office formats, and optional workspace packages. These operations preserve the source files. Further guides cover [structural rewrites](docs/rewrites.md), [browser observation](docs/browser.md), [HTML extraction](docs/http.md), [Git history](docs/git.md), [local diagnostics](docs/diagnostics.md), and [workspace storage](docs/storage.md). [External tool recipes](docs/tool-recipes.md) show how to use installed benchmarking, tracing, and code-analysis tools through existing shell jobs.

Use the async helpers for everyday file work. `ws.workspace` and `ws.fs` paths stay anchored to the workspace even if code changes the kernel's current directory:

```python
await ws.fs.write("notes.txt", "Hello from Python.\n")
```

All clients share imports, globals, and filesystem changes. Store caller-specific values in `ws.local`; they persist across cells from the same logical client. Kernel resets and crashes clear all in-memory local dictionaries.

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

`read(path, *, start_line=1, end_line=None, start_byte=None, max_bytes=32768)` returns UTF-8 `text`, a SHA-256 `revision`, file `size`, line information, and `truncated`. Lines are one-based and `end_line` is inclusive. If the output limit cuts a line, continue using `next_cursor["line"]` as `start_line` and `next_cursor["byte"]` as `start_byte`. Compare revisions when reading multiple pages of a file that may change.

`search(pattern=None, *, paths=None, glob=None, fixed=False, ignore_case=False, hidden=False, no_ignore=False, context=0, word=False, line=False, multiline=False, dotall=False, before=None, after=None, regex_engine="default", mode=None, timeout=30, scan_bytes=16*1024*1024, scan_limit=None, max_matches=100, max_bytes=32768, cursor=None, page_cursor=None)` uses a compatible `rg` from the workstation's `PATH` or the shared dependency store. A missing registered tool is prepared automatically by default. A string or list of patterns is accepted and is treated as an OR query. `paths` and `glob` also accept a string or a list. Ignore files and hidden-file rules apply by default.

When `mode` is omitted, a pattern selects `matches` and a pattern-less query selects `files`. Explicit `mode` controls the result collection: `matches` returns matching lines and all match spans, `files` returns matching paths, `counts` returns `{"path": ..., "count": ...}` rows, and `exists` returns a boolean. If an incomplete scan found no match, `exists` is `None`. `matches` includes one-based lines and byte-based columns, zero-based UTF-8 byte offsets, and an exclusive end offset when available. `context` may add surrounding rows; their `kind` is `context`. `pcre2` can be selected with `regex_engine` when the installed `rg` supports it. Invalid patterns and unsupported engines fail before a scan starts.

Search runs as a managed job. `timeout` includes process startup and output collection; `scan_bytes` and `scan_limit` bound the retained scan. The response has `backend`, `complete`, `stop_reason`, `scan_truncated`, `has_more`, and `next_cursor` fields. A timeout or scan limit preserves the partial result with `complete=false`; it is not a complete count. Cancellation propagates to the caller after the managed process is cleaned up. `max_matches` and `max_bytes` bound one returned page. Snapshot files are limited to 64 MiB and are checked for size, regular-file type, and changes during reads. The first request saves a bounded snapshot, so `await ws.fs.search(cursor=page["next_cursor"])` continues without rerunning the command, even after a reset or restart. `page_cursor` rereads the same page with a different `max_bytes` budget. A budget too small for one path and its metadata raises an error; increase `max_bytes`.

Use `search_docs(pattern, *, paths=None, glob=None, mode=None, adapters=None, accurate=False, cache=True, archive_depth=5, timeout=30, scan_bytes=16*1024*1024, scan_limit=None, max_matches=100, max_bytes=32768, cursor=None, page_cursor=None)` for PDF, Office, ebook, archive, and other formats supported by `rga` (ripgrep-all). Missing registered tools and explicitly selected converters are prepared automatically by default. With `adapters=None`, rga handles converters per file; plain-text searches do not require Pandoc. Use `adapters=["pandoc"]` to prepare and select Pandoc explicitly. It exposes the same result modes and completion fields as `search`, with `backend="rga"`. `adapters` selects the rga adapter list, `accurate=True` enables MIME detection, `cache=False` uses a per-query temporary extraction cache that is removed after the search, and `archive_depth` limits nested archive traversal. Converter failures during a scan produce a partial result and warning while matches from other files remain available. If automatic installation is disabled, a missing explicitly selected registered converter produces `dependency_missing` before the scan. Document locations are coordinates in extracted text; they are not byte offsets or editable line positions in the original PDF or Office document. Archive member names may be present in the displayed path or text according to the rga adapter.

Use `search_ast(pattern=None, *, lang=None, rule=None, constraints=None, utils=None, paths=None, glob=None, hidden=False, no_ignore=False, mode=None, strictness="smart", timeout=30, scan_bytes=16*1024*1024, scan_limit=None, max_matches=100, max_bytes=32768, cursor=None, page_cursor=None)` for read-only structural searches through ast-grep. A missing registered ast-grep is prepared automatically by default. Provide `lang` and either a pattern or a rule for a new query. Omit query options when continuing a cursor. `mode=None` selects matches for a new document or AST query and preserves the saved mode for continuation. Patterns use ast-grep metavariables such as `$NAME` and `$$$ARGS`; rules may combine `kind`, `pattern`, `has`, `inside`, `follows`, `precedes`, `constraints`, and `utils`. Results include the source path, language, matched text, an exclusive source range, and metavariable ranges when available. `counts` counts matched AST nodes. Large matches may omit optional submatch or capture details and set `details_truncated=true`; the result retains the minimal path, line, and column metadata, but its source range may also be omitted. `page_cursor` can reread the same page with a larger byte budget. This API never rewrites files or runs project fixes; `backend="ast"` identifies the result.

`await ws.fs.search_backends()` reports whether `rg`, `rga`, and ast-grep are available, their versions, and the document converters visible through the system `PATH` or shared dependency store. Search tools use the current workspace permissions and Linux process namespace; they do not provide a sandbox. Search cache and snapshots are tool data and are excluded from ordinary workspace searches unless selected explicitly through `paths`.

`await ws.fs.tree(path=".", depth=3, max_entries=200, hidden=False)` returns a deterministic directory view with `entries` and `truncated`. Symlinks are listed without traversing their targets. `await ws.fs.stat(path, follow_symlinks=False)` returns file metadata, including kind, size, modification time, mode, and symlink target; it does not hash file contents.

`write(path, text, *, expected_hash=None, overwrite=False, encoding="utf-8", create_parents=False, history=True)` creates a file. Replacing an existing file requires its current revision or explicit `overwrite=True`. `patch(path, edits, *, expected_hash=None, dry_run=False, encoding="utf-8", max_diff_bytes=32768, history=True)` applies an ordered list of exact `{"old": ..., "new": ...}` replacements. Each target must occur once by default; use `count=N` for the first N matches or `count="all"` for all matches. A missing or ambiguous target fails before writing. Results include old/new revisions and a bounded unified diff; `dry_run=True` leaves the file unchanged.

Use `apply_patch(patch, *, expected_hashes=None, dry_run=False, max_diff_bytes=32768, history=True)` for a patch spanning multiple files:

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

The format uses `*** Add File`, `*** Update File`, `*** Delete File`, optional `*** Move to`, and `@@` context hunks inside `*** Begin Patch` / `*** End Patch`. Matching is exact; ambiguous context is rejected. `expected_hashes` maps file paths to revisions, with `None` requiring that a path does not exist. All targets and hunks are validated before mutation, and affected paths share the same locks as `write()` and `patch()`. Files are staged before application; ordinary commit failures trigger rollback. This is not a filesystem-wide atomic transaction: external writers and process or machine crashes can interrupt recovery. Symlink paths, duplicate targets, and hard-link aliases within one patch are rejected. `*** End of File` anchors the final hunk to the end of the file.

`await ws.fs.image("plot.png")` loads a PNG/JPEG for inline MCP image output. Return it as the cell's last expression or pass it to IPython's `display()`. `max_bytes` defaults to 2 MiB, matching the inline image limit; the source file is left unchanged.

Writes use atomic replacement and preserve existing file permissions. Single-file `write()`, `write_bytes()`, and `patch()` follow a symlink while preserving the link itself. `delete()`, `move()`, and `copy()` reject a symlink at the requested source or destination, so a lifecycle operation cannot remove or rename a link target by accident. Edits through these helpers serialize per resolved path, including across clients. Revision checks reject stale content; they do not lock out edits by external programs. Absolute paths are accepted under the current user's permissions. Ordinary Python remains available for other file and data operations.

For non-text files, use `read_bytes(path, start_byte=0, max_bytes=32768)` and `write_bytes(path, data, *, expected_hash=None, overwrite=False, create_parents=False)`. Binary reads return a Base64 payload in `data_base64`, size, revision, and a byte cursor; they never decode or rewrite the payload. Each read streams the whole file to calculate its revision while retaining only the requested byte range, so memory use stays bounded by that range and the read buffer. `delete(path, *, expected_hash)`, `move(source, destination, *, expected_hash, overwrite=False)`, and `copy(source, destination, *, expected_hash=None, overwrite=False)` operate on regular files and use the same workspace locks. Delete and move require the source revision; copy accepts an optional source revision. Destination overwrite is rejected, so the destination must be absent. Move revalidates the source before removal; an external change aborts the move and rolls back the destination if it still matches the staged copy. Move and copy history updates are prepared and committed as one lifecycle transaction; if history persistence fails, the file change is rolled back when its outcome is known, while an unknown history outcome is reported for inspection instead of being silently retried.

`history(path, cursor=None, limit=20)` lists successful changes made through `ws.fs`, including creates, deletes, moves, and binary writes. `read_revision(path, revision, start_byte=0, max_bytes=32768)` reads a stored text or binary revision; a deleted file is represented by the revision `"absent"` and an empty absent result. `restore(path, revision, *, expected_hash=None)` restores one entry only when the current file still matches the precondition. The latest `revision_keep` revisions (50 by default) and the current content are protected by automatic retention; history is local to the workspace and does not watch edits made by external programs.

For a repeated text change, use `replace(pattern, replacement, *, paths=None, glob=None, fixed=True, ignore_case=False, hidden=False, no_ignore=False, max_files=100, max_bytes=16777216, timeout=30, history=True)` to create a bounded plan. Review its files, revisions, and diff, then call `apply_replace(plan_id)` to recheck every source revision and apply the plan. Plans are immutable, expire, and cannot be applied when the search was incomplete. Literal matching is the default; set `fixed=False` for Python regular-expression matching and `ignore_case=True` for case-insensitive matching.

### Git

```python
await ws.git.status()
await ws.git.diff(staged=True, paths=["src"])
await ws.git.show("HEAD", path="README.md")
await ws.git.log(path="src")
await ws.git.blame("README.md", start_line=1, end_line=20)
```

`status(*, cursor=None, max_entries=200, max_bytes=32768)` returns branch and file information, including index/worktree changes, conflicts, and renames. `diff(*, staged=False, rev=None, paths=None, cursor=None, max_bytes=32768)` returns file metadata and patch text. `show(ref="HEAD", *, path=None, cursor=None, max_bytes=32768)` reads a commit or a file at that revision. Input paths are relative to the workspace; returned file paths are relative to the reported repository `root`. `max_bytes` must be at least 1024. Collect both `files` and `patch` across diff pages; a page may contain only file metadata.

These are read-only commands with paging, color, external diff programs, and textconv disabled. Follow `next_cursor` with the same method while `has_more` is true. Pages come from a saved snapshot, so later changes to the worktree do not alter an existing query. Snapshots survive kernel reset and manager restart. A continuation may repeat the original query arguments exactly; conflicting values are rejected. You may change the available page budget while paging: `max_entries` on record pages and `max_bytes` on every page. The snapshot keeps the original query and resolved commit, so moving a branch or ref after the first page does not silently change the result.

`log(..., follow=True)` follows a file across renames when Git can identify its previous path. `commit_info(ref, *, include_files=True, include_patch=False, cursor=None, max_bytes=32768)` returns one commit's parents, author and committer, subject and body, changed-file metadata, and insert/delete totals. Patch text is opt-in and remains bounded. Merge statistics use the first parent as the comparison base and are marked in the result. These helpers remain read-only.

### HTTP

`ws.http` keeps named native `httpx2.AsyncClient` instances alive in the Python kernel. Clients are private to the current logical client by default; pass `shared=True` when every client should use the same cookie jar and connection pool. The default HTTP timeout is 30 seconds. Client options are fixed after creation, so close a named client before changing its configuration:

```python
response = await ws.http.get("https://example.com/api", name="api")
response.status_code, response.json()

client = await ws.http.client("upload", base_url="https://example.com", timeout=10)
response = await client.post("/files", content=b"data")
await ws.http.close("upload")
```

`get()`, `post()`, `put()`, `patch()`, `delete()`, `head()`, and `options()` return native responses after consuming the body. They enforce a 16 MiB decoded body limit by default; set `max_bytes=None` only when the caller can safely handle an unbounded response. `stream()` yields the native streaming response for incremental processing. `download()` writes atomically (relative paths use the workspace), refuses to overwrite by default, and limits the decoded response to 256 MiB; pass `overwrite=True` or another `max_bytes` when appropriate. Cancellation removes incomplete downloads. If the target is published but temporary-file cleanup fails, the download still returns its target path; inspect `ws.http.last_warnings` for the current logical client's most recently completed download. The raw client returned by `client()` is an escape hatch for full HTTPX2 behavior and does not apply the convenience request limit.

`extract_html()` and `read_html()` accept `include_structure=True` to include heading hierarchy and bounded document metadata such as canonical URL, description, and language. The structure is optional and has its own `structure_truncated` flag. Headings use the selected content region while document metadata uses the full document.

### Web search

`ws.web` provides manager-owned web search through Kagi, Brave, and Tavily. Configure API key environment-variable names under `[web.providers.<name>]`; the manager reads the values from its own environment and never returns them to Python. Use `await ws.web.providers()` to inspect configured providers, readiness, supported operations, and provider-specific options without making a search request.

```python
await ws.web.providers()
page = await ws.web.search("Python structured concurrency", provider="brave", limit=10)
for result in page["results"]:
    print(result["title"], result["url"])
```

The provider is selected explicitly, then from `web.default_provider`, then from the only enabled provider. mypr never silently falls back to another provider or performs an additional search page request. `search()` returns normalized title, URL, and snippet records. `context()` returns bounded source excerpts where the selected provider supports it, and `extract()` reads content from one or more URLs where supported. Kagi and Tavily provide URL extraction; Brave provides contextual search. For direct HTTP fetching, cookies, custom headers, or browser-rendered content, use `ws.http.read_html()` or pass `page.content()` to `ws.http.extract_html()`.

All web results are bounded client-owned snapshots. Use `await ws.web.page(page["next_cursor"])` to continue reading the same response without another network request. `page_cursor` replays the current page with a different output budget. Page envelopes include `result_count` and `failed_count` for the complete snapshot. Normal pages set `truncated=False`; use `has_more` and `next_cursor` for continuation. Provider result pagination requires a new `search()` call. Fragments repeat source metadata and include text offsets; inspect `failed_results` and provider usage before assuming a response is complete. Authentication, quota, rate-limit, timeout, and provider errors remain distinct from an empty result. Web content is external data and must not be treated as agent instructions.

If the initial page cannot fit its metadata into the requested budget, the call returns an `output_limit` error with `error_info.details.page_cursor`; read that cursor with a larger `max_bytes` instead of repeating the provider request. A complete result larger than the manager snapshot limit fails without caching.

Provider-specific options are passed through `options` after manager-side validation. Kagi sends a POST v1 search request with `workflow="search"` and supports validated lenses, filters, page selection, personalizations, safe search, and optional inline extraction. Brave supports freshness, language, country, and search-page options; Tavily supports search depth, topic, time range, and domain filters. Tavily's deeper search modes and optional raw-content field can increase request cost or output size, so they are opt-in; generated answers are disabled by mypr.

See [web search and extraction](docs/web.md) and [configuration](docs/config.md#web-providers) for provider settings, capabilities, limits, and failure handling.

### Browser automation

`ws.browser` returns native async Playwright objects, so pages, locators, frames, requests, tracing, and other Playwright APIs remain available:

```python
context = await ws.browser.context(
    "shop", browser="chromium", launch_options={"headless": True}
)
page = await context.new_page()
await page.goto("https://example.com")
await page.get_by_role("button", name="Continue").click()
await ws.browser.screenshot(page, "artifacts/shop.png")
```

The first managed context automatically prepares the requested Playwright browser engine when it is missing, using the shared dependency service. The installation is recorded in dependency events and contributes to the active dependency count in `ws.status()` while it runs. A custom `launch_options` `channel` or `executable_path` uses that browser directly and bypasses managed engine installation. Installation runs as a managed workspace job and reuses the configured Playwright browser cache. Waiting for installation does not block unrelated Python cells or shell starts. A forced reset cancels pending managed browser startup and installation. Set `PLAYWRIGHT_BROWSERS_PATH` before starting the manager to select that cache. Managed browsers, contexts, and pages belong to the workspace runtime and are closed during reset. Use `ws.browser.close(...)` to release them earlier.

Use `save_state()` and `load_state()` to persist authentication explicitly; saved state includes IndexedDB by default and is kept under `.mypr/browser`. State is not saved automatically when a context closes:

```python
await ws.browser.save_state(context, name="login")
state = await ws.browser.load_state("login")
restored = await ws.browser.context("restored", storage_state=state)
```

`shared=True` gives all logical clients the same named context or connection; otherwise the name is private to the current client. Use `connect(endpoint, protocol="playwright"|"cdp", name="remote")` for an externally managed browser, then pass `connection="remote"` to `context()`. Closing or resetting mypr-mcp disconnects from external browsers and leaves their processes and pre-existing tabs running. `screenshot()` saves an artifact and returns it as inline image content, subject to the normal 2 MiB image limit. HAR and video paths supplied through Playwright context options are resolved below the workspace.

`observation = await ws.browser.observe(page)` records bounded request, response, console, page-error, and navigation events for that logical client. `await observation.read(cursor=None, types=None, url_contains=None, methods=None, status=None, wait_ms=0)` filters with AND semantics and can wait up to 30 seconds for a matching event. Cursors advance over inspected events, including events excluded by a filter; a returned `has_more` means more matching events remain. Response bodies are opt-in and capped independently. `await observation.request(request_id, body=True, body_timeout=5)` bounds a response-body read; timeout or an unknown/oversized body is returned as a `body_error`. Sensitive URL credentials and tokens are masked in structured URL fields.

### Network diagnostics and scans

`ws.net.resolve(host, port=None)` returns deduplicated IPv4/IPv6 addresses. `connect(host, port, timeout=3)` reports `open`, `closed`, `timeout`, or `unreachable` without raising for ordinary connection failures. `tls()` uses certificate and hostname verification by default and reports the negotiated TLS version, cipher, peer certificate, and SHA-256 fingerprint. Set `verify=False` only for diagnostics; `cert_pem` or `fingerprint` can pin the peer certificate.

TCP scans default to ports 1–1024, 64 concurrent connections, 200 connection attempts per second, and a one-second connection timeout. The native scanner needs no external binary or root privileges. It resolves each hostname once per scan and checks every unique resolved address; each result retains the original `host`, numeric `address`, `family`, port, and connection state. Use `family="ipv4"` or `family="ipv6"` to restrict the default `"any"` selection. Literal IPs bypass DNS, and literal addresses of the other family are excluded.

Set `protocol="udp"` for native UDP scanning. UDP defaults are ports 1–1024, 16 concurrent probes, 20 packets per second, a two-second response timeout, and one retry. `per_host_rate` defaults to one packet per second for UDP and is measured per numeric address and IPv6 scope; TCP has no per-address limit by default. UDP uses Linux `IP_RECVERR`/`IPV6_RECVERR` and `MSG_ERRQUEUE` when available, so an environment without those capabilities fails explicitly instead of silently weakening the scan. UDP does not require an external scanner or root privileges.

Use `probe="auto"` (the default), `"empty"`, `"dns"`, or `"ntp"`. Automatic probes send a non-recursive `mypr.invalid.` A query with a fresh transaction ID on port 53, an NTP version 4 client request on port 123, and an empty datagram on other ports. `payload` accepts up to 4096 bytes and overrides automatic probe selection; an empty `b""` is a meaningful payload, and combining a named probe with `payload` is invalid. `capture_response=True` stores a Base64 response prefix up to `response_bytes` (maximum 4096); response capture is disabled by default. `banner=True` is not supported for UDP.

UDP reports `open` for any response, including a zero-length response; `closed` for an ICMP port-unreachable error; `filtered` for an explicit administrative rejection; `unreachable` for other path errors; and `open|filtered` after all retries finish without a response. A probe profile does not identify the service. Every attempt consumes the global rate, any per-address rate, and the probe budget. `complete=True` means the selected address/port work finished, even when some results are `open|filtered`; `open_only=True` stores confirmed open rows but keeps all state counts. The overall deadline includes resolution, rate waits, sending, receiving, and retries. TCP scans, UDP scans, and Nmap runs are managed background jobs:

`resolve(timeout=5)` bounds DNS worker execution and cleanup. Scans reuse the guarded resolver with a five-second lookup limit. `scan()` accepts `max_probes` and `max_duration`; its overall deadline includes DNS, rate waiting, connections, retries, and banner reads. Cleanup may add bounded time after that deadline. Nmap accepts the same overall duration bound and preserves completed host results when the deadline ends. The output limit remains independent: by default collection stops when the result store is full; pass `continue_after_output_limit=True` only when partial result loss is acceptable.

Set `retries=1` to retry connection timeouts once, up to three retries per address and port. Every connection attempt, including retries, consumes the shared rate and probe budget. Refused connections are not retried. `banner=True` passively reads bytes sent by an open TCP service without sending application data; `banner_timeout=0.5` and `banner_bytes=1024` bound the read, with a maximum of 4096 bytes. A banner timeout or read error preserves the `open` connection state and adds `banner_error`, retaining any bytes already read. Binary banners use UTF-8 replacement decoding. Banner reads collect multiple TCP fragments and report `banner_bytes`, `banner_truncated` when excess bytes were observed, and `banner_complete` only after EOF. They do not identify services that require a client request or TLS handshake.

`open_only=True` stores only open ports while retaining counts for all connection outcomes. The summary separates `attempts` from `completed` address/port checks, reports `state_counts`, `resolve_errors`, and `discarded_results`, and sets `complete=True` only after the selected targets were exhausted without an error, limit, cancellation, or output loss. `estimate` counts address/port checks before retries; hostname estimates become available after all addresses are resolved and queued. DNS failures produce host diagnostics with `phase="resolve"` and `port=None` unless `open_only` is enabled; they do not count as connection attempts. Local socket resource or permission errors fail the scan instead of being reported as remote host failures. Inspect `complete`, `stop_reason`, and `error` before treating an empty result as evidence that no ports are open.

```python
scan = await ws.net.scan(
    ["127.0.0.1"], ports="22,80,443", concurrency=64, rate=200, timeout=1,
    retries=1, banner=True, open_only=True,
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

Awaiting a scan returns its terminal summary, including failed or cancelled states; check `state` and `error`. `result()` returns that summary once it is ready.

Scan handles support the normal task methods (`status()`, `read()`, `expect()`, `output()`, `result()`, `cancel()`, and `await handle`) as well as `summary()` and paged `results()`. `await ws.tasks.attach(scan_id)` reconnects to a retained scan after a reset or manager restart. TCP results and parsed Nmap results are retained under `.mypr/scans`; each result store is capped at 16 MiB and each page defaults to 100 entries and 32 KiB. Nmap owns its XML output channel, so output flags such as `-oX`, `-oA`, and `-oN` are rejected. The `--` separator is also rejected because mypr appends and controls the target arguments; pass scan options only. Install Nmap and arrange privileges explicitly when a scan requires them.

HTTP clients, managed browser resources, and active scans are attached to the workspace runtime. A reset closes clients and managed browser resources and cancels active scans; saved browser state, completed scan records, and files remain available afterward.

### Shell and async tasks

```python
await ws.shell.run(["git", "status", "--short"])
await ws.shell.run("make -j2", timeout=120, check=True)
await ws.shell.run(["sort"], input="bravo\nalpha\n")
```

`run(command, *, cwd=None, env=None, inherit_env=True, input=None, timeout=None, check=False, max_bytes=32768, pty=False, rows=24, cols=80)` waits asynchronously for completion and returns `id`, `state`, `returncode`, separate `stdout`/`stderr`, `timed_out`, and `truncated`. The byte budget is shared between stdout and stderr, with stdout first. The full retained combined output remains in `ws.tasks.get(result["id"]).output()`. A nonzero exit is returned normally; `check=True` raises `ShellError` with the result in `exception.result`. A timeout cancels the process group and returns `timed_out=True`; cancelling the awaiting cell also cancels the command. If persisted run or shell metadata is malformed, the operation remains readable with a bounded warning and an unknown outcome is treated as lost; inspect `warnings` before retrying a state-changing command.

`await ws.shell.start(command, *, cwd=None, env=None, inherit_env=True, input=None, stdin=False, pty=False, rows=24, cols=80)` starts a command in its own process group and returns a handle immediately. A string is interpreted by `/bin/sh`; a list executes argv directly. Standard input is closed by default. `input` supplies UTF-8 text and then closes stdin; `stdin=True` keeps the pipe open for later `await job.write(text)` calls. Use `await job.write(eof=True)` to close it. stdout and stderr are captured together in the handle's output.

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

PTY mode provides a controlling terminal and combines stdout/stderr into stdout. Terminal echo, ANSI sequences, and CRLF line endings are preserved. `write()` is available without `stdin=True`. `write(eof=True)` sends the terminal's EOF control character instead of closing its master descriptor; programs in raw mode decide how to interpret it. Use `cancel()` to terminate the job. `resize(rows, cols)` updates the terminal size and notifies its foreground process group.

The default `cwd` is the kernel's current directory. `env` overlays the kernel's environment; a `None` value removes that variable. Set `inherit_env=False` to start with an empty environment and use only the supplied values. This applies to both `run` and `start`, including PTY jobs. Existing callers that relied on replacement must pass `inherit_env=False` explicitly.

```python
ws.local["job"] = await ws.shell.start(
    ["python", "--version"],
    cwd=ws.workspace,
    env={"PYTHONUNBUFFERED": "1", "VARIABLE_TO_REMOVE": None},
)
```

`ws.tasks.start(awaitable, *, task_id=None, visible=True, persist_result=False)` schedules a detached awaitable in the kernel's event loop and returns a handle without `await`:

```python
import asyncio

ws.local["job"] = ws.tasks.start(asyncio.sleep(2, result="done"))
```

Set `persist_result=True` when a detached visible Python task's JSON result must remain available after the handle is evicted or the client reconnects. Only strict JSON values up to 256 KiB are stored; the task itself still succeeds when its result is not serializable, too large, or cannot be persisted, and `status()` reports `result_persisted` and warnings. `await job.wait_saved()` waits for the save attempt and returns `None` only after confirmed persistence; it raises `ResultUnavailable` for an unavailable, unserializable, unknown, or garbage-collected saved result. Historical `result()` remains the synchronous computation result and raises `ResultUnavailable` when no saved value exists.

Cell, remote-job, and Python-task handles share one ID namespace. Custom task IDs cannot replace existing handles or use generated ID forms: 32 lowercase hexadecimal characters, `task-…-<number>`, or the `remote-watch:` prefix. Automatic task IDs use a monotonically increasing counter without retaining old IDs in memory. Custom IDs remain reserved until kernel reset, including IDs used with `visible=False`.

Async tasks must yield to the event loop. Blocking or CPU-heavy work should run in a separate process, for example through `ws.shell.start(...)`. Use `ws.tasks.start()` for detached work that needs managed status and captured output. Raw `asyncio.create_task()` children are not managed; output emitted after their submitting cell finishes is not retained by that cell.

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
| `await job.wait_saved()` | Waits for persisted JSON; returns `None` or raises `ResultUnavailable` |
| `await job` | Waits for completion and returns the result |
| `await job.cancel()` | Returns `False` if already terminal, otherwise requests cancellation and returns `True` |

Async jobs return the awaitable's value and propagate its exception. Successful shell jobs return `{"returncode": 0}`; a failed shell job raises `RPCError` when its result is retrieved. Cancelled jobs raise `asyncio.CancelledError`. Waiting with `await job` suspends the current cell until completion while other runnable cells continue.

`read()` supports stdout/stderr selection and waits up to 30 seconds for output or completion. Its opaque cursor belongs to that job and stream; it is separate from the character cursor used by `output(cursor=...)`. Check `truncated` and `warnings` for output loss. `expect()` distinguishes a match, EOF, timeout, and scan limit. A match advances its cursor just past the matched text; an unmatched result keeps the starting cursor for retry. Cancelling a read or expect wait does not cancel the job.

`await ws.tasks.attach(task_id_or_history_id)` returns an existing live handle or reconnects to retained shell/package output. Historical Python and cell handles expose saved output and status but cannot restore Python values; `result()` raises `ResultUnavailable`. Use generation-qualified `history_id` for a specific historical Python task. Legacy records may only contain a limited output prefix.

Every submitted cell also has a task handle. Retrieve it with `ws.tasks.get(exec_id)`. Its status has `kind="cell"`, and `result()` returns the cell's actual last-expression value. `await` on a cell handle waits for that cell only; a cell cannot await its own handle.

Task IDs are shared across the kernel; omit `task_id` to generate one. An explicit ID can be reused after reset. Each kernel generation keeps a separate history record: `await ws.history.get(task_id)` returns the latest record, while `await ws.history.get(record["history_id"])` retrieves a specific generation. The `python:<generation>:...` history-ID namespace is reserved. `ws.tasks.list()` includes completed visible tasks, while `ws.tasks.active()` returns active handles. `visible=False` omits an async task from discovery, history reporting, and the reset guard; keep the default for managed work.

### MCP services

External MCP servers use global defaults with workspace overrides. The workspace layer is stored in `.mypr/config.toml`; the global layer is selected by `MYPR_GLOBAL_CONFIG` or the user's XDG configuration directory. Use `ws.config` to inspect layers and apply persisted changes explicitly. A workspace override replaces a server entry as a whole; `enabled = false` disables an inherited server and `ws.config.unset()` reveals the global entry again. See [layered configuration](docs/config.md).

```toml
[mcp.servers.reports]
command = "reports-mcp"
cwd = "/absolute/path/to/workspace"

[mcp.servers.reports.env_from]
REPORTS_TOKEN = "REPORTS_TOKEN"
```

The stdio service uses `command`, optional `args`, optional `cwd`, and optional `env_from`. `command` can also be an argument list. HTTP services use `url`, optional `headers_from`, and are connected lazily. Environment mappings name variables in the manager's environment; their secret values are not written to the config. A stdio server's default `cwd` is the workspace; a relative `cwd` is resolved against it.

For an HTTP server:

```toml
[mcp.servers.remote]
url = "https://example.com/mcp"

[mcp.servers.remote.headers_from]
Authorization = "REMOTE_AUTHORIZATION"
```

`REMOTE_AUTHORIZATION` must contain the complete header value, including any required scheme such as `Bearer `.

The API is:

```python
await ws.mcp.list_servers()
await ws.mcp.list_tools("reports")
await ws.mcp.call_tool("reports", "fetch", {"id": "42"})
await ws.mcp.list_resources("reports")
await ws.mcp.list_resource_templates("reports")
await ws.mcp.read_resource("reports", "reports://latest")
await ws.mcp.list_prompts("reports")
await ws.mcp.get_prompt("reports", "summary", {"id": "42"})
```

Remote results are dictionaries serialized from MCP models. Tool responses may contain `content`, `structuredContent`, and `isError`; check `isError` before using a tool result. A tool-reported error can be returned normally, while a transport or bridge failure raises `RPCError`.

Tool, resource, and prompt listing methods require a server name and accept `cursor=` for pagination. Pass the response's `nextCursor` to the next call when it is non-null. Server discovery uses `servers` and `next_cursor` instead; for additional pages use `await ws.mcp.request("list_servers", cursor=next_cursor, limit=50)`. Server-list cursors must be non-negative integers or decimal strings, and limits must be integers from 1 to 1000. Invalid values are rejected.

Calls on the same external MCP connection may run concurrently, with each response kept with its requesting cell. Start a call with `ws.tasks.start(...)` when it should remain detached while the kernel accepts later cells. Calls whose completion or external side effect is uncertain are not retried automatically. Authentication variable names refer to the manager's environment; changing their values still requires restarting the manager.

Add or replace a server directly from the kernel without resetting Python:

```python
await ws.mcp.configure("reports", {
    "command": "/absolute/path/to/server-venv/bin/python",
    "args": ["/absolute/path/to/reports_server.py"],
})
await ws.mcp.list_tools("reports")
```

`configure` persists a complete workspace replacement of that server's configuration. To change selected fields, read the active configuration first:

```python
ws.local["server_config"] = await ws.mcp.get_config("reports")
ws.local["server_config"]["args"] = ["/absolute/path/to/new_server.py"]
await ws.mcp.configure("reports", ws.local["server_config"])

await ws.mcp.restart("reports")  # Reload edited server code using the active config.
await ws.mcp.reload()            # Apply direct edits to config.toml's MCP servers.
await ws.mcp.remove("reports")  # Disconnect and remove the saved server entry.
```

The shared kernel, variables, `ws.local`, and unrelated server connections stay alive. New or changed configurations connect lazily on the next call; `restart` establishes a fresh initialized connection. It does not reread the config file. This lets an agent write or improve a local MCP server, reconnect it, and use its updated tools in the same Python session.

Initial connection and protocol initialization have a 30-second deadline, including lazy first use. A timeout fails queued calls instead of leaving that server blocked indefinitely; a later request can retry once cleanup finishes. This startup deadline does not limit the duration of an initialized server's tool calls.

Changes affecting active or queued calls are rejected by default. Use `force=True` on these management methods to close the affected connections and fail their pending calls. Other servers remain usable. Closing a connection does not undo completed external side effects.

Configuration is validated before changes are applied. Writes are atomic and preserve unrelated TOML sections and comments. If the file changed since the last load, `configure` and `remove` ask for `reload()` rather than overwriting those edits. `ws.config.set()` and `unset()` persist without applying; call `ws.config.reload()` explicitly. Reload applies only added, changed, or removed MCP entries; unchanged connections stay open. A valid configuration does not guarantee that its server can start or authenticate: connection failures are reported when connecting. Management changes and their outcomes are recorded in workspace history. Once accepted, a management operation completes even if its caller disconnects or stops waiting; inspect configuration and history to confirm the outcome.

### Client-local state and history

The first `init()` call assigns a readable logical client ID such as `calm-otter` or `swift-fox`. It randomly combines one of 256 adjectives with one of 256 animal names, giving 65,536 possible IDs per workspace. Automatically issued IDs are reserved in `.mypr/history.sqlite3` and never reused while that database is retained, even after disconnects or manager restarts. Explicit IDs can create a new client or resume an existing one; IDs in history remain reserved. If every automatic combination has been used, allocation fails with an explicit exhaustion error.

Connection IDs remain random UUIDs. Read the bound identity through `ws.client.id` and this particular connection through `ws.client.connection_id`. Before `init`, the connection has no logical client ID. These IDs identify callers for attribution and coordination; they are not authentication or security boundaries.

The shared Python namespace is deliberately common to every client. Use the client identity and local mapping for values that belong to the current agent:

```python
ws.client.id
ws.local["review_job"] = ws.tasks.start(do_review())
```

`ws.local` is persisted in the running kernel and is namespaced by logical client ID. `ws.client.id` identifies the caller that submitted the current cell; `connection_id` identifies this particular MCP connection. They prevent accidental name reuse only when code follows the `ws.local` convention; ordinary globals remain shared. Reconnecting with the same ID resumes the same local dictionary while the kernel remains alive.

Use `await ws.status()` for compact manager and kernel health, version, workspace identity, generation, and connection, active, and queued counts. `connection_count` includes connections waiting to call `init`; `client_count` includes only initialized logical clients. Pass `detail=True` to include connection records, active and queued execution IDs, and manager instructions. Use `await ws.history.list(...)` and `await ws.history.get(exec_id)` to inspect execution records from Python. History records include the logical client and connection IDs, timestamps, state, and output metadata. A task inherits its creator's identity even while another client executes. IDs are organizational labels, not access-control boundaries.

```python
state = await ws.status()
state["connection_count"], state["client_count"]
state["active_count"], state["queued_count"]
details = await ws.status(detail=True)
details["connections"]
await ws.history.list(client_id=ws.client.id, limit=20)
await ws.history.get(exec_id)  # Also accepts a background task ID.
await ws.history.logs(client_id=ws.client.id, limit=20)
```

History methods accept `limit=1..200` (default 20). Omit `client_id` to include all callers. Continue `list()` with its `next_cursor` until it is null. For logs, reuse the returned cursor with the same filter:

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

Without a cursor, `logs()` returns recent events; `cursor=0` starts at the beginning. Log cursors, history-list cursors, task-output cursors, and MCP `poll` cursors belong to different APIs and must not be interchanged.

`await ws.performance()` returns rolling timing summaries for manager, storage, kernel, and bridge work. Each label keeps its latest 256 samples and a `total_count` observed since manager start, with `p50`, `p95`, and `max` in milliseconds. Bridge timing for the most recent completed request appears on the next call; concurrent or disconnected calls may not all be reported. Execution results include `timing_ms` in MCP `_meta` (the Python SDK exposes `result.meta`), outside printed content and `structuredContent`. Request errors raised before a result is produced may lack these timings. Manager and RPC times include requested notification waits and are not overhead-only measurements. Nested spans overlap, so do not sum them to infer unmeasured overhead; kernel round-trip includes IPC and output persistence, not only Python execution. Time outside the MCP server, including host scheduling, external transport, and model execution, is not measured. Samples remain in memory until manager restart.

See [the performance benchmark](benchmarks/README.md) for reproducible request latency, output size, and before/after comparisons.

`connections` contains connection and activity timestamps, active execution IDs, and owned task IDs. Open IPC connections determine liveness, so a killed client is removed without cancelling its workspace jobs.

History is stored in `.mypr/history.sqlite3`. Lists return `items` and `next_cursor`; logs return ascending events and a cursor for subsequent reads. Execution details include a paged output view (continue with MCP `poll`). Background task details retain up to 64 KiB of output and mark truncation; live handles retain their normal output buffers. Logs stream cell, shell, and Python task output while work is running. Malformed persisted history entities or event data are retained as readable corrupt records or events with bounded `warnings`; unavailable fields are omitted and later valid records remain visible. Inspect these warnings before treating a history query as complete.

### Coordinating shared work

```python
async with ws.locks.acquire("module:review", "file:report", timeout=10):
    await ws.fs.write("report.txt", "Done.\n", overwrite=True)
ws.locks.list()
```

Locks belong to the current asyncio task. Names are normalized and acquired together; reacquiring while the same task owns locks raises an error. Leaving the context, completing or cancelling the task, or resetting the kernel releases them. Disconnecting a client does not end its running tasks or release their locks. `list()` shows owners and waiters. These are cooperative locks: filesystem writes and other clients must explicitly use the same names to participate.

### Client messages

Every logical client has a persistent inbox in the workspace SQLite history. Messages remain available when the recipient is offline, across Python resets, and after a manager restart. The recipient must already be registered in the workspace, but does not need to remain connected.

```python
await ws.messages.send("bright-fox", "The review is complete.")
ws.local["inbox"] = await ws.messages.read()
# Handle the messages before acknowledging them.
await ws.messages.ack([message["id"] for message in ws.local["inbox"]["messages"]])
```

`send(to, text, data=None, reply_to=None)` uses the current logical client as the sender and accepts non-empty text up to 16 KiB in UTF-8. It returns `id`, `from`, `to`, `text`, and `created_at`; text whose JSON escaping would exceed a 32 KiB page is rejected. `data` accepts bounded JSON-serializable values. `reply(message_id, text, data=None)` sends to the original sender and records `reply_to`; only the original recipient can reply, including after acknowledging the original message. `read(limit=20, after=None, wait_ms=0, sender=None, reply_to=None)` returns unacknowledged messages in ID order with `messages`, `next_cursor`, and `has_more`. The limit is 1–100, the serialized page is at most 32 KiB, and `wait_ms` is limited to 30 seconds. Use the returned `next_cursor` as `after` to continue paging. A non-zero `wait_ms` waits only when no messages match both the cursor and the optional sender/reply filters. Unrelated messages do not end a filtered wait. If stored message data is malformed, that message remains readable with `data=None`, `data_corrupt=true`, and a bounded warning; the rest of the page is still returned.

`ack(ids)` explicitly acknowledges messages belonging to the current client. Acknowledgement is idempotent, and reading or previewing a message never marks it as read. It returns the number newly acknowledged; unknown or foreign IDs reject the entire batch. These methods require an initialized client because messages are scoped to its logical ID.

The MCP responses from `init`, `execute`, and `poll` include up to five short inbox previews (within a 4 KiB budget), plus the total unacknowledged count. The `inbox` field contains `unacked`, `messages`, and `has_more`; each preview has `id`, `from`, `text`, `reply_to`, and `truncated`. Structured `data` is omitted from previews; use `read()` for full content. Messages can end an `execute` or `poll` wait early without cancelling the cell: check its state and continue polling if needed. Polling another client's execution still returns your own inbox. Before `init`, `poll` omits the inbox. They do not acknowledge the previews automatically. An agent that is not calling an MCP tool is not woken when a message arrives; use `read(wait_ms=...)` from a running Python task when a bounded wait is useful.

`await ws.messages.clients(prefix=None, connected=None, limit=50, cursor=None)` lists registered logical clients with their current connection state, last activity, registration time, and unacknowledged message count. Use it before addressing a peer whose ID is not already known. The list is paged with `next_cursor`; offline clients remain addressable for persistent delivery.

### Timers

Timers are persistent, one-shot deadlines owned by the current logical client. Schedule a duration or an absolute timezone-aware deadline:

```python
ws.local["timer"] = await ws.timers.start(seconds=3600, label="review")
await ws.timers.start(at="2026-10-02T09:00:00+09:00", label="check")
```

Use `await ws.timers.check(timer_id)` or `await ws.timers.list()` to inspect timers, `await ws.timers.cancel(timer_id)` to cancel a scheduled timer, and `await ws.timers.ack([timer_id])` after handling an expired alert. `list(state=None, limit=50, cursor=None)` returns bounded pages with `items`, `has_more`, and `next_cursor`, including acknowledged records. Timer states are `scheduled`, `expired`, and `cancelled`; acknowledgment is tracked separately.

Durations use the manager's UTC wall clock and continue to elapse while the manager is stopped. Zero duration or a past deadline expires immediately; absolute deadlines must include a timezone. Timers and acknowledgments survive disconnect, kernel reset, and manager restart. Reconnect using the same logical client ID to resume them.

Expired, unacknowledged timers are attached automatically to initialized clients' `init`, `execute`, and `poll` responses in the `timers` field and in readable response text. This field contains `unacked`, up to five previews in `items` within a 4 KiB JSON budget, and `has_more`. Alerts repeat until acknowledged and may end a tool's wait while Python execution continues; no tool call means no agent wake-up. In a normal response from a timer-capable runtime, the absence of a `timers` field means there were no unacknowledged expired timer alerts. Polling before `init` omits timer alerts.

### Mail

Mail access is provided by the manager through configured IMAP and SMTP accounts. Start by checking the account and connection state; account passwords are referenced by environment variable name in configuration and are never sent through Python calls or returned in results:

```python
await ws.mail.accounts()
await ws.mail.status()
page = await ws.mail.search(mailbox="INBOX", unread=True, limit=20)
message = await ws.mail.read(page["items"][0]["id"])
```

Search returns bounded header pages with opaque message references. `read()` fetches a bounded parsed body and attachment metadata without marking the message as seen. Use `ws.mail.download_attachment(message_id, attachment_id, path)` for a workspace file with overwrite protection. Message references are tied to the account's IMAP namespace and become invalid after a UIDVALIDITY or account identity change. `mark_read()` and `mark_unread()` change server flags explicitly.

Outgoing addresses preserve display names and convert international domain names using IDNA2008 with nontransitional UTS46 processing. Distinct domains such as `faß.test` and `fass.test` remain distinct, and valid IPv4/IPv6 domain literals are preserved. Mailbox local parts must be ASCII; drafts with international local parts are rejected because SMTPUTF8 is not supported.

Create a draft before sending. Drafts are immutable and are validated and persisted before any network delivery:

```python
draft = await ws.mail.draft(
    to=["recipient@example.test"],
    subject="Review complete",
    text="The requested review is complete.",
)
send = await ws.mail.send(draft["id"], request_id="review-2026-10-02")
```

`reply_to` and `forward` create derived drafts using the original message's account unless another account is specified; they cannot be combined. Attachments must resolve inside the workspace. Incoming reads return at most 64 attachment metadata records and include `attachment_total` plus `attachments_truncated` when metadata was omitted. Forwarding attachment data is capped at 32 attachments; forwarding a message whose attachment data is incomplete is rejected, and attachments are preserved as separate parts without deduplication. Draft MIME and individual attachments are limited to 25 MiB. `send()` returns a queued record; inspect `await ws.mail.get_send(send["id"])` until it settles. Omitting `request_id` uses a draft-bound key, and reusing a key returns its existing send record. Retry a definitively `failed` send with a new key. `accepted` means SMTP acceptance, and `partial` lists recipients the server refused. Both prevent resubmitting that draft. `unknown` means acceptance could not be confirmed and also prevents resubmission; verify delivery outside mypr before creating another draft. `sent_mailbox` opts in to an append copy whose failure does not undo SMTP acceptance.

Cancellation waits for blocking mail work to settle before releasing the account's active-work tracking. A confirmed SMTP outcome remains `accepted` or `partial` even when the awaiting task is cancelled; account reconfiguration stays deferred while the underlying operation is running.

Mail watches belong to the current logical client and share manager connections across clients:

```python
watch = await ws.mail.watch(mailbox="INBOX")
notifications = await ws.mail.notifications()
await ws.mail.ack([item["id"] for item in notifications["items"]])
```

Watch cursors and notifications survive disconnects, reset, and manager restart. New arrivals appear as bounded `mail` previews on later `init`, `execute`, and `poll` responses and may end a tool wait early; the Python execution continues. A preview or notification acknowledgment does not mark a message read. Use `watches()` to inspect synchronization and errors and `unwatch(watch_id)` to remove a subscription. The `mail` field is absent when there are no cached notifications, pending or uncertain sends, or reportable watch issues; that absence does not prove that a mailbox is empty. Watches use IMAP IDLE when supported and otherwise poll every 30 seconds. Watch sockets close when all subscribed clients disconnect and catch up when a subscriber reconnects. Unsent drafts, active sends, uncertain outcomes, and unacknowledged notifications remain protected from GC; terminal MIME and acknowledged notification records become eligible after 30 days.

### Skills and reusable Python

Workspace skills live at `.mypr/skills/<name>/SKILL.md`. Discover and read them with:

```python
await ws.skills.list()
ws.skills.read("review")
```

`await ws.skills.list(limit=100, max_metadata_bytes=65536, max_response_bytes=65536)` discovers nested names such as `group/review` and returns bounded YAML metadata plus each file's `path`. The item limit, per-skill metadata budget, and aggregate response byte budget are enforced; a response that stops early marks its last item with `list_truncated` and `omitted`. The returned `name` is always its relative directory path and can be passed directly to `read()`. A different front-matter name is preserved as `declared_name`. Malformed YAML, invalid metadata types, and front matter exceeding the inspection budget are reported on that item through an `error` field while other skills remain available. Metadata shortened to fit structural or response limits is marked with `metadata_truncated`. Files resolving outside `.mypr/skills` are skipped. Internal links to skill files or leaf skill directories work; directory symlinks are not recursively expanded. `read(name, max_bytes=1048576)` returns bounded Markdown; pass `max_bytes=None` explicitly to read the whole file. Both read from disk, so edits are visible on the next call without a reload.

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

Skill text is interpreted by the agent; reading it does not execute its instructions or scripts. `validate()` accepts legacy Markdown without front matter with a warning, rejects malformed YAML and invalid metadata types, and reports missing or escaping local Markdown links. Existing skills require `expected_hash=page["revision"]` when updated. Pass `dry_run=True` to preview a bounded diff without writing.

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

`modules.list()` is synchronous; `read()`, `check()`, and `write()` are async. Module names are public dotted Python names and cannot escape `ws_lib`. `check()` compiles the candidate and runs optional test code in the workspace Python environment with its own timeout. It returns the SHA-256 of the checked source. `write()` validates syntax, uses an atomic CAS write, and never activates the module. `load()` and `reload()` accept `expected_hash`; when supplied, they execute only the exact bytes returned by `check()`, so a source edit between verification and activation is rejected. Cold activation executes the target module once even when its parent package imports it during initialization. They bind a fresh module only after successful execution; failed reloads leave the old module binding intact. References already held elsewhere keep pointing to the previous module after a successful reload. Imports can have external side effects; a failed reload does not undo those side effects.

Module and skill writes retain content revisions. Use `await ws.modules.history(name)` or `await ws.skills.history(name)` to list revisions, `read_revision(name, revision)` to read one, and `restore(name, revision, expected_hash=current_revision)` to restore a file. Restoring a module does not reload it; existing Python references continue to point to the loaded code. See [revision history](docs/revisions.md) for paging, consistency, and recovery behavior.

### Packages and inspection

`await ws.packages.add(*specs)` installs package requirements into the workspace's `.mypr/venv` and returns a background handle. For example:

```python
ws.local["install"] = await ws.packages.add("httpx", "rich>=13")
ws.local["install"].status()
```

Retrieve the result in a later cell with `await ws.local["install"]` before importing the new packages. Installation changes the kernel environment, not the environments of separately launched MCP servers.

The installation uses `uv pip` and writes the resulting freeze to `.mypr/requirements.txt`. Installation, freeze, and manifest replacement are serialized per workspace. Failures before manifest replacement leave the previous manifest intact. If replacement succeeds but directory durability cannot be confirmed, the job succeeds with a `manifest_durability_unknown` warning in its status and saved output. Package installation can already have changed the environment before a later freeze or manifest failure. Packages already imported by the current kernel may need a kernel reset before an upgrade is visible.

Built-in features use the dependency service for the registered optional packages listed in [Dependencies](#dependencies). Those packages are prepared automatically when `dependencies.auto_install` is true, and existing compatible versions are preserved. Use `await ws.dependencies.ensure("pymupdf")` to prepare one explicitly; `ws.packages.add(...)` remains the API for arbitrary package requirements.

`await ws.inspect()` returns the workspace path, kernel generation, visible variable names and types, task summaries, and discovered skills. `await ws.status()` returns compact manager health and counts; pass `detail=True` for connection records, active and queued execution IDs, and manager instructions.

`await ws.doctor()` checks whether the workspace is ready for the requested workflow: Python packages, registered dependency tools and models, search backends, configured LSP servers, browser engine, OCR language data, MCP configuration, runtime workers, and storage. Checks are reported as ready, missing, invalid, or unknown with a bounded reason. Doctor does not install packages or dependency artifacts or modify configuration. The same check is available before a manager starts with `uvx mypr-mcp doctor`.

`await ws.storage.usage()` reports managed, protected, and whole-workspace disk summaries through a metadata-only scan; it does not hash or read file contents. The legacy `total_bytes`, `total_files`, and `categories` fields cover logical bytes in known managed categories. Each summary also reports `logical_bytes`, `allocated_bytes`, `unique_inodes`, `hardlinks`, `has_hardlinks`, and `truncated`; logical bytes count paths, while allocated bytes count each device/inode once within the summary. Hard-linked paths can therefore make logical totals larger than physical allocation, and adding managed and protected allocation can double-count a cross-group hardlink. Use `await ws.storage.gc(dry_run=True)` to create a deletion plan and `await ws.storage.gc_apply(plan_id)` to apply that exact plan. Automatic GC runs periodically and removes expired data using the 30-day policy; when managed data exceeds the soft 1 GiB target, it also selects the oldest eligible recent data until the target is reached. Active jobs, current files, the latest `revision_keep` revisions per resource (50 by default), and referenced shared blobs are protected. Set the policy as global defaults or workspace overrides, then apply it with `await ws.config.reload()`:

```toml
[storage]
enabled = true
retention_days = 30
max_bytes = 1073741824
revision_keep = 50
gc_interval_seconds = 300
```

GC can also compact old history bodies, execution JSON bodies, and events. Terminal records past the retention cutoff lose bulky code, output, and event bodies while retaining entity IDs, request-deduplication fields, and a `code_sha256` for removed source. Matching execution JSON metadata loses the same bulky fields; the database report includes `json_compacted` and reports metadata-write failures in `errors`. Restart preserves eviction markers and the retention age of unchanged terminal records. Each pass plans and applies at most 1,000 entities and 1,000 events, revalidating hashes and timestamps. `ws.history.logs()` reports `history_truncated` and a monotonic `pruned_through_seq` watermark when earlier event rows were removed, and cursors continue across the pruned range. Mail send cursors use a stable `send_seq` column; startup migrates legacy rowids before compaction so existing cursors survive. VACUUM is rate-limited to once per day and requires a completed mail cursor migration, a database of at least 16 MiB, at least 4 MiB and 25% free pages, a successful WAL checkpoint, and sufficient filesystem space. Message, timer, client, mail reference, watch, and send identity rows remain outside this history-body compaction; mail's own retention rules still govern eligible MIME files and acknowledged notifications.

`await ws.status()` includes `storage_maintenance` with `running`, `last_run`, `last_deleted_bytes`, and `last_error` for the automatic pass.

### Workstation resources

`ws.system` collects a bounded snapshot of the workstation visible to the kernel. Collection runs in short-lived guarded helper processes, so vendor utilities or a slow filesystem do not block other Python cells. The result of each method is a JSON-compatible envelope with `collected_at`, `duration_seconds`, `scope`, `sources`, `warnings`, and `truncated` fields.

```python
info = await ws.system.info()
usage = await ws.system.usage(interval=0.5)
processes = await ws.system.processes(
    sort="rss", limit=10, interval=0.5, cmdline=True
)
gpus = await ws.system.gpus(processes=True)
workspace_disk = await ws.system.disks(path=".")
```

The `info` envelope contains `os`, `python`, `cpu`, `memory`, `storage`, and `limits`. `usage` contains interval-based `cpu`, `memory`, `swap`, per-interface `network`, per-device `disk_io`, `disks`, and `gpus` data. `processes` returns a bounded `processes` list with PID, creation time, status, CPU percentage, RSS, and I/O counters; `sort` accepts `cpu`, `rss`, `read`, or `write`, and `limit` is capped at 200. The `pids` and `user` filters narrow that list, while `cmdline` is false by default. `gpus` reports available vendor metrics and optional GPU processes. `disks(path=...)` reports filesystem capacity for the workspace-relative path (or an absolute path).

All sizes are bytes and all rates are bytes per second. Whole-machine CPU usage is reported from 0 to 100%; process CPU usage uses one logical CPU as 100%, so a multi-threaded process can exceed 100%. Unsupported or inaccessible values are `None`, with the reason recorded in `warnings` or `sources`, rather than being reported as zero. A missing driver, inaccessible process, or unavailable vendor utility does not discard other sections. NVIDIA metrics use `nvidia-smi`; AMD uses `amd-smi`; Intel uses `xpu-smi` and, where available, `intel_gpu_top`, with Linux DRM/sysfs fallback. The API does not install operating-system tools automatically. Vendor utilization uses the driver's measurement window; DRM client counters use the requested interval. DRM engine activity and client memory are separate from whole-device utilization and VRAM. Shared DRM descriptors are counted once, with their owning PIDs listed. Vendor process PIDs can refer to a different PID namespace from the Python kernel.

The snapshot uses the kernel's PID namespace and filesystem view. `limits` separately reports CPU affinity and cgroup v2 quota or memory headroom when discoverable; these are execution limits and available headroom, not a reservation of those resources. Responses are limited to 32 KiB; `truncated` and `omitted` identify details removed to fit that budget. A timeout stops the guarded helper, and cleanup can make the completed call slightly longer than the requested deadline.

The default measurement interval is 0.5 seconds and must be between 0.1 and 10 seconds. The default timeout is 5 seconds and must exceed the interval. Repeated snapshots are best scheduled as ordinary async cells or background tasks; the API does not create a resident monitor or retain historical samples.

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
