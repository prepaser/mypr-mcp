# Code navigation

`ws.code` connects to an installed Language Server Protocol (LSP) server for code navigation and revision-checked edit previews. It does not install servers. A configured command runs with the workspace as its working directory and has the current user's permissions.

```python
await ws.code.configure(
    "clangd",
    command=["clangd", "-j=2"],
    languages=["c", "cpp"],
)

await ws.code.definition("clangd", "src/main.cpp", line=12, character=8)
await ws.code.references("clangd", "src/main.cpp", line=12, character=8)
await ws.code.hover("clangd", "src/main.cpp", line=12, character=8)
await ws.code.document_symbols("clangd", "src/main.cpp", max_bytes=32768)
await ws.code.workspace_symbols("clangd", "Widget", max_bytes=32768)
await ws.code.calls("clangd", "src/main.cpp", line=12, character=8, direction="incoming", max_bytes=32768)
await ws.code.diagnostics("clangd", "src/main.cpp")
await ws.code.rename("clangd", "src/main.cpp", line=12, character=8, new_name="Widget2")
await ws.code.actions("clangd", "src/main.cpp", line=12, character=8)
ws.code.status()
await ws.code.close("clangd")
```

`configure(name, command, languages, timeout=10, persist=True)` starts and initializes a server. Use an explicit stdio command such as `["pyright-langserver", "--stdio"]`; the executable must already be installed. With `persist=True`, the command, language IDs, and timeout are saved as a workspace configuration override and restored lazily after reset or restart. Global defaults can be inspected with `ws.config.get(scope="global")`; use `ws.config.reload()` after persisted configuration changes. Repeating an identical configuration reuses its process. Reconfiguring a name starts the replacement before closing the previous server. At most four servers can be configured per Python kernel. A failed save leaves the previous configuration active. Once a save commits, the matching runtime configuration is applied before caller cancellation is reported; inspect `status()` or reload before retrying an uncertain request.

Paths may be workspace-relative or absolute, but must resolve inside the workspace. Files must be UTF-8 and no larger than 2 MiB. The server receives the full current file on first use and a full-text `didChange` notification after edits. Open documents are capped at 128; least recently used documents are closed when the cap is reached.

`line` and `character` are one-based Unicode code-point coordinates, so the first character is `(1, 1)`. mypr translates them to LSP's negotiated UTF-16 positions and converts returned ranges back. `language` can override the file-extension mapping when the server supports that LSP language ID.

Definitions and references return at most 500 locations and mark truncated results. Document symbols preserve their parent/child tree; workspace symbol search returns bounded file locations. These three APIs accept `max_bytes` from 512 bytes through 1 MiB, defaulting to 32 KiB. Call hierarchy takes `direction="incoming"` or `"outgoing"` and returns one hop only. If a position resolves to overloaded or otherwise ambiguous symbols, each candidate has its own result group. It shares the configured server timeout across preparation and all candidate requests, then allows up to 100 ms for a best-effort LSP cancellation notification when timed out or canceled. A timeout can therefore take up to that cleanup grace beyond the query deadline. These features require the server to advertise `documentSymbolProvider`, `workspaceSymbolProvider`, or `callHierarchyProvider`; an unsupported feature raises `CodeError` instead of looking like an empty result. Structured navigation results identify their one-based Unicode code-point coordinate system and document version when the file is open in LSP. Each converted range reports whether source text was available; unavailable text yields `range: null` with `source_unavailable`, rather than silently dropping the symbol or call. Hover text, symbol results, calls, and diagnostics are bounded and mark truncation. Diagnostics include the document version. A response with `ready: false` and `diagnostics: null` means the server has not published a current snapshot yet. Its `state` is `pending`, `stale`, or `version_unknown`; only an empty list with `ready: true` means it reported no diagnostics for that revision. For push diagnostics, `wait_ms` is the total deadline for a snapshot matching the current document version. Stale or unversioned notifications do not end the wait or extend that deadline; if it expires, the response preserves the latest known state. Servers that advertise pull diagnostics are queried directly.

`rename()` and `actions()` return bounded edit metadata and do not change files. Use `prepare_action(action_id)` to resolve one selected action, then inspect its revision-checked preview and call `apply_edit(plan_id)`. Inline code actions also retain the revisions or absence of all referenced files at listing time; changed targets invalidate the action before a plan is created. Targets first disclosed by `codeAction/resolve` are captured when that response arrives. Rename previews and code actions remain tied to the document version and contents used by the LSP request; a later document or disk change requires a fresh request. Code action IDs expire after one hour, with the oldest IDs evicted after 1,024 retained actions; closing or replacing their server invalidates them. Edit plans are stored under `.mypr/change-plans` and survive a kernel reset or manager restart until their TTL expires. Newly prepared plans retain file revisions and open-document versions, including the request document when the edit changes only another file; `apply_edit()` rechecks these conditions before committing. Removed, expired, or evicted plans cannot be applied from an in-memory cache. Once a validated plan successfully changes files, concurrent plan removal does not turn that success into an expired-plan error. Resource operations are interpreted in order and committed atomically using the final file states. Each existing edit target is limited to 2 MiB. Workspace edits are limited to UTF-8 regular files inside the workspace; overlapping edits, stale document versions, unsupported resource operations, and edits requiring command execution are rejected. mypr never executes LSP commands or server `workspace/applyEdit` requests. `workspace_diagnostics()` requests project-wide diagnostics only when the server advertises that capability; push-only servers are not treated as clean workspace diagnostics. `await ws.code.close()` stops all configured servers without deleting persistent configuration; Python reset also closes their processes. A process guard cleans up the server tree when its Python client exits, including an unexpected kernel exit.

Protocol details follow [LSP 3.17](https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/).
