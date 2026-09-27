# Code navigation

`ws.code` connects to an installed Language Server Protocol (LSP) server for read-only code navigation. It does not install servers. A configured command runs with the workspace as its working directory and has the current user's permissions.

```python
await ws.code.configure(
    "clangd",
    command=["clangd", "-j=2"],
    languages=["c", "cpp"],
)

await ws.code.definition("clangd", "src/main.cpp", line=12, character=8)
await ws.code.references("clangd", "src/main.cpp", line=12, character=8)
await ws.code.hover("clangd", "src/main.cpp", line=12, character=8)
await ws.code.diagnostics("clangd", "src/main.cpp")
ws.code.status()
await ws.code.close("clangd")
```

`configure(name, command, languages, timeout=10)` starts and initializes a server. Use an explicit stdio command such as `["pyright-langserver", "--stdio"]`; the executable must already be installed. Repeating an identical configuration reuses its process. Reconfiguring a name starts the replacement before closing the previous server. At most four servers can be configured per Python kernel.

Paths may be workspace-relative or absolute, but must resolve inside the workspace. Files must be UTF-8 and no larger than 2 MiB. The server receives the full current file on first use and a full-text `didChange` notification after edits. Open documents are capped at 128; least recently used documents are closed when the cap is reached.

`line` and `character` are one-based Unicode code-point coordinates, so the first character is `(1, 1)`. mypr translates them to LSP's negotiated UTF-16 positions and converts returned ranges back. `language` can override the file-extension mapping when the server supports that LSP language ID.

Definitions and references return at most 500 locations and mark truncated results. Hover text and diagnostics are bounded. Diagnostics include the document version. A response with `ready: false` and `diagnostics: null` means the server has not published a current snapshot yet. Its `state` is `pending`, `stale`, or `version_unknown`; only an empty list with `ready: true` means it reported no diagnostics for that revision. For push diagnostics, `wait_ms` is the total deadline for a snapshot matching the current document version. Stale or unversioned notifications do not end the wait or extend that deadline; if it expires, the response preserves the latest known state. Servers that advertise pull diagnostics are queried directly.

Only definition, references, hover, diagnostics, status, and lifecycle operations are exposed. Workspace edits requested by a server are declined. `await ws.code.close()` stops all configured servers; Python reset also closes them.

Protocol details follow [LSP 3.17](https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/).
