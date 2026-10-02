# Layered configuration

`ws.config` controls the managed configuration for the current workspace. It exposes the global defaults and the workspace overrides without making Python code edit TOML directly. The known managed sections are `mcp`, `lsp`, `mail`, `limits`, `storage`, and `dependencies`; unrelated TOML data remains outside this API.

## Create a configuration file

[config.example.toml](../config.example.toml) lists every supported field, the built-in limits and storage defaults, and examples for stdio MCP, HTTP MCP, LSP, mail accounts, and disabled inherited entries. Everything is commented out: copying it preserves inheritance and built-in defaults. Uncomment the relevant table header and only the fields you want to override. Server commands and URLs are examples to replace, not preconfigured services.

From a repository checkout, copy the example to the default global location:

```sh
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/mypr"
cp -n config.example.toml "${XDG_CONFIG_HOME:-$HOME/.config}/mypr/config.toml"
```

For workspace overrides, copy it inside the target workspace:

```sh
mkdir -p .mypr
cp -n /path/to/mypr-mcp/config.example.toml .mypr/config.toml
```

These commands leave an existing file untouched. If `MYPR_GLOBAL_CONFIG` selects another global file, copy the example there instead. You can also create a small file containing only the settings you need, or use the Python API or CLI below.

Configuration files are not generated at startup. Missing files use inherited settings and built-in defaults; reads do not create files. The first `set()` or persisted server registration creates the selected file. Commenting out or removing a scalar field restores inheritance after reload; for a server, remove its entire workspace entry. Saving a value equal to today's default pins that value, so it will not follow future default changes.

## Paths and precedence

| Layer | Location | Priority |
| --- | --- | --- |
| Built-in defaults | Defined by mypr | Lowest |
| Global | `MYPR_GLOBAL_CONFIG`, otherwise `$XDG_CONFIG_HOME/mypr/config.toml`, otherwise `~/.config/mypr/config.toml` | Overrides built-in defaults |
| Workspace | `<workspace>/.mypr/config.toml` | Highest |

The manager freezes its selected global path at startup. Reload rereads that file; changing the selected path requires starting the manager with the updated environment. Global defaults are shared by every workspace that uses the same file, while Python memory, client state, and workspace data remain separate.

Scalar fields inherit individually. Lists replace the inherited list. MCP and LSP server entries and mail accounts are complete replacements by name, so a workspace entry does not inherit omitted fields from the global entry. For example, a workspace replacement of a global MCP server must repeat its `command` or `url`; omitted environment mappings are not inherited.

To disable an inherited server, use an entry containing only `enabled = false`:

```toml
[mcp.servers.reports]
enabled = false

[lsp.servers.python]
enabled = false
```

Remove the entire workspace entry, or call `ws.config.unset()` on that entry, to restore global inheritance. `enabled = true` is not a server setting; replace a disabled entry with a valid server definition to enable it. The same TOML format works in both global and workspace files.

## Configuration reference

All byte sizes use integers: 1 KiB = 1,024 bytes, 1 MiB = 1,048,576 bytes, and 1 GiB = 1,073,741,824 bytes. Booleans do not count as integers. Server strings and environment mappings cannot contain NUL. Both files must be valid UTF-8 TOML and no larger than 16 MiB. Known fields are validated in each layer before merging, so an invalid global value is not hidden by a workspace override. Unrelated TOML sections and comments are preserved by writes.

The optional top-level `version` field is the configuration format version. Its default and only supported value is integer `1`; it is unrelated to the package version and is outside the `ws.config` managed API.

### Limits

| Field under `[limits]` | Default | Allowed value | Purpose | Apply with |
| --- | --- | --- | --- | --- |
| `output_bytes` | `16777216` (16 MiB) | Integer ≥ `1024` | Retained execution output per run | Manager and kernel restart |
| `response_bytes` | `32768` (32 KiB) | Integer `1024`–`1048576` | Default `execute`/`poll` output-page budget | Reload |
| `completed_tasks` | `128` | Integer ≥ `1` | Completed handles retained by the Python task manager, in addition to active handles | Manager and kernel restart |
| `completed_records` | `128` | Integer ≥ `1` | Completed entries retained by each manager record and shell-output cache | Reload |
| `cache_bytes` | `33554432` (32 MiB) | Integer ≥ `1024` | Serialized cache budget, shared equally between manager records and shell output | Reload |

`response_bytes` limits one output page, not the complete MCP response. Per-call `max_bytes` can override it within the same range. Reducing cache limits evicts completed cache entries only; active work and saved journals remain available. Arbitrary Python objects held by user code are outside these limits. See [completed-work retention](../README.md#completed-work-retention).

### Storage

| Field under `[storage]` | Default | Allowed value | Purpose |
| --- | --- | --- | --- |
| `enabled` | `true` | Boolean | Enable automatic storage cleanup; manual inspection and GC remain available |
| `revision_keep` | `50` | Integer ≥ `1` | Protect the latest revisions per resource |
| `retention_days` | `30` | Integer ≥ `1` | Retention age in days used by automatic GC and as the default for manual GC |
| `max_bytes` | `1073741824` (1 GiB) | Integer ≥ `1` | Soft target for managed disk use; protected data can exceed it |
| `gc_interval_seconds` | `300` | Integer ≥ `1` | Interval between automatic maintenance passes |

All storage settings apply through reload. Reload updates the policy and reschedules maintenance without immediately running GC; the next maintenance pass uses the new policy. Active work, current files, and other protected data are not removed merely to meet the target. See [workspace storage](storage.md) for cleanup plans and protected data.

### Dependencies

| Field under `[dependencies]` | Default | Allowed value | Purpose |
| --- | --- | --- | --- |
| `auto_install` | `true` | Boolean | Prepare registered dependencies automatically when a built-in feature needs them |

`auto_install` applies to registered binary tools, OCR models, workspace Python packages, and managed Playwright browser engines. It is merged globally and per workspace and applies after `await ws.config.reload()`. Set it to `false` to make feature calls report a missing dependency; `await ws.dependencies.ensure("name")` remains an explicit installation request and bypasses this setting. `await ws.dependencies.list()` and `ws.doctor()` only inspect state.

Binary tools and OCR models are shared under `$XDG_DATA_HOME/mypr` (`~/.local/share/mypr` by default), with temporary downloads under `$XDG_CACHE_HOME/mypr` (`~/.cache/mypr` by default). Workspace Python packages are installed into `.mypr/venv`; the uv download cache is shared globally. The initial binary catalog contains `rg` 15.2.0, `ast-grep` 0.45.3, `rga` 0.10.10, and `pandoc` 3.12. OCR models come from the pinned `tessdata_fast` 4.1.0 catalog. Existing compatible Python package versions are preserved.

The supported automatic Python packages are `pillow`, `pymupdf`, `trafilatura`, `cssselect` when a CSS selector is requested, `python-docx`, `python-pptx`, and `openpyxl`. `ws.packages.add()` remains the explicit API for arbitrary packages. Tesseract, FFmpeg, Poppler, Git, LSP servers, and external services remain manual dependencies. Explicit `TESSDATA_PREFIX` is never overridden; when system OCR data already provides every requested language, no model download is needed.

### Mail

Mail accounts are configured under `[mail.accounts.<name>]`. An account definition is complete by name: a workspace entry replaces the corresponding global entry instead of inheriting individual endpoint fields. An entry containing only `enabled = false` hides an inherited account. `default_account` is optional; an empty value lets the mail API select the only enabled account and reports an error when that is ambiguous.

```toml
[mail]
default_account = "work"

[mail.accounts.work]
from = "Me <me@example.com>"
sent_mailbox = "Sent"

[mail.accounts.work.imap]
host = "imap.example.com"
security = "ssl"
username = "me@example.com"
password_from = "MYPR_IMAP_PASSWORD"

[mail.accounts.work.smtp]
host = "smtp.example.com"
security = "starttls"
username = "me@example.com"
password_from = "MYPR_SMTP_PASSWORD"
```

Each account requires a sender address, an IMAP endpoint, and an SMTP endpoint. `security` accepts `ssl`, `starttls`, or `plain`; omitted ports default to 993/143 for IMAP and 465/587/25 for SMTP respectively. IMAP authentication is required. SMTP authentication is optional for a trusted relay, and `password_from` names an environment variable rather than storing a password. `ca_file` can point to an additional CA bundle. `sent_mailbox` opts in to appending accepted outgoing messages to the provider's Sent mailbox; without it, sent copies remain in the local outbox history only.

The manager resolves password variables from its own environment when a connection opens. Status reports source availability and cached connection state without contacting a provider. Missing variables and connection failures are reported lazily when a mailbox or send operation opens a connection, and values are never returned by `ws.config` or written to history. Changing a client process environment does not change an already-running manager.

Mail settings apply through `await ws.config.reload()`. Reload keeps existing connections when their account definition is unchanged. A busy account is reported under `deferred` until its current send or mailbox operation settles, including when `force=True`; this prevents a configuration reload from interrupting an SMTP transaction. The applied configuration reported by `ws.config.explain()` reflects only accounts that the manager has actually reconfigured.

### MCP servers

Define each server under `[mcp.servers.<name>]`. No servers are configured by default. Names must contain 1–128 characters and cannot be blank or contain NUL. Quote names containing dots, spaces, or other TOML punctuation, such as `[mcp.servers."reports.dev"]`.

| Field | Required | Default when omitted | Allowed value / behavior |
| --- | --- | --- | --- |
| `command` | Stdio only | None | Nonempty executable string or array of nonempty argument strings |
| `args` | No; stdio only | `[]` | Array of strings appended to `command` |
| `cwd` | No; stdio only | Consuming workspace | Nonempty path string; relative paths resolve against that workspace, including global definitions |
| `env_from` | No; stdio only | `{}` | Map child environment-variable names to variable names in the manager environment |
| `url` | HTTP only | None | HTTP(S) URL with a host; mutually exclusive with `command` |
| `headers_from` | No; HTTP only | `{}` | Map HTTP header names to variable names in the manager environment |
| `enabled` | Disabled entry only | Not present | Only `false`, with no other fields; hides an inherited server |

For an active definition, choose exactly one transport: `command` with optional `args`, `cwd`, and `env_from`, or `url` with optional `headers_from`. Other server fields are rejected. Environment mappings contain variable names, not literal values; missing source variables fail when the connection opens. An authorization source variable must contain the complete header value, including `Bearer ` when required. The manager reads its own environment, so changing a client's environment does not update an existing manager's variables.

Stdio commands run directly, without shell expansion. Use an argument array when a command needs fixed arguments:

```toml
[mcp.servers.reports]
command = ["python", "/absolute/path/to/reports_server.py"]
args = ["--stdio"]

[mcp.servers.reports.env_from]
REPORTS_TOKEN = "REPORTS_TOKEN"
```

Reload applies saved server definitions without starting new connections. Added and changed servers connect lazily on their next call; unchanged connections remain open. Busy affected connections defer the MCP component unless `force=True` permits interruption. See [MCP services](../README.md#mcp-services) for discovery, calls, and server restart.

### LSP servers

Define each language server under `[lsp.servers.<name>]`. No language servers are configured by default. Names must match `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`; quote dotted names in TOML.

| Field | Required | Default when omitted | Allowed value / behavior |
| --- | --- | --- | --- |
| `command` | Active definition | None | Nonempty array of nonempty argument strings; commands run from the workspace |
| `languages` | Active definition | None | Nonempty array of nonempty language IDs, such as `python`, `c`, or `cpp` |
| `timeout` | No | `10.0` | Number from `1` to `60`, in seconds |
| `enabled` | Disabled entry only | Not present | Only `false`, with no other fields; hides an inherited language server |

Other LSP fields are rejected. Executables must already be installed. Reload changes saved definitions without eagerly starting stopped servers and keeps unchanged processes. Busy affected servers defer the LSP component unless `force=True` permits interruption; an in-progress configuration mutation is deferred even with force. At most four language servers can run per Python kernel. See [code navigation](code.md) for configuration, startup, navigation, and edits.

## Python API

Read the desired configuration with:

```python
effective = await ws.config.get()
global_defaults = await ws.config.get(scope="global")
workspace_overrides = await ws.config.get(scope="workspace")
server = await ws.config.get('mcp.servers."reports"')
```

Paths use TOML dotted syntax. Quote a key when it contains punctuation or spaces. `get(path=None, scope="effective")` returns the desired on-disk configuration. It does not report which parts are currently running; use `explain(path)` for that.

Persist a value in one layer with `set()` or remove it with `unset()`:

```python
await ws.config.set("limits.response_bytes", 65536)
await ws.config.set("storage.gc_interval_seconds", 600, scope="global")
await ws.config.unset('mcp.servers."reports"', scope="workspace")
```

These methods persist only. They do not implicitly reconnect MCP servers, restart language servers, change manager caches, or reset Python. Use `await ws.config.explain(path)` to inspect `desired`, `applied`, `source`, `revisions`, `pending`, and `restart_required` before applying changes.

Apply persisted settings explicitly:

```python
result = await ws.config.reload()
```

The result reports `applied`, `deferred`, `errors`, `restart_required`, and the resulting `revision`. `force=True` allows the manager to interrupt work when an affected resource requires it. Reload keeps the workspace namespace, Python state, and client identity. It applies hot settings, MCP/LSP changes, and mail account changes that can be admitted safely; settings that shape process startup remain in `restart_required`.

Manager response and completed-record/cache limits can be changed by reload. `limits.output_bytes` and `limits.completed_tasks` require a manager and kernel restart; reload reports them as restart-required and does not perform an implicit reset. Storage policy changes are picked up by the next storage-maintenance pass. The workspace namespace and saved Python state remain workspace-specific even when global defaults are shared.

`mypr-mcp config reload --all` reloads every registered manager in the same profile after a global file change. A normal workspace reload affects only the selected manager. Inspect `explain()` after reload when another manager or an external editor may have changed a layer concurrently.

## CLI

Run workspace commands from the workspace directory. `--global` selects the global file and does not read workspace configuration. File inspection and persistence do not start a manager.

```sh
uvx mypr-mcp config get
uvx mypr-mcp config get mcp.servers --global
uvx mypr-mcp config set limits.response_bytes 65536 --global
uvx mypr-mcp config set mcp.servers.reports '{"command":"reports-server"}'
uvx mypr-mcp config set mcp.servers.reports '{"enabled":false}'
uvx mypr-mcp config unset mcp.servers.reports
uvx mypr-mcp config explain limits.response_bytes
uvx mypr-mcp config reload
uvx mypr-mcp config reload --all
```

`set` takes a JSON value. Workspace `explain` includes the active manager's view when one is reachable; `explain --global` reports only the global file. `--global` applies to file commands and is rejected for reload. Reload requires an existing manager; `--all` selects registered managers using the same global file and verifies their identity before applying changes. It reports each manager separately, with at most four concurrent reloads. Add `--force` only when affected requests may be interrupted. Reload exits with a nonzero status if it reports errors, deferred changes, or settings that require a restart.
