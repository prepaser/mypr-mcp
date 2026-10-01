# AST rewrites

`ws.fs.rewrite_ast(..., history=True)` builds a bounded preview from ast-grep matches. It never changes source files. Apply a preview separately with `ws.fs.apply_rewrite(plan_id)`. History-enabled application holds the workspace storage transaction through preflight, file changes, and history publication, so GC cannot rewrite its revision index concurrently. A successful apply records the old and new bytes in filesystem history; set `history=False` only when that recovery entry is not needed.

```python
preview = await ws.fs.rewrite_ast(
    "logger.info($MSG)",
    replacement="logger.debug($MSG)",
    lang="python",
    paths="src",
    glob="*.py",
)
if preview["applicable"]:
    print(preview["diff"])
    result = await ws.fs.apply_rewrite(preview["plan_id"])
```

Pass either an ast-grep pattern or a bare matcher rule, plus a replacement string and language. `paths` and `glob` accept a string or non-empty list of strings; paths default to the workspace root. ast-grep applies its normal ignore and hidden-file rules. The optional `glob` further includes or excludes paths.

The preview returns `plan_id`, `applicable`, `complete`, `reason`, `changes`, bounded unified `diff`, `diff_truncated`, `changed_files`, `original_bytes`, and `planned_bytes`. `scanned_files` counts matching files whose immutable source buffers were rechecked through ast-grep stdin; it does not count every file visited during discovery. A `null` count means the run stopped before a complete count was available. `complete` is true only when discovery and all matched-file rechecks finished within the limits. An incomplete preview has no applicable plan.

Each operation is limited to 100 matching files, 16 MiB total for captured original source buffers and planned output, and 30 seconds. The 16 MiB source budget applies to matching files rechecked from immutable buffers; ast-grep's initial walk is bounded by the deadline and a combined 16 MiB JSON output cap, not by bytes read from non-matching files. A timeout or any limit that could hide matches marks the preview incomplete. Files must be regular UTF-8 files. Rewrites use ast-grep's byte offsets against the exact captured source bytes, reject overlapping ranges, and keep untouched bytes unchanged. Newline characters in replacements follow the nearest source line ending.

Plans are stored under `.mypr/rewrites` for one hour, with a limit of 16 plans and 64 MiB total. Creating a plan can evict the oldest plans after the new plan is durably installed. Plans are bound to the canonical workspace path and directory identity, so copying or moving a workspace invalidates them. Applying one checks every original file hash before writing any file, then uses the filesystem's multi-file transaction and rollback path. A changed source rejects the entire apply before writing. A successful apply consumes the plan; an interrupted or stale plan expires automatically.

Install ast-grep in the workstation environment to use this API. No Python package is required.

## Text replacements

`ws.fs.replace(pattern, replacement, paths=None, glob=None, fixed=True, ignore_case=False, hidden=False, no_ignore=False, max_files=100, max_bytes=16777216, timeout=30, history=True)` creates a multi-file preview for ordinary text files. Literal matching is the default; pass `fixed=False` for Python regular-expression matching and `ignore_case=True` when needed. The search must complete before a plan can be applied. `max_files`, `max_bytes`, and `timeout` bound the preview, and `history=True` records the old and new bytes when it is applied. File reads and substitution output are checked against the remaining byte budget before a full oversized result is allocated. A truncated diff still retains metadata for every changed file in `changes`.

```python
preview = await ws.fs.replace(
    "timeout=10",
    "timeout=30",
    paths="src",
    glob="*.py",
    fixed=True,
)
if preview["applicable"]:
    result = await ws.fs.apply_replace(preview["plan_id"])
```

The apply step rechecks every source revision and uses the same transaction and rollback path as other multi-file edits. Stale or expired plans fail without partially applying the remaining files.
