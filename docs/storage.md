# Workspace storage

Workspace runtime data lives below `.mypr/`. The manager keeps execution journals, background-job output, search, Git, and LSP diagnostic snapshots, document results, browser and scan artifacts, change plans, task results, content-addressed revisions, and immutable mail drafts. Workspace files, the Python environment, skills, modules, configuration, messages, and request-deduplication state are managed separately from the cleanup candidates. Shared dependency binaries and OCR models live under the XDG data directory, and their download cache is outside workspace storage; workspace GC never removes them.

Inspect usage before changing retention. The usage scan reads directory metadata only; it does not hash or read file contents. It skips symlinks and stops at its bounded file-count limit. Check each summary's `truncated` flag before treating it as complete; the top-level `truncated` flag belongs to the legacy managed-category scan.

```python
usage = await ws.storage.usage()
usage["total_bytes"], usage["categories"]
```

The legacy `total_bytes`, `total_files`, and `categories` fields describe logical bytes in the known managed categories used by GC. The `managed`, `protected`, and `workspace` summaries provide the fuller view. `managed` covers known GC categories, `protected` covers other `.mypr` data such as the virtual environment, skills, configuration, and SQLite files, and `workspace` covers every regular file below `.mypr`. Each summary contains `files`, `logical_bytes`, `allocated_bytes`, `unique_inodes`, `hardlinks`, `has_hardlinks`, and `truncated`; `bytes` is an alias for `logical_bytes`. Logical bytes count every path. Allocated bytes count each device/inode once within that summary, so hard-linked paths do not represent extra blocks. Adding the managed and protected allocated totals can double-count an inode linked across those groups. Allocated totals describe blocks visible through these files, not space exclusively owned by the workspace or guaranteed to be freed by deletion; links outside `.mypr` and filesystem-level shared extents can retain those blocks.

The policy is configured in the layered `storage` section. Workspace values in `.mypr/config.toml` override global defaults. Use `ws.config.set()` to persist a value and `ws.config.reload()` to apply it; the next storage-maintenance pass uses the updated policy:

```toml
[storage]
enabled = true
retention_days = 30
max_bytes = 1073741824
revision_keep = 50
gc_interval_seconds = 300
```

`enabled` controls automatic cleanup. `retention_days`, `max_bytes`, and `revision_keep` are the defaults used by `ws.storage.gc()` when its optional arguments are omitted. `gc_interval_seconds` controls the background pass; manual `usage()` and `gc()` remain available when automatic cleanup is off. Protected files are included in usage summaries but are never selected as ordinary file-deletion candidates.

Build a plan first. A dry run does not delete anything:

```python
plan = await ws.storage.gc(dry_run=True, older_than_days=30)
plan["plan_id"], plan["candidates"], plan["protected"]
```

Apply only the plan you reviewed:

```python
result = await ws.storage.gc_apply(plan["plan_id"])
```

Automatic cleanup removes data older than 30 days regardless of current quota. When managed data exceeds the soft 1 GiB target, it also selects the oldest eligible recent data until the target is reached. The latest `revision_keep` revisions per resource (50 by default) and the current content are protected. Active jobs, pending transactions, retained execution records, request deduplication data, configuration, virtualenv, skills, modules, and messages are protected as well. Shared content-addressed blobs remain until no retained record references them.

Plans expire after one hour, are bound to the workspace identity, and are revalidated under the storage lock before deletion. Output records that need a history tombstone are not removed until the manager has persisted that marker. Protected data is never deleted merely to meet the quota. Automatic cleanup uses the same planner and application path; `await ws.status()` exposes its `running`, `last_run`, `last_deleted_bytes`, and `last_error` fields under `storage_maintenance`.

The GC plan can include a `database` section for old history bodies and events. After the retention cutoff, only terminal records are selected. Applying the plan removes bulky code, output, and event-body fields while preserving the entity ID, state metadata, request-deduplication fields, and a `code_sha256` value when source code was removed. Event rows are deleted only when their terminal owner is still valid. Selection and application are bounded to at most 1,000 entity rows and 1,000 event rows per pass, with revalidation against the recorded hashes and timestamps. The database result reports pruned records and bytes separately from physically reclaimed bytes. `ws.history.logs()` reports `history_truncated=true` and a monotonic `pruned_through_seq` watermark when earlier events were removed; cursors continue to advance over the missing sequence numbers.

Mail send pagination uses an explicit `send_seq` cursor. On an older database, startup migrates the existing mail send rowids into that column before history compaction; existing cursor values remain valid. VACUUM is attempted at most once per day after a successful history commit and WAL checkpoint, only when the database is at least 16 MiB with at least 4 MiB and 25% free pages. It is skipped when mail cursor migration is incomplete, the database is busy, filesystem space is insufficient, or the free-page thresholds are not met. Message, timer, client identity, mail reference, watch, and send identity rows are not removed by history-body compaction; mail's separate retention rules still govern eligible MIME files and acknowledged notifications.

Database size and filesystem space are measured after the initial checkpoint; VACUUM requires free filesystem space of at least twice that database size. A second checkpoint after VACUUM completes physical reclamation, and `reclaimed_bytes` reports the measured main-file reduction. If that checkpoint is busy, a later maintenance pass retries the checkpoint even during the daily VACUUM limit.

Mail MIME files appear in the `mail` usage category. Unsent drafts and drafts with queued, sending, or unknown send outcomes are protected. Terminal drafts become cleanup candidates only after 30 days; lowering the general retention setting or exceeding the storage target does not make protected mail eligible. Draft paths are stored relative to the workspace, so they remain usable after moving the workspace and reconnecting its manager. GC rechecks outbox state under the storage lock before deletion, then removes associated draft and send rows. Acknowledged mail notifications are pruned after 30 days; unacknowledged notifications, watches, and message references remain durable. An unreadable mail snapshot protects mail files for that pass.

Malformed persisted history produces bounded diagnostics. If damaged history prevents establishing file ownership, the cleanup planner returns no candidates and applying a saved plan stops before deletion or revision pruning. Repair or remove the damaged metadata explicitly before retrying cleanup.
