# Workspace storage

Runtime data lives below `.mypr/`. The manager keeps execution journals, background-job output, search and Git snapshots, document results, browser and scan artifacts, change plans, task results, and content-addressed revisions. Workspace files, the Python environment, skills, modules, configuration, messages, and request-deduplication state are managed separately from the cleanup candidates.

Inspect usage before changing retention. The usage scan reads directory metadata only; it does not hash or read file contents:

```python
usage = await ws.storage.usage()
usage["total_bytes"], usage["categories"]
```

The policy is configured in `.mypr/config.toml`:

```toml
[storage]
enabled = true
retention_days = 30
max_bytes = 1073741824
revision_keep = 50
gc_interval_seconds = 300
```

`enabled` controls automatic cleanup. `retention_days`, `max_bytes`, and `revision_keep` are the defaults used by `ws.storage.gc()` when its optional arguments are omitted. `gc_interval_seconds` controls the background pass; manual `usage()` and `gc()` remain available when automatic cleanup is off.

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
