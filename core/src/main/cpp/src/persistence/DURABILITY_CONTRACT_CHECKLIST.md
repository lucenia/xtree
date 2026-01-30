# Durability Contract Checklist

This checklist ensures our persistent COW + MVCC design maintains crash consistency and recovery guarantees:

## 1. Commit Path (Writer):

- [ ] Write node/page data to the data files.
- [ ] Append corresponding OT deltas to the OT Delta Log (one record per mutated handle).
- [ ] fsync the OT Delta Log to ensure deltas are durable.
- [ ] Update and publish the superblock (root ID, commit epoch).
- [ ] fsync the superblock file.

## 2. Recovery Path (Cold Start):

- [ ] Load the manifest to locate the latest checkpoint and associated delta logs.
- [ ] Map the checkpoint into memory.
- [ ] Replay delta logs in order, stopping at the first invalid/truncated frame.
- [ ] Apply changes to bring the OT to the committed superblock epoch.

## 3. Checkpointing:

- [ ] Periodically write a full OT snapshot to a temp file.
- [ ] Include a header/footer with CRC for integrity.
- [ ] fsync the temp file, then atomically rename it to the checkpoint path.
- [ ] fsync the parent directory to persist the rename.
- [ ] Update the manifest to point to the new checkpoint and truncate eligible logs.

## 4. Manifest Updates:

- [ ] Always treat the manifest as the single source of truth for checkpoints, logs, and data files.
- [ ] Write manifest updates atomically (temp file + rename + directory fsync).

## 5. Log GC:

- [ ] After a checkpoint at epoch E, remove all delta logs where end_epoch ≤ E.
- [ ] Ensure logs are removed only after the manifest references the new checkpoint.

## 6. State Encoding:

- **Free**: birth=0, retire=MAX, kind=Invalid.
- **Live**: birth>0, retire=MAX.
- **Retired**: birth>0, retire<MAX.
- Use Tombstone only for explicitly deleted logical records, not for free slots.

## 7. Ordering Guarantees:

- [ ] Never publish a superblock that references data not yet fsynced to log and data files.
- [ ] Never delete or overwrite logs until a checkpoint covering their epochs is durable and published.

> This ordering is the foundation of durability. Breaking the sequence risks torn writes, orphaned entries, or inconsistent recovery.

---

# Durability & Crash-Consistency Checklist (Contract)

Use this as a go/no‑go checklist before shipping changes that touch persistence.

## Global Invariants

- [ ] **Epoch 0 is reserved**. Free = birth=0 && retire=UINT64_MAX. Live = birth>0 && retire=UINT64_MAX. Retired = birth>0 && retire<UINT64_MAX.
- [ ] **NodeID stability**. Handle index never changes; tag only increments (prevents ABA).
- [ ] **Write‑after‑publish never visible**. Readers switch snapshots only via the superblock.
- [ ] **All on‑disk files are little‑endian**; headers/footers have CRC32C.

## Commit Path (Writer) — must happen in this order

1. Apply data changes in size‑class files (COW pages, CRC-protected).
2. Append OT delta frames (one per affected handle):
   - Frame = `[len][OTDeltaRec][crc32c]`.
   - crc32c is over OTDeltaRec only.
3. Durably flush the delta log: `fdatasync/fsync(log)`.
4. Publish superblock snapshot:
   - `root_id.store(new_root, release)` → `commit_epoch.store(new_epoch, release)` → bump generation.
   - Recompute header_crc32c (zeroed field), then msync/flush_view, fsync(file), fsync(parent dir).
5. (Optional) Manifest update is NOT part of the commit. It's updated by checkpointing/rotation, not per‑txn.

## Checkpoint Creation (Background)

1. Stable snapshot of live OT rows (by handle).
2. Write `checkpoint.tmp`:
   - 4KiB header (magic/version/epoch/row_size/block_bytes + header_crc32c).
   - Stream rows; compute entries_crc32c.
   - Footer {total_bytes, entries_crc32c, footer_crc32c}.
3. `fsync(checkpoint.tmp)` → atomic rename to `checkpoint.bin` → `fsync(parent dir)`.
4. Update manifest: write `manifest.json.tmp` → fsync → atomic rename → `fsync(parent dir)`.
5. (Optionally) rotate delta log if large/old (close current, open next).

## Recovery Path (Cold Start)

1. Load manifest; pick the referenced checkpoint + ordered logs strictly after checkpoint epoch.
2. Map checkpoint; verify header+entries+footer CRCs; bulk‑restore OT by handle index.
3. Replay delta logs in order:
   - For each frame: verify CRC; apply only if record_epoch > checkpoint_epoch.
   - On first bad frame: truncate log to last_good_offset and stop.
4. Adopt superblock snapshot: set in‑memory root to root_id and epoch from the flushed superblock.
5. Schedule a new checkpoint soon if replay length/age exceeds thresholds.

## Manifest Contract

- [ ] Is the single source of truth for: active checkpoint, ordered delta logs after it, and data files.
- [ ] Stored atomically: temp → fsync → rename → fsync(dir).
- [ ] Recovery must not scan directories unless manifest is missing/unreadable.

## Reclaimer & Log GC

- **Reclaimer safe point**: `safe_epoch = mvcc.min_active_epoch()`.
- **Reclaim in three phases**:
  1. Under OT lock, select retired handles with retire_epoch < safe_epoch (do not clear entries yet).
  2. Outside lock, free storage for selected handles.
  3. Under lock, reset entries to Free (birth=0, retire=MAX, kind=Invalid) and return handles to freelist.
- **Log GC**: After checkpoint at epoch E, any log whose max frame epoch ≤ E is eligible for deletion/compaction.

## File‑format & I/O Requirements

- **Delta Log**: framed records; per‑frame CRC32C; truncate on first corruption.
- **Checkpoint**: header/footer CRC32C; reject if (size - header - footer) not divisible by row_size.
- **Superblock**: first publish sets magic last; every publish flushes view, fsyncs file and parent dir; header_crc32c maintained.
- **PlatformFS**: all rename operations are atomic; directories are fsynced after renames.

## Testing Checklist

- [ ] Corrupt each artifact (magic/header CRC/entries CRC/footer CRC) ⇒ loader rejects.
- [ ] Truncate delta log mid‑frame ⇒ recovery truncates to last good frame and succeeds.
- [ ] Crash injection between each step of commit ordering ⇒ recovery yields a consistent, previously published root.
- [ ] Multiple checkpoints present ⇒ recovery uses manifest's referenced one; ignores stray .tmp files.
- [ ] Cross‑platform: run tests on Linux/macOS/Windows with fsync/rename semantics verified.

## Observability

- [ ] Count: frames appended/replayed/truncated, checkpoints written, generations published.
- [ ] Record the largest replay window at startup and time to recovery.
- [ ] Export min_active_epoch and reclaimed bytes per cycle.

> **Important**: If any item here changes, update both this checklist and the unit tests that enforce it. This contract is what keeps writers fast and recovery bounded + correct.