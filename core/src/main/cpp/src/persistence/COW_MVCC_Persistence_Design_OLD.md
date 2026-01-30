# XTree Persistent Copy‑on‑Write with MVCC, Size‑Class Segments, and Defrag‑Friendly Compaction

**Status:** Draft for review
**Author:** Nick Knize
**Last updated:** 2025‑08‑10

---

## 0. Goals & Constraints

* **Zero‑downtime concurrent reads** while writes occur.
* **Many readers, single publisher (to start)** with option to batch multiple writers via combiner.
* **Minimal fragmentation** under heavy split churn and supernodes.
* **Fast snapshots** (O(1) root flip). Crash safe.
* **mmap‑friendly** on Linux; no dependency on page‑cache quirks.
* **Bounded write amplification**; compaction moves only what’s necessary.
* **Portable on‑disk format**, forward‑compatible.

---

## 1. High‑Level Architecture

The XTree persists via **copy‑on‑write (COW)** and a **stable indirection layer**:

* **NodeID**: 64‑bit opaque handle = `[63:8] handle_index | [7:0] tag` (anti‑ABA). Stored in all parent → child references.
* **Object Table (OT)**: in‑RAM array mapping `handle_index → {file, segment, offset, length, class, kind, tag, birth_epoch, retire_epoch}`. Persisted via an **append‑only delta log** + periodic **checkpoints**.
* **Global commit epoch (u64)**: MVCC version of the entire tree. Readers pin an epoch; reclaimer frees anything retired before the minimum active reader epoch.
* **Size‑class segments**: Storage is partitioned into power‑of‑two classes (e.g., 4K, 8K, 16K, 32K, 64K, 128K, 256K). Each class owns multiple **append‑only segments** (regions within a small set of files). Allocation is bump‑pointer; intra‑segment reuse optional via a bitmap.
* **Out‑of‑line child vectors for supernodes**: Node header stays small/fixed; oversized child arrays live in a separate object (also addressed by NodeID). This avoids relocating parent headers during growth.
* **Superblock**: tiny structure containing `(root_id, commit_epoch, generation, crc)`. The only in‑place overwrite per commit.

---

## 2. On‑Disk Layout

### 2.1 Files

* **Data files**: a small number (1–N) of files, each subdivided into **segments** grouped by **size class**. Example: `xtree.data0`, `xtree.data1`.
* **OT delta log**: append‑only file recording OT updates.
* **OT checkpoint**: periodic compact snapshot of the OT (sorted by handle index).
* **Superblock**: fixed‑size header area (first 4KB of `xtree.meta`).

> Rationale: Avoids “one file per level” (bad locality) while isolating fragmentation within size classes; compaction can evacuate a holey segment and reclaim space deterministically.

### 2.2 Segment Header (per segment)

* `magic, class_id, segment_id, capacity_bytes, write_cursor`
* Optional free‑bitmap if enabling intra‑segment reuse (later optimization).

### 2.3 Node Encodings

* **Internal/Leaf Node Header** (fixed small): `{kind, flags, fanout, mbr_count, child_vec_id|inline_count}`
* **MBRs** stored **SoA** for SIMD‑friendly comparisons (unchanged from current XTree).
* **Children**: either an **inline compact array** (for small fanouts) or a **child vector object** referenced by `NodeID`.
* **Child Vector Object**: `{capacity, length, NodeID children[length]}`; capacity grows in powers of two.

---

## 3. Identifiers & Indirection

### 3.1 NodeID (ABA‑safe)

`NodeID = (handle_index << 8) | tag`

* **handle\_index (56b):** index into OT.
* **tag (8b):** increments on each reuse of the handle; prevents ABA when a stale NodeID is compared/loaded.
* **Single‑bump invariant:** Tag is incremented exactly once when a handle transitions from free/retired to live state (in `mark_live_reserve()` only). Tags cycle through [1..255], skipping 0 (period of 255).

### 3.2 Object Table (OT)

In‑RAM vector of entries; persisted by **delta log** records:

* `file_id, segment_id, file_offset, length, class_id, kind, tag, birth_epoch, retire_epoch`
* **Birth/retire epochs** bound the visibility interval for MVCC.
* **Checkpoints** serialize the OT compactly every *M* MB or *T* seconds.
* **Multi‑field catalog support:** The OT can track multiple named roots for different data structures within the same persistence layer.

---

## 4. Allocation Strategy (Fragmentation Control)

### 4.1 Size Classes

* Defaults: 4K, 8K, 16K, 32K, 64K, 128K, 256K. Tune based on observed node sizes (headers vs leaf payloads vs vectors).
* Node type → class policy:

* **Internal headers**: 4K–16K.
* **Child vectors**: grow in power‑of‑two capacities mapped to classes.
* **Leaves**: typically larger (32K–128K) depending on record packing.

### 4.2 Append‑Only Segments

* Each class maintains one **active segment** (bump pointer). When it fills, open a new segment.
* Freed slots go to a **per‑class freelist** (optional bitmap). Compactor prefers allocating into nearly‑empty/active segments to consolidate.

> Outcome: External fragmentation never crosses class boundaries; internal fragmentation bounded by class granularity and power‑of‑two growth.

---

## 5. Supernodes & Overflow Handling

### 5.1 Growth Policy

* Allow up to 2× overflow for contentious nodes.
* **Child array out‑of‑line** as `ChildVec` object. Parent header remains fixed; only the vector moves/grows.

### 5.2 Deferred Logical Split

* Under high overlap, prefer temporary supernode via vector growth. Schedule a **background logical split** that computes a better partition off the read path; swap in the new structure with one parent update.

### 5.3 Compaction Assist

* Compactor opportunistically **re‑packs supernodes** whose overlap/entropy score is poor, reducing future churn.

---

## 6. MVCC & Concurrency

### 6.1 Global Commit Epoch

* 64‑bit, monotonically increasing.
* Stored in superblock; also mirrored in memory as `global_epoch`.

### 6.2 Readers

* On query start: atomically read `(root_id, epoch)` from superblock; **pin epoch** in TLS.
* Traverse entirely immutable nodes referenced from that root. No locks.

### 6.3 Writers (Single Publisher)

* Build changes via COW (new nodes in active segments, updates staged as OT deltas).
* **Publish**: write payloads → append OT deltas (fsync) → store `root_id` → store `epoch` → fsync superblock → advance `global_epoch`.
* Multi‑writer upgrade: introduce a **combiner** thread that batches many writers’ deltas into one publish per interval.

### 6.4 Reclamation

* Background process computes `safe_epoch = min_active_reader_epoch()` and reclaims any OT row with `retire_epoch < safe_epoch`.
* Physical space returned to the class allocator; handle moved to free list (tag incremented on reuse).

---

## 7. Compaction (Defragmentation)

### 7.1 Victim Selection

* Choose segments with **dead\_ratio ≥ θ** (e.g., 40–60%). Prioritize classes with low free space and high allocation pressure.

### 7.2 Evacuation

* Copy live objects to the active segment(s) of the same class.
* For each moved object, **update only the OT row** (no parent rewrites). This is the key benefit of NodeID indirection.

### 7.3 Segment Retirement

* When all OT rows in a segment are either dead or evacuated, truncate/delete segment.

### 7.4 Locality Strategy

* Compactor can be **subtree‑aware**: evacuate siblings together to improve traversal locality.

---

## 8. Crash Safety & Recovery

### 8.1 Commit Order

1. **Payloads**: write node bodies (mmap + `msync(MS_SYNC)` if needed) and child vectors.
2. **OT Delta Log**: append all handle updates for this commit; `fsync`.
3. **Superblock**: store `root_id` then `commit_epoch`; `fsync`.

### 8.2 Recovery

1. Read superblock; validate `magic`/`crc`.
2. Load last OT checkpoint; replay tail of OT delta log.
3. Set `global_epoch = commit_epoch`; set root to `root_id`. Readers can proceed.

---

## 9. Core Data Structures (Sketch)

```c++
using NodeID = uint64_t; // [63:8]=handle, [7:0]=tag

enum class NodeKind : uint8_t { Internal=0, Leaf=1, ChildVec=2, Tombstone=255 };

struct OTEntry {
  uint32_t file_id;
  uint32_t segment_id;
  uint64_t file_offset;
  uint32_t length; // sized to class
  uint8_t class_id;
  NodeKind kind;
  std::atomic<uint8_t> tag; // mirrors NodeID tag
  std::atomic<uint64_t> birth_epoch; // visibility start
  std::atomic<uint64_t> retire_epoch; // visibility end or U64_MAX
  // Optional cached vaddr for fast deref: void* addr;
};

struct Superblock {
  uint64_t magic;
  uint64_t version;
  std::atomic<uint64_t> root_id; // NodeID
  std::atomic<uint64_t> commit_epoch; // MVCC epoch
  uint64_t generation;
  uint64_t crc64;
};

```

---

## 10. APIs & Flow

### 10.1 Write Flow (Single Publisher)

* `begin_tx()` → allocate/mutate nodes via COW → stage OT deltas → `set_root(name, root_id)` → `commit_tx()`
* `commit_tx` does group commit of deltas and flips `(roots, epoch)` atomically.
* **Multi‑field support:** Multiple named roots can be set before commit using `set_root(name, NodeID)`.

### 10.2 Read Flow

* `snapshot = load_root_and_epoch(name)` → `pin(snapshot.epoch)` → traverse using NodeIDs → return.
* **Multi‑field support:** `get_root(name)` retrieves the NodeID for a specific named root.

### 10.3 Reclaim Flow

* Periodic: `safe = min_active_reader_epoch()` → scan retired rows → free → recycle handles.

---

## 11. Parameter Defaults

* **Size classes:** 4K, 8K, 16K, 32K, 64K, 128K, 256K (tune after profiling).
* **Commit batching:** every 1–2 ms or 256 KB OT deltas, whichever first.
* **Compaction trigger:** segment dead ≥ 50%, or class free < 5%.
* **Supernode overflow cap:** ×2 growth before deferring split.

---

## 12. Performance Notes

* **Lock‑free reads**; per‑node deref is 1 OT array load + address add.
* **L0/L1 locality:** pin root and first two levels in a tiny L0 cache (or hugepage segments) to reduce TLB misses.
* **MBR SoA** vectorization unchanged; prefetch child OT rows while scanning MBR candidates.
* **Fsync amortization:** group commit hides latency; use `io_uring`/AIO for OT log when available.

---

## 13. Telemetry & Observability

* Counters: allocations per class, segment dead/live bytes, compaction moved bytes, epoch lag (global − min active).
* Latencies: commit time (payload, log, superblock), iterator node visit, OT lookup miss ratio.

---

## 14. Record MVCC & Tombstones

Overview

The persistence design supports record-level multi-version concurrency control (MVCC) inside leaves, with tombstones marking deletions or masking older versions. This complements the node-level MVCC already provided by birth_epoch and retire_epoch in OTEntry.

### 14.1 Leaf Layout for MVCC

Each leaf contains a compact record array:

```
struct RecordHeader {
  uint64_t key_id;         // or key hash/encoded key reference
  uint64_t birth_epoch;    // first visible commit epoch
  uint64_t retire_epoch;   // U64_MAX if live; else last visible epoch
  uint8_t  flags;          // bit 0: TOMBSTONE, others reserved
};
// Followed by payload (document/geometry), if not tombstone
```

Storage order in leaf (SoA for efficiency):

```
LeafNode {
  HeaderBlock: [RecordHeader N]
  KeyBlock:    [keys or key refs]
  PayloadBlock:[payloads]
  LiveBitmap:  bitset(N) for fast skip of dead/tombstoned slots
}
```

* LiveBitmap allows skipping dead/tombstoned records without scanning headers.
* Payloads can be inlined or pointer/offset into a larger blob region.

### 14.2 Update Flow

To update a record:

1. Insert new version into the appropriate leaf: birth_epoch = next_epoch, retire_epoch = U64_MAX, flags=0.
2. Add tombstone in the old leaf: birth_epoch = next_epoch, retire_epoch = U64_MAX, flags|=TOMBSTONE.
3. Publish (root_id, next_epoch) once — both changes become visible atomically.

### 14.3 Delete Flow

1. Insert a tombstone for the key with birth_epoch = next_epoch, retire_epoch = U64_MAX, flags|=TOMBSTONE, no payload.

### 14.4 Reader Visibility

A reader pinned to epoch E sees a record if:

* birth_epoch <= E, and
* retire_epoch == U64_MAX or retire_epoch > E, and
* (flags & TOMBSTONE) == 0.

Tombstones mask older versions from readers with epoch >= tombstone.birth_epoch.

### 14.5 Compaction & Reclamation

* In-leaf compaction: When tombstone ratio or dead space in a leaf exceeds a threshold, copy only live records to a new leaf and update OT.
* Tombstone cleanup: Remove tombstones once retire_epoch < min_active_reader_epoch() and all masked versions are also past that threshold.
* If a leaf becomes empty after compaction, retire it and free via OT reclaimer.

### 14.6 Advantages

* Full MVCC: Readers see a consistent snapshot without locks.
* Efficient deletes: Tombstones are just COW writes and don’t require tree rebalancing.
* Space recovery: Compactor removes tombstones and frees space once safe.

### 14.7 Optional Optimizations

* Maintain a KeyDir mapping key_id → (leaf NodeID, slot) to locate old versions quickly for tombstoning.
* Use out-of-line version chains for very hot keys to avoid rewriting large leaves frequently.

### 14.8 Summary

Tombstones integrate seamlessly into the persistence layer:
* Stored inside leaves with {birth_epoch, retire_epoch, flags}.
* Skip at query time via LiveBitmap.
* Cleaned up by compactor when safe by MVCC rules. This requires no redesign of the persistence layer, just an extension to leaf record format and iterator logic.



Record MVCC & Tombstones (Leaf-Level Design)

This section specifies record-level multi-version semantics inside leaf nodes, complementing the node/object-level MVCC already handled by the Object Table (OT).

Goals

Support updates and deletes as append-only logical operations per key.

Guarantee point-in-time visibility for readers pinned to a commit epoch.

Keep compaction simple and effective under high update/delete churn.

Record Header & Flags

Each record in a leaf carries a tiny MVCC header:

struct RecHdr {
  uint64_t key_id;        // stable logical key or hash
  uint64_t birth_epoch;   // first visible epoch (<= reader_epoch)
  uint64_t retire_epoch;  // U64_MAX if live; otherwise last visible epoch
  uint8_t  flags;         // bit 0 = TOMBSTONE, others reserved
  uint8_t  reserved[7];   // pad to 32B header (optional)
};

Visibility for a reader pinned to epoch E:

A record is visible iff birth_epoch <= E and (retire_epoch == ULLONG_MAX or retire_epoch > E) and (flags & TOMBSTONE) == 0.

Leaf Node Physical Layout

Leafs are optimized for scanning and selective rewrites. Use Structure-of-Arrays (SoA) for geometry/MBRs and keep variable payloads out-of-line where possible.

+----------------------+  LeafHeader (fixed)
| LeafHeader           |  - node kind, fanout/slot_count
|  - slot_count        |  - mbr_count
|  - has_out_of_line   |  - byte offsets to sections
+----------------------+
| RecHdr[slot_count]   |  // tightly packed headers for binary search by key_id
+----------------------+
| LiveBitset           |  // 1 bit per slot; optional fast-path filter
+----------------------+
| Keys/MBRs (SoA)      |  // if spatial: minX[], maxX[], minY[], maxY[], ...
+----------------------+
| Payload index        |  // varint offsets/lengths for per-record payloads
+----------------------+
| Payload blob area    |  // concatenated values (doc refs, postings, etc.)
+----------------------+

Notes

LiveBitset is redundant with MVCC but useful for fast scans; on delete/update, flip the bit to 0 when you COW the leaf.

If values are large, store them out-of-line via a small ValueVec object (addressed by NodeID), so leaf rewrites remain small.

Update & Delete Flows

Replace (update) key K:

Insert new version of K into the destination leaf (may differ if geometry/MBR changed):

RecHdr{ key_id=K, birth_epoch=next_epoch, retire_epoch=U64_MAX, flags=0 }

In the old leaf, append a tombstone for K or COW the header+livebit to mark the old slot as retired:

Tombstone form: RecHdr{ key_id=K, birth_epoch=next_epoch, retire_epoch=U64_MAX, flags=TOMBSTONE }

Or: keep a single live record per key and just set retire_epoch on the old record while flipping its live bit to 0.

Publish once with (root_id, next_epoch).

Delete key K:

Append a tombstone record in the current leaf of K with birth_epoch = next_epoch.

Publish as above.

Readers pinned < next_epoch still see the old version; newer readers see the new version or no version (if tombstoned).

Key Directory (optional but recommended)

Maintain a tiny KeyDir mapping key_id → (leaf NodeID, slot_index) (or short version chain). On update/delete, this lets the writer find the prior version without scanning. The KeyDir is an in-memory aid; it can be rebuilt from leaves during recovery if needed.

Leaf Compaction Policy

Tombstones and retired records create internal fragmentation. The compactor rewrites leaves when:

dead_ratio ≥ θ_dead (e.g., 40–60%), or

tombstone_ratio ≥ θ_ts, or

payload slack exceeds a size threshold.

Compaction steps:

Scan the leaf; copy visible-at-head live records (or, if doing historical snapshots, copy all records with retire_epoch >= safe_epoch).

Omit records with retire_epoch < safe_epoch and omit tombstones that only mask versions already older than safe_epoch.

Write the compacted leaf to a fresh extent; update its OT entry only (parents remain untouched unless MBR shrank/grew).

Old leaf’s OT entry gets retire_epoch = head_epoch and will be reclaimed once safe.

Interplay with OT-Level MVCC

Node-level MVCC (OT birth/retire_epoch) controls when entire nodes/child-vectors become visible and when their space can be reclaimed.

Record-level MVCC (leaf RecHdr) controls per-key visibility inside a leaf. This allows updates and deletes without reshaping parent nodes unless spatial placement changes.

No extra fsync ordering is needed: tombstones are normal payload writes within the same commit epoch.

On-Disk Compatibility & Cross-Platform Notes

Headers are little-endian with fixed sizes; varints for payload index. Align sections to 8 or 16 bytes to keep SIMD-friendly access.

Use the existing platform I/O abstraction for flushing (msync/FlushViewOfFile) and prefetching (madvise/F_RDADVISE/PrefetchVirtualMemory).

Constants & Defaults

θ_dead = 0.5, θ_ts = 0.4 (tune under load).

Live bitset optional; enable when update/delete rate is high.

For spatial leaves, keep SoA for MBRs to preserve vectorized filters on scans.

---

## 15. Cold Start Mode (Cross‑Platform)

### 15.1 Requirements

The system must start quickly on Linux, macOS, and Windows, even when the OS page cache is cold. All persistence and mapping APIs should be chosen to work cleanly across platforms.

### 15.2 Cold Start Procedure

1. **Map Superblock** and read `{root_id, commit_epoch}`.
2. **Load OT checkpoint**:

* Use a compact binary array format so it can be `mmap`’d and directly used as the in‑RAM OT structure.
* On Windows, use `CreateFileMapping`/`MapViewOfFile` with `FILE_FLAG_RANDOM_ACCESS`.
* On Linux/macOS, use `mmap(MAP_PRIVATE)` or `mmap(MAP_SHARED)`.
3. **Replay OT delta log** sequentially.
4. **Hotset Prefetch**:

* Place root, L1, and optionally L2 nodes in a dedicated **hotset segment**.
* On Linux: `madvise(MADV_WILLNEED)` or `readahead()`.
* On macOS: `fcntl(F_RDADVISE)`.
* On Windows: `PrefetchVirtualMemory` or read sequentially.
5. **Lazy Page‑In**: Map all other segments without pre‑faulting; the OS will load them on first touch.

### 15.3 Optional Warmup Routine

* A configurable “warmup” pass can traverse the top K nodes to pull them into memory.
* This is run asynchronously after the system starts accepting queries.

### 15.4 Platform‑Specific Notes

* **Linux**: Use `posix_fadvise(POSIX_FADV_WILLNEED)` for SSD/NVMe to get early read scheduling.
* **macOS**: Ensure file mappings are aligned to page boundaries; use `fcntl` hints for read‑ahead.
* **Windows**: Use large pages for the hotset mapping if `SeLockMemoryPrivilege` is available.

---

## 16. Risks & Mitigations

* **OT growth:** mitigate via recycling handles and periodic checkpoints.
* **fsync cost:** group commit; align OT log records to 4K; allow relaxed durability (config) for bulk ingest.
* **Compaction CPU:** backpressure via class‑level allocation pressure; throttle to keep <10% CPU under steady state.

---

## 17. Open Questions

* Final size‑class map after measuring real node/leaf sizes.
* Do we want intra‑segment bitmaps in v1 or keep pure append + compactor?
* Child vector capacity multipliers (×1.5 vs ×2) vs cache footprint.

---

## 18. Next Steps


1. Land `NodeID`, OT, and Superblock scaffolding.
2. Convert parent→child storage to NodeID (remove raw offsets).
3. Implement size‑class allocators + append‑only segments.
4. Teach writer publish path (payload → OT log → root+epoch) and recovery.
5. Add epoch‑based reclaimer + basic compactor.
6. Benchmark with random real‑world datasets; tune size classes and thresholds.

---

## Appendix A: OT Delta Log Record (Binary)

```
struct OTDlt {
 uint64_t handle_idx;
 uint16_t tag;
 uint8_t class_id;
 uint8_t kind;
 uint32_t file_id;
 uint32_t segment_id;
 uint64_t file_offset;
 uint32_t length;
 uint64_t birth_epoch;
 uint64_t retire_epoch; // U64_MAX if live
 uint64_t crc32; // record crc (optional)
};

```


## Appendix B: Commit Pseudocode


```
// Writer thread
payload_write(); // mmap writes / msync as needed
append_ot_deltas(); // write OTDlt records
fsync(ot_log);
sb.root_id.store(new_root, release);
sb.commit_epoch.store(new_epoch, release);
fsync(superblock);
global_epoch.store(new_epoch, release);
```

## Appendix C: Reader Fast Path

```
snap = { sb.root_id.load(acq), sb.commit_epoch.load(acq) }
pin_epoch_tls(snap.epoch)
traverse(NodeID = snap.root)
// each child: OT[handle].addr + offset (verify tag if debugging)
```

## Appendix D: persistence/ Directory Layout & Dependency Diagram

### D.1 Files and Responsibilities

```
persistence/
  platform_fs.h/.cpp       # mmap/flush/prefetch/atomic-replace, per-OS
  superblock.hpp/.cpp      # Superblock I/O, publish (root,epoch)
  object_table.h/.cpp      # NodeID, OTEntry, ObjectTable API
  ot_delta_log.h/.cpp      # Append-only OT log writer/reader, replay
  segment_allocator.h/.cpp # Size-class segments, bump alloc, freelist/bitmap
  compactor.h/.cpp         # Victim select, evacuation, locality packing
  reclaimer.h/.cpp         # Epoch-based GC, handle recycle, space free
  recovery.h/.cpp          # Boot: load superblock, map OT, replay log
  hotset.h/.cpp            # Pack/warmup root/L1/L2, cold-start hints
```

### D.2 Public Interfaces (minimal headers)

```
// object_table.h
namespace persist {
  using NodeID = uint64_t; // [63:8]=handle, [7:0]=tag
  enum class NodeKind : uint8_t { Internal, Leaf, ChildVec };
  struct OTAddr { uint32_t file_id, segment_id; uint64_t offset; void* vaddr; uint32_t length; };
  struct OTEntry; // opaque
  class ObjectTable {
   public:
    NodeID allocate(NodeKind kind, uint8_t class_id, uint32_t length, OTAddr addr,
                    uint64_t birth_epoch);
    void   retire(NodeID id, uint64_t retire_epoch);
    const OTEntry& get(uint64_t handle_idx) const; // fast array index
    uint8_t current_tag(uint64_t handle_idx) const;
  };
}
```
```
// superblock.hpp
namespace persist {
  struct Snapshot { uint64_t epoch; NodeID root; };
  class Superblock {
   public:
    Snapshot load() const;              // atomic acquire
    void publish(NodeID new_root, uint64_t new_epoch); // fsync ordering
  };
}
```
```
// segment_allocator.h
namespace persist {
  struct Alloc { uint32_t file_id, segment_id; uint64_t offset; uint32_t length; };
  class SegmentAllocator {
   public:
    Alloc alloc(uint8_t class_id, uint32_t length);
    void  free(uint8_t class_id, uint32_t file, uint32_t seg, uint64_t off, uint32_t len);
  };
}
```

### D.3 Layering & Allowed Dependencies

[xtree/*] ──depends on──> [persistence/object_table.h, superblock.h]
                         [persistence/segment_allocator.h]

[persistence/*].cpp may include:
  - platform_fs.h (OS syscalls)
  - object_table.h (within persistence only)
  - ot_delta_log.h (used by recovery/commit paths)

Forbidden:
  - xtree/* must not include platform_fs.h or call OS APIs directly.
  - persistence/* must not include xtree internals (no circular deps).

### D.4 Include Graph (high level)

xtree/xtree.h
  └─ uses persist::NodeID, persist::ObjectTable
xtree/xtiter.h
  └─ uses persist::ObjectTable::get() to resolve NodeID → vaddr
xtree/indexdetails.hpp (store)
  └─ owns persist::Superblock, SegmentAllocator, ObjectTable

persistence/recovery.cpp
  ├─ superblock.hpp
  ├─ object_table.h
  ├─ ot_delta_log.h
  └─ platform_fs.h

persistence/commit.cpp (or in superblock.cpp)
  ├─ superblock.hpp
  ├─ ot_delta_log.h
  └─ platform_fs.h

### D.5 Build Targets (CMake example)

add_library(persistence
  platform_fs.cpp superblock.cpp object_table.cpp ot_delta_log.cpp
  segment_allocator.cpp compactor.cpp reclaimer.cpp recovery.cpp hotset.cpp)

target_include_directories(persistence PUBLIC ${CMAKE_SOURCE_DIR})

add_library(xtree_core xtree/xtree.cpp xtree/xtiter.cpp)
target_link_libraries(xtree_core PUBLIC persistence)

### D.6 Unit Tests to Add

ot_delta_log_test: round-trip records, crash-recovery tail replay.
object_table_test: allocate/retire/recycle; tag increments; ABA checks.
segment_allocator_test: class isolation, fragmentation bounds, free/alloc stress.
superblock_test: publish ordering, torn-write detection.
recovery_test: cold start from checkpoint + log; Windows/Linux/macOS mapping paths (mocked).
compactor_test: evacuation updates only OT; parent NodeIDs unchanged.

### D.7 Integration Points in XTree

In XTreeBucket child links: store persist::NodeID (not offsets).

In iterators: on cache miss, resolve NodeID → OTAddr.vaddr via ObjectTable.

In writer path: allocate via SegmentAllocator, register in ObjectTable, append OT deltas, then Superblock::publish().
