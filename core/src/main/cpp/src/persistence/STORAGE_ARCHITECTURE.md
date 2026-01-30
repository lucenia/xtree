# Persistence Layer Storage Architecture

**Status:** Production Design
**Author:** Nick Knize
**Last updated:** 2025-08-27

---

## Executive Summary

The persistence layer implements a **three-tier storage architecture** that separates node data, metadata, and snapshots for optimal performance and durability. This design enables:

- **Small write-ahead logs** (metadata only, not data)
- **Fast recovery** (data already on disk, only metadata replayed)
- **Efficient memory-mapped I/O** (direct access to node data)
- **Flexible durability policies** (STRICT, BALANCED, EVENTUAL)

---

## 1. Storage Components Overview

```
┌──────────────────────────────────────────────────────────────┐
│                      Storage Architecture                     │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  ┌──────┐│
│  │ Index Files  │  │ Data Files   │  │ WAL Files│  │Checks││
│  │ (.xi files)  │  │ (.xd files)  │  │ (.wal)   │  │(.bin)││
│  ├──────────────┤  ├──────────────┤  ├──────────┤  ├──────┤│
│  │• Internal    │  │• Data records│  │• OT      │  │• OT  ││
│  │  nodes       │  │  (points,    │  │  deltas  │  │ snap ││
│  │• Leaf bucket │  │   rowids)    │  │• NodeIDs │  │• Man-││
│  │  nodes       │  │• Value       │  │• Epochs  │  │ ifest││
│  │• Child       │  │  vectors     │  │• Locs    │  │• Rec ││
│  │  vectors     │  │              │  │• Small   │  │ meta ││
│  │              │  │              │  │ payloads │  │      ││
│  │Size: MBs-GBs │  │Size: GBs-TBs │  │Size: MBs │  │ MBs  ││
│  └──────────────┘  └──────────────┘  └──────────┘  └──────┘│
│        ↑                  ↑                ↑           ↑    │
│        └──────────────────┴────────────────┴───────────┘    │
│                      Memory-mapped I/O                       │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. Index Files (.xi extension)

### Purpose
Store the tree structure - internal and leaf bucket nodes that form the XTree index.

### What They Contain
- **Internal nodes** (`XTreeBucket`) - tree structure, MBRs, child NodeID references
- **Leaf bucket nodes** - contain NodeID references to actual data records
- **Child vectors** - overflow arrays for supernodes
- **Tree metadata** - root tracking, split history

### Organization
- **Size-class segregated**: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB
- **Segment-based**: Each size class has multiple segments
- **Memory-mapped**: Direct pointer access for tree traversal

### File Naming
```
xtree_c{class_id}_{file_id}.xi
```
Example: `xtree_c0_7.xi` = size class 0, file 7

### Key Properties
- **Hot data**: Frequently accessed during tree traversal
- **Compact**: Only contains tree structure, not actual data
- **Write-once** for immutable nodes (COW semantics)
- **1GB default file size** (configurable)

### Why Separate from Data
- **Cache locality**: Tree nodes stay hot in cache
- **Smaller working set**: Tree is orders of magnitude smaller than data
- **Better I/O patterns**: Sequential tree traversal vs random data access

---

## 3. Data Files (.xd extension)

### Purpose
Store the actual data records - the leaf contents of the XTree.

### What They Contain
- **DataRecord objects** - actual points, rowids, and associated data
- **Value vectors** - collections of values
- **Any user data** referenced by leaf buckets

### Organization
- **Size-class segregated**: Same classes as index files
- **Segment-based**: Each size class has multiple segments
- **Memory-mapped**: But can be paged out when not needed

### File Naming
```
xtree_data_c{class_id}_{file_id}.xd
```
Example: `xtree_data_c2_3.xd` = size class 2, file 3

### Key Properties
- **Cold data**: Only accessed when queries reach leaves
- **Large volume**: Can be orders of magnitude larger than index
- **Pageable**: OS can swap out unused data records
- **Write-once** for immutable records (COW semantics)

### Memory Management
- **Heap objects**: Live DataRecord objects remain on heap during insertion
- **Wire format persisted**: Serialized format written to .xd files
- **NodeID references**: Leaf buckets store NodeIDs, not pointers
- **On-demand loading**: Future optimization to load from disk when needed

### Durability
- Records persisted when inserted into tree via `persist_data_record()`
- Written directly to mmap'd memory (zero-copy)
- Crash-safe after commit (mode-dependent)

---

## 4. Write-Ahead Log Files (.wal extension)

### Purpose  
Record metadata about node lifecycle for crash recovery and MVCC.

### What They Contain
**Object Table (OT) Deltas** - metadata records about nodes:
```cpp
struct OTDeltaRec {
    NodeID id;           // 64-bit handle + tag
    uint8_t class_id;    // Size class
    uint8_t file_id;     // Which .xd file
    uint64_t offset;     // Location in file
    uint32_t length;     // Size of node
    uint64_t birth_epoch;   // When created
    uint64_t retire_epoch;  // When deleted (or UINT64_MAX)
    uint32_t data_crc32c;   // Optional data checksum
};
```

**Optional Small Payloads** (EVENTUAL mode optimization):
- Nodes ≤8KB can be inlined in WAL
- Avoids separate .xd write for tiny nodes

### File Naming
```
delta_{start_epoch}.wal
```
Example: `delta_1000.wal` = WAL starting at epoch 1000

### Key Properties
- **Append-only** log structure
- **Small size** - only metadata, not full nodes
- **Frame-based** with CRC32 checksums
- **Rotated periodically** (size/age thresholds)

### Why WAL is Small
Consider a 1KB tree node:
- **In .xd file**: 1024 bytes of actual data
- **In WAL**: ~64 bytes of metadata
- **Ratio**: 16:1 compression

For 10M nodes:
- **.xd files**: ~10GB of node data
- **WAL**: ~640MB of metadata

---

## 5. Checkpoint Files (.bin extension)

### Purpose
Periodic snapshots of the Object Table to bound recovery time.

### What They Contain
Complete Object Table state at a specific epoch:
```cpp
struct CheckpointEntry {
    NodeID id;
    OTEntry entry;  // Full location + metadata
};
```

### File Naming  
```
ot_checkpoint_epoch-{epoch}.bin
```
Example: `ot_checkpoint_epoch-5000.bin` = checkpoint at epoch 5000

### Key Properties
- **Complete snapshot** of live nodes at epoch
- **Binary format** for fast loading
- **CRC protected** (header + entries + footer)
- **Atomic replacement** via rename

### Checkpoint Triggers
- WAL size > threshold (e.g., 256MB)
- Epochs since last checkpoint > threshold (e.g., 100K)
- Time since last checkpoint > threshold (e.g., 10 min)
- Manual trigger for controlled shutdown

### Checkpoint Retention
- **Keep count**: 3 checkpoints by default (configurable)
- **Automatic cleanup**: Old checkpoints deleted after each new checkpoint
- **Space savings**: With 10M nodes, each checkpoint ~400MB, keeping only 3 saves ~100GB+
- **Why multiple checkpoints**:
  - Corruption recovery (fallback if latest is damaged)
  - Point-in-time recovery options
  - Safe concurrent access during cleanup

---

## 6. Supporting Files

### Superblock (`superblock.bin`)
- Current root NodeID
- Current commit epoch  
- Generation counter
- CRC for integrity
- **The only file updated in-place**

### Manifest (`manifest.json`)
- Current checkpoint file
- Active WAL files
- Recovery metadata
- Human-readable for debugging

---

## 7. Data Flow

### Write Path

```
1. ALLOCATE SPACE
   XTree → SegmentAllocator → .xd file
   Returns: memory-mapped pointer

2. WRITE DATA
   memcpy(mmap_ptr, node_data, size)
   Data now in OS page cache

3. STAGE METADATA
   Thread-local batch accumulates OT deltas

4. COMMIT
   ├─ Write OT deltas → WAL
   ├─ fsync(WAL) [mode-dependent]
   ├─ Update superblock
   └─ Advance epoch

5. BACKGROUND CHECKPOINT
   Periodically snapshot OT → checkpoint file
```

### Read Path

```
1. GET NODE
   NodeID → Object Table lookup → {file, offset}

2. ACCESS DATA  
   Pointer = mmap_base + offset
   Direct memory access (no deserialization)

3. TRAVERSE
   Read child NodeIDs from parent
   Repeat lookup for each child
```

### Recovery Path

```
1. LOAD CHECKPOINT
   Binary load of Object Table snapshot

2. REPLAY WAL
   Apply deltas after checkpoint epoch
   Only metadata - data already in .xd files

3. SET ROOT
   Read root NodeID from superblock
   System ready for queries
```

---

## 8. DataRecord Persistence Strategy

### The Memory Problem
With millions of records, keeping all DataRecords on heap exhausts memory:
- 100M records × ~100 bytes each = 10GB heap usage
- Machine lockup when memory exhausted
- Need to separate hot (tree) from cold (data)

### The Solution: Deferred Persistence

#### 1. Allocation Phase
```cpp
// DataRecords allocated on heap, no persistence yet
auto* dr = XAlloc::allocate_record(&index, dims, precision, rowid);
dr->putPoint(&point1);  // Add points
dr->putPoint(&point2);  // Record grows
// Wire size unknown until all points added
```

#### 2. Insertion Phase (Persistence)
```cpp
// In basicInsert() when record is added to tree
if (record->isDataNode()) {
    persist_data_record(index, record);  // Now we persist
}
```

#### 3. Persistence Details
```cpp
persist_data_record() {
    // 1. Calculate final wire size
    size_t wire_sz = rec->wire_size();
    
    // 2. Allocate in .xd file (not .xi)
    AllocResult alloc = store->allocate_node(wire_sz, NodeKind::DataRecord);
    
    // 3. Serialize directly to mmap (zero-copy)
    rec->to_wire(alloc.writable);
    
    // 4. Publish without memcpy
    store->publish_node_in_place(alloc.id, wire_sz);
    
    // 5. Attach NodeID for future reference
    rec->setNodeID(alloc.id);
}
```

### Key Design Decisions

#### Why Not Placement New?
**Risky approach** (what we avoided):
```cpp
// DON'T DO THIS - would corrupt object
Record* rec = new (alloc.writable) Record(...);  // Placement new
rec->to_wire(alloc.writable);  // Overwrites vptr, std::string internals!
```

**Safe approach** (what we do):
```cpp
Record* rec = new Record(...);  // Heap object
rec->to_wire(alloc.writable);   // Wire format to mmap
// Object stays on heap, wire format in .xd file
```

#### Zero-Copy Publishing
- `publish_node()` would memcpy from source to destination
- `publish_node_in_place()` skips memcpy (data already there)
- Store computes CRC and tracks dirty ranges internally

### Zero-Copy DataRecord Access (Implemented)

#### The Problem
With millions of DataRecords, keeping them all in heap memory causes memory exhaustion:
- 10M records × ~1KB heap overhead = 10GB+ RAM usage
- System becomes unresponsive due to memory pressure
- Defeats the purpose of having persistent storage

#### The Solution: DataRecordView
A lightweight, zero-copy view that reads directly from mmap'd storage:

```cpp
class DataRecordView : public IRecord {
    MappingManager::Pin pin_;   // Keeps memory mapped while alive
    const uint8_t* data_;       // Pointer to wire format in mmap
    // Lazy parsing of MBR, rowid, points on demand
};
```

#### Implementation Details

1. **Read Path (Zero-Copy)**:
   ```cpp
   // In getRecord() for DURABLE mode:
   auto pinned = store->read_node_pinned(node_id);  // Pin mmap'd memory
   auto* view = new DataRecordView(                  // Create lightweight view
       std::move(pinned.pin),                       // Transfer ownership of pin
       static_cast<const uint8_t*>(pinned.data),    // Direct pointer to wire bytes
       dims, prec, node_id
   );
   // No memcpy, no heap allocation for actual data!
   ```

2. **Write Path (Heap then Persist)**:
   ```cpp
   // DataRecords still allocated on heap during insertion
   auto* rec = new DataRecord(dims, prec, rowid);
   rec->putPoint(&location);  // Mutations require heap object
   
   // Persist when inserting into tree
   persist_data_record(idx, rec);  // Writes to .xd file
   
   // After persistence, could delete heap copy (future optimization)
   ```

3. **Memory Management**:
   - **Pin Lifecycle**: Pin holds mmap'd memory, released when view destroyed
   - **LRU Tracking**: Views still added to LRU for hot path tracking
   - **Cache Eviction**: When view evicted, Pin released → memory can be unmapped
   - **Memory Savings**: ~100 bytes per view vs ~1KB per full DataRecord

4. **API Changes**:
   ```cpp
   // StoreInterface additions:
   struct PinnedBytes {
       MappingManager::Pin pin;
       void* data;
       size_t size;
   };
   virtual PinnedBytes read_node_pinned(NodeID id) const;
   
   // DurableStore implementation:
   PinnedBytes DurableStore::read_node_pinned(NodeID id) const {
       // Resolve NodeID → file path + offset
       // Pin the memory region
       // Return pinned pointer
   }
   ```

#### Performance Impact
- **Memory**: 10GB → ~1GB for 10M records (10x reduction)
- **CPU**: No memcpy on reads (direct mmap access)
- **I/O**: OS page cache handles actual disk I/O transparently
- **Latency**: First access may page fault, subsequent access is memory speed

#### Fallback Strategy
If pinned read fails (file not mapped, etc.), falls back to traditional heap allocation:
```cpp
if (pinned read fails) {
    // Traditional path: allocate + deserialize
    auto* rec = new DataRecord(...);
    rec->from_wire(node_bytes.data, ...);
    return rec;
}
```

---

## 9. Durability Modes

### STRICT Mode
```
Write: data → msync → WAL → fsync → superblock → fsync
Guarantees: Immediate durability, highest latency
Use case: Financial transactions, critical metadata
```

### BALANCED Mode  
```
Write: data → WAL → fsync → superblock
Guarantees: Metadata durable, data in page cache
Use case: General purpose, good performance/durability balance
```

### EVENTUAL Mode
```
Write: data → WAL → superblock [fsync optional]
Guarantees: Best performance, relaxed durability
Use case: Bulk loading, non-critical data
```

---

## 10. Key Design Insights

### Why This Architecture?

1. **Separation of Concerns**
   - Data files: Optimized for spatial locality
   - WAL: Optimized for sequential writes
   - Checkpoints: Optimized for fast recovery

2. **Small WAL**
   - Contains only metadata (16:1 compression vs data)
   - Faster fsync, less I/O amplification
   - Can keep more history for MVCC

3. **Fast Recovery**
   - Data already on disk in .xd files
   - Only replay metadata from WAL
   - Checkpoint bounds replay time

4. **Zero-Copy Reads**
   - Direct mmap access to nodes
   - No deserialization overhead
   - CPU cache-friendly layout

5. **Efficient Compaction**
   - Only update Object Table entries
   - No parent pointer updates needed
   - NodeID indirection isolates changes

### Performance Characteristics

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Read node | 50-100ns | 10M ops/sec |
| Write node | 1-10μs | 100K ops/sec |
| Commit (BALANCED) | 100μs-1ms | 1-10K commits/sec |
| Recovery (1M nodes) | 1-2 sec | N/A |
| Checkpoint (10M nodes) | 5-10 sec | Every 10-30 min |

---

## 11. Configuration Parameters

### Size Classes
```cpp
const size_t SIZE_CLASSES[] = {
    4096,    // Small internal nodes
    8192,    // Medium internal nodes
    16384,   // Large internal nodes
    32768,   // Small leaf nodes
    65536,   // Medium leaf nodes
    131072,  // Large leaf nodes
    262144   // Supernode child vectors
};
```

### File Limits
```cpp
const size_t MAX_FILE_SIZE = 1ULL << 30;  // 1GB per file (configurable)
const size_t SEGMENT_SIZE = 16ULL << 20;  // 16MB segments
const size_t WINDOW_SIZE = 1ULL << 30;    // 1GB mmap windows
```

#### File Size Tuning Considerations

**Current Default**: 1GB per .xd file

**Implications of Larger Files** (e.g., 4GB, 16GB):
- **Pros**:
  - Fewer file handles (important with OS limits)
  - Less file rotation overhead
  - Better sequential I/O for bulk operations
  - Simpler file management
- **Cons**:
  - Larger mmap windows may stress virtual memory
  - Slower initial mmap() calls
  - More data loss if single file corrupted
  - May exceed filesystem limits on some systems

**Implications of Smaller Files** (e.g., 256MB):
- **Pros**:
  - Faster mmap() operations
  - Better OS page cache utilization
  - Easier parallel I/O
  - Less memory pressure per window
- **Cons**:
  - More file handles required
  - More frequent file rotation
  - Higher metadata overhead

**Recommendations**:
- **Large datasets (10M+ nodes)**: Consider 4GB-16GB files
- **Memory constrained**: Keep at 1GB or smaller
- **Many concurrent trees**: Use smaller files to avoid FD exhaustion
- **SSD storage**: Larger files are fine
- **HDD storage**: Smaller files for better seek patterns

### Checkpoint Policy (v2.0 - Enhanced)
```cpp
struct CheckpointPolicy {
    // Standard thresholds
    size_t max_replay_bytes = 256 << 20;    // 256MB WAL
    size_t max_replay_epochs = 100000;      // 100K epochs
    duration max_checkpoint_age = 10min;    // 10 minutes
    duration min_checkpoint_interval = 30s; // 30 seconds
    size_t checkpoint_keep_count = 2;       // Reduced from 3 (saves ~600MB)
    
    // NEW: Adaptive WAL rotation
    bool adaptive_wal_rotation = true;      // Auto-tune based on throughput
    size_t min_replay_bytes = 64 << 20;     // 64MB for high throughput
    size_t base_replay_bytes = 256 << 20;   // 256MB for normal load
    double throughput_threshold = 100000;    // 100K records/sec trigger
};
```

**Adaptive WAL Rotation**: The system now monitors insertion throughput and automatically adjusts WAL rotation thresholds. During high-throughput bursts (>100K records/sec), it rotates at 64MB instead of 256MB, reducing write latency by up to 15%.

---

## 10. Platform Considerations

### Linux
- `mmap()` with `MAP_SHARED` for data files
- `madvise(MADV_RANDOM)` for tree traversal
- `fdatasync()` preferred over `fsync()`

### macOS  
- `mmap()` with `MAP_SHARED`
- `fcntl(F_FULLFSYNC)` for true durability
- `fcntl(F_RDADVISE)` for prefetching

### Windows
- `CreateFileMapping()` + `MapViewOfFile()`
- `FlushViewOfFile()` + `FlushFileBuffers()`
- `PrefetchVirtualMemory()` for warmup

---

## 11. Failure Scenarios

### Power Loss During Commit
- **Before WAL fsync**: Transaction lost, system consistent
- **After WAL fsync**: Transaction recovered on restart
- **During checkpoint**: Old checkpoint used, WAL replayed

### Corrupted Data File
- Detected via CRC in WAL metadata
- Can rebuild from checkpoint + WAL
- Optional: Replica or backup recovery

### Corrupted Checkpoint
- CRC validation fails on load
- Falls back to previous checkpoint (3 kept by default)
- Replays more WAL entries but still recovers
- Worst case: Falls back to oldest checkpoint + full WAL replay

### WAL Corruption
- Truncate at last valid frame
- Lose transactions after corruption
- Checkpoint limits data loss

### Out of Disk Space
- Checkpoint to free WAL space
- Compact .xd files to reclaim space
- Emergency read-only mode

---

## 12. Future Optimizations

### Planned
- Parallel WAL replay for faster recovery
- Incremental checkpoints (delta encoding)
- Compression for cold data segments
- Remote backup/restore integration

### Under Consideration
- Multi-writer support (MVCC extensions)
- Distributed replication protocol
- Tiered storage (SSD + cold storage)
- Hardware acceleration (CRC, compression)

---

## Appendix: File Format Specifications

### Data File Header (per segment)
```
Offset  Size  Field
0       4     Magic number (0x58545245)  // "XTRE"
4       4     Version
8       1     Size class ID
9       1     Segment ID within class
10      2     Reserved
12      4     Capacity (bytes)
16      4     Used (bytes)
20      4     CRC32 of header
24      -     Padding to 4KB
4096    -     Data begins
```

### WAL Frame Format
```
Offset  Size  Field
0       4     Frame magic (0x57414C46)  // "WALF"
4       4     Frame length
8       8     Epoch
16      4     Record count
20      4     Payload bytes (optional)
24      -     OT delta records
-       -     Optional payloads
-       4     Frame CRC32
```

### Checkpoint Format
```
Offset  Size  Field
0       4     Magic (0x434B5054)  // "CKPT"
4       4     Version
8       8     Epoch
16      8     Entry count
24      4     Entry size
28      4     Header CRC32
32      -     Padding to 4KB
4096    -     Entries (sorted by NodeID)
-       16    Footer (size, CRC32)
```