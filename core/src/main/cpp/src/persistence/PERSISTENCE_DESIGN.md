# Persistent Copy-on-Write with MVCC Design

**Status:** Production Design  
**Author:** Nick Knize  
**Last updated:** 2025-08-27

---

## Overview

This document describes the copy-on-write (COW) persistence layer with multi-version concurrency control (MVCC) that provides:

- **Lock-free concurrent reads** during writes
- **Zero-downtime snapshots** with O(1) root pointer flip  
- **Bounded fragmentation** via size-class allocation
- **Fast crash recovery** with write-ahead logging
- **Flexible durability modes** for different workloads

For detailed storage architecture, see [STORAGE_ARCHITECTURE.md](./STORAGE_ARCHITECTURE.md).

---

## 1. Core Concepts

### 1.1 Copy-on-Write (COW)

Every modification creates a new version rather than updating in place:
- **Immutable nodes**: Once written, nodes never change
- **New root on commit**: Each transaction publishes a new root
- **Reader isolation**: Queries see a consistent snapshot

### 1.2 Multi-Version Concurrency Control (MVCC)

Multiple versions coexist, identified by monotonic epochs:
- **Global epoch**: 64-bit counter, incremented on commit
- **Birth/retire epochs**: Define node visibility window
- **Reader epochs**: Pin a snapshot for query duration

### 1.3 NodeID Indirection

All references use indirect 64-bit NodeIDs, not pointers:
```cpp
NodeID = (handle_index << 8) | tag
```
- **handle_index (56 bits)**: Index into Object Table
- **tag (8 bits)**: ABA prevention counter

Benefits:
- **Stable references** across compaction
- **Compact storage** (8 bytes per reference)
- **Safe recycling** via tag validation

---

## 2. Architecture Components

### 2.1 Object Table (OT)

Central directory mapping NodeIDs to physical locations:

```cpp
struct OTEntry {
    // Location
    uint32_t file_id;      // Which data file
    uint32_t segment_id;   // Which segment in file
    uint64_t offset;       // Byte offset in segment
    uint32_t length;       // Size of node
    
    // Metadata
    uint8_t  class_id;     // Size class
    NodeKind kind;         // Internal/Leaf/ChildVec
    uint8_t  tag;          // Must match NodeID tag
    
    // MVCC
    uint64_t birth_epoch;  // First visible epoch
    uint64_t retire_epoch; // Last visible epoch (or MAX)
};
```

The OT is:
- **In-memory** for O(1) lookups
- **Persisted** via delta log + checkpoints
- **Thread-safe** for concurrent access

### 2.2 Segment Allocator

Manages space in data files with size-class segregation:

```cpp
class SegmentAllocator {
    // Size classes: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB
    static constexpr size_t SIZE_CLASSES[] = {
        4096, 8192, 16384, 32768, 65536, 131072, 262144
    };
    
    Allocation allocate(size_t size);  // Returns mmap'd pointer
    void free(Allocation& a);          // Returns to free list
};
```

Benefits:
- **No external fragmentation** across size classes
- **Bounded internal fragmentation** within classes
- **Fast allocation** via bump pointer
- **Efficient compaction** per size class

### 2.3 MVCC Context

Manages epochs and reader coordination:

```cpp
class MVCCContext {
    std::atomic<uint64_t> global_epoch{0};
    
    uint64_t advance_epoch();           // Increment on commit
    uint64_t pin_epoch();               // Start reader session
    void unpin_epoch(uint64_t e);      // End reader session
    uint64_t min_active_epoch() const;  // For safe reclamation
};
```

### 2.4 Checkpoint Coordinator

Manages durability and recovery:

```cpp
class CheckpointCoordinator {
    void append_deltas(const std::vector<OTDelta>& deltas);
    void do_checkpoint(uint64_t epoch);
    void recover(const std::string& checkpoint_path);
};
```

Background thread periodically checkpoints based on:
- WAL size threshold
- Epoch count threshold  
- Time threshold
- Manual trigger

Checkpoint retention:
- Keeps 3 most recent checkpoints (configurable)
- Automatic cleanup after each new checkpoint
- Prevents unbounded disk usage

---

## 3. Transaction Flow

### 3.1 Write Transaction

```cpp
// 1. Begin transaction (reader epoch)
auto epoch = mvcc.pin_epoch();

// 2. Traverse to find modification points
// ... tree traversal using current root ...

// 3. Allocate new nodes (COW)
auto alloc = allocator.allocate(node_size);
Node* new_node = new (alloc.ptr) Node(...);

// 4. Stage OT delta
deltas.push_back({
    .id = allocate_node_id(),
    .location = alloc.location,
    .birth_epoch = 0,  // Set on commit
    .retire_epoch = UINT64_MAX
});

// 5. Stage retirements
if (old_node) {
    deltas.push_back({
        .id = old_node_id,
        .retire_epoch = 0  // Set on commit
    });
}

// 6. Commit
uint64_t commit_epoch = mvcc.advance_epoch();
stamp_epochs(deltas, commit_epoch);
wal.append(deltas);
wal.fsync();
superblock.publish(new_root, commit_epoch);
mvcc.unpin_epoch(epoch);
```

### 3.2 Read Transaction

```cpp
// 1. Snapshot root and epoch
auto snapshot = superblock.load();
auto epoch = snapshot.epoch;
auto root_id = snapshot.root_id;

// 2. Pin epoch for consistency
mvcc.pin_epoch(epoch);

// 3. Traverse using NodeIDs
void* node_ptr = resolve_node(root_id);
// ... traverse tree ...

// 4. Unpin when done
mvcc.unpin_epoch(epoch);
```

### 3.3 Node Resolution

```cpp
void* resolve_node(NodeID id) {
    uint64_t handle = id >> 8;
    uint8_t tag = id & 0xFF;
    
    const OTEntry& entry = object_table[handle];
    
    // Validate tag (ABA check)
    if (entry.tag != tag) return nullptr;
    
    // Check MVCC visibility
    if (entry.birth_epoch > reader_epoch) return nullptr;
    if (entry.retire_epoch <= reader_epoch) return nullptr;
    
    // Return memory-mapped pointer
    return segments[entry.segment_id].base + entry.offset;
}
```

---

## 4. Durability Guarantees

### 4.1 Commit Ordering

Critical for crash consistency:

```
1. Write node data to mmap'd segments
2. Append OT deltas to WAL
3. fsync(WAL)
4. Update superblock (root + epoch)  
5. fsync(superblock)
6. Advance in-memory epoch
```

### 4.2 Recovery Process

```
1. Load superblock → get last committed root + epoch
2. Load latest checkpoint → restore Object Table
3. Replay WAL from checkpoint epoch → apply deltas
4. Set root and epoch from superblock
5. System ready for queries
```

### 4.3 Durability Modes

| Mode | Data Durability | Metadata Durability | Performance |
|------|-----------------|---------------------|-------------|
| STRICT | Immediate (msync) | Immediate (fsync) | Slowest |
| BALANCED | Page cache | Immediate (fsync) | Moderate |  
| EVENTUAL | Page cache | Configurable | Fastest |

---

## 5. Compaction & Reclamation

### 5.1 Epoch-Based Reclamation

Safe deletion when no readers need old versions:

```cpp
void reclaim() {
    uint64_t safe_epoch = mvcc.min_active_epoch();
    
    for (auto& entry : object_table) {
        if (entry.retire_epoch < safe_epoch) {
            allocator.free(entry.location);
            entry.tag++;  // Increment for ABA safety
            entry.kind = NodeKind::Free;
            free_list.push(entry.handle);
        }
    }
}
```

### 5.2 Segment Compaction

Defragment within size classes:

```cpp
void compact_segment(Segment* victim) {
    // Only if > 50% dead space
    if (victim->dead_ratio() < 0.5) return;
    
    // Copy live nodes to active segment
    for (auto& node : victim->live_nodes()) {
        auto new_loc = allocator.allocate(node.size);
        memcpy(new_loc.ptr, node.ptr, node.size);
        
        // Update only OT entry (no parent updates!)
        object_table[node.id].location = new_loc;
    }
    
    // Retire entire segment
    victim->retire();
}
```

---

## 6. Optimizations

### 6.1 Supernode Handling

For nodes exceeding normal fanout:

```cpp
struct Supernode {
    NodeHeader header;      // Fixed size
    NodeID child_vec_id;    // Reference to overflow array
};

struct ChildVector {
    uint32_t capacity;
    uint32_t count;
    NodeID children[];      // Variable size
};
```

Benefits:
- Parent stays small during growth
- Child vector can grow independently
- Better cache locality for headers

### 6.2 Hot Path Optimizations

- **L1/L2 caching**: Pin root and top levels
- **Prefetching**: `madvise()` for sequential access
- **SIMD operations**: MBR comparisons vectorized
- **Lock-free reads**: No synchronization needed

### 6.3 Group Commit

Batch multiple writers for efficiency:

```cpp
void group_commit() {
    collect_deltas_for_5ms();
    single_wal_append();
    single_fsync();
    notify_all_writers();
}
```

---

## 7. Configuration

### 7.1 Size Class Tuning

```cpp
// Tune based on workload analysis
struct SizeClassConfig {
    size_t internal_small = 4096;    // Most internal nodes
    size_t internal_large = 16384;   // Supernodes
    size_t leaf_small = 32768;       // Regular leaves
    size_t leaf_large = 131072;      // Dense leaves
    size_t vector_max = 262144;      // Child vectors
};
```

### 7.2 Checkpoint Policy

```cpp
struct CheckpointPolicy {
    // Triggers
    size_t max_wal_bytes = 256 << 20;      // 256MB
    size_t max_epochs = 100000;            // 100K commits
    duration max_age = minutes(10);        // 10 minutes
    
    // Throttling
    duration min_interval = seconds(30);   // Avoid thrashing
    bool checkpoint_on_shutdown = true;    // Clean shutdown
    
    // Retention
    size_t checkpoint_keep_count = 3;      // Keep 3 most recent
};
```

**Note**: With large datasets (10M+ nodes), checkpoints can be 300-400MB each. 
Without cleanup, hundreds of checkpoints could consume 100GB+ of disk space.

### 7.3 Compaction Policy

```cpp
struct CompactionPolicy {
    float dead_ratio_threshold = 0.5;      // Trigger at 50% dead
    size_t min_segment_size = 1 << 20;     // Don't compact tiny segments
    size_t max_concurrent = 2;             // Parallel compaction limit
    float cpu_limit = 0.1;                 // Max 10% CPU for background
};
```

---

## 8. Performance Analysis

### 8.1 Space Overhead

| Component | Overhead | Notes |
|-----------|----------|-------|
| NodeID references | 8 bytes per child | vs raw pointers |
| Object Table | 64 bytes per node | In-memory index |
| WAL metadata | 64 bytes per operation | Not data |
| Tag (ABA) | 1 byte per node | Safety overhead |

### 8.2 Time Complexity

| Operation | Complexity | Typical Latency |
|-----------|------------|-----------------|
| Node lookup | O(1) | 50-100ns |
| Allocation | O(1) | 100-500ns |
| Commit | O(deltas) | 100μs-1ms |
| Checkpoint | O(nodes) | 1-10s |
| Recovery | O(checkpoint + WAL) | 1-5s |

### 8.3 Scalability Limits

| Dimension | Limit | Reason |
|-----------|-------|--------|
| Nodes | 2^56 | Handle space |
| Concurrent readers | 10,000+ | Lock-free |
| File size | 1TB+ | Platform limits |
| Transaction size | 1M deltas | Memory |

---

## 9. Testing Strategy

### 9.1 Correctness Tests

- **MVCC isolation**: Readers see consistent snapshots
- **Crash recovery**: All durability modes recover correctly
- **ABA prevention**: Tag validation catches recycled handles
- **Compaction safety**: No data loss during defrag

### 9.2 Performance Tests

- **Throughput**: Measure ops/sec under various loads
- **Latency**: P50, P99, P99.9 for operations
- **Scalability**: Performance vs data size
- **Concurrency**: Reader scaling, writer batching

### 9.3 Stress Tests

- **Long-running**: 24+ hour workloads
- **Crash injection**: Kill -9 at random points
- **Space exhaustion**: Handle disk full gracefully
- **Memory pressure**: Operate under constrained RAM

---

## 10. Related Documents

- [STORAGE_ARCHITECTURE.md](./STORAGE_ARCHITECTURE.md) - Detailed storage design
- [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) - Development roadmap
- [DURABILITY_CONTRACT_CHECKLIST.md](./DURABILITY_CONTRACT_CHECKLIST.md) - Correctness requirements
- [DURABILITY_POLICIES.md](./DURABILITY_POLICIES.md) - Mode configurations

---

## Appendix A: Key Algorithms

### A.1 Tag Increment (ABA Prevention)

```cpp
uint8_t next_tag(uint8_t current) {
    // Cycle through [1..255], skip 0
    return (current == 255) ? 1 : (current + 1);
}
```

### A.2 Epoch Visibility Check

```cpp
bool is_visible(const OTEntry& e, uint64_t reader_epoch) {
    return e.birth_epoch <= reader_epoch &&
           (e.retire_epoch == UINT64_MAX || 
            e.retire_epoch > reader_epoch);
}
```

### A.3 Size Class Selection

```cpp
size_t select_size_class(size_t requested) {
    // Round up to next power of 2
    size_t size = 1;
    while (size < requested && size < MAX_CLASS_SIZE) {
        size <<= 1;
    }
    return size;
}
```

---

## Appendix B: Wire Format

See [STORAGE_ARCHITECTURE.md](./STORAGE_ARCHITECTURE.md) for detailed format specifications.

---

## Appendix C: Platform-Specific Notes

### Linux
- Use `madvise(MADV_RANDOM)` for tree traversal
- `fdatasync()` faster than `fsync()` when metadata unchanged
- Consider `O_DIRECT` for WAL writes

### macOS
- Must use `fcntl(F_FULLFSYNC)` for true durability
- `mmap()` coherency requires `msync()` before `fsync()`
- Avoid `F_NOCACHE` with mmap

### Windows
- `FILE_FLAG_NO_BUFFERING` requires aligned I/O
- `FlushViewOfFile()` before `FlushFileBuffers()`
- Consider `FILE_FLAG_WRITE_THROUGH` for WAL

---

## Appendix D: Future Extension - Record-Level MVCC & Tombstones

*Note: This section describes a potential future enhancement for record-level versioning within leaf nodes.*

### Overview

While the current design provides node-level MVCC, record-level MVCC inside leaves would enable:
- Fine-grained updates without rewriting entire leaves
- Logical deletes via tombstones
- Version chains for frequently updated records

### Proposed Leaf Layout

```cpp
struct RecordHeader {
    uint64_t key_id;         // Logical key identifier
    uint64_t birth_epoch;    // First visible epoch
    uint64_t retire_epoch;   // UINT64_MAX if live
    uint8_t  flags;          // bit 0: TOMBSTONE
};

struct LeafNode {
    RecordHeader headers[];  // Sorted by key_id
    LiveBitmap bitmap;       // Fast skip of dead records
    MBR mbrs[];             // Spatial data (SoA layout)
    Payload payloads[];     // Variable-length data
};
```

### Visibility Rules

A record is visible to reader at epoch E if:
- `birth_epoch <= E`
- `retire_epoch > E` (or `UINT64_MAX`)
- `!(flags & TOMBSTONE)`

### Operations

**Update**: Insert new version + tombstone old version in same commit
**Delete**: Insert tombstone record
**Compaction**: Remove tombstones when `retire_epoch < min_reader_epoch`

This extension would complement the existing node-level COW with fine-grained record versioning, reducing write amplification for hot records.