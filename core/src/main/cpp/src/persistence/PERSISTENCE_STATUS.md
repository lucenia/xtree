# XTree Persistence Layer Status Report

**Date**: August 27, 2025  
**Status**: XTree Integration Complete / Segment Allocator Scalability Issue  
**Test Status**: Passes with 500K records, fails at 1M records

## Executive Summary

The XTree persistence layer is **functionally complete and integrated**. The system successfully implements NodeID-based COW MVCC persistence with crash recovery, checkpointing, and sharded object tables. However, there is a **critical scalability issue** in the segment allocator that causes file descriptor exhaustion at ~1M records.

## Current Architecture

```
XTree → IndexDetails → StoreInterface → DurableStore → DurableRuntime
                                                            ├── ObjectTableSharded (64 shards)
                                                            ├── CheckpointCoordinator
                                                            ├── SegmentAllocator ⚠️ (FD issue)
                                                            ├── Superblock
                                                            └── WAL/DeltaLog
```

## What's Working ✅

### 1. Complete XTree Integration
- XTree fully migrated from raw pointers to NodeID-based addressing
- All tree operations (insert, split, search) work with persistence layer
- XTreeBucket properly allocates nodes via StoreInterface
- IndexDetails supports both IN_MEMORY and DURABLE modes

### 2. Core Persistence Features
- **MVCC**: Birth/retire epochs on all nodes for snapshot isolation
- **COW**: All mutations create new nodes, old ones retired
- **Crash Recovery**: WAL replay restores to consistent state
- **Checkpointing**: Adaptive policies, background thread, atomic rotation
- **Object Table Sharding**: 64 shards with <1% overhead, progressive activation

### 3. Test Validation
```bash
# This works perfectly:
./build/native/bin/xtree_tests --gtest_filter="XTreeDurabilityStressTest.HeavyLoadDurableMode"
# With NUM_RECORDS = 500000 → PASSES
# Inserts, queries, closes, recovers, queries again successfully
```

### 4. Performance Achievements
- O(1) bitmap segment allocation
- O(1) NodeID to pointer resolution via lock-free lookup
- Hardware-accelerated CRC32C checksums
- Group commit support for batching
- Near-linear scaling with thread count (sharded OT)

## Critical Issue 🚨

### File Descriptor Exhaustion
```
Failed to open file /tmp/xtree_durable_stress_xxx/xtree_c0_3.xd: Too many open files
```

**Root Cause**: 
- Current design creates **one mmap per segment**
- Each mmap holds a file descriptor open
- 1M records → ~250+ segments → exceeds system FD limit (typically 1024)

**Code Location**: `segment_allocator.cpp:420-421`
```cpp
// Problem: Every segment gets its own mmap
seg->mapped = PlatformFS::map_file(path, READ_WRITE, base_offset, capacity);
```

## Scalability Requirements

To support thousands of concurrent data structures (XTree, BTree, InvertedIndex, etc.), we need:

1. **Window-based mmap strategy**:
   - One FD per data file (not per segment)
   - Large mmap windows (1GB) covering multiple segments
   - Window manager with pin/unpin reference counting

2. **Expected Scale**:
   - 1000+ concurrent data structures
   - 100M+ records per structure
   - Total segments: potentially 100,000+
   - Must work within ~1000 FD budget

## Test Commands

```bash
# Build everything
./gradlew build

# Run the stress test (currently fails at 1M records)
./build/native/bin/xtree_tests --gtest_filter="XTreeDurabilityStressTest.HeavyLoadDurableMode"

# Run all persistence tests (217 tests, all pass)
./build/native/bin/xtree_tests --gtest_filter="*Persist*:*Durable*:*Object*:*Segment*"
```

## Files of Interest

- **Test**: `core/src/main/cpp/test/test_xtree_durability_stress.cpp`
- **Problem Code**: `core/src/main/cpp/src/persistence/segment_allocator.cpp:420-421`
- **Integration Point**: `core/src/main/cpp/src/indexdetails.hpp` (DURABLE mode)
- **XTree Allocator**: `core/src/main/cpp/src/xtree_allocator_traits.hpp`

## Next Steps

1. **Immediate**: Design and implement windowed mmap strategy for segment allocator
2. **Short-term**: Test with 10M+ records to validate scalability
3. **Medium-term**: Production hardening (metrics, monitoring, fsck tool)
4. **Long-term**: Leaf MVCC, compaction, performance optimization

## Summary

The persistence layer is a **technical success** - all the complex pieces (MVCC, COW, sharding, recovery) work correctly. The XTree integration is complete and functional. The only remaining issue is a straightforward engineering problem: managing file descriptors more efficiently in the segment allocator.

Once the FD issue is resolved, the system should scale to support thousands of concurrent data structures with millions of records each, as designed.