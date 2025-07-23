# Missing Snapshot Reload Functionality

## The Problem

The current XTree implementation:
1. Saves snapshots to disk (via DirectMemoryCOWManager)
2. **Never loads them back on restart**
3. Always starts with an empty tree

This makes persistence pointless!

## What Should Happen

When creating an IndexDetails with persistence enabled:

```cpp
IndexDetails(..., PersistenceMode::MMAP, "xtree.snapshot") {
    // Current: Always creates new COW manager
    cow_manager_ = new DirectMemoryCOWManager<Record>(...);
    
    // Missing: Should check if snapshot exists and load it!
    if (snapshot_exists("xtree.snapshot")) {
        // Load the snapshot
        // Restore the tree structure
        // Continue from where we left off
    }
}
```

## Why This Wasn't Noticed

1. The save functionality works (creates snapshot files)
2. But without reload, every restart loses all data
3. Tests probably only tested within a single session

## Current State of Components

- **COWMemoryMappedFile**: Used for file I/O operations (works)
- **COWMMapManager**: Created but never properly used
- **DirectMemoryCOWManager**: Saves snapshots but can't reload them
- **load_snapshot()**: Method exists but is never called

## Solutions

1. **Quick Fix**: Add snapshot loading to IndexDetails constructor
2. **Better Fix**: Use CompactAllocator approach for instant reload
3. **Best Fix**: Redesign the whole persistence layer

The compact allocator approach already solves this - it automatically loads snapshots on startup!