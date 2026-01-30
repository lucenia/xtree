# ABA Protection Design - Single-Bump Invariant

**Status**: Implemented and Tested  
**Last Updated**: 2025-08-17  
**Author**: Nick Knize

## Overview

This document describes the ABA protection mechanism used in the persistence layer's ObjectTable, specifically the single-bump invariant that ensures tag correctness when handles are reused.

## Background: The ABA Problem

The ABA problem occurs when:
1. Thread A reads a value (e.g., NodeID pointing to handle H)
2. Thread B frees handle H and it gets reused for different data
3. Thread A uses the stale NodeID, accessing wrong data

Without protection, this causes data corruption or crashes.

## Solution: Tagged NodeIDs

Our NodeID structure embeds an 8-bit tag:
```
NodeID = [63:8] handle_index | [7:0] tag
```

When a handle is reused, its tag changes, making stale NodeIDs detectable.

## The Single-Bump Invariant

### Core Principle
**A tag is incremented exactly once per handle lifecycle** - specifically when transitioning from free/retired to live state.

### Implementation Details

1. **allocate()** - Returns NodeID with CURRENT tag (no increment)
   ```cpp
   NodeID ObjectTable::allocate(...) {
       // ... find free handle h ...
       uint8_t current_tag = entries_[h].tag.load(std::memory_order_relaxed);
       // DO NOT bump tag here - only mark_live_reserve bumps the tag
       return NodeID(h, current_tag);
   }
   ```

2. **mark_live_reserve()** - Bumps tag ONLY on free→live transition
   ```cpp
   NodeID ObjectTable::mark_live_reserve(NodeID id, uint64_t birth_epoch) {
       // Check if already live
       bool is_live = (current_birth != 0 && current_retire == ~uint64_t{0});
       
       if (!is_live) {
           // Handle is transitioning from free/retired to live
           // Bump the tag for ABA safety (single-bump invariant)
           uint8_t new_tag = current_tag + 1;
           if (new_tag == 0) new_tag = 1;  // Skip 0
           entries_[h].tag.store(new_tag, std::memory_order_relaxed);
           return NodeID(h, new_tag);
       }
       // Already live - return with existing tag
       return id;
   }
   ```

3. **mark_live_commit()** - Never bumps tag
   ```cpp
   void ObjectTable::mark_live_commit(NodeID id, uint64_t birth_epoch) {
       // Just sets epochs, no tag change
       entries_[h].birth_epoch.store(birth_epoch, std::memory_order_release);
   }
   ```

### Tag Wrapping Behavior

- Tags cycle through values [1..255], skipping 0
- Period is 255 (not 256) due to skip-0 logic
- After 255 comes 1 (not 0)

Example sequence:
```
... → 254 → 255 → 1 → 2 → ...
```

### Mathematical Properties

For computing expected tag after k reuses:
```cpp
auto expected_after = [](uint8_t start_tag, int k) -> uint8_t {
    // Normalize to [0..254], add k, mod 255, map back to [1..255]
    const int s = (start_tag == 0 ? 1 : start_tag) - 1;
    const int r = (s + (k % 255) + 255) % 255;
    return static_cast<uint8_t>(r + 1);
};
```

## Bug History: The Double-Bump Problem

### Original Bug
Previously, both `allocate()` and `mark_live_reserve()` were bumping tags:
1. `allocate()` would bump when reusing a handle
2. `mark_live_reserve()` would bump again on the same reuse

This caused tags to increment by 2 per reuse cycle.

### Symptoms
- After 257 reuses, expected tag=2 but got tag=3
- Mathematical proof: 1 + 2×257 = 515 ≡ 3 (mod 256)
- Tests failed with incorrect tag expectations

### The Fix
Removed tag increment from `allocate()`, keeping it only in `mark_live_reserve()`:
- Ensures exactly one bump per reuse
- Maintains ABA protection
- Simplifies reasoning about tag values

## Commit/Recovery Flow

### During Commit
1. `allocate_node()` returns NodeID with pre-reserve tag
2. `set_root()` stores this NodeID temporarily
3. `commit()` calls `mark_live_reserve()` which may bump tag
4. `get_root()` after commit returns NodeID with reserved (possibly bumped) tag

### During Recovery
1. Replay sets handles to retired state
2. Next allocation reuses handle
3. `mark_live_reserve()` bumps tag since handle is retired→live
4. Fresh NodeID has different tag than before crash

## Testing Strategy

### Test Coverage
- **Basic ABA Test**: Verifies tag changes on reuse
- **Multiple Reuses**: Tests tag increments across many cycles
- **Tag Wrap Test**: Verifies skip-0 behavior at 255→1 boundary
- **Multiple Tag Wraps**: Tests 257+ reuses with correct period-255 math
- **Recovery Tests**: Ensures tags increment correctly after crash

### Key Test Patterns
```cpp
// Save initial state
NodeID root1 = store.allocate_node(...);
store.set_root("", root1);
store.commit(epoch1);
NodeID committed1 = store.get_root("");  // May differ from root1!

// Retire and reuse
store.retire_node(committed1, epoch2);
NodeID root2 = store.allocate_node(...);  // Gets same handle
store.set_root("", root2);
store.commit(epoch2);
NodeID committed2 = store.get_root("");  // Tag bumped from committed1

// Verify ABA protection
EXPECT_NE(get_tag(committed1), get_tag(committed2));
```

## Performance Impact

The single-bump invariant has minimal performance impact:
- One less atomic operation per allocation (removed bump from allocate())
- Tag check/bump in mark_live_reserve() was already needed
- No additional memory barriers or locks required

## Summary

The single-bump invariant provides robust ABA protection with:
- **Correctness**: Exactly one tag increment per handle reuse
- **Simplicity**: Tag changes only in mark_live_reserve()
- **Performance**: No overhead vs. double-bump approach
- **Testability**: Predictable tag values enable thorough testing

This design ensures safe handle reuse in the presence of concurrent readers while maintaining high performance and implementation clarity.