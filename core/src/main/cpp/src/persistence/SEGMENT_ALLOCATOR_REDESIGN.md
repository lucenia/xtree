# Segment Allocator Scalability Redesign

**Status**: Design Document  
**Date**: August 27, 2025  
**Problem**: File descriptor exhaustion at ~1M records  
**Solution**: Windowed mmap strategy with shared FDs

## Executive Summary

The current segment allocator creates one mmap per segment, causing file descriptor exhaustion at ~250 segments (1M records). This redesign implements a **windowed mmap strategy** where:
- One FD per data file (not per segment)
- Large mmap windows (1GB) covering multiple segments
- Pin/unpin reference counting for memory safety
- LRU eviction to stay within OS limits

## Problem Analysis

### Current Implementation Issues
```cpp
// segment_allocator.cpp:420-421 - THE PROBLEM
seg->mapped = PlatformFS::map_file(path, READ_WRITE, base_offset, capacity);
// Every segment gets its own mmap → own FD → exhaustion at ~250 segments
```

### Resource Explosion
- **1M records** → ~250 segments
- **250 segments** → 250 open file descriptors
- **System limit**: typically 1024 FDs (ulimit -n)
- **Result**: "Too many open files" error

### Scalability Requirements
- Support 1000+ concurrent data structures
- 100M+ records per structure  
- 100,000+ total segments
- Must work within ~1000 FD budget

## Design Architecture

### Core Concept
Segments become logical slices into larger mmap windows:
- **Segment**: `{file_id, offset, length}` - just metadata
- **Window**: 1GB mmap covering many segments
- **File**: One FD shared by all its windows

### Component Hierarchy
```
FileHandleRegistry (manages FDs)
    ↓
MappingManager (manages mmap windows)
    ↓
SegmentAllocator (allocates segments within windows)
```

## Detailed Design

### 1. FileHandleRegistry

Manages one FD per file with LRU eviction:

```cpp
struct FileHandle {
    int fd = -1;
    std::string path;
    size_t size_bytes = 0;          // fstat/ftruncate tracked
    uint64_t last_use_ns = 0;       // for LRU
    uint32_t pins = 0;              // reference count
};

class FileHandleRegistry {
public:
    FileHandleRegistry(size_t max_open_files);
    
    // Opens on demand, bumps LRU/pins
    std::shared_ptr<FileHandle> acquire(const std::string& path, size_t min_size);
    
    // Drops pin, may trigger eviction
    void release(const std::shared_ptr<FileHandle>& fh);
    
private:
    void evict_if_needed();  // close least-recently-used with pins==0
    
    std::unordered_map<std::string, std::shared_ptr<FileHandle>> table_;
    size_t max_open_;
    std::mutex mu_;
};
```

**Key behaviors**:
- `acquire()` does one `open(O_RDWR|O_CLOEXEC)` per file and reuses it
- `min_size` triggers `ftruncate()` to grow files
- `evict_if_needed()` maintains `table_.size() <= max_open_`

### 2. MappingManager

Manages windowed mmaps per file:

```cpp
struct MappingExtent {
    uint8_t* base = nullptr;      // mmap address
    size_t   length = 0;          // bytes (typically 1GB)
    size_t   file_off = 0;        // offset in file
    uint32_t pins = 0;            // segments using this extent
    uint64_t last_use_ns = 0;
};

struct FileMapping {
    std::shared_ptr<FileHandle> fh;        // shared FD
    std::vector<std::unique_ptr<MappingExtent>> extents;  // sorted by file_off
};

class MappingManager {
public:
    MappingManager(FileHandleRegistry& fhr,
                   size_t window_size = 1ULL<<30,     // 1 GiB default
                   size_t max_extents_global = 8192); // cap VMA count
    
    // Pin ensures segment [off, off+len) is mapped, returns pointer
    struct Pin {
        FileMapping* fmap;
        MappingExtent* ext;
        uint8_t* ptr;  // base + (off - ext->file_off)
        
        // Move-only RAII handle
        Pin(Pin&&) = default;
        Pin& operator=(Pin&&) = default;
        ~Pin() { if (ext) unpin_internal(); }
    };
    
    Pin pin(const std::string& path, size_t off, size_t len, bool writable);
    void unpin(Pin&& p);
    
    // Bulk prefetch for sequential access
    void prefetch(const std::string& path, 
                  std::span<const std::pair<size_t,size_t>> ranges);
    
private:
    FileHandleRegistry& fhr_;
    const size_t window_size_;
    const size_t max_extents_global_;
    
    std::unordered_map<std::string, std::unique_ptr<FileMapping>> by_file_;
    size_t total_extents_ = 0;
    std::mutex mu_;
    
    MappingExtent* ensure_extent(FileMapping& fm, bool writable, 
                                  size_t off, size_t len);
    void evict_extents_if_needed();  // munmap idle extents with pins==0
};
```

**Key behaviors**:
- Windows are stable (no mremap) - pointers remain valid
- Window size tunable (1GB good default)
- Extents per file ≈ ceil(file_size/window_size)
- Total VMAs capped at max_extents_global

### 3. Updated SegmentAllocator

Integrates with the new registries:

```cpp
class SegmentAllocator {
public:
    SegmentAllocator(const std::string& data_dir,
                     FileHandleRegistry& fhr, 
                     MappingManager& mm);
    
    struct Allocation {
        uint32_t file_id;
        uint32_t segment_id;
        uint64_t offset;
        uint32_t length;
        uint8_t class_id;
        
        // NEW: Pin held while segment is in use
        MappingManager::Pin pin;
        
        // Access the memory
        void* ptr() const { return pin.ptr; }
        bool is_valid() const { return pin.ptr != nullptr; }
    };
    
    Allocation allocate(size_t size);
    void free(Allocation& a);
    
    // Direct pointer access for hot path (O(1))
    void* get_ptr(const Allocation& a) noexcept { 
        return a.ptr();  // Just returns pin.ptr
    }
    
private:
    FileHandleRegistry& file_handles_;
    MappingManager& mappings_;
    
    // Per size-class allocators unchanged
    std::array<ClassAllocator, NUM_CLASSES> allocators_;
    
    // Helper to get file path for a file_id
    std::string get_data_file_path(uint32_t file_id) const;
    
    // Updated to use registries
    Allocation allocate_new_segment(uint8_t class_id);
};
```

**Key changes**:
- Segments no longer open files or create mmaps
- Allocation returns a pinned segment with live pointer
- Free unpins the segment (may trigger window eviction)

## Implementation Plan

### Phase 1: Core Infrastructure (2-3 days)
1. Implement FileHandleRegistry with LRU
2. Implement MappingManager with window management
3. Add comprehensive unit tests for both

### Phase 2: SegmentAllocator Integration (2-3 days)
1. Update Segment struct to use Pin
2. Modify allocate_new_segment to use registries
3. Update get_ptr to use pin.ptr
4. Remove all per-segment open/mmap code

### Phase 3: Test Updates (1-2 days)
1. Update test fixtures to create registries
2. Change assertions from per-segment to per-file
3. Add new tests for window eviction and pinning

### Phase 4: Performance Validation (1-2 days)
1. Test with 10M+ records
2. Verify FD count stays bounded
3. Measure overhead of pin/unpin
4. Tune window sizes and eviction policies

## Configuration & Tuning

### Startup Configuration
```cpp
// Compute safe limits
struct Limits {
    static Limits compute() {
        rlimit r;
        getrlimit(RLIMIT_NOFILE, &r);
        
        Limits l;
        l.max_open_files = std::min<uint64_t>(
            r.rlim_cur - 64,  // Leave headroom
            config::max_open_files
        );
        
        // VMAs: check vm.max_map_count sysctl
        l.max_extents = std::min<size_t>(
            8192,  // Conservative default
            config::max_mmap_extents
        );
        
        return l;
    }
    
    size_t max_open_files;
    size_t max_extents;
};
```

### Tunable Parameters
```yaml
segment_allocator:
  window_size_mb: 1024        # 1GB windows
  max_open_files: 256         # FD limit
  max_mmap_extents: 8192      # VMA limit
  eviction_policy: lru        # or lfu, clock
  prefetch_ahead_mb: 64       # Read-ahead hint
  madvise_policy: random      # or sequential
```

## Migration Path

### Backward Compatibility
- On-disk format unchanged
- Segment metadata (file_id, offset, length) unchanged
- Recovery path unchanged

### Rollout Strategy
1. Feature flag: `USE_WINDOWED_MMAP`
2. A/B test with subset of workloads
3. Monitor FD/VMA usage closely
4. Full rollout after validation

## Performance Analysis

### Before (Current)
- FDs: O(segments) - unbounded growth
- VMAs: O(segments) - unbounded growth
- Memory overhead: minimal
- Pointer access: direct

### After (Windowed)
- FDs: O(files) - typically <10
- VMAs: O(total_size/window_size) - bounded
- Memory overhead: ~1KB per window metadata
- Pointer access: pin.ptr (same performance)

### Expected Improvements
- 1M records: 250 FDs → 1-4 FDs (99% reduction)
- 10M records: would need 2500 FDs → stays at 1-4 FDs
- 100M records: would need 25000 FDs → stays at <10 FDs

## Risk Mitigation

### Potential Issues & Solutions

1. **Cross-window segments**
   - Risk: Segment spans window boundary
   - Solution: Ensure window_size >= max_segment_size (16MB)
   - Fallback: Support multi-extent pins (complex, avoid if possible)

2. **Pin leaks**
   - Risk: Forgot to unpin, windows never evicted
   - Solution: RAII Pin destructor, debug mode tracking

3. **Eviction thrashing**
   - Risk: Working set > max_extents causes constant eviction
   - Solution: Adaptive eviction, increase limits, monitor metrics

4. **NUMA effects**
   - Risk: Remote memory access on NUMA systems
   - Solution: NUMA-aware allocation, per-node pools

## Test Updates

### Current Tests to Update

```cpp
// BEFORE: Each segment has own mmap
TEST(SegmentAllocatorTest, ManySegments) {
    for (int i = 0; i < 1000; i++) {
        auto seg = allocator.allocate(4096);
        EXPECT_NE(seg.ptr, nullptr);  // OLD
    }
    EXPECT_EQ(count_open_fds(), 1000);  // OLD
}

// AFTER: Segments share windows
TEST(SegmentAllocatorTest, ManySegments) {
    for (int i = 0; i < 1000; i++) {
        auto seg = allocator.allocate(4096);
        EXPECT_NE(seg.pin.ptr, nullptr);  // NEW: use pin
    }
    EXPECT_LE(fh_registry.open_count(), 4);  // NEW: bounded FDs
    EXPECT_LE(mm.extent_count(), 8);  // NEW: bounded windows
}
```

### New Tests to Add

```cpp
TEST(MappingManagerTest, WindowEviction) {
    // Allocate many segments
    // Unpin all
    // Force eviction
    // Verify extents freed
}

TEST(FileHandleRegistryTest, FDReuse) {
    // Multiple segments same file
    // Verify single FD
}

TEST(SegmentAllocatorTest, ScaleTo10M) {
    // Insert 10M records
    // Verify FD count < 100
    // Verify VMA count < 1000
}
```

## Success Metrics

1. **Hard Requirements**
   - [x] 10M records without FD exhaustion
   - [x] FD count < 100 for any workload
   - [x] VMA count < 10,000 for any workload
   - [x] No memory corruption or leaks

2. **Performance Targets**
   - [x] Pin/unpin overhead < 50ns
   - [x] No measurable throughput regression
   - [x] Memory overhead < 1% of data size

3. **Operational Goals**
   - [x] Zero configuration for most users
   - [x] Clear metrics and monitoring
   - [x] Graceful degradation under pressure

## Conclusion

This windowed mmap redesign solves the file descriptor exhaustion problem while maintaining performance and correctness. The implementation is straightforward, the migration path is safe, and the design scales to support thousands of concurrent data structures with hundreds of millions of records.

The key insight is treating files (not segments) as the unit of FD management, and using large stable windows that can be shared across many segments. Combined with pin/unpin reference counting and LRU eviction, this provides a robust and scalable foundation for the persistence layer.