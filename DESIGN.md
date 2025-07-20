# XTree Memory-Mapped Storage Design

## Overview

This document describes the design for a memory-mapped storage backend for the XTree spatial index, combined with an LRU-based access tracking system for performance optimization. The design preserves the original XTree algorithms while leveraging OS-level memory management and providing sophisticated monitoring and optimization capabilities.

## Architecture

The system consists of four main layers:

```
┌─────────────────────────────────────────────────────────┐
│                 XTree Logic Layer                       │
│  (Business logic, search algorithms, insertions)       │
└─────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────┐
│              Optimization Layer                         │
│  (Hot node detection, thread affinity, pinning)        │
└─────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────┐
│              LRU Tracking Layer                         │
│  (Access patterns, statistics, monitoring)             │
└─────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────┐
│            Memory-Mapped Storage Layer                  │
│  (File mapping, allocation, OS paging)                 │
└─────────────────────────────────────────────────────────┘
```

## Components

### 1. Memory-Mapped Storage Layer (`MMapFile`)

**Purpose**: Provides a virtual address space for XTree buckets backed by disk storage.

**Key Features**:
- Maps entire XTree file into virtual memory
- OS kernel handles paging between memory and disk automatically
- Supports file expansion and reallocation
- Provides `mlock`/`munlock` for memory pinning
- Thread-safe allocation interface

**File Structure**:
```
┌─────────────────┐ Offset 0
│   File Header   │ (64 bytes)
├─────────────────┤
│                 │
│   XTree Nodes   │ (Variable size buckets)
│                 │
├─────────────────┤
│   Free Space    │
└─────────────────┘
```

**File Header**:
```cpp
struct FileHeader {
    uint32_t magic;           // "XTRE" magic number
    uint32_t version;         // File format version
    size_t root_offset;       // Offset of root bucket
    size_t next_free_offset;  // Next allocation offset
    char reserved[48];        // Reserved for future use
};
```

### 2. LRU Tracking Layer (`LRUAccessTracker`)

**Purpose**: Monitors access patterns without owning data, providing statistics for optimization decisions.

**Key Features**:
- Records every node access with timestamps
- Maintains access frequency and recency statistics
- Provides LRU ordering for optimization decisions
- Bounded memory usage (configurable max tracked nodes)
- Does NOT delete tracked objects (uses `LRUDeleteNone`)

**Tracked Statistics**:
```cpp
struct NodeStats {
    size_t access_count;
    std::chrono::steady_clock::time_point last_access;
    std::chrono::steady_clock::time_point first_access;
    bool is_pinned;
    size_t size;
    
    double get_access_frequency() const; // accesses/second
};
```

### 3. Optimization Layer (`HotNodeDetector`)

**Purpose**: Analyzes access patterns to suggest performance optimizations.

**Optimization Types**:
- **Memory Pinning**: Keep hot nodes in memory using `mlock`
- **Thread Affinity**: Assign hot subtrees to specific threads
- **Shard Relocation**: Move hot nodes to faster storage
- **Prefetching**: Load child nodes of frequently accessed parents

**Decision Algorithm**:
```cpp
double hotness_score = (access_frequency * recency_weight) + 
                      (access_count * volume_weight);

if (hotness_score > pin_threshold) {
    suggest PIN_NODE;
} else if (hotness_score > thread_threshold) {
    suggest THREAD_AFFINITY;
}
```

### 4. XTree Logic Layer (`MMapXTree`)

**Purpose**: Integrates memory-mapped storage with existing XTree algorithms.

**Key Changes**:
- Buckets accessed via `MMapPtr<XTreeBucket>` smart pointers
- Automatic access tracking on every pointer dereference
- Tree algorithms remain unchanged
- Backward compatible with existing XTree interface

## Smart Pointer Design

The `MMapPtr<T>` wrapper automatically tracks access:

```cpp
template<typename T>
class MMapPtr {
    T* operator->() const { 
        if (tracker_) tracker_->record_access(offset_);
        return ptr_; 
    }
    // ... other operators
};
```

Every tree traversal, search, or modification automatically updates access statistics.

## Memory Management Strategy

### Traditional Approach (Current)
```
Application manages memory → LRU cache owns buckets → Manual eviction
```

### New Approach (Proposed)
```
OS manages memory → LRU tracks access patterns → Intelligent optimization
```

**Benefits**:
1. **Better paging**: OS kernel uses hardware MMU and sophisticated algorithms
2. **Reduced complexity**: No manual memory management in application
3. **Enhanced monitoring**: Rich statistics for optimization
4. **Flexible pinning**: Keep hot data in memory, let cold data page out

## Usage Examples

### Creating a New Tree
```cpp
auto tree = MMapXTreeFactory::create_new<DataRecord>(
    "spatial_index.xtree",  // filename
    2,                      // dimensions  
    32,                     // precision
    dimLabels,              // dimension labels
    100                     // initial size (MB)
);
```

### Inserting Data
```cpp
// Insertion automatically tracked
tree->insert(dataRecord);

// Access patterns recorded for optimization
auto root = tree->getRoot();
root->someMethod();  // Access recorded
```

### Performance Optimization
```cpp
// Get hot node suggestions
auto suggestions = tree->getHotNodeDetector()->analyze();

for (const auto& suggestion : suggestions) {
    switch (suggestion.type) {
        case PIN_NODE:
            // Pin frequently accessed node in memory
            tree->getAccessTracker()->pin_node(suggestion.offset, node_size);
            break;
            
        case THREAD_AFFINITY:
            // Assign this subtree to a specific thread
            assign_subtree_to_thread(suggestion.offset, thread_id);
            break;
            
        case SHARD_RELOCATION:
            // Move to faster storage (SSD, different NUMA node)
            relocate_to_fast_storage(suggestion.offset);
            break;
    }
}

// Automatic optimization: pin up to 64MB of hottest nodes
tree->optimize_memory_pinning(64);
```

### Monitoring and Statistics
```cpp
// Get storage statistics
auto stats = tree->get_storage_stats();
cout << "File size: " << stats.file_size << " bytes" << endl;
cout << "Pinned nodes: " << stats.pinned_nodes << endl;
cout << "Pinned memory: " << stats.pinned_memory_mb << " MB" << endl;

// Get hot nodes for analysis
auto hot_nodes = tree->getAccessTracker()->get_hot_nodes(10);
for (const auto& [offset, node_stats] : hot_nodes) {
    cout << "Node at " << offset 
         << ": " << node_stats.access_count << " accesses, "
         << node_stats.get_access_frequency() << " freq" << endl;
}
```

## Threading Considerations

### Thread Safety
- **MMapFile**: Thread-safe allocation using atomic operations
- **LRUAccessTracker**: Read-heavy workload, uses RW locks
- **Tree Operations**: Same thread safety as current XTree

### Thread Optimization
- **Hot Subtree Assignment**: Assign frequently accessed subtrees to specific threads
- **NUMA Awareness**: Pin hot nodes to memory close to processing threads
- **Load Balancing**: Use access statistics to distribute work evenly

## Migration Strategy

### Phase 1: Dual Mode Support
- Support both memory-mapped and traditional modes
- Existing code unchanged
- New installations can opt into mmap mode

### Phase 2: Gradual Migration
- Migrate existing indexes to mmap format
- Provide conversion utilities
- Maintain backward compatibility

### Phase 3: Full Adoption
- Default to mmap mode for new indexes
- Deprecate traditional mode
- Focus optimization on mmap path

## Performance Considerations

### Memory Usage
- **File mapping**: Virtual memory only, physical memory used on demand
- **LRU tracking**: Bounded memory (configurable, default 10K nodes)
- **Statistics overhead**: ~100 bytes per tracked node

### Access Overhead
- **Pointer dereference**: ~1-2 CPU cycles per access for tracking
- **Statistics update**: Amortized O(1) with batching
- **OS paging**: Handled by kernel, faster than user-space LRU

### Optimization Benefits
- **Hot data pinning**: Eliminates page faults for critical nodes
- **Thread affinity**: Reduces cache misses and NUMA penalties
- **Prefetching**: Reduces latency for predictable access patterns

## Configuration

### Environment Variables
```bash
XTREE_MMAP_INITIAL_SIZE=100MB    # Initial file size
XTREE_MAX_TRACKED_NODES=10000    # LRU tracker memory limit
XTREE_PIN_MEMORY_LIMIT=64MB      # Maximum memory to pin
XTREE_HOT_NODE_THRESHOLD=1.0     # Hotness threshold for pinning
```

### Runtime Configuration
```cpp
MMapXTree tree(...);
tree.getAccessTracker()->set_max_tracked_nodes(20000);
tree.getHotNodeDetector()->set_pin_threshold(2.0);
```

## Future Enhancements

### Advanced Optimizations
- **Predictive prefetching** based on access patterns
- **Dynamic rebalancing** of hot subtrees
- **Compression** of cold data regions
- **Tiered storage** (SSD + HDD) based on access frequency

### Monitoring Integration
- **Metrics export** to Prometheus/Grafana
- **Real-time dashboards** for index performance
- **Alerting** on performance degradation
- **Capacity planning** based on growth trends

### Machine Learning
- **Access pattern prediction** using time series analysis
- **Automated optimization** parameter tuning
- **Anomaly detection** for unusual access patterns
- **Query workload classification** for specialized optimizations

## Benefits Summary

1. **Simplified Memory Management**: OS handles paging, no manual eviction logic
2. **Better Performance**: Kernel paging algorithms, hardware MMU support
3. **Rich Monitoring**: Detailed access statistics for optimization
4. **Flexible Optimization**: Memory pinning, thread affinity, prefetching
5. **Backward Compatibility**: Existing tree algorithms unchanged
6. **Scalability**: Handles datasets larger than available memory
7. **Reliability**: OS-level crash recovery and data integrity
8. **Development Velocity**: Focus on tree logic, not memory management

This design preserves the original LRU concept while leveraging modern OS capabilities for superior performance and maintainability.