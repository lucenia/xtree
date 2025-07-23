# XTree Integration with Durability Substrate

**Status**: Design Document  
**Date**: 2024-12-22  
**Goal**: Wire XTree data structure into the persistence layer with minimal changes

## Overview

This document describes how to integrate XTree with the new durability substrate, supporting both:
- **IN_MEMORY mode**: Direct memory pointers for maximum performance (transient tables)
- **DURABLE mode**: NodeID-based indirection for persistence with minimal overhead

The goal is to maintain zero overhead for IN_MEMORY mode while adding durability as an option.

## Current XTree Architecture

### Key Components
1. **XTreeBucket**: Base node structure (internal/leaf nodes)
2. **CompactXTreeAllocator**: Current memory allocator
3. **IndexDetails**: Index metadata and configuration
4. **Direct Pointers**: Parent-child relationships use raw memory addresses

### Current Allocation Flow
```cpp
// Current (direct memory)
XTreeBucket* bucket = allocator->allocate(size);
bucket->parent = parent_ptr;
bucket->children[i] = child_ptr;
```

## Proposed Integration Architecture

### 1. NodeID-Based References
Replace all direct pointers with NodeIDs:

```cpp
// New (NodeID-based)
class XTreeBucket {
    NodeID self_id;           // This node's ID
    NodeID parent_id;         // Parent's NodeID (instead of XTreeBucket*)
    NodeID children_ids[...]; // Children NodeIDs (instead of XTreeBucket*[])
    
    // Cached pointers for hot path (invalidated on snapshot)
    mutable XTreeBucket* parent_cache = nullptr;
    mutable XTreeBucket* children_cache[...] = {nullptr};
};
```

### 2. XTreeBucketAdapter
Bridge between XTree and StoreInterface:

```cpp
class XTreeBucketAdapter {
private:
    persist::StoreInterface* store_;
    persist::ObjectTableSharded* object_table_;
    
public:
    // Allocation with NodeID return
    template<typename BucketType>
    NodeID allocate_bucket(size_t size) {
        // Allocate through store
        NodeID id = store_->publish_node(
            BucketType::NODE_KIND,
            sizeof(BucketType),
            [](void* ptr) {
                new (ptr) BucketType();
            }
        );
        return id;
    }
    
    // Resolution with caching
    XTreeBucket* resolve(NodeID id) {
        if (!id.is_valid()) return nullptr;
        
        // Try object table cache first
        const OTEntry* entry = object_table_->try_get(id);
        if (!entry) return nullptr;
        
        // Get pointer from store
        return static_cast<XTreeBucket*>(
            store_->get_ptr(entry->addr)
        );
    }
    
    // Batch resolution for traversal
    void resolve_children(XTreeBucket* parent) {
        for (int i = 0; i < parent->num_children; ++i) {
            if (parent->children_cache[i] == nullptr) {
                parent->children_cache[i] = resolve(parent->children_ids[i]);
            }
        }
    }
};
```

### 3. Dual-Mode Architecture

#### Permanent Dual-Mode Support
Both modes will be maintained permanently for different use cases:

```cpp
class XTreeBucket {
    // Mode determined at index creation time
    PersistenceMode mode_;
    
    union {
        XTreeBucket* parent_ptr;    // IN_MEMORY mode: direct pointers
        NodeID parent_id;            // DURABLE mode: NodeID indirection
    };
    
    union {
        XTreeBucket** children_ptr;  // IN_MEMORY mode: direct child array
        NodeID* children_ids;        // DURABLE mode: NodeID array
    };
    
    XTreeBucket* get_parent() {
        if (mode_ == PersistenceMode::DURABLE) {
            return adapter->resolve(parent_id);
        }
        return parent_ptr;  // Zero overhead for IN_MEMORY
    }
    
    XTreeBucket* get_child(int idx) {
        if (mode_ == PersistenceMode::DURABLE) {
            return adapter->resolve(children_ids[idx]);
        }
        return children_ptr[idx];  // Zero overhead for IN_MEMORY
    }
};
```

#### Mode Selection at Creation
```cpp
class IndexDetails {
    PersistenceMode mode_;
    
    IndexDetails(..., PersistenceMode mode) : mode_(mode) {
        if (mode == PersistenceMode::IN_MEMORY) {
            // Use MemoryStore - zero persistence overhead
            store_ = std::make_unique<MemoryStore>();
        } else {
            // Use DurableStore with full persistence
            store_ = std::make_unique<DurableStore>(data_dir);
        }
    }
};
```

#### Phase 2: Gradual Conversion
1. Update allocation sites to use NodeIDs
2. Convert traversal code to use resolution
3. Add caching for hot paths
4. Remove legacy pointer code

### 4. Critical Path Optimizations

#### Hot Path Caching
```cpp
class XTreeBucket {
    // Version number for cache invalidation
    uint64_t cache_version = 0;
    static thread_local uint64_t global_version = 0;
    
    XTreeBucket* get_child_fast(int idx) {
        // Check cache validity
        if (cache_version != global_version) {
            invalidate_cache();
            cache_version = global_version;
        }
        
        // Fast path: cached
        if (children_cache[idx]) {
            return children_cache[idx];
        }
        
        // Slow path: resolve
        children_cache[idx] = adapter->resolve(children_ids[idx]);
        return children_cache[idx];
    }
};
```

#### Prefetching for Search
```cpp
void search_internal(NodeID node_id, const Key& key) {
    // Prefetch node
    XTreeBucket* node = resolve(node_id);
    
    // Prefetch likely children while processing current node
    int likely_child = node->find_child_hint(key);
    if (likely_child >= 0) {
        __builtin_prefetch(resolve(node->children_ids[likely_child]), 0, 3);
    }
    
    // Continue search...
}
```

### 5. Compaction Integration

During compaction, nodes move but NodeIDs remain stable:

```cpp
class Compactor {
    void compact_segment(SegmentID seg) {
        // 1. Find all live nodes in segment
        vector<NodeID> live_nodes = find_live_nodes(seg);
        
        // 2. Allocate in new segment
        for (NodeID old_id : live_nodes) {
            OTAddr new_addr = allocator->allocate(size);
            memcpy(new_addr, old_addr, size);
            
            // 3. Update ObjectTable (NodeID unchanged!)
            object_table->update_address(old_id, new_addr);
        }
        
        // 4. Parent pointers remain valid (they use NodeIDs)
    }
};
```

## Integration Checklist

### Required Changes

1. **XTreeBucket Structure**
   - [ ] Add NodeID fields
   - [ ] Add cache fields
   - [ ] Add resolution methods
   - [ ] Update constructors

2. **Allocation Sites**
   - [ ] XTree::insert() - allocate new nodes
   - [ ] XTree::split() - allocate split nodes  
   - [ ] XTree::merge() - handle node merging
   - [ ] Bulk loading - batch allocations

3. **Traversal Code**
   - [ ] Search operations - resolve NodeIDs
   - [ ] Range queries - batch resolution
   - [ ] Updates - track dirty nodes
   - [ ] Deletes - mark tombstones

4. **IndexDetails Integration**
   - [ ] Wire DurableStore
   - [ ] Track root NodeID
   - [ ] Manage transactions
   - [ ] Handle recovery

5. **Memory Management**
   - [ ] Remove CompactXTreeAllocator
   - [ ] Use StoreInterface exclusively
   - [ ] Handle cache invalidation
   - [ ] Implement prefetching

## Performance Considerations

### IN_MEMORY Mode (Transient Tables)
- **Zero Overhead**: Direct pointer access, no indirection
- **Same as Current**: Identical performance to existing implementation
- **Use Cases**: Temporary tables, caches, session state

### DURABLE Mode (Persistent Tables)
- **NodeID Resolution**: ~10ns (OT in L3 cache)
- **Cache Miss**: ~100ns (memory access)
- **Prefetch Benefit**: Hide ~50% of miss latency
- **Net Impact**: <5% for point queries, <10% for range queries
- **Use Cases**: Persistent indexes, durable state, crash recovery needed

### Optimization Opportunities
1. **Batch Operations**: Resolve multiple NodeIDs together
2. **Locality Hints**: Allocate related nodes near each other
3. **Hot Set Caching**: Keep frequently accessed nodes cached
4. **Prefetch Patterns**: Predict and prefetch next nodes

## Future: In-Memory to Durable Conversion

### Design Approach
Allow users to convert a transient index to durable storage:

```cpp
class IndexDetails {
    void convert_to_durable(const std::string& data_dir) {
        if (mode_ == PersistenceMode::DURABLE) {
            throw std::runtime_error("Already durable");
        }
        
        // 1. Create new DurableStore
        auto durable_store = std::make_unique<DurableStore>(data_dir);
        
        // 2. Walk the tree and allocate NodeIDs
        std::unordered_map<void*, NodeID> ptr_to_id;
        convert_subtree(root_ptr, durable_store.get(), ptr_to_id);
        
        // 3. Update all references to use NodeIDs
        update_references(ptr_to_id);
        
        // 4. Switch mode and store
        mode_ = PersistenceMode::DURABLE;
        store_ = std::move(durable_store);
        
        // 5. Commit to make durable
        store_->commit();
    }
    
private:
    NodeID convert_subtree(XTreeBucket* node, 
                           DurableStore* store,
                           std::unordered_map<void*, NodeID>& ptr_to_id) {
        if (!node) return NodeID::invalid();
        
        // Check if already converted
        auto it = ptr_to_id.find(node);
        if (it != ptr_to_id.end()) {
            return it->second;
        }
        
        // Allocate NodeID for this node
        NodeID id = store->publish_node(
            node->is_leaf() ? NodeKind::Leaf : NodeKind::Internal,
            node->size(),
            [node](void* dst) { memcpy(dst, node, node->size()); }
        );
        
        ptr_to_id[node] = id;
        
        // Recursively convert children
        if (!node->is_leaf()) {
            for (int i = 0; i < node->num_children; ++i) {
                NodeID child_id = convert_subtree(
                    node->children_ptr[i], store, ptr_to_id
                );
                // Will update the child reference in update_references()
            }
        }
        
        return id;
    }
};
```

### Challenges
1. **Live Conversion**: Handle concurrent operations during conversion
2. **Memory Overhead**: Temporary dual representation during conversion
3. **Atomicity**: Ensure all-or-nothing conversion
4. **Performance**: Minimize downtime during conversion

## Testing Strategy

### Unit Tests
1. Basic CRUD with NodeIDs
2. Cache invalidation correctness
3. Concurrent access safety
4. Recovery and replay

### Integration Tests
1. Full XTree operations with persistence
2. Crash recovery scenarios
3. Compaction during operations
4. Multi-field indexes

### Performance Tests
1. Compare pointer vs NodeID performance
2. Measure cache hit rates
3. Benchmark under high concurrency
4. Test fragmentation over time

## Migration Timeline

### Week 1: Foundation
- Implement XTreeBucketAdapter
- Add NodeID fields to XTreeBucket
- Create resolution infrastructure

### Week 2: Core Operations
- Convert allocation sites
- Update traversal code
- Add caching layer

### Week 3: Integration
- Wire IndexDetails to DurableStore
- Implement transaction support
- Add recovery logic

### Week 4: Optimization
- Profile and optimize hot paths
- Implement prefetching
- Tune cache sizes

### Week 5: Testing
- Comprehensive testing
- Performance validation
- Bug fixes

## Risks and Mitigations

### Risk 1: Performance Regression
**Mitigation**: Extensive caching, prefetching, and hot path optimization

### Risk 2: Memory Overhead
**Mitigation**: Compact NodeID representation, selective caching

### Risk 3: Complex Migration
**Mitigation**: Dual-mode support, gradual conversion

### Risk 4: Cache Coherency
**Mitigation**: Version-based invalidation, clear ownership model

## Success Criteria

1. **Functional**: All XTree operations work with persistence
2. **Performance**: <10% overhead vs in-memory version
3. **Reliable**: Survives crashes, correct recovery
4. **Scalable**: Handles millions of nodes efficiently
5. **Maintainable**: Clean abstraction, minimal code changes

## Next Steps

1. Review and approve design
2. Create XTreeBucketAdapter prototype
3. Test NodeID resolution performance
4. Begin phased implementation