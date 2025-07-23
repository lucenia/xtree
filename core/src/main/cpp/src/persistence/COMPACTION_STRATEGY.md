# Compaction Strategy for XTree Persistence

**Status**: Design Document  
**Date**: 2024-12-22  
**Goal**: Define efficient compaction strategies for managing fragmentation

## Overview

Based on analysis of high-churn workloads, we need a compaction strategy that:
1. Minimizes write amplification
2. Maintains consistent read performance
3. Operates efficiently in the background
4. Adapts to workload patterns

## Fragmentation Patterns

### Common Scenarios (Benchmarked Results)

1. **Uniform Random Deletes**: **40.31%** fragmentation (10K ops, 40% delete ratio)
2. **Temporal Locality**: **49.10%** fragmentation (higher than random - needs optimization)
3. **Bulk Deletes**: **90.15%** fragmentation (10 batches with 50% bulk deletes)
4. **Growing Dataset**: More inserts than deletes → gradual fragmentation
5. **Long-Running High Churn**: **44-45%** fragmentation (stabilizes after 50K ops)

## Compaction Strategy

### Three-Tier Approach

#### Tier 1: Lazy Compaction (Default)
- **Trigger**: Segment >50% dead space
- **When**: During low activity periods
- **Target**: Segments with best benefit/cost ratio
- **Frequency**: Every 100K operations or 1 hour

```cpp
struct CompactionPolicy {
    double lazy_threshold = 0.5;      // 50% dead space
    size_t lazy_interval_ops = 100000;
    duration lazy_interval_time = 1h;
};
```

#### Tier 2: Selective Compaction
- **Trigger**: Overall fragmentation >25%
- **When**: Background thread, lower priority
- **Target**: Top 3 segments by benefit/cost
- **Frequency**: Every 50K operations or 30 minutes

```cpp
bool should_compact_selective() {
    auto stats = allocator->get_total_stats();
    return stats.fragmentation() > 0.25;
}
```

#### Tier 3: Aggressive Compaction
- **Trigger**: Overall fragmentation >40% OR >50% segments fragmented
- **When**: Dedicated thread, higher priority
- **Target**: All segments >30% fragmented
- **Frequency**: Continuous until threshold met

```cpp
bool should_compact_aggressive() {
    auto stats = allocator->get_total_stats();
    size_t fragmented = count_fragmented_segments(0.3);
    return stats.fragmentation() > 0.4 || 
           fragmented > segment_count() * 0.5;
}
```

### Benefit/Cost Analysis

For each segment, calculate:
```cpp
double benefit_cost_ratio(const Segment& seg) {
    size_t dead_bytes = seg.capacity - seg.live_bytes;
    size_t live_bytes = seg.live_bytes;
    
    // Benefit: space reclaimed
    // Cost: bytes to copy + I/O overhead
    double benefit = dead_bytes;
    double cost = live_bytes + (live_bytes * 0.1); // 10% I/O overhead
    
    return benefit / (cost + 1);  // +1 to avoid divide by zero
}
```

### Compaction Algorithm

```cpp
class Compactor {
    void compact_segment(SegmentID victim) {
        // 1. Mark segment as compacting (prevents new allocations)
        mark_compacting(victim);
        
        // 2. Find all live objects via ObjectTable
        auto live_objects = find_live_in_segment(victim);
        
        // 3. Allocate new segment
        auto target = allocator->new_segment(victim.size_class);
        
        // 4. Copy live objects (batch for efficiency)
        for (const auto& batch : make_batches(live_objects, 1MB)) {
            // Copy batch to target
            copy_batch(batch, victim, target);
            
            // Update ObjectTable addresses (NodeIDs unchanged!)
            for (const auto& obj : batch) {
                object_table->update_address(obj.node_id, 
                                           target.offset + obj.new_offset);
            }
        }
        
        // 5. Atomic switch
        atomic_replace_segment(victim, target);
        
        // 6. Schedule victim for deletion after grace period
        schedule_deletion(victim, 2 * max_reader_delay);
    }
};
```

## Benchmark-Based Recommendations

Based on fragmentation analysis benchmarks (bench_fragmentation_simple.cpp):

### Key Findings
1. **Sharded Object Table**: Progressive activation works well (5-26 shards for 50K ops)
2. **Fragmentation Plateau**: Long-running workloads stabilize at ~45%
3. **Temporal Locality Issue**: Causes higher fragmentation (49%) than random (40%)

### Recommended Thresholds (Adjusted from Benchmarks)
```cpp
struct CompactionThresholds {
    // Tier 1: Normal operation
    static constexpr double LAZY_THRESHOLD = 0.50;      // >50% dead
    static constexpr size_t LAZY_INTERVAL = 25000;      // Every 25K ops
    
    // Tier 2: Moderate fragmentation  
    static constexpr double SELECTIVE_THRESHOLD = 0.45; // >45% overall
    static constexpr size_t SELECTIVE_COUNT = 3;        // Top 3 segments
    
    // Tier 3: High fragmentation
    static constexpr double AGGRESSIVE_THRESHOLD = 0.60; // >60% overall
    static constexpr double AGGRESSIVE_SEGMENT = 0.40;   // Segments >40% dead
};
```

### Workload-Specific Strategies
- **Random Workloads**: Standard thresholds work well
- **Temporal Workloads**: Reduce LAZY_THRESHOLD to 0.40
- **Bulk Delete Workloads**: Trigger immediate compaction after bulk ops
- **Long-Running**: Schedule compaction every 25K operations

## Workload Adaptation

### Metrics Collection
```cpp
struct WorkloadMetrics {
    double delete_rate;      // Deletes per second
    double insert_rate;      // Inserts per second  
    double churn_rate;       // (deletes + inserts) / total
    double temporal_locality; // Recent vs old deletes
    size_t working_set_size;
};
```

### Adaptive Thresholds
```cpp
CompactionPolicy adapt_policy(const WorkloadMetrics& m) {
    CompactionPolicy policy;
    
    if (m.churn_rate > 0.5) {
        // High churn: more aggressive
        policy.lazy_threshold = 0.4;
        policy.lazy_interval_ops = 50000;
    } else if (m.delete_rate > m.insert_rate * 2) {
        // Delete heavy: frequent compaction
        policy.lazy_threshold = 0.35;
        policy.lazy_interval_ops = 25000;
    } else if (m.temporal_locality > 0.7) {
        // Good locality: can be lazier
        policy.lazy_threshold = 0.6;
        policy.lazy_interval_ops = 200000;
    }
    
    return policy;
}
```

## Implementation Priorities

### Phase 1: Basic Compaction (Week 1)
- [ ] Implement segment fragmentation tracking
- [ ] Add benefit/cost calculation
- [ ] Create basic single-segment compactor
- [ ] Add ObjectTable address updates

### Phase 2: Policy Engine (Week 2)
- [ ] Implement three-tier policies
- [ ] Add workload metrics collection
- [ ] Create adaptive thresholds
- [ ] Build compaction scheduler

### Phase 3: Optimization (Week 3)
- [ ] Batch copying for efficiency
- [ ] Parallel compaction support
- [ ] Read-during-compaction handling
- [ ] Grace period management

### Phase 4: Testing (Week 4)
- [ ] Unit tests for compaction logic
- [ ] Stress tests under high churn
- [ ] Performance impact measurement
- [ ] Crash during compaction tests

## Expected Outcomes

### Performance Targets
- **Fragmentation**: Maintain <45% average (based on benchmarks)
- **Write Amplification**: <2x for typical workloads
- **Compaction Overhead**: <10% CPU steady-state
- **Space Efficiency**: >55% utilization (allows for 45% fragmentation)

### Monitoring
```cpp
struct CompactionStats {
    size_t segments_compacted;
    size_t bytes_copied;
    size_t bytes_reclaimed;
    duration total_time;
    double write_amplification;
    
    void report() {
        LOG(INFO) << "Compaction: " << segments_compacted << " segments, "
                  << "reclaimed " << bytes_reclaimed << " bytes, "
                  << "amplification " << write_amplification << "x";
    }
};
```

## Future Improvements

1. **Incremental Compaction**: Copy in smaller chunks
2. **Hot/Cold Separation**: Compact cold data less frequently  
3. **Compression**: Compress cold segments
4. **Tiered Storage**: Move cold data to slower storage
5. **Machine Learning**: Predict optimal compaction timing

## Conclusion

This compaction strategy balances:
- **Simplicity**: Three clear tiers with simple triggers
- **Efficiency**: Benefit/cost analysis prevents wasteful compaction
- **Adaptability**: Adjusts to workload patterns
- **Performance**: Minimal impact on foreground operations

The key insight is that NodeID indirection makes compaction transparent to the XTree - we only update ObjectTable entries, not parent pointers.