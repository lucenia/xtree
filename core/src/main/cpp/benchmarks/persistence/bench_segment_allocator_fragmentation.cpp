/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Segment Allocator Fragmentation Benchmark
 * 
 * Simulates high-churn workloads with many deletes/tombstones and new writes
 * to analyze SegmentAllocator fragmentation patterns and determine optimal compaction strategies.
 */

#include <gtest/gtest.h>
#include <random>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <numeric>
#include <filesystem>
#include "persistence/segment_allocator.h"
#include "persistence/segment_classes.hpp"
#include "persistence/object_table.hpp"
#include "persistence/durable_runtime.h"
#include "persistence/durable_store.h"
#include "persistence/mvcc_context.h"
#include "persistence/node_id.hpp"

namespace xtree {
namespace benchmarks {

using namespace persist;

// Workload patterns to simulate
enum class WorkloadPattern {
    UNIFORM_RANDOM,     // Random deletes and inserts
    TEMPORAL_LOCALITY,  // Recent items more likely to be deleted
    BULK_DELETE,        // Periodic bulk deletions
    GROWING_DATASET,    // More inserts than deletes
    STEADY_STATE        // Equal inserts and deletes
};

// Size distribution for allocations
enum class SizeDistribution {
    UNIFORM,            // All sizes equally likely
    SMALL_HEAVY,        // 80% small, 20% large
    LARGE_HEAVY,        // 20% small, 80% large
    BIMODAL,           // Peaks at small and large
    REALISTIC          // Based on typical tree node sizes
};

struct FragmentationStats {
    size_t total_allocated_bytes = 0;
    size_t total_live_bytes = 0;
    size_t total_dead_bytes = 0;
    size_t total_segments = 0;
    size_t fragmented_segments = 0;  // Segments with >20% dead space
    double fragmentation_ratio = 0.0;
    double average_segment_utilization = 0.0;
    std::vector<double> segment_utilizations;
    std::map<size_t, size_t> dead_bytes_by_class;  // size_class -> dead bytes
    std::map<size_t, double> fragmentation_by_class;
    
    void calculate_derived_stats() {
        if (total_allocated_bytes > 0) {
            fragmentation_ratio = static_cast<double>(total_dead_bytes) / total_allocated_bytes;
        }
        
        if (!segment_utilizations.empty()) {
            average_segment_utilization = std::accumulate(
                segment_utilizations.begin(), segment_utilizations.end(), 0.0
            ) / segment_utilizations.size();
        }
        
        fragmented_segments = std::count_if(
            segment_utilizations.begin(), segment_utilizations.end(),
            [](double util) { return util < 0.8; }  // <80% utilization = fragmented
        );
    }
    
    void print() const {
        std::cout << "\n=== Fragmentation Statistics ===" << std::endl;
        std::cout << "Total Allocated: " << total_allocated_bytes << " bytes" << std::endl;
        std::cout << "Total Live: " << total_live_bytes << " bytes" << std::endl;
        std::cout << "Total Dead: " << total_dead_bytes << " bytes" << std::endl;
        std::cout << "Fragmentation Ratio: " << std::fixed << std::setprecision(2) 
                  << (fragmentation_ratio * 100) << "%" << std::endl;
        std::cout << "Total Segments: " << total_segments << std::endl;
        std::cout << "Fragmented Segments (>20% dead): " << fragmented_segments << std::endl;
        std::cout << "Average Segment Utilization: " << std::fixed << std::setprecision(2)
                  << (average_segment_utilization * 100) << "%" << std::endl;
        
        if (!fragmentation_by_class.empty()) {
            std::cout << "\nFragmentation by Size Class:" << std::endl;
            for (const auto& [size_class, frag] : fragmentation_by_class) {
                std::cout << "  " << size_class << " bytes: " 
                          << std::fixed << std::setprecision(2) << (frag * 100) << "%" << std::endl;
            }
        }
    }
};

class SegmentAllocatorFragmentationBenchmark : public ::testing::Test {
protected:
    std::unique_ptr<DurableRuntime> runtime_;
    std::unique_ptr<ObjectTable> object_table_;
    std::unique_ptr<SegmentAllocator> allocator_;
    std::unique_ptr<MVCCContext> mvcc_;
    std::mt19937 rng_{42};  // Fixed seed for reproducibility
    
    // Track allocations for fragmentation analysis
    struct Allocation {
        NodeID id;
        size_t size;
        size_t size_class;
        uint64_t birth_epoch;
        uint64_t retire_epoch;
        bool is_live;
    };
    
    std::vector<Allocation> allocations_;
    std::unordered_map<uint64_t, size_t> handle_to_idx_;
    
    void SetUp() override {
        // Create a temporary directory for the benchmark
        std::string test_dir = "/tmp/fragmentation_bench_" + 
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        std::filesystem::create_directories(test_dir);
        
        // Initialize persistence layer
        allocator_ = std::make_unique<SegmentAllocator>(test_dir);
        object_table_ = std::make_unique<ObjectTable>(100000);
        mvcc_ = std::make_unique<MVCCContext>();
    }
    
    void TearDown() override {
        allocations_.clear();
        handle_to_idx_.clear();
        mvcc_.reset();
        object_table_.reset();
        allocator_.reset();
        runtime_.reset();
    }
    
    size_t select_size(SizeDistribution dist) {
        std::uniform_real_distribution<> uniform(0.0, 1.0);
        double r = uniform(rng_);
        
        switch (dist) {
            case SizeDistribution::UNIFORM: {
                std::uniform_int_distribution<> size_dist(64, 8192);
                return size_dist(rng_);
            }
            
            case SizeDistribution::SMALL_HEAVY: {
                if (r < 0.8) {
                    std::uniform_int_distribution<> small(64, 512);
                    return small(rng_);
                } else {
                    std::uniform_int_distribution<> large(2048, 8192);
                    return large(rng_);
                }
            }
            
            case SizeDistribution::LARGE_HEAVY: {
                if (r < 0.2) {
                    std::uniform_int_distribution<> small(64, 512);
                    return small(rng_);
                } else {
                    std::uniform_int_distribution<> large(2048, 8192);
                    return large(rng_);
                }
            }
            
            case SizeDistribution::BIMODAL: {
                if (r < 0.5) {
                    std::uniform_int_distribution<> small(64, 256);
                    return small(rng_);
                } else {
                    std::uniform_int_distribution<> large(4096, 8192);
                    return large(rng_);
                }
            }
            
            case SizeDistribution::REALISTIC: {
                // Based on typical XTree node sizes
                if (r < 0.4) {
                    return 256;  // Leaf nodes
                } else if (r < 0.8) {
                    return 512;  // Small internal nodes
                } else if (r < 0.95) {
                    return 1024;  // Medium internal nodes
                } else {
                    return 4096;  // Large internal nodes
                }
            }
            
            default:
                return 256;
        }
    }
    
    void perform_allocation(size_t size) {
        uint64_t epoch = mvcc_->advance_epoch();
        uint8_t size_class = persist::size_to_class(size);
        
        auto alloc_result = allocator_->allocate(size);
        OTAddr addr;
        addr.file_id = alloc_result.file_id;
        addr.segment_id = alloc_result.segment_id;
        addr.offset = alloc_result.offset;
        addr.length = alloc_result.length;
        addr.vaddr = nullptr;
        NodeID id = object_table_->allocate(
            NodeKind::Internal, 
            static_cast<uint8_t>(size_class),
            addr,
            epoch
        );
        
        Allocation alloc{id, size, size_class, epoch, UINT64_MAX, true};
        handle_to_idx_[id.handle_index()] = allocations_.size();
        allocations_.push_back(alloc);
    }
    
    void perform_deletion(size_t idx) {
        if (idx >= allocations_.size() || !allocations_[idx].is_live) {
            return;
        }
        
        uint64_t epoch = mvcc_->advance_epoch();
        allocations_[idx].retire_epoch = epoch;
        allocations_[idx].is_live = false;
        
        object_table_->retire(allocations_[idx].id, epoch);
        // Note: We don't actually free from allocator yet (simulating pending reclamation)
    }
    
    void perform_reclamation() {
        uint64_t safe_epoch = mvcc_->min_active_epoch();
        if (safe_epoch == 0) return;
        
        // Reclaim from object table
        size_t reclaimed = object_table_->reclaim_before_epoch(safe_epoch);
        
        // Free from segment allocator
        for (auto& alloc : allocations_) {
            if (!alloc.is_live && alloc.retire_epoch < safe_epoch) {
                // Get the actual address from object table before it's reclaimed
                const OTEntry* entry = object_table_->get_by_handle_unchecked(
                    alloc.id.handle_index()
                );
                if (entry && entry->retire_epoch < safe_epoch) {
                    // Create an Allocation object to free
                    SegmentAllocator::Allocation to_free;
                    to_free.file_id = entry->addr.file_id;
                    to_free.segment_id = entry->addr.segment_id;
                    to_free.offset = entry->addr.offset;
                    to_free.length = alloc.size;
                    to_free.class_id = static_cast<uint8_t>(alloc.size_class);
                    allocator_->free(to_free);
                }
            }
        }
    }
    
    FragmentationStats analyze_fragmentation() {
        FragmentationStats stats;
        
        // Analyze each size class
        for (size_t cls = 0; cls < 8; ++cls) {
            size_t class_size = persist::class_to_size(cls);
            auto class_stats = allocator_->get_stats(cls);
            
            // Use the actual Stats fields
            stats.total_allocated_bytes += (class_stats.live_bytes + class_stats.dead_bytes);
            stats.total_live_bytes += class_stats.live_bytes;
            stats.total_dead_bytes += class_stats.dead_bytes;
            
            if (class_stats.dead_bytes > 0) {
                stats.dead_bytes_by_class[class_size] = class_stats.dead_bytes;
            }
            
            // Calculate fragmentation for this size class
            size_t total = class_stats.live_bytes + class_stats.dead_bytes;
            if (total > 0) {
                double frag = static_cast<double>(class_stats.dead_bytes) / total;
                stats.fragmentation_by_class[class_size] = frag;
            }
            
            // Track segments
            stats.total_segments += class_stats.total_segments;
            
            // Add segment utilization if we have data
            if (class_stats.live_bytes > 0 && total > 0) {
                double utilization = static_cast<double>(class_stats.live_bytes) / total;
                stats.segment_utilizations.push_back(utilization);
            }
        }
        
        stats.calculate_derived_stats();
        return stats;
    }
    
    void simulate_workload(
        WorkloadPattern pattern,
        SizeDistribution size_dist,
        size_t operations,
        double delete_ratio = 0.3
    ) {
        std::uniform_real_distribution<> op_dist(0.0, 1.0);
        std::uniform_int_distribution<> idx_dist;
        
        size_t deletes_performed = 0;
        size_t allocations_performed = 0;
        size_t reclamations = 0;
        
        for (size_t i = 0; i < operations; ++i) {
            double r = op_dist(rng_);
            
            bool should_delete = false;
            switch (pattern) {
                case WorkloadPattern::UNIFORM_RANDOM:
                    should_delete = (r < delete_ratio) && !allocations_.empty();
                    break;
                    
                case WorkloadPattern::TEMPORAL_LOCALITY:
                    // Recent items more likely to be deleted
                    should_delete = (r < delete_ratio) && !allocations_.empty();
                    break;
                    
                case WorkloadPattern::BULK_DELETE:
                    // Every 100 ops, delete 50% of live objects
                    if (i % 100 == 99) {
                        size_t to_delete = allocations_.size() / 2;
                        for (size_t j = 0; j < to_delete; ++j) {
                            if (!allocations_.empty()) {
                                size_t idx = idx_dist(rng_) % allocations_.size();
                                perform_deletion(idx);
                                deletes_performed++;
                            }
                        }
                        continue;
                    }
                    should_delete = false;
                    break;
                    
                case WorkloadPattern::GROWING_DATASET:
                    should_delete = (r < delete_ratio * 0.5) && !allocations_.empty();  // Fewer deletes
                    break;
                    
                case WorkloadPattern::STEADY_STATE:
                    should_delete = (r < 0.5) && !allocations_.empty();  // 50/50
                    break;
            }
            
            if (should_delete) {
                size_t idx;
                if (pattern == WorkloadPattern::TEMPORAL_LOCALITY) {
                    // Bias towards recent allocations
                    size_t recent_window = std::min<size_t>(100, allocations_.size());
                    idx = allocations_.size() - 1 - (idx_dist(rng_) % recent_window);
                } else {
                    idx = idx_dist(rng_) % allocations_.size();
                }
                
                perform_deletion(idx);
                deletes_performed++;
            } else {
                size_t size = select_size(size_dist);
                perform_allocation(size);
                allocations_performed++;
            }
            
            // Periodic reclamation
            if (i % 1000 == 999) {
                perform_reclamation();
                reclamations++;
            }
        }
        
        // Final reclamation
        perform_reclamation();
        
        std::cout << "\nWorkload Summary:" << std::endl;
        std::cout << "  Allocations: " << allocations_performed << std::endl;
        std::cout << "  Deletions: " << deletes_performed << std::endl;
        std::cout << "  Reclamations: " << reclamations << std::endl;
        std::cout << "  Live Objects: " << std::count_if(allocations_.begin(), allocations_.end(),
                     [](const Allocation& a) { return a.is_live; }) << std::endl;
    }
};

TEST_F(SegmentAllocatorFragmentationBenchmark, UniformRandomWorkload) {
    std::cout << "\n=== Uniform Random Workload ===" << std::endl;
    simulate_workload(
        WorkloadPattern::UNIFORM_RANDOM,
        SizeDistribution::REALISTIC,
        10000,
        0.3  // 30% delete ratio
    );
    
    auto stats = analyze_fragmentation();
    stats.print();
    
    // Assertions for expected fragmentation levels
    EXPECT_LT(stats.fragmentation_ratio, 0.4);  // Should be <40% fragmented
}

TEST_F(SegmentAllocatorFragmentationBenchmark, TemporalLocalityWorkload) {
    std::cout << "\n=== Temporal Locality Workload ===" << std::endl;
    simulate_workload(
        WorkloadPattern::TEMPORAL_LOCALITY,
        SizeDistribution::REALISTIC,
        10000,
        0.4  // 40% delete ratio
    );
    
    auto stats = analyze_fragmentation();
    stats.print();
    
    // Temporal locality should lead to better compaction opportunities
    EXPECT_LT(stats.fragmentation_ratio, 0.35);
}

TEST_F(SegmentAllocatorFragmentationBenchmark, BulkDeleteWorkload) {
    std::cout << "\n=== Bulk Delete Workload ===" << std::endl;
    simulate_workload(
        WorkloadPattern::BULK_DELETE,
        SizeDistribution::REALISTIC,
        10000,
        0.0  // Delete ratio handled by bulk delete logic
    );
    
    auto stats = analyze_fragmentation();
    stats.print();
    
    // Bulk deletes create high fragmentation
    EXPECT_GT(stats.fragmentation_ratio, 0.3);
    EXPECT_GT(stats.fragmented_segments, stats.total_segments * 0.3);
}

TEST_F(SegmentAllocatorFragmentationBenchmark, BimodalSizeDistribution) {
    std::cout << "\n=== Bimodal Size Distribution ===" << std::endl;
    simulate_workload(
        WorkloadPattern::STEADY_STATE,
        SizeDistribution::BIMODAL,
        10000,
        0.5  // 50% delete ratio for steady state
    );
    
    auto stats = analyze_fragmentation();
    stats.print();
    
    // Bimodal should show different fragmentation in different size classes
    EXPECT_GT(stats.fragmentation_by_class.size(), 1);
}

TEST_F(SegmentAllocatorFragmentationBenchmark, CompactionStrategyAnalysis) {
    std::cout << "\n=== Compaction Strategy Analysis ===" << std::endl;
    
    // Simulate a high-churn workload
    simulate_workload(
        WorkloadPattern::UNIFORM_RANDOM,
        SizeDistribution::REALISTIC,
        20000,
        0.4  // 40% delete ratio
    );
    
    auto pre_compaction = analyze_fragmentation();
    std::cout << "\nPre-Compaction Stats:" << std::endl;
    pre_compaction.print();
    
    // Analyze which segments would be best to compact
    struct CompactionCandidate {
        size_t size_class;
        double fragmentation;
        size_t dead_bytes;
        double benefit_cost_ratio;  // Benefit (space reclaimed) / Cost (bytes to copy)
    };
    
    std::vector<CompactionCandidate> candidates;
    for (const auto& [size_class, frag] : pre_compaction.fragmentation_by_class) {
        if (frag > 0.2) {  // Only consider segments >20% fragmented
            CompactionCandidate candidate;
            candidate.size_class = size_class;
            candidate.fragmentation = frag;
            candidate.dead_bytes = pre_compaction.dead_bytes_by_class[size_class];
            
            // Calculate benefit/cost ratio
            // Benefit: dead bytes reclaimed
            // Cost: live bytes to copy
            size_t live_bytes = (1.0 - frag) * (candidate.dead_bytes / frag);
            candidate.benefit_cost_ratio = 
                static_cast<double>(candidate.dead_bytes) / (live_bytes + 1);
            
            candidates.push_back(candidate);
        }
    }
    
    // Sort by benefit/cost ratio
    std::sort(candidates.begin(), candidates.end(),
        [](const CompactionCandidate& a, const CompactionCandidate& b) {
            return a.benefit_cost_ratio > b.benefit_cost_ratio;
        });
    
    std::cout << "\n=== Compaction Recommendations ===" << std::endl;
    std::cout << "Priority order (by benefit/cost ratio):" << std::endl;
    
    for (size_t i = 0; i < std::min<size_t>(5, candidates.size()); ++i) {
        const auto& c = candidates[i];
        std::cout << i+1 << ". Size class " << c.size_class << " bytes:" << std::endl;
        std::cout << "   Fragmentation: " << std::fixed << std::setprecision(2) 
                  << (c.fragmentation * 100) << "%" << std::endl;
        std::cout << "   Dead bytes: " << c.dead_bytes << std::endl;
        std::cout << "   Benefit/Cost ratio: " << std::fixed << std::setprecision(2)
                  << c.benefit_cost_ratio << std::endl;
    }
    
    // Recommend compaction strategy
    std::cout << "\n=== Recommended Compaction Strategy ===" << std::endl;
    
    if (pre_compaction.fragmentation_ratio > 0.4) {
        std::cout << "HIGH FRAGMENTATION DETECTED (" 
                  << (pre_compaction.fragmentation_ratio * 100) << "%)" << std::endl;
        std::cout << "Recommendation: AGGRESSIVE COMPACTION" << std::endl;
        std::cout << "- Compact all segments with >30% fragmentation" << std::endl;
        std::cout << "- Run compaction in background with higher priority" << std::endl;
    } else if (pre_compaction.fragmentation_ratio > 0.25) {
        std::cout << "MODERATE FRAGMENTATION (" 
                  << (pre_compaction.fragmentation_ratio * 100) << "%)" << std::endl;
        std::cout << "Recommendation: SELECTIVE COMPACTION" << std::endl;
        std::cout << "- Focus on top " << std::min<size_t>(3, candidates.size()) 
                  << " size classes by benefit/cost ratio" << std::endl;
        std::cout << "- Run during low-activity periods" << std::endl;
    } else {
        std::cout << "LOW FRAGMENTATION (" 
                  << (pre_compaction.fragmentation_ratio * 100) << "%)" << std::endl;
        std::cout << "Recommendation: LAZY COMPACTION" << std::endl;
        std::cout << "- Only compact segments with >50% fragmentation" << std::endl;
        std::cout << "- Can defer compaction to off-peak hours" << std::endl;
    }
    
    // Additional heuristics
    if (pre_compaction.fragmented_segments > pre_compaction.total_segments * 0.5) {
        std::cout << "\nNote: High number of fragmented segments (" 
                  << pre_compaction.fragmented_segments << "/" 
                  << pre_compaction.total_segments << ")" << std::endl;
        std::cout << "Consider more frequent but smaller compaction runs" << std::endl;
    }
}

TEST_F(SegmentAllocatorFragmentationBenchmark, LongRunningHighChurn) {
    std::cout << "\n=== Long Running High Churn Test ===" << std::endl;
    std::cout << "Simulating extended high-churn workload..." << std::endl;
    
    // Track fragmentation over time
    std::vector<double> fragmentation_timeline;
    const size_t checkpoint_interval = 5000;
    const size_t total_ops = 50000;
    
    for (size_t checkpoint = 0; checkpoint < total_ops; checkpoint += checkpoint_interval) {
        simulate_workload(
            WorkloadPattern::UNIFORM_RANDOM,
            SizeDistribution::REALISTIC,
            checkpoint_interval,
            0.45  // 45% delete ratio - high churn
        );
        
        auto stats = analyze_fragmentation();
        fragmentation_timeline.push_back(stats.fragmentation_ratio);
        
        std::cout << "After " << (checkpoint + checkpoint_interval) << " ops: "
                  << std::fixed << std::setprecision(2) 
                  << (stats.fragmentation_ratio * 100) << "% fragmented" << std::endl;
    }
    
    // Analyze fragmentation trend
    bool increasing = true;
    for (size_t i = 1; i < fragmentation_timeline.size(); ++i) {
        if (fragmentation_timeline[i] < fragmentation_timeline[i-1]) {
            increasing = false;
            break;
        }
    }
    
    if (increasing) {
        std::cout << "\nWARNING: Fragmentation continuously increasing!" << std::endl;
        std::cout << "Recommendation: Implement continuous background compaction" << std::endl;
    }
    
    // Final analysis
    auto final_stats = analyze_fragmentation();
    final_stats.print();
    
    std::cout << "\n=== Compaction Frequency Recommendation ===" << std::endl;
    if (final_stats.fragmentation_ratio > 0.5) {
        std::cout << "Run compaction every " << (total_ops / 10) << " operations" << std::endl;
    } else if (final_stats.fragmentation_ratio > 0.3) {
        std::cout << "Run compaction every " << (total_ops / 5) << " operations" << std::endl;
    } else {
        std::cout << "Run compaction every " << (total_ops / 2) << " operations" << std::endl;
    }
}

}  // namespace benchmarks
}  // namespace xtree