/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Object Table Fragmentation Benchmark
 * Tests fragmentation patterns at the ObjectTable level in high-churn scenarios
 */

#include <gtest/gtest.h>
#include <random>
#include <vector>
#include <map>
#include <chrono>
#include <iomanip>
#include "persistence/object_table.hpp"
#include "persistence/object_table_sharded.hpp"
#include "persistence/mvcc_context.h"

namespace xtree {
namespace benchmarks {

using namespace persist;

class ObjectTableFragmentationBenchmark : public ::testing::Test {
protected:
    std::unique_ptr<ObjectTableSharded> object_table_;
    std::unique_ptr<MVCCContext> mvcc_;
    std::mt19937 rng_{42};
    
    struct LiveNode {
        NodeID id;
        size_t size;
        uint64_t birth_epoch;
        bool is_live = true;
    };
    
    std::vector<LiveNode> nodes_;
    size_t total_allocated_ = 0;
    size_t total_retired_ = 0;
    size_t bytes_allocated_ = 0;
    size_t bytes_freed_ = 0;
    
    void SetUp() override {
        object_table_ = std::make_unique<ObjectTableSharded>(1000000, 64);
        mvcc_ = std::make_unique<MVCCContext>();
    }
    
    NodeID allocate_node(size_t size) {
        uint64_t epoch = mvcc_->advance_epoch();
        
        // Simulate allocation - just use size as a proxy for class_id
        uint8_t class_id = size <= 256 ? 0 : 
                          size <= 512 ? 1 : 
                          size <= 1024 ? 2 : 
                          size <= 4096 ? 3 : 4;
        
        // Create a dummy address
        OTAddr addr;
        addr.segment_id = class_id;
        addr.offset = total_allocated_ * 64;  // Dummy offset
        addr.length = size;
        
        NodeID id = object_table_->allocate(
            NodeKind::Internal,
            class_id,
            addr,
            epoch
        );
        
        total_allocated_++;
        bytes_allocated_ += size;
        
        LiveNode node{id, size, epoch, true};
        nodes_.push_back(node);
        
        return id;
    }
    
    void retire_node(size_t idx) {
        if (idx >= nodes_.size() || !nodes_[idx].is_live) {
            return;
        }
        
        uint64_t epoch = mvcc_->advance_epoch();
        nodes_[idx].is_live = false;
        
        object_table_->retire(nodes_[idx].id, epoch);
        total_retired_++;
        bytes_freed_ += nodes_[idx].size;
    }
    
    void perform_reclamation() {
        uint64_t safe_epoch = mvcc_->min_active_epoch();
        if (safe_epoch > 0) {
            object_table_->reclaim_before_epoch(safe_epoch);
        }
    }
    
    double calculate_fragmentation() {
        size_t live_count = 0;
        size_t live_bytes = 0;
        
        for (const auto& node : nodes_) {
            if (node.is_live) {
                live_count++;
                live_bytes += node.size;
            }
        }
        
        double fragmentation = 0.0;
        if (bytes_allocated_ > 0) {
            size_t dead_bytes = bytes_allocated_ - live_bytes;
            fragmentation = static_cast<double>(dead_bytes) / bytes_allocated_;
        }
        
        return fragmentation;
    }
    
    void print_stats(const std::string& phase) {
        size_t live_count = 0;
        size_t live_bytes = 0;
        
        for (const auto& node : nodes_) {
            if (node.is_live) {
                live_count++;
                live_bytes += node.size;
            }
        }
        
        double fragmentation = calculate_fragmentation();
        
        std::cout << "\n=== " << phase << " ===" << std::endl;
        std::cout << "Total Allocated: " << total_allocated_ << " nodes, " 
                  << bytes_allocated_ << " bytes" << std::endl;
        std::cout << "Total Retired: " << total_retired_ << " nodes, "
                  << bytes_freed_ << " bytes" << std::endl;
        std::cout << "Currently Live: " << live_count << " nodes, "
                  << live_bytes << " bytes" << std::endl;
        std::cout << "Fragmentation: " << std::fixed << std::setprecision(2)
                  << (fragmentation * 100) << "%" << std::endl;
        
        // Get shard metrics
        auto stats = object_table_->get_aggregate_metrics();
        std::cout << "Object Table Stats:" << std::endl;
        std::cout << "  Active handles: " << stats.active_handles << std::endl;
        std::cout << "  Free handles: " << stats.free_handles << std::endl;
        std::cout << "  Active shards: " << object_table_->active_shards() 
                  << "/" << object_table_->num_shards() << std::endl;
    }
};

TEST_F(ObjectTableFragmentationBenchmark, UniformRandomChurn) {
    std::cout << "\n=== Uniform Random Churn Test ===" << std::endl;
    std::cout << "Simulating high-churn workload with 40% delete ratio..." << std::endl;
    
    const size_t operations = 10000;
    const double delete_ratio = 0.4;
    
    std::uniform_real_distribution<> op_dist(0.0, 1.0);
    std::uniform_int_distribution<> size_dist(128, 4096);
    
    for (size_t i = 0; i < operations; ++i) {
        if (op_dist(rng_) < delete_ratio && !nodes_.empty()) {
            // Delete a random node
            std::uniform_int_distribution<> idx_dist(0, nodes_.size() - 1);
            retire_node(idx_dist(rng_));
        } else {
            // Allocate a new node
            size_t size = size_dist(rng_);
            allocate_node(size);
        }
        
        // Periodic reclamation
        if (i % 1000 == 999) {
            perform_reclamation();
        }
    }
    
    print_stats("After 10K operations");
    
    double final_fragmentation = calculate_fragmentation();
    EXPECT_LT(final_fragmentation, 0.5);  // Should be <50% fragmented
}

TEST_F(ObjectTableFragmentationBenchmark, TemporalLocalityChurn) {
    std::cout << "\n=== Temporal Locality Churn Test ===" << std::endl;
    std::cout << "Recent items more likely to be deleted..." << std::endl;
    
    const size_t operations = 10000;
    const double delete_ratio = 0.4;
    
    std::uniform_real_distribution<> op_dist(0.0, 1.0);
    std::uniform_int_distribution<> size_dist(128, 4096);
    
    for (size_t i = 0; i < operations; ++i) {
        if (op_dist(rng_) < delete_ratio && !nodes_.empty()) {
            // Delete a recent node (bias towards end of vector)
            size_t window = std::min<size_t>(100, nodes_.size());
            std::uniform_int_distribution<> idx_dist(
                nodes_.size() - window, nodes_.size() - 1
            );
            retire_node(idx_dist(rng_));
        } else {
            size_t size = size_dist(rng_);
            allocate_node(size);
        }
        
        if (i % 1000 == 999) {
            perform_reclamation();
        }
    }
    
    print_stats("After 10K operations with temporal locality");
    
    double final_fragmentation = calculate_fragmentation();
    EXPECT_LT(final_fragmentation, 0.4);  // Better than uniform random
}

TEST_F(ObjectTableFragmentationBenchmark, BulkDeletePattern) {
    std::cout << "\n=== Bulk Delete Pattern Test ===" << std::endl;
    std::cout << "Periodic bulk deletions create fragmentation spikes..." << std::endl;
    
    std::uniform_int_distribution<> size_dist(128, 4096);
    
    for (int batch = 0; batch < 10; ++batch) {
        // Allocation phase
        std::cout << "\nBatch " << batch + 1 << " - Allocating..." << std::endl;
        for (int i = 0; i < 1000; ++i) {
            allocate_node(size_dist(rng_));
        }
        
        // Bulk delete phase - delete 50% of live nodes
        std::cout << "Batch " << batch + 1 << " - Bulk deleting..." << std::endl;
        std::vector<size_t> live_indices;
        for (size_t i = 0; i < nodes_.size(); ++i) {
            if (nodes_[i].is_live) {
                live_indices.push_back(i);
            }
        }
        
        std::shuffle(live_indices.begin(), live_indices.end(), rng_);
        size_t to_delete = live_indices.size() / 2;
        for (size_t i = 0; i < to_delete && i < live_indices.size(); ++i) {
            retire_node(live_indices[i]);
        }
        
        perform_reclamation();
        
        if (batch % 3 == 2) {
            print_stats("After batch " + std::to_string(batch + 1));
        }
    }
    
    print_stats("Final state after bulk deletes");
    
    double final_fragmentation = calculate_fragmentation();
    EXPECT_GT(final_fragmentation, 0.3);  // High fragmentation expected
}

TEST_F(ObjectTableFragmentationBenchmark, LongRunningHighChurn) {
    std::cout << "\n=== Long Running High Churn Test ===" << std::endl;
    std::cout << "Tracking fragmentation over extended period..." << std::endl;
    
    const size_t checkpoint_interval = 5000;
    const size_t total_operations = 50000;
    const double delete_ratio = 0.45;
    
    std::uniform_real_distribution<> op_dist(0.0, 1.0);
    std::uniform_int_distribution<> size_dist(128, 4096);
    
    std::vector<double> fragmentation_history;
    
    for (size_t checkpoint = 0; checkpoint < total_operations; 
         checkpoint += checkpoint_interval) {
        
        for (size_t i = 0; i < checkpoint_interval; ++i) {
            if (op_dist(rng_) < delete_ratio && !nodes_.empty()) {
                std::uniform_int_distribution<> idx_dist(0, nodes_.size() - 1);
                retire_node(idx_dist(rng_));
            } else {
                allocate_node(size_dist(rng_));
            }
            
            if ((checkpoint + i) % 1000 == 999) {
                perform_reclamation();
            }
        }
        
        double frag = calculate_fragmentation();
        fragmentation_history.push_back(frag);
        
        std::cout << "After " << (checkpoint + checkpoint_interval) 
                  << " ops: " << std::fixed << std::setprecision(2)
                  << (frag * 100) << "% fragmented" << std::endl;
    }
    
    // Analyze trend
    bool increasing = true;
    for (size_t i = 1; i < fragmentation_history.size(); ++i) {
        if (fragmentation_history[i] < fragmentation_history[i-1] * 0.95) {
            increasing = false;
            break;
        }
    }
    
    print_stats("Final state after 50K operations");
    
    if (increasing) {
        std::cout << "\nWARNING: Fragmentation continuously increasing!" << std::endl;
        std::cout << "Recommendation: Implement continuous background compaction" << std::endl;
    } else {
        std::cout << "\nFragmentation stabilized - reclamation is keeping up" << std::endl;
    }
    
    // Compaction recommendations
    double final_frag = calculate_fragmentation();
    std::cout << "\n=== Compaction Strategy Recommendation ===" << std::endl;
    if (final_frag > 0.5) {
        std::cout << "HIGH FRAGMENTATION (" << (final_frag * 100) << "%)" << std::endl;
        std::cout << "Recommended: AGGRESSIVE compaction every 10K operations" << std::endl;
    } else if (final_frag > 0.3) {
        std::cout << "MODERATE FRAGMENTATION (" << (final_frag * 100) << "%)" << std::endl;
        std::cout << "Recommended: SELECTIVE compaction every 25K operations" << std::endl;
    } else {
        std::cout << "LOW FRAGMENTATION (" << (final_frag * 100) << "%)" << std::endl;
        std::cout << "Recommended: LAZY compaction every 50K operations" << std::endl;
    }
}

}  // namespace benchmarks
}  // namespace xtree