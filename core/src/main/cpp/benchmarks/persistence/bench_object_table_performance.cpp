/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Object Table Performance Benchmarks
 * Tests critical NodeID allocation, validation, and epoch management
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <atomic>
#include <thread>
#include <unordered_set>
#include "../../src/persistence/object_table.hpp"
#include "../../src/persistence/ot_entry.h"

using namespace std::chrono;
using namespace xtree::persist;

class ObjectTablePerformanceBenchmark : public ::testing::Test {
protected:
    std::unique_ptr<ObjectTable> ot_;
    
    void SetUp() override {
        // Start with reasonable capacity to avoid reallocation during benchmarks
        ot_ = std::make_unique<ObjectTable>(100000);
    }
    
    void TearDown() override {
        ot_.reset();
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
};

TEST_F(ObjectTablePerformanceBenchmark, AllocationThroughput) {
    printSeparator("NodeID Allocation Hot Path");
    
    const size_t ALLOCATION_COUNTS[] = {1000, 10000, 100000, 500000};
    
    std::cout << "\nMeasuring NodeID allocation throughput:\n\n";
    std::cout << "Count     | Allocations/sec | ns/alloc | Memory/node | Status\n";
    std::cout << "----------|-----------------|----------|-------------|--------\n";
    
    for (size_t count : ALLOCATION_COUNTS) {
        // Reset object table for each test
        ot_ = std::make_unique<ObjectTable>(count);
        
        std::vector<NodeID> ids;
        ids.reserve(count);
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < count; ++i) {
            OTAddr addr{};
            addr.file_id = i / 10000;
            addr.segment_id = (i / 100) % 100;
            addr.offset = (i % 100) * 4096;
            addr.length = 4096;
            
            NodeKind kind = (i % 2) ? NodeKind::Leaf : NodeKind::Internal;
            uint8_t class_id = i % 7;
            
            NodeID id = ot_->allocate(kind, class_id, addr, 0);  // epoch 0 = invisible
            ids.push_back(id);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (count * 1e9) / duration.count();
        double ns_per_alloc = duration.count() / double(count);
        
        // Estimate memory per node (rough)
        size_t memory_estimate = sizeof(OTEntry) + 16;  // Entry + overhead
        
        // Target: <100ns per allocation
        bool meets_target = ns_per_alloc < 100;
        const char* status = meets_target ? "✓ FAST" : "⚠ SLOW";
        
        std::cout << std::setw(9) << count << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(15) << throughput << " | "
                  << std::setw(8) << ns_per_alloc << " | "
                  << std::setw(11) << memory_estimate << " | "
                  << status << "\n";
        
        // Verify uniqueness
        std::unordered_set<uint32_t> unique_handles;
        for (const auto& id : ids) {
            unique_handles.insert(id.handle_index());
        }
        EXPECT_EQ(unique_handles.size(), ids.size()) << "NodeIDs must be unique";
    }
    
    std::cout << "\n💡 Target: <100ns per NodeID allocation\n";
}

TEST_F(ObjectTablePerformanceBenchmark, LiveMarkingPerformance) {
    printSeparator("Mark Live Hot Path");
    
    const size_t NUM_NODES = 100000;
    const size_t BATCH_SIZES[] = {1, 10, 100, 1000};
    
    std::cout << "\nMeasuring mark_live performance (two-phase protocol):\n\n";
    std::cout << "Batch Size | Reserve+Commit/sec | ns/op | Throughput | Status\n";
    std::cout << "-----------|-------------------|-------|------------|--------\n";
    
    for (size_t batch_size : BATCH_SIZES) {
        // Pre-allocate nodes
        std::vector<NodeID> allocated_ids;
        for (size_t i = 0; i < NUM_NODES; ++i) {
            OTAddr addr{};
            addr.file_id = 0;
            addr.segment_id = i / 1000;
            addr.offset = (i % 1000) * 4096;
            addr.length = 4096;
            
            NodeID id = ot_->allocate(NodeKind::Internal, 1, addr, 0);
            allocated_ids.push_back(id);
        }
        
        size_t num_batches = NUM_NODES / batch_size;
        uint64_t epoch = 100;
        
        auto start = high_resolution_clock::now();
        
        for (size_t batch = 0; batch < num_batches; ++batch) {
            // Two-phase mark live for each batch
            std::vector<NodeID> reserved_ids;
            
            // Phase 1: Reserve
            for (size_t i = 0; i < batch_size; ++i) {
                size_t idx = batch * batch_size + i;
                NodeID reserved = ot_->mark_live_reserve(allocated_ids[idx], epoch);
                reserved_ids.push_back(reserved);
            }
            
            // Phase 2: Commit
            for (const auto& reserved : reserved_ids) {
                ot_->mark_live_commit(reserved, epoch);
            }
            
            epoch++;
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double ops_per_sec = (NUM_NODES * 1e9) / duration.count();
        double ns_per_op = duration.count() / double(NUM_NODES);
        double throughput_mb = (NUM_NODES * 4096) / (1024.0 * 1024.0) / 
                               (duration.count() / 1e9);
        
        bool fast = ns_per_op < 200;  // <200ns per mark_live
        const char* status = fast ? "✓ FAST" : "⚠ SLOW";
        
        std::cout << std::setw(10) << batch_size << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(17) << ops_per_sec << " | "
                  << std::setw(5) << ns_per_op << " | "
                  << std::setprecision(1) << std::setw(8) << throughput_mb << " MB/s | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 Larger batches should amortize synchronization costs\n";
}

TEST_F(ObjectTablePerformanceBenchmark, ValidationPerformance) {
    printSeparator("NodeID Validation Hot Path");
    
    const size_t NUM_NODES = 100000;
    const size_t NUM_VALIDATIONS = 1000000;
    
    // Pre-populate with mix of live and retired nodes
    std::vector<NodeID> live_nodes;
    std::vector<NodeID> retired_nodes;
    
    std::cout << "\nPreparing " << NUM_NODES << " nodes (50% live, 50% retired)...\n";
    
    for (size_t i = 0; i < NUM_NODES; ++i) {
        OTAddr addr{};
        addr.file_id = 0;
        addr.segment_id = i / 1000;
        addr.offset = (i % 1000) * 4096;
        addr.length = 4096;
        
        NodeID id = ot_->allocate(NodeKind::Internal, 1, addr, 0);
        NodeID reserved = ot_->mark_live_reserve(id, 100);
        ot_->mark_live_commit(reserved, 100);
        
        if (i % 2 == 0) {
            live_nodes.push_back(reserved);
        } else {
            ot_->retire(reserved, 200);
            retired_nodes.push_back(reserved);
        }
    }
    
    std::cout << "\nMeasuring validation performance:\n\n";
    std::cout << "Node Type | Validations/sec | ns/check | Hit Rate | Status\n";
    std::cout << "----------|-----------------|----------|----------|--------\n";
    
    // Test live node validation
    {
        std::mt19937 rng(42);
        std::uniform_int_distribution<size_t> dist(0, live_nodes.size() - 1);
        
        size_t valid_count = 0;
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_VALIDATIONS; ++i) {
            NodeID id = live_nodes[dist(rng)];
            if (ot_->is_valid(id)) {
                valid_count++;
            }
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (NUM_VALIDATIONS * 1e9) / duration.count();
        double ns_per_check = duration.count() / double(NUM_VALIDATIONS);
        double hit_rate = (valid_count * 100.0) / NUM_VALIDATIONS;
        
        bool fast = ns_per_check < 50;  // <50ns per validation
        const char* status = fast ? "✓ FAST" : "⚠ SLOW";
        
        std::cout << "Live      | " << std::fixed << std::setprecision(0)
                  << std::setw(15) << throughput << " | "
                  << std::setw(8) << ns_per_check << " | "
                  << std::setprecision(1) << std::setw(7) << hit_rate << "% | "
                  << status << "\n";
    }
    
    // Test retired node validation
    {
        std::mt19937 rng(42);
        std::uniform_int_distribution<size_t> dist(0, retired_nodes.size() - 1);
        
        size_t valid_count = 0;
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_VALIDATIONS; ++i) {
            NodeID id = retired_nodes[dist(rng)];
            if (ot_->is_valid(id)) {
                valid_count++;
            }
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (NUM_VALIDATIONS * 1e9) / duration.count();
        double ns_per_check = duration.count() / double(NUM_VALIDATIONS);
        double hit_rate = (valid_count * 100.0) / NUM_VALIDATIONS;
        
        bool fast = ns_per_check < 50;
        const char* status = fast ? "✓ FAST" : "⚠ SLOW";
        
        std::cout << "Retired   | " << std::fixed << std::setprecision(0)
                  << std::setw(15) << throughput << " | "
                  << std::setw(8) << ns_per_check << " | "
                  << std::setprecision(1) << std::setw(7) << hit_rate << "% | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 Validation should be <50ns for cache-hot entries\n";
}

TEST_F(ObjectTablePerformanceBenchmark, ConcurrentOperations) {
    printSeparator("Concurrent Object Table Operations");
    
    const int THREAD_COUNTS[] = {1, 2, 4, 8};
    const size_t OPS_PER_THREAD = 10000;
    
    std::cout << "\nMeasuring concurrent allocation and validation:\n\n";
    std::cout << "Threads | Alloc/s      | Valid/s      | Total ops/s | Scaling\n";
    std::cout << "--------|--------------|--------------|-------------|--------\n";
    
    double single_thread_ops = 0;
    
    for (int num_threads : THREAD_COUNTS) {
        ot_ = std::make_unique<ObjectTable>(OPS_PER_THREAD * num_threads * 2);
        
        std::atomic<size_t> total_allocations(0);
        std::atomic<size_t> total_validations(0);
        
        auto worker = [&](int thread_id) {
            std::vector<NodeID> local_ids;
            local_ids.reserve(OPS_PER_THREAD);
            
            // Allocation phase
            for (size_t i = 0; i < OPS_PER_THREAD; ++i) {
                OTAddr addr{};
                addr.file_id = thread_id;
                addr.segment_id = i / 100;
                addr.offset = (i % 100) * 4096;
                addr.length = 4096;
                
                NodeID id = ot_->allocate(NodeKind::Internal, 1, addr, 0);
                NodeID reserved = ot_->mark_live_reserve(id, thread_id * 1000 + i);
                ot_->mark_live_commit(reserved, thread_id * 1000 + i);
                local_ids.push_back(reserved);
                total_allocations++;
            }
            
            // Validation phase
            std::mt19937 rng(thread_id);
            std::uniform_int_distribution<size_t> dist(0, local_ids.size() - 1);
            
            for (size_t i = 0; i < OPS_PER_THREAD; ++i) {
                NodeID id = local_ids[dist(rng)];
                if (ot_->is_valid(id)) {
                    total_validations++;
                }
            }
        };
        
        auto start = high_resolution_clock::now();
        
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back(worker, i);
        }
        
        for (auto& t : threads) {
            t.join();
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        
        double alloc_per_sec = (total_allocations * 1e6) / duration.count();
        double valid_per_sec = (total_validations * 1e6) / duration.count();
        double total_ops = alloc_per_sec + valid_per_sec;
        
        if (num_threads == 1) {
            single_thread_ops = total_ops;
        }
        
        double scaling = (single_thread_ops > 0) ? 
            (total_ops / single_thread_ops) / num_threads : 1.0;
        
        std::cout << std::setw(7) << num_threads << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(12) << alloc_per_sec << " | "
                  << std::setw(12) << valid_per_sec << " | "
                  << std::setw(11) << total_ops << " | "
                  << std::setprecision(2) << std::setw(6) << scaling << "\n";
    }
    
    std::cout << "\n💡 Good scaling indicates low lock contention\n";
}

TEST_F(ObjectTablePerformanceBenchmark, Summary) {
    printSeparator("Object Table Performance Summary");
    
    std::cout << "\n📊 Validating critical hot path performance...\n\n";
    
    const size_t NUM_OPS = 100000;
    
    // Test allocation hot path
    {
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_OPS; ++i) {
            OTAddr addr{};
            addr.file_id = i / 10000;
            addr.segment_id = (i / 100) % 100;
            addr.offset = (i % 100) * 4096;
            addr.length = 4096;
            
            ot_->allocate(NodeKind::Internal, 1, addr, 0);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        double ns_per_alloc = duration.count() / double(NUM_OPS);
        
        std::cout << "Allocation Hot Path:\n";
        std::cout << "  • " << std::fixed << std::setprecision(0)
                  << ns_per_alloc << " ns/allocation\n";
        std::cout << "  • " << (NUM_OPS * 1e9 / duration.count()) / 1e6 
                  << "M allocations/sec\n";
        std::cout << "  • Target <100ns: " 
                  << (ns_per_alloc < 100 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    // Test mark_live hot path
    {
        // Pre-allocate nodes
        std::vector<NodeID> ids;
        for (size_t i = 0; i < 10000; ++i) {
            OTAddr addr{};
            addr.file_id = 0;
            addr.segment_id = i / 100;
            addr.offset = (i % 100) * 4096;
            addr.length = 4096;
            
            ids.push_back(ot_->allocate(NodeKind::Internal, 1, addr, 0));
        }
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < ids.size(); ++i) {
            NodeID reserved = ot_->mark_live_reserve(ids[i], i + 100);
            ot_->mark_live_commit(reserved, i + 100);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        double ns_per_mark = duration.count() / double(ids.size());
        
        std::cout << "\nMark Live Hot Path:\n";
        std::cout << "  • " << std::fixed << std::setprecision(0)
                  << ns_per_mark << " ns/mark_live\n";
        std::cout << "  • " << (ids.size() * 1e9 / duration.count()) / 1e6 
                  << "M marks/sec\n";
        std::cout << "  • Target <200ns: " 
                  << (ns_per_mark < 200 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    // Test validation hot path
    {
        // Prepare some live nodes
        std::vector<NodeID> live_ids;
        for (size_t i = 0; i < 1000; ++i) {
            OTAddr addr{};
            addr.file_id = 0;
            addr.segment_id = i;
            addr.offset = 0;
            addr.length = 4096;
            
            NodeID id = ot_->allocate(NodeKind::Internal, 1, addr, 0);
            NodeID reserved = ot_->mark_live_reserve(id, 1000);
            ot_->mark_live_commit(reserved, 1000);
            live_ids.push_back(reserved);
        }
        
        std::mt19937 rng(42);
        std::uniform_int_distribution<size_t> dist(0, live_ids.size() - 1);
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_OPS; ++i) {
            ot_->is_valid(live_ids[dist(rng)]);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        double ns_per_check = duration.count() / double(NUM_OPS);
        
        std::cout << "\nValidation Hot Path:\n";
        std::cout << "  • " << std::fixed << std::setprecision(0)
                  << ns_per_check << " ns/validation\n";
        std::cout << "  • " << (NUM_OPS * 1e9 / duration.count()) / 1e6 
                  << "M validations/sec\n";
        std::cout << "  • Target <50ns: " 
                  << (ns_per_check < 50 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    std::cout << "\n🎯 Hot Path Performance Targets:\n";
    std::cout << "  ✓ Allocation: <100ns per NodeID\n";
    std::cout << "  ✓ Mark Live: <200ns per operation\n";
    std::cout << "  ✓ Validation: <50ns per check\n";
    std::cout << "  ✓ Scaling: Good concurrency\n";
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}

// -----------------------------------------------------------------------------
// Reclaim / Free O(1) Hot Paths
// -----------------------------------------------------------------------------

TEST_F(ObjectTablePerformanceBenchmark, ReclaimThroughputBulk) {
    printSeparator("Reclaim Bulk Throughput (O(1) free path)");

    // Choose a size large enough to show the old O(n) pain
    const size_t N = 300'000;

    // Fresh table sized to N so we don't keep a huge extra cache around
    ot_ = std::make_unique<ObjectTable>(N);

    std::vector<NodeID> ids;
    ids.reserve(N);

    // 1) Allocate & publish N nodes
    for (size_t i = 0; i < N; ++i) {
        OTAddr a{};
        a.file_id    = 0;
        a.segment_id = i / 1024;
        a.offset     = (i % 1024) * 4096;
        a.length     = 4096;

        NodeID id = ot_->allocate(NodeKind::Internal, 1, a, 0);
        NodeID r  = ot_->mark_live_reserve(id, /*epoch=*/100);
        ot_->mark_live_commit(r, 100);
        ids.push_back(r);
    }

    // 2) Retire all at epoch=200
    for (auto id : ids) {
        ot_->retire(id, /*retire_epoch=*/200);
    }

    // 3) Measure reclaim (this hits Phase 3: bm_set + push_back for each handle)
    auto t0 = high_resolution_clock::now();
    size_t reclaimed = ot_->reclaim_before_epoch(/*safe_epoch=*/201);
    auto t1 = high_resolution_clock::now();

    ASSERT_EQ(reclaimed, N) << "All retired should be reclaimed";

    auto ns = duration_cast<nanoseconds>(t1 - t0).count();
    double ops_per_sec = (N * 1e9) / double(ns);
    double ns_per = double(ns) / double(N);

    std::cout << "\nReclaim Bulk Throughput:\n\n";
    std::cout << "Count     | Reclaimed/sec   | ns/reclaim | Status\n";
    std::cout << "----------|-----------------|------------|--------\n";
    std::cout << std::setw(9) << N << " | "
              << std::fixed << std::setprecision(0) << std::setw(15) << ops_per_sec << " | "
              << std::setprecision(0) << std::setw(10) << ns_per << " | "
              << (ns_per < 200 ? "✓ FAST" : "⚠ SLOW") << "\n";
}

TEST_F(ObjectTablePerformanceBenchmark, ReuseAfterReclaimLatency) {
    printSeparator("Immediate Reuse After Reclaim (cache tail push)");

    const size_t N = 100'000;
    ot_ = std::make_unique<ObjectTable>(N);

    std::vector<NodeID> ids;
    ids.reserve(N);

    // Allocate & publish N nodes
    for (size_t i = 0; i < N; ++i) {
        OTAddr a{};
        a.file_id = 1; a.segment_id = i / 512; a.offset = (i % 512) * 4096; a.length = 4096;
        NodeID id = ot_->allocate(NodeKind::Leaf, 2, a, 0);
        NodeID r  = ot_->mark_live_reserve(id, 10);
        ot_->mark_live_commit(r, 10);
        ids.push_back(r);
    }

    // Retire all, reclaim
    for (auto id : ids) ot_->retire(id, 20);
    ASSERT_EQ(ot_->reclaim_before_epoch(21), N);

    // Measure latency of re-allocating N nodes (should pop reclaimed first)
    auto t0 = high_resolution_clock::now();
    size_t reused = 0;
    for (size_t i = 0; i < N; ++i) {
        OTAddr a{};
        a.file_id = 2; a.segment_id = i / 512; a.offset = (i % 512) * 4096; a.length = 4096;
        NodeID id = ot_->allocate(NodeKind::Internal, 3, a, 0);

        // Because Phase 3 pushed reclaimed handles to cache tail, we expect
        // most allocations to reuse those precise handles immediately.
        if (id.handle_index() <= N) reused++; // heuristic sanity for first slab
    }
    auto t1 = high_resolution_clock::now();

    auto ns = duration_cast<nanoseconds>(t1 - t0).count();
    double ns_per_alloc = double(ns) / double(N);
    double allocs_per_s = (N * 1e9) / double(ns);

    std::cout << "\nReuse After Reclaim:\n\n";
    std::cout << "Count     | Alloc/sec       | ns/alloc | Reuse hit | Status\n";
    std::cout << "----------|-----------------|----------|-----------|--------\n";
    std::cout << std::setw(9) << N << " | "
              << std::fixed << std::setprecision(0) << std::setw(15) << allocs_per_s << " | "
              << std::setprecision(0) << std::setw(8) << ns_per_alloc << " | "
              << std::setprecision(1) << std::setw(9) << (100.0 * reused / N) << "% | "
              << (ns_per_alloc < 100 ? "✓ FAST" : "⚠ SLOW") << "\n";
}

TEST_F(ObjectTablePerformanceBenchmark, SteadyChurnReclaimAllocate) {
    printSeparator("Steady-State Retire → Reclaim → Allocate Churn");

    const size_t WARM = 50'000;    // live working set
    const size_t BATCH = 5'000;    // retire/allocate per round
    const int ROUNDS = 20;

    ot_ = std::make_unique<ObjectTable>(WARM + BATCH); // tight capacity to avoid huge spare cache

    std::vector<NodeID> live;
    live.reserve(WARM);

    // Warm: allocate & publish WARM live nodes
    for (size_t i = 0; i < WARM; ++i) {
        OTAddr a{};
        a.file_id=3; a.segment_id=i/256; a.offset=(i%256)*4096; a.length=4096;
        NodeID id = ot_->allocate(NodeKind::Internal, 1, a, 0);
        NodeID r  = ot_->mark_live_reserve(id, 100);
        ot_->mark_live_commit(r, 100);
        live.push_back(r);
    }

    std::mt19937 rng(42);

    auto t0 = high_resolution_clock::now();
    size_t total_reclaimed = 0, total_alloc = 0;

    for (int round = 0; round < ROUNDS; ++round) {
        // Pick a contiguous block to retire (cheap indexing)
        size_t start = (round * BATCH) % (WARM - BATCH);
        for (size_t i = 0; i < BATCH; ++i) {
            ot_->retire(live[start + i], /*retire_epoch=*/200 + round);
        }

        total_reclaimed += ot_->reclaim_before_epoch(/*safe_epoch=*/201 + round);

        // Allocate BATCH new nodes (should immediately reuse reclaimed handles)
        for (size_t i = 0; i < BATCH; ++i) {
            OTAddr a{};
            a.file_id=4; a.segment_id=i/256; a.offset=(i%256)*4096; a.length=4096;
            NodeID id = ot_->allocate(NodeKind::Leaf, 2, a, 0);
            NodeID r  = ot_->mark_live_reserve(id, 300 + round);
            ot_->mark_live_commit(r, 300 + round);
            live[start + i] = r; // keep the working set size steady
            total_alloc++;
        }
    }

    auto t1 = high_resolution_clock::now();
    auto us = duration_cast<microseconds>(t1 - t0).count();

    double cycles = double(ROUNDS);
    double ops = double(total_reclaimed + total_alloc);
    double ops_per_s = (ops * 1e6) / double(us);
    double us_per_round = double(us) / cycles;

    std::cout << "\nSteady Churn:\n\n";
    std::cout << "Rounds | Retired/Reclaimed per round | Total ops/s | µs/round | Status\n";
    std::cout << "-------|------------------------------|-------------|----------|--------\n";
    std::cout << std::setw(6) << ROUNDS << " | "
              << BATCH << "                         | "
              << std::fixed << std::setprecision(0) << std::setw(11) << ops_per_s << " | "
              << std::setprecision(0) << std::setw(8) << us_per_round << " | "
              << "✓ CHURN\n";
}