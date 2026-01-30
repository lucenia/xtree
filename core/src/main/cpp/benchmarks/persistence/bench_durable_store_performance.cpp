/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Durable Store Performance Benchmarks
 * Tests critical node allocation, read, write, and commit paths
 */

#include <gtest/gtest.h>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <filesystem>
#include <atomic>
#include <thread>
#include <cstring>
#include "../../src/persistence/durable_runtime.h"
#include "../../src/persistence/durable_store.h"
#include "../../src/persistence/durability_policy.h"

namespace fs = std::filesystem;
using namespace std::chrono;
using namespace xtree::persist;

class DurableStorePerformanceBenchmark : public ::testing::Test {
protected:
    std::string test_dir_;
    std::unique_ptr<DurableRuntime> runtime_;
    std::unique_ptr<DurableContext> ctx_;
    std::unique_ptr<DurableStore> store_;
    Paths paths_;
    CheckpointPolicy ckpt_policy_;
    
    void SetUp() override {
        test_dir_ = "/tmp/durable_bench_" + std::to_string(getpid());
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
        
        paths_ = {
            .data_dir = test_dir_,
            .manifest = test_dir_ + "/manifest.json",
            .superblock = test_dir_ + "/superblock.bin",
            .active_log = test_dir_ + "/ot_delta.wal"
        };
        
        // Long checkpoint interval to avoid interference
        ckpt_policy_ = {
            .max_replay_bytes = 100 * 1024 * 1024,  // 100MB
            .max_replay_epochs = 100000,
            .max_age = std::chrono::seconds(3600),
            .min_interval = std::chrono::seconds(3600)
        };
        
        InitializeStore(DurabilityMode::BALANCED);
    }
    
    void TearDown() override {
        store_.reset();
        ctx_.reset();
        runtime_.reset();
        fs::remove_all(test_dir_);
    }
    
    void InitializeStore(DurabilityMode mode) {
        runtime_ = DurableRuntime::open(paths_, ckpt_policy_);
        
        ctx_ = std::make_unique<DurableContext>(DurableContext{
            .ot = runtime_->ot(),
            .alloc = runtime_->allocator(),
            .coord = runtime_->coordinator(),
            .mvcc = runtime_->mvcc(),
            .runtime = *runtime_
        });
        
        DurabilityPolicy policy;
        policy.mode = mode;
        if (mode == DurabilityMode::BALANCED) {
            policy.dirty_flush_bytes = 64 * 1024 * 1024;  // 64MB
            policy.dirty_flush_age = std::chrono::seconds(10);
        }
        
        store_ = std::make_unique<DurableStore>(*ctx_, "bench", policy);
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
};

TEST_F(DurableStorePerformanceBenchmark, NodeAllocationThroughput) {
    printSeparator("Node Allocation Hot Path");
    
    const size_t NODE_SIZES[] = {256, 512, 1024, 4096, 8192, 16384};
    const size_t NUM_ALLOCATIONS = 10000;
    
    std::cout << "\nMeasuring node allocation throughput:\n\n";
    std::cout << "Size    | Allocations/sec | MB/s    | ns/alloc | Status\n";
    std::cout << "--------|-----------------|---------|----------|--------\n";
    
    for (size_t node_size : NODE_SIZES) {
        std::vector<AllocResult> results;
        results.reserve(NUM_ALLOCATIONS);
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_ALLOCATIONS; ++i) {
            auto alloc = store_->allocate_node(node_size, NodeKind::Leaf);
            results.push_back(alloc);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (NUM_ALLOCATIONS * 1e9) / duration.count();
        double mb_per_sec = (NUM_ALLOCATIONS * node_size) / (1024.0 * 1024.0) / 
                           (duration.count() / 1e9);
        double ns_per_alloc = duration.count() / double(NUM_ALLOCATIONS);
        
        // Verify all allocations succeeded
        size_t valid_count = 0;
        for (const auto& alloc : results) {
            if (alloc.writable != nullptr) valid_count++;
        }
        
        // Target: <500ns per allocation
        bool meets_target = ns_per_alloc < 500;
        const char* status = meets_target ? "✓ FAST" : "⚠ SLOW";
        
        std::cout << std::setw(7) << node_size << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(15) << throughput << " | "
                  << std::setprecision(1) << std::setw(7) << mb_per_sec << " | "
                  << std::setprecision(0) << std::setw(8) << ns_per_alloc << " | "
                  << status << "\n";
        
        EXPECT_EQ(valid_count, NUM_ALLOCATIONS) << "Some allocations failed";
    }
    
    std::cout << "\n💡 Target: <500ns per node allocation\n";
}

TEST_F(DurableStorePerformanceBenchmark, ReadWritePerformance) {
    printSeparator("Read/Write Hot Path");
    
    const size_t NODE_SIZE = 4096;
    const size_t NUM_NODES = 10000;
    
    // Pre-allocate and write nodes
    std::vector<NodeID> node_ids;
    std::vector<uint8_t> write_data(NODE_SIZE);
    std::mt19937 rng(42);
    std::generate(write_data.begin(), write_data.end(), [&]() { return rng() & 0xFF; });
    
    std::cout << "\nPreparing " << NUM_NODES << " nodes...\n";
    
    for (size_t i = 0; i < NUM_NODES; ++i) {
        auto alloc = store_->allocate_node(NODE_SIZE, NodeKind::Leaf);
        std::memcpy(alloc.writable, write_data.data(), NODE_SIZE);
        store_->publish_node(alloc.id, alloc.writable, NODE_SIZE);
        node_ids.push_back(alloc.id);
    }
    
    // Commit to make nodes visible
    store_->commit(1);
    
    std::cout << "\nMeasuring read/write performance:\n\n";
    std::cout << "Operation | Ops/sec      | MB/s    | ns/op   | Status\n";
    std::cout << "----------|--------------|---------|---------|--------\n";
    
    // Measure write performance (updates)
    {
        auto start = high_resolution_clock::now();
        
        for (const auto& id : node_ids) {
            auto bytes = store_->read_node(id);
            std::memcpy(const_cast<void*>(bytes.data), write_data.data(), NODE_SIZE);
            store_->publish_node(id, const_cast<void*>(bytes.data), NODE_SIZE);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (NUM_NODES * 1e9) / duration.count();
        double mb_per_sec = (NUM_NODES * NODE_SIZE) / (1024.0 * 1024.0) / 
                           (duration.count() / 1e9);
        double ns_per_op = duration.count() / double(NUM_NODES);
        
        bool fast = ns_per_op < 1000;  // <1μs per write
        
        std::cout << "Write     | " << std::fixed << std::setprecision(0)
                  << std::setw(12) << throughput << " | "
                  << std::setprecision(1) << std::setw(7) << mb_per_sec << " | "
                  << std::setprecision(0) << std::setw(7) << ns_per_op << " | "
                  << (fast ? "✓ FAST" : "⚠ SLOW") << "\n";
    }
    
    // Measure read performance
    {
        std::uniform_int_distribution<size_t> dist(0, node_ids.size() - 1);
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_NODES; ++i) {
            NodeID id = node_ids[dist(rng)];
            auto bytes = store_->read_node(id);
            EXPECT_NE(bytes.data, nullptr);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        
        double throughput = (NUM_NODES * 1e9) / duration.count();
        double mb_per_sec = (NUM_NODES * NODE_SIZE) / (1024.0 * 1024.0) / 
                           (duration.count() / 1e9);
        double ns_per_op = duration.count() / double(NUM_NODES);
        
        bool fast = ns_per_op < 200;  // <200ns per read
        
        std::cout << "Read      | " << std::fixed << std::setprecision(0)
                  << std::setw(12) << throughput << " | "
                  << std::setprecision(1) << std::setw(7) << mb_per_sec << " | "
                  << std::setprecision(0) << std::setw(7) << ns_per_op << " | "
                  << (fast ? "✓ FAST" : "⚠ SLOW") << "\n";
    }
    
    std::cout << "\n💡 Targets: <1μs writes, <200ns reads\n";
}

TEST_F(DurableStorePerformanceBenchmark, CommitLatency) {
    printSeparator("Commit Latency by Durability Mode");
    
    const size_t NODES_PER_COMMIT[] = {1, 10, 100, 1000};
    const size_t NODE_SIZE = 4096;
    const size_t NUM_COMMITS = 100;
    
    std::cout << "\nMeasuring commit latency for different batch sizes:\n\n";
    
    for (DurabilityMode mode : {DurabilityMode::BALANCED, DurabilityMode::STRICT}) {
        InitializeStore(mode);
        
        std::cout << "\n" << (mode == DurabilityMode::STRICT ? "STRICT" : "BALANCED") 
                  << " Mode:\n";
        std::cout << "Batch Size | Avg Latency | P50     | P99     | Throughput\n";
        std::cout << "-----------|-------------|---------|---------|------------\n";
        
        for (size_t batch_size : NODES_PER_COMMIT) {
            std::vector<double> latencies;
            std::vector<uint8_t> data(NODE_SIZE, 0x42);
            
            for (size_t commit = 0; commit < NUM_COMMITS; ++commit) {
                // Allocate and write nodes
                std::vector<NodeID> batch_nodes;
                for (size_t i = 0; i < batch_size; ++i) {
                    auto alloc = store_->allocate_node(NODE_SIZE, NodeKind::Leaf);
                    std::memcpy(alloc.writable, data.data(), NODE_SIZE);
                    store_->publish_node(alloc.id, alloc.writable, NODE_SIZE);
                    batch_nodes.push_back(alloc.id);
                }
                
                // Measure commit time
                auto start = high_resolution_clock::now();
                store_->commit(commit + 1);
                auto end = high_resolution_clock::now();
                
                auto duration = duration_cast<microseconds>(end - start);
                latencies.push_back(duration.count() / 1000.0);  // Convert to ms
            }
            
            // Calculate statistics
            std::sort(latencies.begin(), latencies.end());
            double avg = 0;
            for (double l : latencies) avg += l;
            avg /= latencies.size();
            
            double p50 = latencies[latencies.size() / 2];
            double p99 = latencies[latencies.size() * 99 / 100];
            double throughput = 1000.0 / avg;  // commits/sec
            
            std::cout << std::setw(10) << batch_size << " | "
                      << std::fixed << std::setprecision(2)
                      << std::setw(11) << avg << " ms | "
                      << std::setw(7) << p50 << " ms | "
                      << std::setw(7) << p99 << " ms | "
                      << std::setprecision(0) << std::setw(8) << throughput << "/s\n";
        }
    }
    
    std::cout << "\n💡 BALANCED should be 5-10x faster than STRICT\n";
}

TEST_F(DurableStorePerformanceBenchmark, ConcurrentReaders) {
    printSeparator("Concurrent Read Scalability");
    
    const size_t NUM_NODES = 10000;
    const size_t NODE_SIZE = 4096;
    const size_t READS_PER_THREAD = 100000;
    const int THREAD_COUNTS[] = {1, 2, 4, 8, 16};
    
    // Pre-populate nodes
    std::vector<NodeID> node_ids;
    std::vector<uint8_t> data(NODE_SIZE, 0x42);
    
    for (size_t i = 0; i < NUM_NODES; ++i) {
        auto alloc = store_->allocate_node(NODE_SIZE, NodeKind::Leaf);
        std::memcpy(alloc.writable, data.data(), NODE_SIZE);
        store_->publish_node(alloc.id, alloc.writable, NODE_SIZE);
        node_ids.push_back(alloc.id);
    }
    store_->commit(1);
    
    std::cout << "\nMeasuring concurrent read scaling:\n\n";
    std::cout << "Threads | Total Reads/sec | Per-Thread | Scaling | Status\n";
    std::cout << "--------|-----------------|------------|---------|--------\n";
    
    double single_thread_throughput = 0;
    
    for (int num_threads : THREAD_COUNTS) {
        std::atomic<size_t> total_reads(0);
        
        auto worker = [&](int thread_id) {
            std::mt19937 rng(thread_id);
            std::uniform_int_distribution<size_t> dist(0, node_ids.size() - 1);
            
            for (size_t i = 0; i < READS_PER_THREAD; ++i) {
                NodeID id = node_ids[dist(rng)];
                auto bytes = store_->read_node(id);
                if (bytes.data != nullptr) {
                    total_reads++;
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
        
        double total_throughput = (total_reads * 1e6) / duration.count();
        double per_thread = total_throughput / num_threads;
        
        if (num_threads == 1) {
            single_thread_throughput = total_throughput;
        }
        
        double scaling = (single_thread_throughput > 0) ? 
            (total_throughput / single_thread_throughput) : 1.0;
        
        bool good_scaling = scaling >= num_threads * 0.8;  // >80% linear scaling
        const char* status = good_scaling ? "✓ GOOD" : "⚠ CONT";
        
        std::cout << std::setw(7) << num_threads << " | "
                  << std::fixed << std::setprecision(0)
                  << std::setw(15) << total_throughput << " | "
                  << std::setw(10) << per_thread << " | "
                  << std::setprecision(2) << std::setw(7) << scaling << "x | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 Reads should scale near-linearly with thread count\n";
}

TEST_F(DurableStorePerformanceBenchmark, Summary) {
    printSeparator("Durable Store Performance Summary");
    
    std::cout << "\n📊 Validating critical hot path performance...\n\n";
    
    const size_t NUM_OPS = 100000;
    const size_t NODE_SIZE = 4096;
    
    // Test allocation hot path
    {
        auto start = high_resolution_clock::now();
        std::vector<AllocResult> results;
        
        for (size_t i = 0; i < NUM_OPS; ++i) {
            results.push_back(store_->allocate_node(NODE_SIZE, NodeKind::Leaf));
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        double ns_per_alloc = duration.count() / double(NUM_OPS);
        
        std::cout << "Allocation Hot Path:\n";
        std::cout << "  • " << std::fixed << std::setprecision(0)
                  << ns_per_alloc << " ns/allocation\n";
        std::cout << "  • " << (NUM_OPS * 1e9 / duration.count()) / 1e6 
                  << "M allocations/sec\n";
        std::cout << "  • Target <500ns: " 
                  << (ns_per_alloc < 500 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    // Test read hot path
    {
        // Prepare some nodes
        std::vector<NodeID> ids;
        std::vector<uint8_t> data(NODE_SIZE, 0x42);
        
        for (size_t i = 0; i < 1000; ++i) {
            auto alloc = store_->allocate_node(NODE_SIZE, NodeKind::Leaf);
            std::memcpy(alloc.writable, data.data(), NODE_SIZE);
            store_->publish_node(alloc.id, alloc.writable, NODE_SIZE);
            ids.push_back(alloc.id);
        }
        store_->commit(1);
        
        std::mt19937 rng(42);
        std::uniform_int_distribution<size_t> dist(0, ids.size() - 1);
        
        auto start = high_resolution_clock::now();
        
        for (size_t i = 0; i < NUM_OPS; ++i) {
            auto bytes = store_->read_node(ids[dist(rng)]);
            EXPECT_NE(bytes.data, nullptr);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
        double ns_per_read = duration.count() / double(NUM_OPS);
        
        std::cout << "\nRead Hot Path:\n";
        std::cout << "  • " << std::fixed << std::setprecision(0)
                  << ns_per_read << " ns/read\n";
        std::cout << "  • " << (NUM_OPS * 1e9 / duration.count()) / 1e6 
                  << "M reads/sec\n";
        std::cout << "  • Target <200ns: " 
                  << (ns_per_read < 200 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    // Test commit hot path (BALANCED mode)
    {
        const size_t COMMITS = 1000;
        const size_t NODES_PER_COMMIT = 10;
        
        auto start = high_resolution_clock::now();
        
        for (size_t c = 0; c < COMMITS; ++c) {
            std::vector<uint8_t> data(NODE_SIZE, c & 0xFF);
            
            for (size_t i = 0; i < NODES_PER_COMMIT; ++i) {
                auto alloc = store_->allocate_node(NODE_SIZE, NodeKind::Leaf);
                std::memcpy(alloc.writable, data.data(), NODE_SIZE);
                store_->publish_node(alloc.id, alloc.writable, NODE_SIZE);
            }
            
            store_->commit(c + 2);
        }
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        double ms_per_commit = duration.count() / (COMMITS * 1000.0);
        
        std::cout << "\nCommit Hot Path (BALANCED):\n";
        std::cout << "  • " << std::fixed << std::setprecision(2)
                  << ms_per_commit << " ms/commit\n";
        std::cout << "  • " << std::setprecision(0)
                  << 1000.0 / ms_per_commit << " commits/sec\n";
        std::cout << "  • Target <2ms: " 
                  << (ms_per_commit < 2 ? "✓ PASS" : "✗ FAIL") << "\n";
    }
    
    std::cout << "\n🎯 Performance Targets:\n";
    std::cout << "  ✓ Allocation: <500ns per node\n";
    std::cout << "  ✓ Read: <200ns per node\n";
    std::cout << "  ✓ Commit: <2ms (BALANCED mode)\n";
    std::cout << "  ✓ Scaling: >80% linear for reads\n";
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}