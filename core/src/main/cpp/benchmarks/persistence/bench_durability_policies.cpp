/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Durability Policy Performance Benchmarks
 * Tests STRICT, BALANCED, and EVENTUAL durability modes
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
#include "../../src/persistence/checkpoint_coordinator.h"

namespace fs = std::filesystem;
using namespace std::chrono;
using namespace xtree::persist;

class DurabilityPolicyBenchmark : public ::testing::Test {
protected:
    std::string test_dir_;
    Paths paths_;
    CheckpointPolicy checkpoint_policy_;
    
    void SetUp() override {
        test_dir_ = "/tmp/durability_bench_" + std::to_string(getpid());
        fs::create_directories(test_dir_);
        
        // Setup paths
        paths_ = {
            .data_dir = test_dir_,
            .manifest = test_dir_ + "/manifest.json",
            .superblock = test_dir_ + "/superblock.bin",
            .active_log = test_dir_ + "/ot_delta.wal"
        };
        
        // Setup checkpoint policy
        checkpoint_policy_ = {
            .max_replay_bytes = 100 * 1024 * 1024,  // 100MB
            .max_replay_epochs = 100000,
            .max_age = std::chrono::seconds(600),
            .min_interval = std::chrono::seconds(30)
        };
    }
    
    void TearDown() override {
        try {
            fs::remove_all(test_dir_);
        } catch (...) {}
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
    
    std::vector<uint8_t> generatePayload(size_t size) {
        std::vector<uint8_t> payload(size);
        std::mt19937 rng(42);
        std::uniform_int_distribution<uint8_t> dist(0, 255);
        for (auto& byte : payload) {
            byte = dist(rng);
        }
        return payload;
    }
};

TEST_F(DurabilityPolicyBenchmark, StrictModePerformance) {
    printSeparator("STRICT Mode Performance (Sync Everything)");
    
    const size_t NODE_SIZES[] = {1024, 4096, 8192, 16384, 32768};
    const size_t NUM_COMMITS = 100;
    
    std::cout << "\nMeasuring STRICT mode commit latency:\n\n";
    std::cout << "Node Size | Commit Latency | Throughput | MB/s   | Status\n";
    std::cout << "----------|----------------|------------|--------|--------\n";
    
    for (size_t node_size : NODE_SIZES) {
        // Create runtime with STRICT policy
        DurabilityPolicy policy;
        policy.mode = DurabilityMode::STRICT;
        
        auto runtime = DurableRuntime::open(paths_, checkpoint_policy_);
        ASSERT_NE(runtime, nullptr);
        
        DurableContext ctx{
            .ot = runtime->ot(),
            .alloc = runtime->allocator(),
            .coord = runtime->coordinator(),
            .mvcc = runtime->mvcc(),
            .runtime = *runtime
        };
        
        DurableStore store(ctx, "test", policy);
        
        std::vector<double> latencies;
        auto payload = generatePayload(node_size);
        
        for (size_t i = 0; i < NUM_COMMITS; ++i) {
            auto start = high_resolution_clock::now();
            
            // Allocate and write node
            auto alloc = store.allocate_node(node_size, NodeKind::Leaf);
            memcpy(alloc.writable, payload.data(), node_size);
            store.publish_node(alloc.id, alloc.writable, node_size);
            
            // Commit with STRICT mode (syncs data + WAL)
            store.commit(i + 1);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start);
            latencies.push_back(duration.count() / 1000.0);  // Convert to ms
        }
        
        // Calculate statistics
        double avg_latency = 0;
        for (double lat : latencies) {
            avg_latency += lat;
        }
        avg_latency /= latencies.size();
        
        double throughput = NUM_COMMITS / (avg_latency * NUM_COMMITS / 1000.0);  // commits/sec
        double mb_per_sec = (NUM_COMMITS * node_size) / (1024.0 * 1024.0) / 
                           (avg_latency * NUM_COMMITS / 1000.0);
        
        // Expected: 5-50ms for STRICT mode
        bool meets_expectation = (avg_latency >= 5 && avg_latency <= 50);
        const char* status = meets_expectation ? "✓ EXPECTED" : "⚠ CHECK";
        
        std::cout << std::setw(9) << node_size << " | "
                  << std::fixed << std::setprecision(2)
                  << std::setw(13) << avg_latency << "ms | "
                  << std::setprecision(0) << std::setw(10) << throughput << " | "
                  << std::setprecision(1) << std::setw(6) << mb_per_sec << " | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 STRICT mode should show 5-50ms latency due to synchronous flushing\n";
}

TEST_F(DurabilityPolicyBenchmark, EventualModePerformance) {
    printSeparator("EVENTUAL Mode Performance (Payload-in-WAL)");
    
    const size_t NODE_SIZES[] = {512, 1024, 2048, 4096, 8192, 16384};
    const size_t NUM_COMMITS = 500;
    
    std::cout << "\nMeasuring EVENTUAL mode with payload-in-WAL:\n\n";
    std::cout << "Node Size | Commit Latency | Throughput | WAL MB/s | Status\n";
    std::cout << "----------|----------------|------------|----------|--------\n";
    
    for (size_t node_size : NODE_SIZES) {
        // Create runtime with EVENTUAL policy
        DurabilityPolicy policy;
        policy.mode = DurabilityMode::EVENTUAL;
        policy.max_payload_in_wal = 8192;  // Embed up to 8KB in WAL
        
        auto runtime = DurableRuntime::open(paths_, checkpoint_policy_);
        ASSERT_NE(runtime, nullptr);
        
        DurableContext ctx{
            .ot = runtime->ot(),
            .alloc = runtime->allocator(),
            .coord = runtime->coordinator(),
            .mvcc = runtime->mvcc(),
            .runtime = *runtime
        };
        
        DurableStore store(ctx, "test", policy);
        
        std::vector<double> latencies;
        auto payload = generatePayload(node_size);
        
        for (size_t i = 0; i < NUM_COMMITS; ++i) {
            auto start = high_resolution_clock::now();
            
            // Allocate and write node
            auto alloc = store.allocate_node(node_size, NodeKind::Leaf);
            memcpy(alloc.writable, payload.data(), node_size);
            store.publish_node(alloc.id, alloc.writable, node_size);
            
            // Commit with EVENTUAL mode (WAL-only, payload embedded)
            store.commit(i + 1);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start);
            latencies.push_back(duration.count() / 1000.0);  // Convert to ms
        }
        
        // Calculate statistics
        double avg_latency = 0;
        for (double lat : latencies) {
            avg_latency += lat;
        }
        avg_latency /= latencies.size();
        
        double throughput = NUM_COMMITS / (avg_latency * NUM_COMMITS / 1000.0);
        double wal_mb_per_sec = (NUM_COMMITS * node_size) / (1024.0 * 1024.0) / 
                               (avg_latency * NUM_COMMITS / 1000.0);
        
        // Expected: 0.5-2ms for EVENTUAL mode
        bool meets_expectation = (avg_latency >= 0.5 && avg_latency <= 2.0);
        const char* status = meets_expectation ? "✓ FAST" : 
                            (node_size > 8192 ? "⚠ LARGE" : "⚠ SLOW");
        
        std::cout << std::setw(9) << node_size << " | "
                  << std::fixed << std::setprecision(2)
                  << std::setw(13) << avg_latency << "ms | "
                  << std::setprecision(0) << std::setw(10) << throughput << " | "
                  << std::setprecision(1) << std::setw(8) << wal_mb_per_sec << " | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 EVENTUAL mode embeds small payloads in WAL for fast commits\n";
}

TEST_F(DurabilityPolicyBenchmark, BalancedModePerformance) {
    printSeparator("BALANCED Mode Performance (Default, Coalesced Flush)");
    
    const size_t NODE_SIZES[] = {1024, 4096, 8192, 16384, 32768, 65536};
    const size_t NUM_COMMITS = 1000;
    
    std::cout << "\nMeasuring BALANCED mode with coalesced flushing:\n\n";
    std::cout << "Node Size | Commit Latency | Throughput | MB/s   | Status\n";
    std::cout << "----------|----------------|------------|--------|--------\n";
    
    for (size_t node_size : NODE_SIZES) {
        // Create runtime with BALANCED policy (default)
        DurabilityPolicy policy;
        policy.mode = DurabilityMode::BALANCED;
        policy.dirty_flush_bytes = 128 * 1024 * 1024;  // 128MB
        policy.dirty_flush_age = std::chrono::seconds(3);
        
        auto runtime = DurableRuntime::open(paths_, checkpoint_policy_);
        ASSERT_NE(runtime, nullptr);
        
        DurableContext ctx{
            .ot = runtime->ot(),
            .alloc = runtime->allocator(),
            .coord = runtime->coordinator(),
            .mvcc = runtime->mvcc(),
            .runtime = *runtime
        };
        
        DurableStore store(ctx, "test", policy);
        
        std::vector<double> latencies;
        auto payload = generatePayload(node_size);
        
        for (size_t i = 0; i < NUM_COMMITS; ++i) {
            auto start = high_resolution_clock::now();
            
            // Allocate and write node
            auto alloc = store.allocate_node(node_size, NodeKind::Leaf);
            memcpy(alloc.writable, payload.data(), node_size);
            store.publish_node(alloc.id, alloc.writable, node_size);
            
            // Commit with BALANCED mode (WAL-only, coalesced data flush)
            store.commit(i + 1);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start);
            latencies.push_back(duration.count() / 1000.0);  // Convert to ms
        }
        
        // Calculate statistics
        double avg_latency = 0;
        for (double lat : latencies) {
            avg_latency += lat;
        }
        avg_latency /= latencies.size();
        
        double throughput = NUM_COMMITS / (avg_latency * NUM_COMMITS / 1000.0);
        double mb_per_sec = (NUM_COMMITS * node_size) / (1024.0 * 1024.0) / 
                           (avg_latency * NUM_COMMITS / 1000.0);
        
        // Expected: 1-3ms for BALANCED mode
        bool meets_expectation = (avg_latency >= 1.0 && avg_latency <= 3.0);
        const char* status = meets_expectation ? "✓ OPTIMAL" : "⚠ CHECK";
        
        std::cout << std::setw(9) << node_size << " | "
                  << std::fixed << std::setprecision(2)
                  << std::setw(13) << avg_latency << "ms | "
                  << std::setprecision(0) << std::setw(10) << throughput << " | "
                  << std::setprecision(1) << std::setw(6) << mb_per_sec << " | "
                  << status << "\n";
    }
    
    std::cout << "\n💡 BALANCED mode provides best throughput/safety trade-off\n";
}

TEST_F(DurabilityPolicyBenchmark, ModeComparison) {
    printSeparator("Durability Mode Comparison");
    
    const size_t NODE_SIZE = 4096;
    const size_t NUM_COMMITS = 200;
    
    std::cout << "\nComparing all three modes with " << NODE_SIZE << " byte nodes:\n\n";
    std::cout << "Mode      | Avg Latency | 99% Latency | Throughput | Recovery | Status\n";
    std::cout << "----------|-------------|-------------|------------|----------|--------\n";
    
    struct ModeResult {
        const char* name;
        DurabilityMode mode;
        double avg_latency;
        double p99_latency;
        double throughput;
        const char* recovery_complexity;
    };
    
    std::vector<ModeResult> results;
    auto payload = generatePayload(NODE_SIZE);
    
    // Test each mode
    for (auto mode : {DurabilityMode::STRICT, DurabilityMode::EVENTUAL, DurabilityMode::BALANCED}) {
        DurabilityPolicy policy;
        policy.mode = mode;
        if (mode == DurabilityMode::EVENTUAL) {
            policy.max_payload_in_wal = 8192;
        }
        
        auto runtime = DurableRuntime::open(paths_, checkpoint_policy_);
        ASSERT_NE(runtime, nullptr);
        
        DurableContext ctx{
            .ot = runtime->ot(),
            .alloc = runtime->allocator(),
            .coord = runtime->coordinator(),
            .mvcc = runtime->mvcc(),
            .runtime = *runtime
        };
        
        DurableStore store(ctx, "test", policy);
        
        std::vector<double> latencies;
        
        for (size_t i = 0; i < NUM_COMMITS; ++i) {
            auto start = high_resolution_clock::now();
            
            auto alloc = store.allocate_node(NODE_SIZE, NodeKind::Leaf);
            memcpy(alloc.writable, payload.data(), NODE_SIZE);
            store.publish_node(alloc.id, alloc.writable, NODE_SIZE);
            store.commit(i + 1);
            
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<microseconds>(end - start);
            latencies.push_back(duration.count() / 1000.0);
        }
        
        // Calculate statistics
        std::sort(latencies.begin(), latencies.end());
        double avg_latency = 0;
        for (double lat : latencies) {
            avg_latency += lat;
        }
        avg_latency /= latencies.size();
        
        double p99_latency = latencies[size_t(latencies.size() * 0.99)];
        double throughput = NUM_COMMITS / (avg_latency * NUM_COMMITS / 1000.0);
        
        const char* name = (mode == DurabilityMode::STRICT) ? "STRICT" :
                          (mode == DurabilityMode::EVENTUAL) ? "EVENTUAL" : "BALANCED";
        const char* recovery = (mode == DurabilityMode::STRICT) ? "Simple" :
                              (mode == DurabilityMode::EVENTUAL) ? "Slower" : "Fast";
        
        results.push_back({name, mode, avg_latency, p99_latency, throughput, recovery});
    }
    
    // Print comparison
    for (const auto& r : results) {
        const char* status = (r.mode == DurabilityMode::BALANCED) ? "✓ DEFAULT" :
                            (r.mode == DurabilityMode::STRICT) ? "SAFE" : "FAST";
        
        std::cout << std::setw(9) << r.name << " | "
                  << std::fixed << std::setprecision(2)
                  << std::setw(10) << r.avg_latency << "ms | "
                  << std::setw(10) << r.p99_latency << "ms | "
                  << std::setprecision(0) << std::setw(10) << r.throughput << " | "
                  << std::setw(8) << r.recovery_complexity << " | "
                  << status << "\n";
    }
    
    std::cout << "\n📊 Performance Summary:\n";
    std::cout << "  • STRICT: Maximum safety, highest latency\n";
    std::cout << "  • EVENTUAL: Lowest latency via payload-in-WAL\n";
    std::cout << "  • BALANCED: Best throughput/safety trade-off (recommended)\n";
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}