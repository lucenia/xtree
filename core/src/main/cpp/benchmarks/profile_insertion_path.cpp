/*
 * Profile the insertion path to identify bottlenecks
 * Run with: ./xtree_benchmarks --gtest_filter="ProfileInsertionPath.*"
 */

#include <gtest/gtest.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <filesystem>
#include <unordered_map>
#include <algorithm>
#include "../src/xtree.hpp"
#include "../src/datarecord.hpp"
#include "../src/indexdetails.hpp"
#include "../src/lru.h"
#include "../src/persistence/durable_store.h"
#include "../src/persistence/checkpoint_coordinator.h"

using namespace xtree;
using namespace std::chrono;

// Profiling regions
struct ProfileRegion {
    std::string name;
    duration<double, std::milli> elapsed;
    size_t count;
};

class InsertionProfiler {
public:
    void start(const std::string& region) {
        start_times_[region] = high_resolution_clock::now();
    }
    
    void end(const std::string& region) {
        auto end_time = high_resolution_clock::now();
        auto it = start_times_.find(region);
        if (it != start_times_.end()) {
            auto elapsed = duration_cast<duration<double, std::milli>>(end_time - it->second);
            regions_[region].name = region;
            regions_[region].elapsed += elapsed;
            regions_[region].count++;
        }
    }
    
    void report() const {
        std::cout << "\n=== Insertion Path Profile ===" << std::endl;
        std::cout << std::setw(40) << "Region" 
                  << std::setw(15) << "Total (ms)"
                  << std::setw(10) << "Count"
                  << std::setw(15) << "Avg (us)"
                  << std::setw(10) << "%" << std::endl;
        std::cout << std::string(90, '-') << std::endl;
        
        // Calculate total time
        double total_ms = 0;
        for (const auto& [name, region] : regions_) {
            total_ms += region.elapsed.count();
        }
        
        // Sort by elapsed time
        std::vector<std::pair<std::string, ProfileRegion>> sorted;
        for (const auto& [name, region] : regions_) {
            sorted.emplace_back(name, region);
        }
        std::sort(sorted.begin(), sorted.end(), 
                  [](const auto& a, const auto& b) {
                      return a.second.elapsed.count() > b.second.elapsed.count();
                  });
        
        // Print sorted results
        for (const auto& [name, region] : sorted) {
            double avg_us = region.count > 0 ? 
                (region.elapsed.count() * 1000.0 / region.count) : 0;
            double percent = total_ms > 0 ? 
                (region.elapsed.count() / total_ms * 100.0) : 0;
            
            std::cout << std::setw(40) << name
                      << std::setw(15) << std::fixed << std::setprecision(2) 
                      << region.elapsed.count()
                      << std::setw(10) << region.count
                      << std::setw(15) << std::fixed << std::setprecision(2) << avg_us
                      << std::setw(10) << std::fixed << std::setprecision(1) 
                      << percent << std::endl;
        }
    }

private:
    std::unordered_map<std::string, high_resolution_clock::time_point> start_times_;
    std::unordered_map<std::string, ProfileRegion> regions_;
};

void profile_durable_insertions(size_t num_records) {
    std::cout << "\n=== Profiling DURABLE Mode Insertions ===" << std::endl;
    std::cout << "Inserting " << num_records << " records..." << std::endl;
    
    InsertionProfiler profiler;
    
    // Setup
    const std::string test_dir = "/tmp/xtree_profile_" + 
                                 std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(test_dir);
    
    // Create IndexDetails with DURABLE mode
    std::vector<const char*> dim_labels = {"x", "y", "z"};
    IndexDetails<DataRecord> idx(
        3,        // dims
        6,        // precision
        &dim_labels,  // dimension labels
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "profile_durable",  // field name
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir
    );
    
    // Get the store for commits
    auto* store = idx.getStore();
    // Note: Coordinator access for throughput updates would require internal API access
    // For now, we'll skip automatic throughput updates in the profiling benchmark
    
    // Create root bucket
    auto* root = new XTreeBucket<DataRecord>(&idx, true);
    auto* cachedRoot = idx.getCache().add(idx.getNextNodeID(), root);
    
    // Random data generation
    std::mt19937 gen(42);
    std::uniform_real_distribution<> coord_dist(0.0, 100.0);
    
    // Batch tracking for throughput updates
    const size_t batch_size = 1000;
    size_t batch_count = 0;
    
    auto start_total = high_resolution_clock::now();
    
    for (size_t i = 0; i < num_records; ++i) {
        // Create DataRecord
        profiler.start("1.CreateDataRecord");
        auto dr = new DataRecord(3, 6, "row_" + std::to_string(i));
        std::vector<double> point = {
            coord_dist(gen), coord_dist(gen), coord_dist(gen)
        };
        dr->putPoint(&point);
        profiler.end("1.CreateDataRecord");
        
        // Tree traversal and insertion
        profiler.start("2.TreeInsertion");
        root->xt_insert(cachedRoot, dr);
        profiler.end("2.TreeInsertion");
        
        // Within tree insertion, these happen:
        if (idx.getPersistenceMode() == IndexDetails<DataRecord>::PersistenceMode::DURABLE) {
            // Note: These are conceptual - actual profiling would need to be added to the implementation
            // profiler.start("2a.FindInsertionPoint");
            // profiler.end("2a.FindInsertionPoint");
            
            // profiler.start("2b.AllocateNode");
            // profiler.end("2b.AllocateNode");
            
            // profiler.start("2c.PersistDataRecord");
            // profiler.end("2c.PersistDataRecord");
            
            // profiler.start("2d.UpdateObjectTable");
            // profiler.end("2d.UpdateObjectTable");
            
            // profiler.start("2e.AppendWAL");
            // profiler.end("2e.AppendWAL");
        }
        
        // Update throughput metrics (disabled - requires internal API)
        batch_count++;
        if (batch_count >= batch_size) {
            // In production, coordinator->update_throughput(batch_count) would be called here
            batch_count = 0;
        }
        
        // Periodic commit (every 10K records)
        if (i > 0 && i % 10000 == 0) {
            profiler.start("4.PeriodicCommit");
            if (store) {
                store->commit(i);  // Use record count as epoch
            }
            profiler.end("4.PeriodicCommit");
            
            std::cout << "  Inserted " << i << " records..." << std::endl;
        }
    }
    
    // Final commit
    profiler.start("5.FinalCommit");
    if (store) {
        store->commit(num_records);  // Use final record count as epoch
    }
    profiler.end("5.FinalCommit");
    
    auto end_total = high_resolution_clock::now();
    auto total_time = duration_cast<duration<double>>(end_total - start_total);
    
    std::cout << "\nTotal insertion time: " << std::fixed << std::setprecision(2) 
              << total_time.count() << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(0)
              << (num_records / total_time.count()) << " records/sec" << std::endl;
    
    // Print profile report
    profiler.report();
    
    // Cleanup
    std::filesystem::remove_all(test_dir);
}

void profile_memory_insertions(size_t num_records) {
    std::cout << "\n=== Profiling IN_MEMORY Mode Insertions ===" << std::endl;
    std::cout << "Inserting " << num_records << " records..." << std::endl;
    
    InsertionProfiler profiler;
    
    // Create IndexDetails with IN_MEMORY mode
    std::vector<const char*> dim_labels = {"x", "y", "z"};
    IndexDetails<DataRecord> idx(
        3,        // dims
        6,        // precision
        &dim_labels,  // dimension labels
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "profile_memory",  // field name
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    // Create root bucket
    auto* root = new XTreeBucket<DataRecord>(&idx, true);
    auto* cachedRoot = idx.getCache().add(idx.getNextNodeID(), root);
    
    // Random data generation
    std::mt19937 gen(42);
    std::uniform_real_distribution<> coord_dist(0.0, 100.0);
    
    auto start_total = high_resolution_clock::now();
    
    for (size_t i = 0; i < num_records; ++i) {
        // Create DataRecord
        profiler.start("1.CreateDataRecord");
        auto dr = new DataRecord(3, 6, "row_" + std::to_string(i));
        std::vector<double> point = {
            coord_dist(gen), coord_dist(gen), coord_dist(gen)
        };
        dr->putPoint(&point);
        profiler.end("1.CreateDataRecord");
        
        // Tree insertion
        profiler.start("2.TreeInsertion");
        root->xt_insert(cachedRoot, dr);
        profiler.end("2.TreeInsertion");
        
        if (i > 0 && i % 10000 == 0) {
            std::cout << "  Inserted " << i << " records..." << std::endl;
        }
    }
    
    auto end_total = high_resolution_clock::now();
    auto total_time = duration_cast<duration<double>>(end_total - start_total);
    
    std::cout << "\nTotal insertion time: " << std::fixed << std::setprecision(2) 
              << total_time.count() << " seconds" << std::endl;
    std::cout << "Throughput: " << std::fixed << std::setprecision(0)
              << (num_records / total_time.count()) << " records/sec" << std::endl;
    
    // Print profile report
    profiler.report();
}

// Test fixture for profiling benchmarks
class ProfileInsertionPath : public ::testing::Test {
protected:
    static constexpr size_t DEFAULT_RECORDS = 10000;  // Smaller for tests
    
    void SetUp() override {
        // Clean up any existing test directories
        std::filesystem::remove_all("/tmp/xtree_profile_*");
    }
    
    void TearDown() override {
        // Clean up test directories
        std::filesystem::remove_all("/tmp/xtree_profile_*");
    }
};

TEST_F(ProfileInsertionPath, InMemoryMode) {
    std::cout << "\n=== Profiling IN_MEMORY Mode ===" << std::endl;
    profile_memory_insertions(DEFAULT_RECORDS);
}

TEST_F(ProfileInsertionPath, DurableMode) {
    std::cout << "\n=== Profiling DURABLE Mode ===" << std::endl;
    profile_durable_insertions(DEFAULT_RECORDS);
}

TEST_F(ProfileInsertionPath, ComparisonBenchmark) {
    std::cout << "\nXTree Insertion Path Profiler" << std::endl;
    std::cout << "=============================" << std::endl;
    
    // Profile IN_MEMORY mode first (baseline)
    profile_memory_insertions(DEFAULT_RECORDS);
    
    // Profile DURABLE mode
    profile_durable_insertions(DEFAULT_RECORDS);
    
    std::cout << "\n=== Profiling Complete ===" << std::endl;
    std::cout << "\nFor detailed profiling, compile with -pg and use gprof, or:" << std::endl;
    std::cout << "  perf record -g ./xtree_benchmarks --gtest_filter=\"ProfileInsertionPath.*\"" << std::endl;
    std::cout << "  perf report" << std::endl;
}