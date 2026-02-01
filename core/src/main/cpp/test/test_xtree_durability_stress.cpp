/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Comprehensive stress test for XTree durability using the proper API
 * Tests real xt_insert operations and search queries in both modes
 */

#include <gtest/gtest.h>
#include "./util/log.h"
#include "./util/logmanager.h"
#include "./util/log_control.h"
#include "indexdetails.hpp"
#include "cache_policy.hpp"
#include "persistence/durable_store.h"
#include "xtree.h"
#include "xtree.hpp"
#include "xtree_allocator_traits.hpp"
#include "persistence/memory_store.h"
#include "persistence/durable_runtime.h"
#include <memory>
#include <filesystem>
#include <ctime>
#include <chrono>
#include <set>
#include <iostream>
#include <iomanip>
#include <cmath>
#include <random>
#include <algorithm>
#include <sys/types.h>
#include <unistd.h>
#ifdef __APPLE__
#include <mach/mach.h>
#endif

namespace xtree {

class XTreeDurabilityStressTest : public ::testing::Test {
protected:


    // Utility function to get directory size in bytes
    static size_t getDirectorySize(const std::string& path) {
        size_t total_size = 0;
        try {
            for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
                if (entry.is_regular_file()) {
                    total_size += entry.file_size();
                }
            }
        } catch (...) {
            // Directory might not exist or be accessible
        }
        return total_size;
    }
    
    // Get memory usage of current process
    static size_t getMemoryUsage() {
        #ifdef __APPLE__
        struct mach_task_basic_info info;
        mach_msg_type_number_t info_count = MACH_TASK_BASIC_INFO_COUNT;
        if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, 
                     (task_info_t)&info, &info_count) == KERN_SUCCESS) {
            return info.resident_size;  // Physical memory in use
        }
        #elif __linux__
        // Parse /proc/self/status for VmRSS
        std::ifstream status("/proc/self/status");
        std::string line;
        while (std::getline(status, line)) {
            if (line.find("VmRSS:") == 0) {
                size_t kb;
                if (sscanf(line.c_str(), "VmRSS: %zu kB", &kb) == 1) {
                    return kb * 1024;
                }
            }
        }
        #endif
        return 0;
    }
    
    // Structure to hold storage metrics
    struct StorageMetrics {
        size_t total_disk_bytes = 0;
        size_t xd_file_bytes = 0;        // Data files (.xd)
        size_t xi_file_bytes = 0;        // Index/tree files (.xi)
        size_t wal_bytes = 0;             // WAL files
        size_t checkpoint_bytes = 0;      // Checkpoint files
        size_t other_bytes = 0;           // Other files
        int num_xd_files = 0;
        int num_xi_files = 0;
        int num_checkpoints = 0;
        int num_wal_files = 0;
        double fragmentation_ratio = 0.0; // Estimate based on file usage
        size_t preallocated_count = 0;    // Files at exactly 1GB (likely pre-allocated)
        
        void analyze(const std::string& dir) {
            total_disk_bytes = 0;
            xd_file_bytes = 0;
            xi_file_bytes = 0;
            wal_bytes = 0;
            checkpoint_bytes = 0;
            other_bytes = 0;
            num_xd_files = 0;
            num_xi_files = 0;
            num_checkpoints = 0;
            num_wal_files = 0;
            
            try {
                for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
                    if (entry.is_regular_file()) {
                        size_t size = entry.file_size();
                        total_disk_bytes += size;
                        
                        std::string filename = entry.path().filename().string();
                        if (filename.find(".xd") != std::string::npos) {
                            xd_file_bytes += size;
                            num_xd_files++;
                        } else if (filename.find(".xi") != std::string::npos) {
                            xi_file_bytes += size;
                            num_xi_files++;
                        } else if (filename.find(".wal") != std::string::npos || 
                                  filename.find("delta") != std::string::npos) {
                            wal_bytes += size;
                            num_wal_files++;
                        } else if (filename.find("checkpoint") != std::string::npos && 
                                  filename.find(".bin") != std::string::npos) {
                            checkpoint_bytes += size;
                            num_checkpoints++;
                        } else {
                            other_bytes += size;
                        }
                    }
                }
                
                // Check for pre-allocated files (files at exactly 1GB boundary)
                size_t one_gb = 1ULL << 30;
                size_t files_at_1gb = 0;
                
                for (const auto& entry2 : std::filesystem::recursive_directory_iterator(dir)) {
                    if (entry2.is_regular_file() && entry2.file_size() == one_gb) {
                        files_at_1gb++;
                    }
                }
                
                // Estimate fragmentation: if files are not full (1GB expected), they have wasted space
                if (num_xd_files > 0) {
                    size_t expected_full_size = num_xd_files * (1ULL << 30);  // Assuming 1GB files
                    if (expected_full_size > 0) {
                        fragmentation_ratio = 1.0 - (double(xd_file_bytes) / double(expected_full_size));
                    }
                }
                
                if (files_at_1gb > 0) {
                    // Note: files at exactly 1GB are likely pre-allocated
                    preallocated_count = files_at_1gb;
                }
            } catch (...) {
                // Directory might not exist
            }
        }
        
        void print() const {
            std::cout << "\n=== Storage Metrics ===" << std::endl;
            std::cout << "Total disk usage: " << formatBytes(total_disk_bytes) << std::endl;
            std::cout << "  Data files (.xd): " << formatBytes(xd_file_bytes) 
                     << " (" << num_xd_files << " files";
            if (num_xd_files > 0) {
                std::cout << ", avg " << formatBytes(xd_file_bytes / num_xd_files);
            }
            std::cout << ")" << std::endl;
            std::cout << "  Index files (.xi): " << formatBytes(xi_file_bytes) 
                     << " (" << num_xi_files << " files";
            if (num_xi_files > 0) {
                std::cout << ", avg " << formatBytes(xi_file_bytes / num_xi_files);
            }
            std::cout << ")" << std::endl;
            std::cout << "  WAL files: " << formatBytes(wal_bytes) 
                     << " (" << num_wal_files << " files)" << std::endl;
            std::cout << "  Checkpoints: " << formatBytes(checkpoint_bytes) 
                     << " (" << num_checkpoints << " files)" << std::endl;
            if (other_bytes > 0) {
                std::cout << "  Other files: " << formatBytes(other_bytes) << std::endl;
            }
            
            if (total_disk_bytes > 0) {
                double data_percentage = (xd_file_bytes * 100.0) / total_disk_bytes;
                double index_percentage = (xi_file_bytes * 100.0) / total_disk_bytes;
                double metadata_percentage = ((wal_bytes + checkpoint_bytes) * 100.0) / total_disk_bytes;
                std::cout << "\nEfficiency breakdown:" << std::endl;
                std::cout << "  Data records: " << std::fixed << std::setprecision(1) 
                         << data_percentage << "%" << std::endl;
                std::cout << "  Tree structure: " << std::fixed << std::setprecision(1) 
                         << index_percentage << "%" << std::endl;
                std::cout << "  Metadata (WAL+checkpoints): " << std::fixed << std::setprecision(1) 
                         << metadata_percentage << "%" << std::endl;
                std::cout << "  Fragmentation estimate: " << std::fixed << std::setprecision(1)
                         << (fragmentation_ratio * 100.0) << "%" << std::endl;
                         
                if (preallocated_count > 0) {
                    std::cout << "\nPre-allocation analysis:" << std::endl;
                    std::cout << "  Files at exactly 1GB: " << preallocated_count << " files" << std::endl;
                    std::cout << "  Likely pre-allocated space: " << formatBytes(preallocated_count * (1ULL << 30)) << std::endl;
                }
            }
        }
        
        static std::string formatBytes(size_t bytes) {
            std::ostringstream oss;
            if (bytes >= (1ULL << 30)) {
                oss << std::fixed << std::setprecision(2) << (bytes / double(1ULL << 30)) << " GB";
            } else if (bytes >= (1ULL << 20)) {
                oss << std::fixed << std::setprecision(2) << (bytes / double(1ULL << 20)) << " MB";
            } else if (bytes >= (1ULL << 10)) {
                oss << std::fixed << std::setprecision(2) << (bytes / double(1ULL << 10)) << " KB";
            } else {
                oss << bytes << " B";
            }
            return oss.str();
        }
    };
    
    void SetUp() override {
        // Set up dimension labels
        dims_ = {"x", "y"};
        dim_ptrs_.clear();
        for (const auto& dim : dims_) {
            dim_ptrs_.push_back(dim.c_str());
        }
    }
    
    void TearDown() override {
        // Reset to unlimited cache policy
        IndexDetails<DataRecord>::applyCachePolicy("unlimited");

        // Clean up any test directories
        for (const auto& dir : test_dirs_) {
            std::filesystem::remove_all(dir);
        }

        // Clear the global cache to prevent interference between tests
        // (each test may allocate the same NodeIDs, so stale cache entries
        // from previous tests would cause "Duplicate id" assertions)
        IndexDetails<DataRecord>::clearCache();
    }
    
    std::vector<std::string> dims_;
    std::vector<const char*> dim_ptrs_;
    std::vector<std::string> test_dirs_;
    std::vector<std::string> test_log_files_;
};

// Comprehensive stress test with real xt_insert and search operations
TEST_F(XTreeDurabilityStressTest, HeavyLoadInMemoryMode) {
    std::cout << "\n=== Heavy Load XTree Test (IN_MEMORY) ===\n" << std::flush;
    
    // Create index in IN_MEMORY mode
    std::cout << "Creating index...\n" << std::flush;
    IndexDetails<DataRecord> index(
        2,  // dimensions
        32, // precision
        &dim_ptrs_,
        nullptr,  // JNIEnv
        nullptr,  // jobject
        "heavy_load_memory",
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
    );
    
    std::cout << "Index created, persistence mode: " << (int)index.getPersistenceMode() << "\n" << std::flush;
    
    // Create root bucket using proper API
    std::cout << "Creating root bucket...\n" << std::flush;
    ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());
    std::cout << "Root initialized\n" << std::flush;
    
    const int NUM_RECORDS = 1000;  // Back to larger dataset
    std::mt19937 gen(42);  // Reproducible random
    std::uniform_real_distribution<> coord_dist(-1000.0, 1000.0);
    
    std::cout << "Inserting " << NUM_RECORDS << " randomly distributed points...\n" << std::flush;
    auto start = std::chrono::high_resolution_clock::now();
    
    // Insert records
    int actualInserted = 0;
    for (int i = 0; i < NUM_RECORDS; ++i) {
        double x = coord_dist(gen);
        double y = coord_dist(gen);
        
        DataRecord* dr = new DataRecord(2, 32, "pt_" + std::to_string(i));
        std::vector<double> point = {x, y};
        dr->putPoint(&point);
        dr->putPoint(&point);  // Add twice for point MBR
        
        // Insert using real API - always get fresh root
        index.root_bucket<DataRecord>()->xt_insert(index.root_cache_node(), dr);
        actualInserted++;
        
        if (i % 100 == 99) {
            std::cout << "  Inserted " << (i + 1) << " records...\n" << std::flush;
        }
    }
    
    auto insertEnd = std::chrono::high_resolution_clock::now();
    auto insertDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        insertEnd - start);
    
    // Get fresh root reference for final stats
    auto* finalRoot = index.root_bucket<DataRecord>();
    std::cout << "\nInsertion complete:";
    std::cout << "\n  Inserted: " << actualInserted << " records";
    std::cout << "\n  Total time: " << insertDuration.count() << " ms";
    if (insertDuration.count() > 0) {
        std::cout << "\n  Throughput: " << (actualInserted * 1000.0 / insertDuration.count())
                  << " records/sec";
    }
    std::cout << "\n  Final root has " << finalRoot->n() << " children";
    std::cout << "\n  Tree depth: " << (finalRoot->n() > 0 ? "at least 2 levels" : "single level");
    
    // Perform various searches
    std::cout << "\nPerforming range searches...\n";
    
    // Search 1: Point query (tiny area)
    DataRecord* pointQuery = new DataRecord(2, 32, "point_query");
    std::vector<double> pt1 = {100.0, 100.0};
    std::vector<double> pt2 = {100.1, 100.1};
    pointQuery->putPoint(&pt1);
    pointQuery->putPoint(&pt2);
    
    // Get root references with safety checks
    auto* bucket = index.root_bucket<DataRecord>();
    auto* cacheNode = index.root_cache_node();
    ASSERT_NE(bucket, nullptr);
    ASSERT_NE(cacheNode, nullptr);

    auto searchStart = std::chrono::high_resolution_clock::now();
    auto iter = bucket->getIterator(cacheNode, pointQuery, INTERSECTS);
    int pointCount = 0;
    while (iter->hasNext()) {
        iter->next();
        pointCount++;
    }
    delete iter;
    auto searchEnd = std::chrono::high_resolution_clock::now();
    auto searchDuration = std::chrono::duration_cast<std::chrono::microseconds>(
        searchEnd - searchStart);
    
    std::cout << "  Point query: " << pointCount << " results in " 
              << searchDuration.count() << " μs\n";
    delete pointQuery;
    
    // Search 2: Medium range query
    DataRecord* mediumQuery = new DataRecord(2, 32, "medium_query");
    pt1 = {-100.0, -100.0};
    pt2 = {100.0, 100.0};
    mediumQuery->putPoint(&pt1);
    mediumQuery->putPoint(&pt2);
    
    searchStart = std::chrono::high_resolution_clock::now();
    iter = bucket->getIterator(cacheNode, mediumQuery, INTERSECTS);
    int mediumCount = 0;
    while (iter->hasNext()) {
        iter->next();
        mediumCount++;
    }
    delete iter;
    searchEnd = std::chrono::high_resolution_clock::now();
    searchDuration = std::chrono::duration_cast<std::chrono::microseconds>(
        searchEnd - searchStart);
    
    std::cout << "  Medium range: " << mediumCount << " results in " 
              << searchDuration.count() << " μs\n";
    delete mediumQuery;
    
    // Search 3: Large range query
    DataRecord* largeQuery = new DataRecord(2, 32, "large_query");
    pt1 = {-500.0, -500.0};
    pt2 = {500.0, 500.0};
    largeQuery->putPoint(&pt1);
    largeQuery->putPoint(&pt2);
    
    searchStart = std::chrono::high_resolution_clock::now();
    iter = bucket->getIterator(cacheNode, largeQuery, INTERSECTS);
    int largeCount = 0;
    while (iter->hasNext()) {
        iter->next();
        largeCount++;
    }
    delete iter;
    searchEnd = std::chrono::high_resolution_clock::now();
    searchDuration = std::chrono::duration_cast<std::chrono::microseconds>(
        searchEnd - searchStart);
    
    std::cout << "  Large range: " << largeCount << " results in " 
              << searchDuration.count() << " μs\n";
    delete largeQuery;
    
    // Verify search results make sense
    EXPECT_GE(pointCount, 0);
    EXPECT_LE(pointCount, mediumCount);
    EXPECT_LE(mediumCount, largeCount);
    EXPECT_LE(largeCount, NUM_RECORDS);
    
    // With random distribution from -1000 to 1000:
    // Medium range [-100,100] covers ~10% of area, expect >1% of points
    // Large range [-500,500] covers ~25% of area, expect >5% of points  
    EXPECT_GT(mediumCount, actualInserted / 200);  // At least 0.5%
    EXPECT_GT(largeCount, actualInserted / 20);     // At least 5%
    
    std::cout << "\nIN_MEMORY stress test completed successfully!\n";
}

// Stress test with DURABLE mode including recovery
TEST_F(XTreeDurabilityStressTest, HeavyLoadDurableMode) {
    // Force logfile to null to ensure logs go to stderr
    Logger::setLogFile(nullptr);
    
    // Initialize logging from environment
    initLoggingFromEnv();
    
    std::cout << "\n=== Heavy Load XTree Test (DURABLE) ===\n";
    std::cout << "Log level: " << logLevel.load() << " (0=TRACE, 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=SEVERE)\n";
    
    // Test logging at different levels
    std::cerr << "Testing logging output...\n";
    error() << "TEST: This is an ERROR message";
    warn() << "TEST: This is a WARNING message";
    info() << "TEST: This is an INFO message";
    debug() << "TEST: This is a DEBUG message";

    const std::string field = "heavy_load_durable";
    std::string test_dir = "/tmp/xtree_durable_stress_" + std::to_string(std::time(nullptr));
    test_dirs_.push_back(test_dir);
    test_log_files_.push_back(test_dir + "/test.log");

    // Track records before and after recovery to identify missing ones
    std::set<std::string> preCloseRecords;
    int preCloseCount = 0;

    // Define query range for testing
    const double QUERY_MIN_X = -100.0;
    const double QUERY_MIN_Y = -100.0;
    const double QUERY_MAX_X = 300.0;
    const double QUERY_MAX_Y = 300.0;

    // Track which records SHOULD be found in the query range
    std::set<std::string> expectedInRange;

    // -------- Phase 1: create, insert, commit --------
    {
        IndexDetails<DataRecord> index(
            /*dims*/2,
            /*precision*/32,
            &dim_ptrs_,
            /*JNIEnv*/nullptr,
            /*jobject*/nullptr,
            field,
            IndexDetails<DataRecord>::PersistenceMode::DURABLE,
            test_dir
        );

        ASSERT_TRUE(index.hasDurableStore());
        auto* store = index.getStore();
        ASSERT_NE(store, nullptr);

        // Bootstrap root (no manual cache/node-ID plumbing)
        ASSERT_TRUE(index.ensure_root_initialized<DataRecord>());

        // Verify root is initialized
        ASSERT_NE(index.root_cache_node(), nullptr);
        ASSERT_NE(index.root_bucket<DataRecord>(), nullptr);
        ASSERT_TRUE(index.root_bucket<DataRecord>()->hasNodeID());

        // CRITICAL: Must commit the root so it's properly visible in the ObjectTable
        std::cout << "Root NodeID: " << index.root_bucket<DataRecord>()->getNodeID().raw() << std::endl;
        store->commit(0);  // Commit the root creation

        // CRITICAL: After external commit, invalidate cache to force reload from durable state
        index.invalidate_root_cache();  // Force next access to rebuild from persistent bytes

        // Re-fetch root after invalidation - this exercises the lazy rebuild
        auto* rootAfterCommit = index.root_bucket<DataRecord>();

        // Check if root is in ObjectTable
        persist::NodeKind kind;
        bool found = store->get_node_kind(rootAfterCommit->getNodeID(), kind);
        std::cout << "Root in OT: " << found
                  << " (kind=" << static_cast<int>(kind) << ")" << std::endl;

        // Verify root state after invalidation
        ASSERT_NE(rootAfterCommit, nullptr);
        ASSERT_EQ(rootAfterCommit->n(), 0) << "Root should have no children immediately after creation/commit";
        // Note: isLeaf() is protected, but n()==0 implies it's a leaf for a freshly created root

        std::cout << "Root NodeID after invalidation: " << rootAfterCommit->getNodeID().raw() << std::endl;
        std::cout << "Root children count: " << rootAfterCommit->n() << std::endl;
        std::cout << "Root should be leaf (n==0): " << (rootAfterCommit->n() == 0) << std::endl;

        const int NUM_RECORDS = 10000000;  // 10M records - stress test
        const int COMMIT_INTERVAL = 100000; // Commit every 100K records

        // Apply 500MB cache memory budget using the policy system
        const size_t CACHE_MEMORY_BUDGET = 500 * 1024 * 1024;  // 500 MB target
        IndexDetails<DataRecord>::applyCachePolicy(
            std::make_shared<FixedMemoryCachePolicy>(CACHE_MEMORY_BUDGET));
        std::cout << "Cache memory budget: " << (CACHE_MEMORY_BUDGET / (1024.0 * 1024)) << " MB\n";

        std::cout << "Inserting " << NUM_RECORDS << " clustered points...\n" << std::flush;
        std::cout << "Query range: [" << QUERY_MIN_X << "," << QUERY_MIN_Y
                  << "] to [" << QUERY_MAX_X << "," << QUERY_MAX_Y << "]\n" << std::flush;

        // Also try logging with different levels to see what works
        error() << "ERROR: Starting insertion of " << NUM_RECORDS << " records";
        warn() << "WARN: Starting insertion of " << NUM_RECORDS << " records";
        info() << "INFO: Starting insertion of " << NUM_RECORDS << " records";
        std::mt19937 gen(42);
        std::normal_distribution<> cluster_dist(0, 20);

        // Track some sample points to debug
        int debugSampleCount = 0;
        const int MAX_DEBUG_SAMPLES = 10;

        auto t0 = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < NUM_RECORDS; ++i) {
            // clustered points
            int cluster_id = i / 200;
            double cx = cluster_id * 200.0, cy = cluster_id * 200.0;
            double x = cx + cluster_dist(gen);
            double y = cy + cluster_dist(gen);

            // Track if this point falls within our test query range
            std::string rowId = "dpt_" + std::to_string(i);
            if (x >= QUERY_MIN_X && x <= QUERY_MAX_X &&
                y >= QUERY_MIN_Y && y <= QUERY_MAX_Y) {
                expectedInRange.insert(rowId);

                // Debug: show some sample points that should be found
                if (debugSampleCount < MAX_DEBUG_SAMPLES) {
                    std::cout << "  Expected in range: " << rowId
                              << " at (" << x << ", " << y << ")"
                              << " cluster=" << cluster_id << "\n";
                    debugSampleCount++;
                }
            }

            auto* dr = XAlloc<DataRecord>::allocate_record(&index, 2, 32, rowId);
            std::vector<double> p = {x, y};
            dr->putPoint(&p);
            dr->putPoint(&p);

            // Debug: Check root state before insert (first insert only)
            if (i == 0) {
                auto* dbgRoot = index.root_bucket<DataRecord>();
                auto* dbgCachedRoot = index.root_cache_node();
                std::cout << "  [DEBUG] First insert. Root n=" << dbgRoot->n()
                          << ", NodeID=" << dbgRoot->getNodeID().raw() << std::endl;
                std::cout << "  [DEBUG] Root pointer=" << (void*)dbgRoot
                          << ", cachedRoot pointer=" << (void*)dbgCachedRoot << std::endl;
                std::cout << "  [DEBUG] cachedRoot->object=" << (void*)dbgCachedRoot->object << std::endl;
            }

            // Insert using fresh root pointers every time
            // This ensures we always use the latest root even after splits
            index.root_bucket<DataRecord>()->xt_insert(index.root_cache_node(), dr);

            // Periodic commits with progress indicator
            if ((i + 1) % COMMIT_INTERVAL == 0) {
                // Flush dirty buckets before commit
                index.flush_dirty_buckets();
                store->commit((i + 1) / COMMIT_INTERVAL);

                // Evict cache to stay under memory budget
                size_t evicted = IndexDetails<DataRecord>::evictCacheToMemoryBudget();

                auto elapsed = std::chrono::high_resolution_clock::now() - t0;
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
                double rate = (i + 1) * 1000.0 / std::max<int64_t>(1, ms);
                size_t currentMem = IndexDetails<DataRecord>::getCacheCurrentMemory();
                std::cout << "  Progress: " << (i + 1) << " records"
                          << " (" << std::fixed << std::setprecision(0) << rate << " rec/s)"
                          << " cache=" << StorageMetrics::formatBytes(currentMem)
                          << " evicted=" << evicted << "\n";
            }
        }

        // Final flush and commit (ensures all deltas durable)
        index.flush_dirty_buckets();
        store->commit(NUM_RECORDS / COMMIT_INTERVAL + 1);

        auto t1 = std::chrono::high_resolution_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
        // Get fresh root reference for final stats
        auto* finalRoot = index.root_bucket<DataRecord>();
        std::cout << "\nDurable insertion complete:"
                  << "\n  Total time: " << ms << " ms"
                  << "\n  Throughput: " << (NUM_RECORDS * 1000.0 / std::max<int64_t>(1, ms)) << " rec/s"
                  << "\n  Final root has " << finalRoot->n() << " children"
                  << "\n  Tree depth: " << (finalRoot->n() > 0 ? "at least 2 levels" : "single level")
                  << "\n  Expected records in query range: " << expectedInRange.size() << "\n";
        
        // Collect memory metrics
        size_t memory_after_insert = getMemoryUsage();
        std::cout << "\nMemory footprint: " << StorageMetrics::formatBytes(memory_after_insert) << std::endl;

        // Cache memory stats
        size_t cache_current = IndexDetails<DataRecord>::getCacheCurrentMemory();
        size_t cache_max = IndexDetails<DataRecord>::getCacheMaxMemory();
        auto cache_stats = IndexDetails<DataRecord>::getCache().getStats();
        std::cout << "\n=== Cache Memory Stats ===" << std::endl;
        std::cout << "Cache budget: " << StorageMetrics::formatBytes(cache_max)
                  << (cache_max == 0 ? " (unlimited)" : "") << std::endl;
        std::cout << "Cache used: " << StorageMetrics::formatBytes(cache_current) << std::endl;
        std::cout << "Cache entries: " << cache_stats.totalNodes << std::endl;
        std::cout << "  Pinned: " << cache_stats.totalPinned << std::endl;
        std::cout << "  Evictable: " << cache_stats.totalEvictable << std::endl;
        if (cache_stats.totalNodes > 0) {
            std::cout << "Avg bytes/entry: " << (cache_current / cache_stats.totalNodes) << std::endl;
        }

        // Get segment utilization before storage metrics
        if (store) {
            auto* durable_store = dynamic_cast<persist::DurableStore*>(store);
            if (durable_store) {
                auto seg_util = durable_store->get_segment_utilization();
                std::cout << "\n=== Segment Utilization ===" << std::endl;
                std::cout << "Total segments: " << seg_util.total_segments << std::endl;
                std::cout << "Total capacity: " << std::fixed << std::setprecision(2) 
                         << (seg_util.total_capacity / (1024.0 * 1024.0)) << " MB" << std::endl;
                std::cout << "Total used: " << std::fixed << std::setprecision(2)
                         << (seg_util.total_used / (1024.0 * 1024.0)) << " MB" << std::endl;
                std::cout << "Total wasted: " << std::fixed << std::setprecision(2)
                         << (seg_util.total_wasted / (1024.0 * 1024.0)) << " MB" << std::endl;
                std::cout << "Average utilization: " << std::fixed << std::setprecision(1) 
                         << seg_util.avg_utilization << "%" << std::endl;
                std::cout << "Min utilization: " << std::fixed << std::setprecision(1)
                         << seg_util.min_utilization << "%" << std::endl;
                std::cout << "Max utilization: " << std::fixed << std::setprecision(1)
                         << seg_util.max_utilization << "%" << std::endl;
                std::cout << "Segments < 25% utilized: " << seg_util.segments_under_25_percent << std::endl;
                std::cout << "Segments < 50% utilized: " << seg_util.segments_under_50_percent << std::endl;
                std::cout << "Segments < 75% utilized: " << seg_util.segments_under_75_percent << std::endl;
            }
        }
        
        // Collect storage metrics
        StorageMetrics metrics;
        metrics.analyze(test_dir);
        metrics.print();
        
        // Calculate bytes per record
        if (NUM_RECORDS > 0) {
            double bytes_per_record = double(metrics.xd_file_bytes) / NUM_RECORDS;
            std::cout << "\nPer-record metrics:" << std::endl;
            std::cout << "  Data bytes per record: " << std::fixed << std::setprecision(2) 
                     << bytes_per_record << " bytes" << std::endl;
            std::cout << "  Total bytes per record: " << std::fixed << std::setprecision(2)
                     << (double(metrics.total_disk_bytes) / NUM_RECORDS) << " bytes" << std::endl;
        }

        // Pre-close sanity query - collect record IDs
        auto* q = new DataRecord(2, 32, "pre_close_search");
        std::vector<double> pt1 = {QUERY_MIN_X, QUERY_MIN_Y};
        std::vector<double> pt2 = {QUERY_MAX_X, QUERY_MAX_Y};
        q->putPoint(&pt1);
        q->putPoint(&pt2);

        // Get root references with safety checks
        auto* bucket = index.root_bucket<DataRecord>();
        auto* cacheNode = index.root_cache_node();
        ASSERT_NE(bucket, nullptr);
        ASSERT_NE(cacheNode, nullptr);

        auto* it = bucket->getIterator(cacheNode, q, INTERSECTS);
        int debugCount = 0;
        while (it->hasNext()) {
            auto* record = static_cast<DataRecord*>(it->next());
            if (record) {
                std::string id = record->getRowID();
                preCloseRecords.insert(id);

                // Debug: Show first few records found
                if (debugCount < 5) {
                    std::cout << "  Found record: " << id << "\n";
                    debugCount++;
                }
            }
        }
        delete it;
        delete q;

        preCloseCount = preCloseRecords.size();
        std::cout << "\nPre-close query results:"
                  << "\n  Expected " << expectedInRange.size() << " records in range"
                  << "\n  Actually found: " << preCloseCount << " records\n";

        // Check if we found all expected records
        std::set<std::string> missingRecords;
        std::set_difference(expectedInRange.begin(), expectedInRange.end(),
                           preCloseRecords.begin(), preCloseRecords.end(),
                           std::inserter(missingRecords, missingRecords.begin()));

        if (!missingRecords.empty()) {
            std::cout << "  WARNING: " << missingRecords.size() << " expected records not found!\n";
            if (missingRecords.size() <= 10) {
                std::cout << "  Missing IDs: ";
                for (const auto& id : missingRecords) {
                    std::cout << id << " ";
                }
                std::cout << "\n";
            }
        }

        // Check for unexpected records
        std::set<std::string> unexpectedRecords;
        std::set_difference(preCloseRecords.begin(), preCloseRecords.end(),
                           expectedInRange.begin(), expectedInRange.end(),
                           std::inserter(unexpectedRecords, unexpectedRecords.begin()));

        if (!unexpectedRecords.empty()) {
            std::cout << "  WARNING: " << unexpectedRecords.size() << " unexpected records found!\n";
            if (unexpectedRecords.size() <= 10) {
                std::cout << "  Unexpected IDs: ";
                for (const auto& id : unexpectedRecords) {
                    std::cout << id << " ";
                }
                std::cout << "\n";
            }
        }

        EXPECT_EQ(preCloseCount, expectedInRange.size());
        
        // Clean shutdown of the index
        std::cout << "\nClosing index cleanly...\n";
        index.close();
    }

    // -------- Phase 2: reopen, recover root, query again --------
    std::cout << "\nReopening index to verify persistence...\n";
    {
        // Time the entire recovery process
        auto recoveryStart = std::chrono::high_resolution_clock::now();
        
        IndexDetails<DataRecord> index(
            /*dims*/2,
            /*precision*/32,
            &dim_ptrs_,
            /*JNIEnv*/nullptr,
            /*jobject*/nullptr,
            field,
            IndexDetails<DataRecord>::PersistenceMode::DURABLE,
            test_dir
        );

        ASSERT_TRUE(index.hasDurableStore());
        auto* store = index.getStore();
        ASSERT_NE(store, nullptr);

        // Recover root (no WAL writes during recovery)
        ASSERT_TRUE(index.recover_root<DataRecord>()) << "Failed to recover root from store";
        
        auto recoveryEnd = std::chrono::high_resolution_clock::now();
        auto recoveryDuration = std::chrono::duration_cast<std::chrono::milliseconds>(recoveryEnd - recoveryStart);

        // Verify root was recovered and get fresh reference
        ASSERT_NE(index.root_cache_node(), nullptr);
        auto* rootAfterRecovery = index.root_bucket<DataRecord>();
        ASSERT_NE(rootAfterRecovery, nullptr);

        std::cout << "  Recovery complete:"
                  << "\n    Time: " << recoveryDuration.count() << " ms"
                  << "\n    Root has " << rootAfterRecovery->n() << " children\n";
        
        // Collect memory after recovery
        size_t memory_after_recovery = getMemoryUsage();
        std::cout << "\nPost-recovery memory: " << StorageMetrics::formatBytes(memory_after_recovery) << std::endl;

        // Post-reopen query over same window
        auto* q = new DataRecord(2, 32, "post_reopen_search");
        std::vector<double> pt1 = {QUERY_MIN_X, QUERY_MIN_Y};
        std::vector<double> pt2 = {QUERY_MAX_X, QUERY_MAX_Y};
        q->putPoint(&pt1);
        q->putPoint(&pt2);

        auto* it = rootAfterRecovery->getIterator(index.root_cache_node(), q, INTERSECTS);
        EXPECT_NE(it, nullptr);
        if (it) {
            std::set<std::string> postRecoveryRecords;
            while (it->hasNext()) {
                auto* record = static_cast<DataRecord*>(it->next());
                if (record) {
                    postRecoveryRecords.insert(record->getRowID());
                }
            }
            delete it;
            delete q;

            int postOpenCount = postRecoveryRecords.size();
            std::cout << "\nPost-recovery query results:"
                      << "\n  Expected " << expectedInRange.size() << " records in range"
                      << "\n  Actually found: " << postOpenCount << " records\n";

            // Check against expected records (ground truth)
            std::set<std::string> missingFromExpected;
            std::set_difference(expectedInRange.begin(), expectedInRange.end(),
                               postRecoveryRecords.begin(), postRecoveryRecords.end(),
                               std::inserter(missingFromExpected, missingFromExpected.begin()));

            if (!missingFromExpected.empty()) {
                std::cout << "  ERROR: " << missingFromExpected.size() << " expected records not found after recovery!\n";
                if (missingFromExpected.size() <= 10) {
                    std::cout << "  Missing IDs: ";
                    for (const auto& id : missingFromExpected) {
                        std::cout << id << " ";
                    }
                    std::cout << "\n";
                }
            }

            // Check for unexpected records after recovery
            std::set<std::string> unexpectedAfterRecovery;
            std::set_difference(postRecoveryRecords.begin(), postRecoveryRecords.end(),
                               expectedInRange.begin(), expectedInRange.end(),
                               std::inserter(unexpectedAfterRecovery, unexpectedAfterRecovery.begin()));

            if (!unexpectedAfterRecovery.empty()) {
                std::cout << "  ERROR: " << unexpectedAfterRecovery.size() << " unexpected records found after recovery!\n";
                if (unexpectedAfterRecovery.size() <= 10) {
                    std::cout << "  Unexpected IDs: ";
                    for (const auto& id : unexpectedAfterRecovery) {
                        std::cout << id << " ";
                    }
                    std::cout << "\n";
                }
            }

            // Compare pre and post recovery records
            if (postOpenCount != preCloseCount) {
                std::cout << "\n  WARNING: Pre/post recovery mismatch!\n";
                std::cout << "    Pre-close: " << preCloseCount << " records\n";
                std::cout << "    Post-recovery: " << postOpenCount << " records\n";
                std::cout << "    Difference: " << (preCloseCount - postOpenCount) << " records\n";
                
                // Find which specific records are missing
                std::set<std::string> missingRecords;
                std::set_difference(preCloseRecords.begin(), preCloseRecords.end(),
                                   postRecoveryRecords.begin(), postRecoveryRecords.end(),
                                   std::inserter(missingRecords, missingRecords.begin()));
                
                if (!missingRecords.empty()) {
                    std::cout << "\n  Missing record IDs:\n";
                    int count = 0;
                    for (const auto& id : missingRecords) {
                        std::cout << "    - " << id << "\n";
                        if (++count >= 50) {  // Limit output to first 50
                            if (missingRecords.size() > 50) {
                                std::cout << "    ... and " << (missingRecords.size() - 50) << " more\n";
                            }
                            break;
                        }
                    }
                    
                    // Analyze pattern in missing records
                    if (!missingRecords.empty()) {
                        // Extract numeric IDs to look for patterns
                        std::vector<int> missingNums;
                        for (const auto& id : missingRecords) {
                            if (id.substr(0, 4) == "dpt_") {
                                try {
                                    int num = std::stoi(id.substr(4));
                                    missingNums.push_back(num);
                                } catch (...) {}
                            }
                        }
                        
                        if (!missingNums.empty()) {
                            std::sort(missingNums.begin(), missingNums.end());
                            std::cout << "\n  Pattern analysis of missing records:\n";
                            std::cout << "    First missing: dpt_" << missingNums.front() << "\n";
                            std::cout << "    Last missing: dpt_" << missingNums.back() << "\n";
                            std::cout << "    Range: " << (missingNums.back() - missingNums.front()) << "\n";
                            
                            // Check if they're consecutive
                            bool consecutive = true;
                            for (size_t i = 1; i < missingNums.size(); ++i) {
                                if (missingNums[i] != missingNums[i-1] + 1) {
                                    consecutive = false;
                                    break;
                                }
                            }
                            std::cout << "    Consecutive: " << (consecutive ? "Yes" : "No") << "\n";
                            
                            // Check clustering (which cluster they belong to)
                            std::cout << "    Cluster IDs (approx): ";
                            std::set<int> clusters;
                            for (int num : missingNums) {
                                clusters.insert(num / 200);  // Based on clustering logic in insertion
                            }
                            for (int c : clusters) {
                                std::cout << c << " ";
                            }
                            std::cout << "\n";
                        }
                    }
                }
            }
            
            // Final validation against ground truth
            EXPECT_EQ(postOpenCount, expectedInRange.size())
                << "Post-recovery count doesn't match expected count!";
            EXPECT_EQ(postOpenCount, preCloseCount)
                << "Records lost during recovery!";
            EXPECT_GT(postOpenCount, 0)
                << "No records found after recovery!";
        }
        
        // Clean shutdown of the reopened index
        std::cout << "\nClosing reopened index cleanly...\n";
        index.close();
    }

    std::cout << "\nDURABLE stress test completed successfully!\n";
}

} // namespace xtree