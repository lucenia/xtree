/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Stress test for cache memory policies - verifies eviction works correctly
 * under memory pressure.
 */

#include <gtest/gtest.h>
#include "../src/xtree.h"
#include "../src/indexdetails.hpp"
#include "../src/datarecord.hpp"
#include "../src/cache_policy.hpp"
#include "../src/xtree_allocator_traits.hpp"

#include <random>
#include <iostream>
#include <filesystem>
#include <chrono>
#include <iomanip>

using namespace xtree;
namespace fs = std::filesystem;

class CachePolicyStressTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use unique directory per test to avoid cross-test interference
        test_dir_ = "./test_policy_stress_data_" + std::to_string(std::time(nullptr)) +
                    "_" + std::to_string(reinterpret_cast<uintptr_t>(this));
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
        fs::create_directories(test_dir_);

        // Clear the cache BEFORE each test to ensure no stale references
        IndexDetails<DataRecord>::clearCache();
    }

    void TearDown() override {
        // Reset to unlimited policy
        IndexDetails<DataRecord>::applyCachePolicy("unlimited");

        // Clear cache AFTER each test
        IndexDetails<DataRecord>::clearCache();

        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    std::string test_dir_;
};

TEST_F(CachePolicyStressTest, FixedMemoryBudget512MB) {
    // Apply a 512MB memory budget
    ASSERT_TRUE(IndexDetails<DataRecord>::applyCachePolicy("512MB"));

    auto policy = IndexDetails<DataRecord>::getCachePolicy();
    ASSERT_NE(policy, nullptr);
    EXPECT_EQ(policy->getMaxMemory(), 512ULL * 1024 * 1024);
    std::cout << "Applied policy: " << policy->name()
              << " with budget: " << (policy->getMaxMemory() / (1024.0 * 1024)) << " MB" << std::endl;

    // Create index
    auto* idx = new IndexDetails<DataRecord>(
        2, 6, nullptr, nullptr, nullptr,
        "test_field",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );

    idx->template ensure_root_initialized<DataRecord>();

    // Insert 100K records to test eviction
    const int NUM_RECORDS = 100000;
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_RECORDS; ++i) {
        double x = dist(rng);
        double y = dist(rng);

        std::string rowId = "rec_" + std::to_string(i);
        auto* dr = XAlloc<DataRecord>::allocate_record(idx, 2, 6, rowId);
        std::vector<double> p = {x, y};
        dr->putPoint(&p);
        dr->putPoint(&p);

        auto* root = idx->root_bucket<DataRecord>();
        auto* cachedRoot = idx->root_cache_node();
        root->xt_insert(cachedRoot, dr);

        // Periodically evict to stay under budget
        if (i % 10000 == 0 && i > 0) {
            size_t evicted = IndexDetails<DataRecord>::evictCacheToMemoryBudget();
            size_t currentMem = IndexDetails<DataRecord>::getCacheCurrentMemory();
            std::cout << "  After " << i << " inserts: "
                      << "evicted=" << evicted << ", "
                      << "memory=" << (currentMem / (1024.0 * 1024)) << " MB" << std::endl;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Final eviction
    size_t finalEvicted = IndexDetails<DataRecord>::evictCacheToMemoryBudget();
    size_t finalMem = IndexDetails<DataRecord>::getCacheCurrentMemory();
    size_t budget = IndexDetails<DataRecord>::getCacheMaxMemory();

    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "Inserted: " << NUM_RECORDS << " records" << std::endl;
    std::cout << "Time: " << duration.count() << " ms" << std::endl;
    std::cout << "Throughput: " << (NUM_RECORDS * 1000.0 / duration.count()) << " rec/s" << std::endl;
    std::cout << "Final memory: " << (finalMem / (1024.0 * 1024)) << " MB" << std::endl;
    std::cout << "Budget: " << (budget / (1024.0 * 1024)) << " MB" << std::endl;
    std::cout << "Final evicted: " << finalEvicted << std::endl;

    // Verify memory is under budget
    EXPECT_LE(finalMem, budget) << "Memory should be under budget after eviction";

    delete idx;
}

TEST_F(CachePolicyStressTest, WorkloadPolicyBulkIngestion) {
    // Apply bulk ingestion policy (aggressive eviction)
    ASSERT_TRUE(IndexDetails<DataRecord>::applyCachePolicy("bulk"));

    auto policy = IndexDetails<DataRecord>::getCachePolicy();
    ASSERT_NE(policy, nullptr);
    std::cout << "Applied policy: " << policy->name()
              << " with budget: " << (policy->getMaxMemory() / (1024.0 * 1024)) << " MB" << std::endl;

    // Create index
    auto* idx = new IndexDetails<DataRecord>(
        2, 6, nullptr, nullptr, nullptr,
        "test_field",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );

    idx->template ensure_root_initialized<DataRecord>();

    // Insert 50K records
    const int NUM_RECORDS = 50000;
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

    for (int i = 0; i < NUM_RECORDS; ++i) {
        double x = dist(rng);
        double y = dist(rng);

        std::string rowId = "rec_" + std::to_string(i);
        auto* dr = XAlloc<DataRecord>::allocate_record(idx, 2, 6, rowId);
        std::vector<double> p = {x, y};
        dr->putPoint(&p);
        dr->putPoint(&p);

        auto* root = idx->root_bucket<DataRecord>();
        auto* cachedRoot = idx->root_cache_node();
        root->xt_insert(cachedRoot, dr);
    }

    // Evict to budget
    IndexDetails<DataRecord>::evictCacheToMemoryBudget();

    size_t finalMem = IndexDetails<DataRecord>::getCacheCurrentMemory();
    size_t budget = IndexDetails<DataRecord>::getCacheMaxMemory();

    std::cout << "Final memory: " << (finalMem / (1024.0 * 1024)) << " MB" << std::endl;
    std::cout << "Budget: " << (budget / (1024.0 * 1024)) << " MB" << std::endl;

    EXPECT_LE(finalMem, budget);

    delete idx;
}

TEST_F(CachePolicyStressTest, PercentagePolicy) {
    // Apply 5% of RAM policy
    ASSERT_TRUE(IndexDetails<DataRecord>::applyCachePolicy("5%"));

    auto policy = IndexDetails<DataRecord>::getCachePolicy();
    ASSERT_NE(policy, nullptr);

    size_t expectedBudget = xtree::detail::getTotalSystemMemory() * 5 / 100;
    EXPECT_EQ(policy->getMaxMemory(), expectedBudget);

    std::cout << "Applied 5% policy: " << (policy->getMaxMemory() / (1024.0 * 1024)) << " MB" << std::endl;
}

TEST_F(CachePolicyStressTest, EnvironmentVariablePolicy) {
    // Test that initCachePolicyFromEnv works
    // Note: This will use whatever XTREE_CACHE_POLICY is set to, or default to unlimited
    IndexDetails<DataRecord>::initCachePolicyFromEnv();

    auto policy = IndexDetails<DataRecord>::getCachePolicy();
    ASSERT_NE(policy, nullptr);

    std::cout << "Env policy: " << policy->name()
              << " with budget: " << (policy->getMaxMemory() / (1024.0 * 1024)) << " MB" << std::endl;
}

// Test with a tiny memory budget (1MB) to actually trigger eviction
TEST_F(CachePolicyStressTest, TinyBudgetForcesEviction) {
    // Apply 100KB memory budget to force eviction
    ASSERT_TRUE(IndexDetails<DataRecord>::applyCachePolicy("100KB"));

    auto policy = IndexDetails<DataRecord>::getCachePolicy();
    ASSERT_NE(policy, nullptr);
    std::cout << "Applied policy: " << policy->name()
              << " with budget: " << (policy->getMaxMemory() / (1024.0 * 1024)) << " MB" << std::endl;

    // Create index with precision 32 like HeavyLoadDurableMode
    auto* idx = new IndexDetails<DataRecord>(
        2, 32, nullptr, nullptr, nullptr,  // precision 32 like HeavyLoadDurableMode
        "test_field",
        IndexDetails<DataRecord>::PersistenceMode::DURABLE,
        test_dir_
    );

    idx->template ensure_root_initialized<DataRecord>();

    // Commit root like HeavyLoadDurableMode does
    auto* store = idx->getStore();
    ASSERT_NE(store, nullptr);
    store->commit(0);
    idx->invalidate_root_cache();

    // Insert 20K records - with 100KB budget this should trigger eviction
    const int NUM_RECORDS = 20000;
    std::mt19937 rng(42);
    std::normal_distribution<> cluster_dist(0, 20);

    std::cout << "Inserting " << NUM_RECORDS << " clustered points...\n";

    bool justEvicted = false;
    for (int i = 0; i < NUM_RECORDS; ++i) {
        if (justEvicted) {
            std::cout << "  [DEBUG] Starting insert " << i << " after eviction..." << std::endl;
        }

        // Clustered points like HeavyLoadDurableMode
        int cluster_id = i / 200;
        double cx = cluster_id * 200.0, cy = cluster_id * 200.0;
        double x = cx + cluster_dist(rng);
        double y = cy + cluster_dist(rng);

        std::string rowId = "rec_" + std::to_string(i);

        if (justEvicted) std::cout << "  [DEBUG] Allocating record..." << std::endl;
        auto* dr = XAlloc<DataRecord>::allocate_record(idx, 2, 32, rowId);

        if (justEvicted) std::cout << "  [DEBUG] Setting points..." << std::endl;
        std::vector<double> p = {x, y};
        dr->putPoint(&p);
        dr->putPoint(&p);

        if (justEvicted) std::cout << "  [DEBUG] Getting root bucket..." << std::endl;
        auto* root = idx->root_bucket<DataRecord>();

        if (justEvicted) std::cout << "  [DEBUG] Getting root cache node..." << std::endl;
        auto* cachedRoot = idx->root_cache_node();

        if (justEvicted) std::cout << "  [DEBUG] Calling xt_insert..." << std::endl;
        root->xt_insert(cachedRoot, dr);

        if (justEvicted) {
            std::cout << "  [DEBUG] Insert " << i << " completed successfully" << std::endl;
            justEvicted = false;
        }

        // Periodically evict to stay under tiny budget
        if ((i + 1) % 500 == 0) {
            std::cout << "  [DEBUG] Flushing and committing before eviction at " << (i + 1) << std::endl;
            // CRITICAL: Must persist buckets before eviction, otherwise they can't be reloaded
            idx->flush_dirty_buckets();
            store->commit((i + 1) / 500);

            std::cout << "  [DEBUG] Evicting..." << std::endl;
            size_t evicted = IndexDetails<DataRecord>::evictCacheToMemoryBudget();
            size_t currentMem = IndexDetails<DataRecord>::getCacheCurrentMemory();
            std::cout << "  After " << (i + 1) << " inserts: "
                      << "evicted=" << evicted << ", "
                      << "memory=" << (currentMem / (1024.0 * 1024)) << " MB" << std::endl;
            std::cout << "  [DEBUG] After eviction, continuing..." << std::endl;
            justEvicted = true;
        }
    }

    std::cout << "All inserts completed successfully!\n";

    size_t finalMem = IndexDetails<DataRecord>::getCacheCurrentMemory();
    size_t budget = IndexDetails<DataRecord>::getCacheMaxMemory();
    std::cout << "Final memory: " << (finalMem / (1024.0 * 1024)) << " MB" << std::endl;
    std::cout << "Budget: " << (budget / (1024.0 * 1024)) << " MB" << std::endl;

    delete idx;
}
