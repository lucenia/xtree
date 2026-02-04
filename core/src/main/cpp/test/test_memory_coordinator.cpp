/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Unit tests for MemoryCoordinator.
 */

#include <gtest/gtest.h>
#include "../src/persistence/memory_coordinator.h"
#include "../src/persistence/mapping_manager.h"
#include "../src/indexdetails.hpp"

#include <thread>
#include <chrono>

using namespace xtree;
using namespace xtree::persist;

class MemoryCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset coordinator to default state before each test
        MemoryCoordinator::global().reset();
    }

    void TearDown() override {
        // Reset after each test
        MemoryCoordinator::global().reset();
    }
};

// ============================================================================
// Initialization Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, SingletonInitialization) {
    // Verify singleton returns the same instance
    auto& coord1 = MemoryCoordinator::global();
    auto& coord2 = MemoryCoordinator::global();
    EXPECT_EQ(&coord1, &coord2);
}

TEST_F(MemoryCoordinatorTest, DefaultValues) {
    auto& coord = MemoryCoordinator::global();

    // Default budget is 0 (unlimited)
    EXPECT_EQ(coord.get_total_budget(), 0u);

    // Default ratios
    EXPECT_FLOAT_EQ(coord.get_cache_ratio(), 0.40f);
    EXPECT_FLOAT_EQ(coord.get_mmap_ratio(), 0.60f);

    // Default workload hint
    EXPECT_EQ(coord.get_workload_hint(), WorkloadHint::Auto);
}

// ============================================================================
// Budget Configuration Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, SetTotalBudget) {
    auto& coord = MemoryCoordinator::global();

    // Set 1GB budget
    const size_t budget = 1ULL * 1024 * 1024 * 1024;
    coord.set_total_budget(budget);

    EXPECT_EQ(coord.get_total_budget(), budget);
}

TEST_F(MemoryCoordinatorTest, BudgetSplitCorrectness) {
    auto& coord = MemoryCoordinator::global();

    // Set budget and initial ratios
    const size_t budget = 100 * 1024 * 1024;  // 100MB for easy math
    coord.set_initial_ratios(0.40f, 0.60f);
    coord.set_total_budget(budget);

    // Verify the ratios are applied
    EXPECT_FLOAT_EQ(coord.get_cache_ratio(), 0.40f);
    EXPECT_FLOAT_EQ(coord.get_mmap_ratio(), 0.60f);

    // Verify the sum equals 1.0 (within floating point tolerance)
    EXPECT_NEAR(coord.get_cache_ratio() + coord.get_mmap_ratio(), 1.0f, 0.001f);
}

TEST_F(MemoryCoordinatorTest, RatioNormalization) {
    auto& coord = MemoryCoordinator::global();

    // Set ratios that don't sum to 1.0 - should be normalized
    coord.set_initial_ratios(0.3f, 0.3f);  // Sum = 0.6

    // Should be normalized to sum to 1.0
    EXPECT_NEAR(coord.get_cache_ratio() + coord.get_mmap_ratio(), 1.0f, 0.001f);
}

TEST_F(MemoryCoordinatorTest, RatioClamping) {
    auto& coord = MemoryCoordinator::global();

    // Try to set extreme ratios - should be clamped
    coord.set_initial_ratios(0.95f, 0.05f);  // Exceeds MAX_RATIO

    // Should be clamped to MAX_RATIO (0.80)
    // Use tolerance for floating point comparisons
    EXPECT_LE(coord.get_cache_ratio(), 0.81f);  // Allow small floating point error
    EXPECT_GE(coord.get_cache_ratio(), 0.19f);  // Allow small floating point error
    EXPECT_GE(coord.get_mmap_ratio(), 0.19f);   // Allow small floating point error
}

// ============================================================================
// Workload Hint Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, WorkloadHintBulkIngestion) {
    auto& coord = MemoryCoordinator::global();

    coord.set_workload_hint(WorkloadHint::BulkIngestion);

    EXPECT_EQ(coord.get_workload_hint(), WorkloadHint::BulkIngestion);
    // Bulk ingestion should favor mmap
    EXPECT_GT(coord.get_mmap_ratio(), coord.get_cache_ratio());
}

TEST_F(MemoryCoordinatorTest, WorkloadHintQueryHeavy) {
    auto& coord = MemoryCoordinator::global();

    coord.set_workload_hint(WorkloadHint::QueryHeavy);

    EXPECT_EQ(coord.get_workload_hint(), WorkloadHint::QueryHeavy);
    // Query heavy should favor cache
    EXPECT_GT(coord.get_cache_ratio(), coord.get_mmap_ratio());
}

TEST_F(MemoryCoordinatorTest, WorkloadHintMixed) {
    auto& coord = MemoryCoordinator::global();

    coord.set_workload_hint(WorkloadHint::Mixed);

    EXPECT_EQ(coord.get_workload_hint(), WorkloadHint::Mixed);
    // Mixed should be balanced (50/50)
    EXPECT_FLOAT_EQ(coord.get_cache_ratio(), 0.50f);
    EXPECT_FLOAT_EQ(coord.get_mmap_ratio(), 0.50f);
}

TEST_F(MemoryCoordinatorTest, WorkloadHintMemoryConstrained) {
    auto& coord = MemoryCoordinator::global();

    coord.set_workload_hint(WorkloadHint::MemoryConstrained);

    EXPECT_EQ(coord.get_workload_hint(), WorkloadHint::MemoryConstrained);
    // Memory constrained should favor mmap (can evict to disk)
    EXPECT_GT(coord.get_mmap_ratio(), coord.get_cache_ratio());
}

// ============================================================================
// Tick and Rebalancing Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, TickWithoutBudget) {
    auto& coord = MemoryCoordinator::global();

    // Without a budget, tick should be a no-op
    EXPECT_EQ(coord.get_total_budget(), 0u);
    coord.tick();  // Should not crash

    // Rebalance count should be 0
    EXPECT_EQ(coord.get_rebalance_count(), 0u);
}

TEST_F(MemoryCoordinatorTest, TickWithBudget) {
    auto& coord = MemoryCoordinator::global();

    // Set a budget
    coord.set_total_budget(1ULL * 1024 * 1024 * 1024);  // 1GB
    coord.set_rebalance_interval(std::chrono::seconds{0});  // No delay

    // Tick should work
    coord.tick();

    // Metrics should be collected
    auto metrics = coord.get_metrics();
    // Just verify we can get metrics without crashing
    EXPECT_GE(metrics.cache_utilization, 0.0);
    EXPECT_GE(metrics.mmap_utilization, 0.0);
}

TEST_F(MemoryCoordinatorTest, ForceRebalance) {
    auto& coord = MemoryCoordinator::global();

    // Set a budget
    coord.set_total_budget(1ULL * 1024 * 1024 * 1024);  // 1GB

    // Force rebalance should work
    coord.force_rebalance();

    // Should not crash and metrics should be available
    auto metrics = coord.get_metrics();
    EXPECT_GE(metrics.cache_utilization, 0.0);
}

TEST_F(MemoryCoordinatorTest, RebalanceIntervalThrottling) {
    auto& coord = MemoryCoordinator::global();

    // Set a budget and 5-second rebalance interval
    coord.set_total_budget(1ULL * 1024 * 1024 * 1024);
    coord.set_rebalance_interval(std::chrono::seconds{5});

    size_t initial_count = coord.get_rebalance_count();

    // Multiple rapid ticks should not cause multiple rebalances
    coord.tick();
    coord.tick();
    coord.tick();

    // Rebalance count should not increase significantly
    // (might increase by 1 at most if interval just elapsed)
    EXPECT_LE(coord.get_rebalance_count(), initial_count + 1);
}

// ============================================================================
// Metrics Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, MetricsStructure) {
    auto& coord = MemoryCoordinator::global();

    coord.set_total_budget(1ULL * 1024 * 1024 * 1024);
    coord.tick();

    auto metrics = coord.get_metrics();

    // Verify all metrics are non-negative
    EXPECT_GE(metrics.cache_memory_used, 0u);
    EXPECT_GE(metrics.mmap_memory_used, 0u);
    EXPECT_GE(metrics.cache_entries, 0u);
    EXPECT_GE(metrics.mmap_extents, 0u);

    // Utilization should be 0-1 range
    EXPECT_GE(metrics.cache_utilization, 0.0);
    EXPECT_LE(metrics.cache_utilization, 1.0);
    EXPECT_GE(metrics.mmap_utilization, 0.0);
    EXPECT_LE(metrics.mmap_utilization, 1.0);
}

// ============================================================================
// Reset Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, Reset) {
    auto& coord = MemoryCoordinator::global();

    // Configure various settings
    coord.set_total_budget(1ULL * 1024 * 1024 * 1024);
    coord.set_initial_ratios(0.7f, 0.3f);
    coord.set_workload_hint(WorkloadHint::QueryHeavy);

    // Reset
    coord.reset();

    // Verify defaults are restored
    EXPECT_EQ(coord.get_total_budget(), 0u);
    EXPECT_FLOAT_EQ(coord.get_cache_ratio(), 0.40f);
    EXPECT_FLOAT_EQ(coord.get_mmap_ratio(), 0.60f);
    EXPECT_EQ(coord.get_workload_hint(), WorkloadHint::Auto);
    EXPECT_EQ(coord.get_rebalance_count(), 0u);
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, ConcurrentAccess) {
    auto& coord = MemoryCoordinator::global();
    coord.set_total_budget(1ULL * 1024 * 1024 * 1024);
    coord.set_rebalance_interval(std::chrono::seconds{0});

    const int num_threads = 4;
    const int iterations_per_thread = 100;
    std::vector<std::thread> threads;

    // Launch threads that concurrently tick and read metrics
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&coord, iterations_per_thread]() {
            for (int j = 0; j < iterations_per_thread; ++j) {
                coord.tick();
                auto metrics = coord.get_metrics();
                auto cache_ratio = coord.get_cache_ratio();
                auto mmap_ratio = coord.get_mmap_ratio();

                // Verify invariants
                EXPECT_NEAR(cache_ratio + mmap_ratio, 1.0f, 0.01f);
                EXPECT_GE(metrics.cache_utilization, 0.0);
                EXPECT_GE(metrics.mmap_utilization, 0.0);
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Should not crash and ratios should still be valid
    EXPECT_NEAR(coord.get_cache_ratio() + coord.get_mmap_ratio(), 1.0f, 0.01f);
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST_F(MemoryCoordinatorTest, IntegrationWithCache) {
    auto& coord = MemoryCoordinator::global();

    // Set a small budget
    const size_t budget = 50 * 1024 * 1024;  // 50MB
    coord.set_total_budget(budget);

    // Cache should have its budget set
    size_t cache_budget = IndexDetails<IRecord>::getCacheMaxMemory();

    // Should be approximately 40% of total (default ratio)
    size_t expected_cache = static_cast<size_t>(budget * 0.40f);
    EXPECT_EQ(cache_budget, expected_cache);
}

TEST_F(MemoryCoordinatorTest, IntegrationWithMappingManager) {
    auto& coord = MemoryCoordinator::global();

    // Set a small budget
    const size_t budget = 50 * 1024 * 1024;  // 50MB
    coord.set_total_budget(budget);

    // MappingManager should have its budget set
    size_t mmap_budget = MappingManager::global().get_memory_budget();

    // Should be approximately 60% of total (default ratio)
    size_t expected_mmap = static_cast<size_t>(budget * 0.60f);
    EXPECT_EQ(mmap_budget, expected_mmap);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(MemoryCoordinatorTest, ZeroBudget) {
    auto& coord = MemoryCoordinator::global();

    // Set budget to 0 (unlimited)
    coord.set_total_budget(0);

    EXPECT_EQ(coord.get_total_budget(), 0u);

    // Tick should not crash with 0 budget
    coord.tick();
}

TEST_F(MemoryCoordinatorTest, VerySmallBudget) {
    auto& coord = MemoryCoordinator::global();

    // Set very small budget (1KB)
    coord.set_total_budget(1024);

    EXPECT_EQ(coord.get_total_budget(), 1024u);

    // Should still maintain valid ratios
    EXPECT_NEAR(coord.get_cache_ratio() + coord.get_mmap_ratio(), 1.0f, 0.01f);
}

TEST_F(MemoryCoordinatorTest, VeryLargeBudget) {
    auto& coord = MemoryCoordinator::global();

    // Set very large budget (100GB)
    const size_t budget = 100ULL * 1024 * 1024 * 1024;
    coord.set_total_budget(budget);

    EXPECT_EQ(coord.get_total_budget(), budget);

    // Should still maintain valid ratios
    EXPECT_NEAR(coord.get_cache_ratio() + coord.get_mmap_ratio(), 1.0f, 0.01f);
}
