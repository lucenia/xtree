/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Unit tests for cache memory policies.
 */

#include <gtest/gtest.h>
#include "../src/cache_policy.hpp"

using namespace xtree;

TEST(CachePolicyTest, UnlimitedPolicy) {
    UnlimitedCachePolicy policy;
    EXPECT_EQ(policy.getMaxMemory(), 0u);
    EXPECT_STREQ(policy.name(), "Unlimited");
}

TEST(CachePolicyTest, FixedMemoryPolicy) {
    FixedMemoryCachePolicy policy(512 * 1024 * 1024);  // 512MB
    EXPECT_EQ(policy.getMaxMemory(), 512u * 1024 * 1024);
    EXPECT_STREQ(policy.name(), "FixedMemory");

    // Test setBudget
    policy.setBudget(1024 * 1024 * 1024);  // 1GB
    EXPECT_EQ(policy.getMaxMemory(), 1024u * 1024 * 1024);
}

TEST(CachePolicyTest, PercentageMemoryPolicy) {
    PercentageMemoryCachePolicy policy(10);  // 10% of RAM
    EXPECT_GT(policy.getMaxMemory(), 0u);
    EXPECT_STREQ(policy.name(), "PercentageMemory");
    EXPECT_EQ(policy.getPercentage(), 10u);

    // Should be clamped to 1-100
    PercentageMemoryCachePolicy lowPolicy(0);
    EXPECT_EQ(lowPolicy.getPercentage(), 1u);

    PercentageMemoryCachePolicy highPolicy(200);
    EXPECT_EQ(highPolicy.getPercentage(), 100u);
}

TEST(CachePolicyTest, PerRecordPolicy) {
    PerRecordCachePolicy policy(1000000, 100);  // 1M records, 100 bytes each
    EXPECT_EQ(policy.getMaxMemory(), 100u * 1000000);
    EXPECT_STREQ(policy.name(), "PerRecord");
}

TEST(CachePolicyTest, WorkloadPolicy) {
    // BulkIngestion should have smaller budget than QueryHeavy
    WorkloadCachePolicy bulkPolicy(WorkloadType::BulkIngestion);
    WorkloadCachePolicy queryPolicy(WorkloadType::QueryHeavy);

    EXPECT_GT(bulkPolicy.getMaxMemory(), 0u);
    EXPECT_GT(queryPolicy.getMaxMemory(), 0u);
    EXPECT_GT(queryPolicy.getMaxMemory(), bulkPolicy.getMaxMemory());

    EXPECT_STREQ(bulkPolicy.name(), "Workload");
}

TEST(CachePolicyTest, AdaptivePolicy) {
    size_t minBudget = 100 * 1024 * 1024;   // 100MB
    size_t maxBudget = 1024 * 1024 * 1024;  // 1GB

    AdaptiveCachePolicy policy(minBudget, maxBudget, 0.90);

    // Initial budget should be between min and max
    size_t initial = policy.getMaxMemory();
    EXPECT_GE(initial, minBudget);
    EXPECT_LE(initial, maxBudget);

    // Simulate low hit rate - should increase budget
    policy.onTick(0, 0.70);
    EXPECT_GE(policy.getMaxMemory(), initial);

    // Simulate high hit rate - should decrease budget
    size_t afterIncrease = policy.getMaxMemory();
    policy.onTick(0, 0.99);
    EXPECT_LE(policy.getMaxMemory(), afterIncrease);

    EXPECT_STREQ(policy.name(), "Adaptive");
}

TEST(CachePolicyTest, CreatePolicyFromString) {
    // Unlimited
    auto unlimited = createCachePolicy("unlimited");
    ASSERT_NE(unlimited, nullptr);
    EXPECT_EQ(unlimited->getMaxMemory(), 0u);

    // Empty string = unlimited
    auto empty = createCachePolicy("");
    ASSERT_NE(empty, nullptr);
    EXPECT_EQ(empty->getMaxMemory(), 0u);

    // Fixed size
    auto fixed = createCachePolicy("512MB");
    ASSERT_NE(fixed, nullptr);
    EXPECT_EQ(fixed->getMaxMemory(), 512u * 1024 * 1024);

    auto fixedGB = createCachePolicy("1GB");
    ASSERT_NE(fixedGB, nullptr);
    EXPECT_EQ(fixedGB->getMaxMemory(), 1024u * 1024 * 1024);

    auto fixedKB = createCachePolicy("1024KB");
    ASSERT_NE(fixedKB, nullptr);
    EXPECT_EQ(fixedKB->getMaxMemory(), 1024u * 1024);

    // Percentage
    auto percent = createCachePolicy("25%");
    ASSERT_NE(percent, nullptr);
    EXPECT_GT(percent->getMaxMemory(), 0u);

    // Workload presets
    auto bulk = createCachePolicy("bulk");
    ASSERT_NE(bulk, nullptr);
    EXPECT_GT(bulk->getMaxMemory(), 0u);

    auto query = createCachePolicy("query");
    ASSERT_NE(query, nullptr);
    EXPECT_GT(query->getMaxMemory(), 0u);

    // Invalid
    auto invalid = createCachePolicy("foobar");
    EXPECT_EQ(invalid, nullptr);
}

TEST(CachePolicyTest, GetDefaultPolicy) {
    // Without XTREE_CACHE_POLICY env var, should return Unlimited
    // Note: This test might fail if the env var is set in the test environment
    auto policy = getDefaultCachePolicy();
    ASSERT_NE(policy, nullptr);
    // Can't reliably test the value since env var might be set
}

TEST(CachePolicyTest, SystemMemoryDetection) {
    // Verify we can detect system memory
    size_t totalMem = detail::getTotalSystemMemory();
    EXPECT_GT(totalMem, 0u);

    // Should be at least 1GB (reasonable minimum for any dev machine)
    EXPECT_GE(totalMem, 1024u * 1024 * 1024);

    std::cout << "Detected system memory: " << (totalMem / (1024.0 * 1024 * 1024)) << " GB" << std::endl;
}
