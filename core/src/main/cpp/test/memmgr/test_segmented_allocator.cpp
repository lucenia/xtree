/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Test and benchmark segmented allocator performance
 */

#include <gtest/gtest.h>
#include "../src/memmgr/compact_allocator.hpp"
#include <chrono>
#include <random>

using namespace xtree;
using namespace std::chrono;

class SegmentedAllocatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use default BALANCED_4TB strategy (10-bit segments)
        allocator = std::make_unique<CompactAllocator>(16 * 1024 * 1024); // 16MB initial
    }
    
    std::unique_ptr<CompactAllocator> allocator;
};

TEST_F(SegmentedAllocatorTest, BasicAllocation) {
    // Test basic allocation
    auto offset1 = allocator->allocate(1024);
    EXPECT_NE(offset1, CompactAllocator::INVALID_OFFSET);
    
    void* ptr1 = allocator->get_ptr(offset1);
    EXPECT_NE(ptr1, nullptr);
    
    // Write and read data
    strcpy(static_cast<char*>(ptr1), "Hello, Segmented World!");
    EXPECT_STREQ(static_cast<char*>(ptr1), "Hello, Segmented World!");
}

TEST_F(SegmentedAllocatorTest, CrossSegmentAllocation) {
    // Allocate enough to cross segment boundary
    std::vector<CompactAllocator::offset_t> offsets;
    size_t total_allocated = 0;
    
    // Allocate 5MB worth of 1KB blocks (will cross into second segment)
    for (int i = 0; i < 5120; i++) {
        auto offset = allocator->allocate(1024);
        EXPECT_NE(offset, CompactAllocator::INVALID_OFFSET);
        offsets.push_back(offset);
        total_allocated += 1024;
        
        // Verify we can write to it
        void* ptr = allocator->get_ptr(offset);
        EXPECT_NE(ptr, nullptr);
        *static_cast<int*>(ptr) = i;
    }
    
    // Verify all allocations
    for (size_t i = 0; i < offsets.size(); i++) {
        void* ptr = allocator->get_ptr(offsets[i]);
        EXPECT_EQ(*static_cast<int*>(ptr), static_cast<int>(i));
    }
    
    std::cout << "Total allocated: " << total_allocated << " bytes across segments\n";
}

TEST_F(SegmentedAllocatorTest, PointerPerformance) {
    // Benchmark pointer access performance
    const int NUM_ALLOCATIONS = 10000;
    const int NUM_ACCESSES = 1000000;
    
    // Allocate many objects
    std::vector<CompactAllocator::offset_t> offsets;
    for (int i = 0; i < NUM_ALLOCATIONS; i++) {
        auto offset = allocator->allocate(64); // Small allocations
        offsets.push_back(offset);
        
        // Initialize with value
        int* ptr = allocator->get_ptr<int>(offset);
        *ptr = i;
    }
    
    // Random access pattern
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, NUM_ALLOCATIONS - 1);
    
    // Benchmark pointer resolution
    auto start = high_resolution_clock::now();
    
    int sum = 0;
    for (int i = 0; i < NUM_ACCESSES; i++) {
        int idx = dis(gen);
        int* ptr = allocator->get_ptr<int>(offsets[idx]);
        sum += *ptr;
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    
    std::cout << "Segmented allocator performance:\n";
    std::cout << "  " << NUM_ACCESSES << " pointer accesses in " << duration << " μs\n";
    std::cout << "  " << (NUM_ACCESSES * 1000.0 / duration) << " accesses/ms\n";
    std::cout << "  Average: " << (duration * 1000.0 / NUM_ACCESSES) << " ns per access\n";
    
    // Prevent optimization
    EXPECT_GT(sum, 0);
}

TEST_F(SegmentedAllocatorTest, CompareStrategies) {
    // Test different segment strategies
    const int NUM_ALLOCATIONS = 10000;
    const int NUM_ACCESSES = 1000000;
    
    struct StrategyResult {
        CompactAllocator::SegmentStrategy strategy;
        const char* name;
        double ns_per_access;
    };
    
    std::vector<StrategyResult> results;
    
    // Test each strategy
    for (auto strategy : {CompactAllocator::SegmentStrategy::FAST_256GB,
                         CompactAllocator::SegmentStrategy::FAST_1TB,
                         CompactAllocator::SegmentStrategy::BALANCED_4TB,
                         CompactAllocator::SegmentStrategy::LARGE_16TB}) {
        
        // Create allocator with this strategy
        auto test_allocator = std::make_unique<CompactAllocator>(16 * 1024 * 1024, strategy);
        
        // Allocate many objects
        std::vector<CompactAllocator::offset_t> offsets;
        for (int i = 0; i < NUM_ALLOCATIONS; i++) {
            auto offset = test_allocator->allocate(64);
            offsets.push_back(offset);
            int* ptr = test_allocator->get_ptr<int>(offset);
            *ptr = i;
        }
        
        // Random access pattern
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, NUM_ALLOCATIONS - 1);
        
        // Benchmark
        auto start = high_resolution_clock::now();
        int sum = 0;
        for (int i = 0; i < NUM_ACCESSES; i++) {
            int idx = dis(gen);
            int* ptr = test_allocator->get_ptr<int>(offsets[idx]);
            sum += *ptr;
        }
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        const char* name = "";
        switch (strategy) {
            case CompactAllocator::SegmentStrategy::FAST_256GB: name = "FAST_256GB (6-bit)"; break;
            case CompactAllocator::SegmentStrategy::FAST_1TB: name = "FAST_1TB (8-bit)"; break;
            case CompactAllocator::SegmentStrategy::BALANCED_4TB: name = "BALANCED_4TB (10-bit)"; break;
            case CompactAllocator::SegmentStrategy::LARGE_16TB: name = "LARGE_16TB (12-bit)"; break;
            default: name = "Unknown"; break;
        }
        
        results.push_back({strategy, name, duration * 1000.0 / NUM_ACCESSES});
        EXPECT_GT(sum, 0); // Prevent optimization
    }
    
    // Print comparison
    std::cout << "\nSegment Strategy Performance Comparison:\n";
    std::cout << "----------------------------------------\n";
    for (const auto& result : results) {
        std::cout << result.name << ": " << result.ns_per_access << " ns per access\n";
    }
    
    // Find fastest
    auto fastest = std::min_element(results.begin(), results.end(),
        [](const StrategyResult& a, const StrategyResult& b) {
            return a.ns_per_access < b.ns_per_access;
        });
    
    std::cout << "\nFastest strategy: " << fastest->name << "\n";
    
    // Calculate overhead vs fastest
    std::cout << "\nOverhead compared to fastest:\n";
    for (const auto& result : results) {
        double overhead = ((result.ns_per_access / fastest->ns_per_access) - 1.0) * 100.0;
        std::cout << result.name << ": +" << overhead << "%\n";
    }
}

TEST_F(SegmentedAllocatorTest, CompareWith32BitOffset) {
    // For comparison, simulate 32-bit offset performance
    const int NUM_ACCESSES = 1000000;
    
    // Simple 32-bit offset simulation
    char* base = new char[64 * 1024 * 1024];
    std::vector<uint32_t> offsets_32;
    
    // Create offsets
    for (int i = 0; i < 10000; i++) {
        offsets_32.push_back(i * 64);
        *reinterpret_cast<int*>(base + offsets_32.back()) = i;
    }
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, offsets_32.size() - 1);
    
    // Benchmark simple 32-bit offset
    auto start = high_resolution_clock::now();
    
    int sum = 0;
    for (int i = 0; i < NUM_ACCESSES; i++) {
        int idx = dis(gen);
        int* ptr = reinterpret_cast<int*>(base + offsets_32[idx]);
        sum += *ptr;
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    
    std::cout << "\nSimple 32-bit offset performance:\n";
    std::cout << "  " << NUM_ACCESSES << " pointer accesses in " << duration << " μs\n";
    std::cout << "  " << (NUM_ACCESSES * 1000.0 / duration) << " accesses/ms\n";
    std::cout << "  Average: " << (duration * 1000.0 / NUM_ACCESSES) << " ns per access\n";
    
    delete[] base;
    EXPECT_GT(sum, 0);
}