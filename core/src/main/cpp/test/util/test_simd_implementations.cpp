/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * The Lucenia project is free software: you can redistribute it
 * and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see:
 * https://www.gnu.org/licenses/agpl-3.0.html
 */

#include <gtest/gtest.h>
#include <vector>
#include <random>
#include <limits>
#include <cmath>
#include <chrono>
#include <iostream>
#include <utility>
#include "../../src/util/cpu_features.h"
#include "../../src/keymbr.h"
#include "../../src/util/float_utils.h"

// Forward declarations for SIMD implementations
namespace xtree {
    typedef bool (*intersects_func_t)(const int32_t*, const int32_t*, int);
    typedef void (*expand_func_t)(int32_t*, const int32_t*, int);
    typedef void (*expand_point_func_t)(int32_t*, const double*, int);
    
    intersects_func_t get_optimal_intersects_func();
    expand_func_t get_optimal_expand_func();
    expand_point_func_t get_optimal_expand_point_func();
    
    namespace simd_impl {
        bool intersects_scalar(const int32_t* box1, const int32_t* box2, int dimensions);
        void expand_scalar(int32_t* target, const int32_t* source, int dimensions);
        void expand_point_scalar(int32_t* box, const double* point, int dimensions);
        
        #if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
        bool intersects_sse2(const int32_t* box1, const int32_t* box2, int dimensions);
        bool intersects_avx2(const int32_t* box1, const int32_t* box2, int dimensions);
        void expand_sse2(int32_t* target, const int32_t* source, int dimensions);
        void expand_avx2(int32_t* target, const int32_t* source, int dimensions);
        void expand_point_sse2(int32_t* box, const double* point, int dimensions);
        void expand_point_avx2(int32_t* box, const double* point, int dimensions);
        #endif
        
        #if defined(__aarch64__) || defined(__arm64__)
        bool intersects_neon(const int32_t* box1, const int32_t* box2, int dimensions);
        void expand_neon(int32_t* target, const int32_t* source, int dimensions);
        void expand_point_neon(int32_t* box, const double* point, int dimensions);
        #endif
    }
}

using namespace xtree;

class SIMDImplementationsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize CPU features detection
        CPUFeatures::get();
    }
    
    // Helper function to create a random MBR
    std::vector<int32_t> createRandomMBR(int dimensions, std::mt19937& rng) {
        std::vector<int32_t> mbr(dimensions * 2);
        std::uniform_real_distribution<float> dist(-1000.0f, 1000.0f);
        
        for (int d = 0; d < dimensions; d++) {
            float min_val = dist(rng);
            float max_val = min_val + std::abs(dist(rng));  // Ensure max > min
            mbr[d * 2] = floatToSortableInt(min_val);
            mbr[d * 2 + 1] = floatToSortableInt(max_val);
        }
        
        return mbr;
    }
    
    // Helper function to create overlapping MBRs
    std::pair<std::vector<int32_t>, std::vector<int32_t>> createOverlappingMBRs(int dimensions, std::mt19937& rng) {
        std::vector<int32_t> mbr1 = createRandomMBR(dimensions, rng);
        std::vector<int32_t> mbr2(dimensions * 2);
        
        std::uniform_real_distribution<float> overlap_dist(0.25f, 0.75f);  // Overlap percentage
        
        for (int d = 0; d < dimensions; d++) {
            float min1 = sortableIntToFloat(mbr1[d * 2]);
            float max1 = sortableIntToFloat(mbr1[d * 2 + 1]);
            float width1 = max1 - min1;
            
            // Create overlapping box - start within first box and extend beyond
            float overlap_ratio = overlap_dist(rng);
            float min2 = min1 + (width1 * overlap_ratio * 0.5f);  // Start inside mbr1
            float max2 = max1 - (width1 * (1.0f - overlap_ratio) * 0.5f) + width1 * 0.5f; // Extend beyond mbr1
            
            // Ensure min2 < max2
            if (min2 >= max2) {
                min2 = min1;
                max2 = max1;
            }
            
            mbr2[d * 2] = floatToSortableInt(min2);
            mbr2[d * 2 + 1] = floatToSortableInt(max2);
        }
        
        return {mbr1, mbr2};
    }
    
    // Helper function to create non-overlapping MBRs
    std::pair<std::vector<int32_t>, std::vector<int32_t>> createNonOverlappingMBRs(int dimensions, std::mt19937& rng) {
        std::vector<int32_t> mbr1 = createRandomMBR(dimensions, rng);
        std::vector<int32_t> mbr2(dimensions * 2);
        
        std::uniform_int_distribution<int> dim_dist(0, dimensions - 1);
        int non_overlap_dim = dim_dist(rng);
        
        for (int d = 0; d < dimensions; d++) {
            float min1 = sortableIntToFloat(mbr1[d * 2]);
            float max1 = sortableIntToFloat(mbr1[d * 2 + 1]);
            
            if (d == non_overlap_dim) {
                // Create non-overlapping in this dimension
                float gap = 10.0f;
                mbr2[d * 2] = floatToSortableInt(max1 + gap);
                mbr2[d * 2 + 1] = floatToSortableInt(max1 + gap + 50.0f);
            } else {
                // Copy same bounds
                mbr2[d * 2] = mbr1[d * 2];
                mbr2[d * 2 + 1] = mbr1[d * 2 + 1];
            }
        }
        
        return {mbr1, mbr2};
    }
};

// Test scalar implementation correctness
TEST_F(SIMDImplementationsTest, ScalarIntersectsCorrectness) {
    std::mt19937 rng(42);
    
    // Test various dimensions
    for (int dims : {1, 2, 3, 4, 5, 8, 10}) {
        // Test overlapping MBRs
        for (int i = 0; i < 100; i++) {
            auto [mbr1, mbr2] = createOverlappingMBRs(dims, rng);
            EXPECT_TRUE(simd_impl::intersects_scalar(mbr1.data(), mbr2.data(), dims))
                << "Failed for overlapping MBRs with " << dims << " dimensions";
        }
        
        // Test non-overlapping MBRs
        for (int i = 0; i < 100; i++) {
            auto [mbr1, mbr2] = createNonOverlappingMBRs(dims, rng);
            EXPECT_FALSE(simd_impl::intersects_scalar(mbr1.data(), mbr2.data(), dims))
                << "Failed for non-overlapping MBRs with " << dims << " dimensions";
        }
    }
}

// Test that all SIMD implementations match scalar results
TEST_F(SIMDImplementationsTest, SIMDIntersectsMatchesScalar) {
    const auto& features = CPUFeatures::get();
    
    std::cout << "Test starting - SSE2 available: " << features.has_sse2 << std::endl;
    
    // First verify we can call the functions directly
    {
        int32_t box1[] = {0, 10, 20, 30};
        int32_t box2[] = {5, 15, 25, 35};
        
        std::cout << "Testing direct scalar call..." << std::endl;
        bool scalar_result = simd_impl::intersects_scalar(box1, box2, 2);
        std::cout << "Scalar result: " << scalar_result << std::endl;
        
        #if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
        if (features.has_sse2) {
            std::cout << "Testing direct SSE2 call..." << std::endl;
            std::cout << "Calling intersects_sse2..." << std::endl;
            std::cout.flush();
            bool sse2_result = simd_impl::intersects_sse2(box1, box2, 2);
            std::cout << "SSE2 result: " << sse2_result << std::endl;
            EXPECT_EQ(scalar_result, sse2_result) << "SSE2 mismatch for 2D intersecting boxes";
        }
        #endif
    }
    
    // Test case 2: 2D boxes that don't intersect
    {
        int32_t box1[] = {0, 10, 20, 30};
        int32_t box2[] = {15, 25, 35, 45};
        
        bool scalar_result = simd_impl::intersects_scalar(box1, box2, 2);
        
        #if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
        if (features.has_sse2) {
            bool sse2_result = simd_impl::intersects_sse2(box1, box2, 2);
            EXPECT_EQ(scalar_result, sse2_result) << "SSE2 mismatch for 2D non-intersecting boxes";
        }
        #endif
    }
    
    // Test case 3: 4D boxes
    {
        int32_t box1[] = {0, 10, 20, 30, 40, 50, 60, 70};
        int32_t box2[] = {5, 15, 25, 35, 45, 55, 65, 75};
        
        bool scalar_result = simd_impl::intersects_scalar(box1, box2, 4);
        
        #if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
        if (features.has_sse2) {
            bool sse2_result = simd_impl::intersects_sse2(box1, box2, 4);
            EXPECT_EQ(scalar_result, sse2_result) << "SSE2 mismatch for 4D boxes";
        }
        #endif
    }
}

// Test edge cases
TEST_F(SIMDImplementationsTest, IntersectsEdgeCases) {
    // Test identical MBRs
    {
        std::vector<int32_t> mbr = {100, 200, 300, 400};
        EXPECT_TRUE(simd_impl::intersects_scalar(mbr.data(), mbr.data(), 2));
        
        auto optimal_func = get_optimal_intersects_func();
        ASSERT_NE(optimal_func, nullptr) << "Optimal function is null";
        EXPECT_TRUE(optimal_func(mbr.data(), mbr.data(), 2));
    }
    
    // Test touching MBRs (share boundary)
    {
        std::vector<int32_t> mbr1 = {100, 200, 100, 200};
        std::vector<int32_t> mbr2 = {200, 300, 100, 200};
        EXPECT_TRUE(simd_impl::intersects_scalar(mbr1.data(), mbr2.data(), 2));
        
        auto optimal_func = get_optimal_intersects_func();
        ASSERT_NE(optimal_func, nullptr) << "Optimal function is null";
        EXPECT_TRUE(optimal_func(mbr1.data(), mbr2.data(), 2));
    }
    
    // Test point MBRs (min == max)
    {
        int32_t point_val = floatToSortableInt(42.0f);
        std::vector<int32_t> point_mbr = {point_val, point_val, point_val, point_val};
        std::vector<int32_t> containing_mbr = {
            floatToSortableInt(0.0f), floatToSortableInt(100.0f),
            floatToSortableInt(0.0f), floatToSortableInt(100.0f)
        };
        
        EXPECT_TRUE(simd_impl::intersects_scalar(point_mbr.data(), containing_mbr.data(), 2));
        
        auto optimal_func = get_optimal_intersects_func();
        ASSERT_NE(optimal_func, nullptr) << "Optimal function is null";
        EXPECT_TRUE(optimal_func(point_mbr.data(), containing_mbr.data(), 2));
    }
}

// Test expand functionality
TEST_F(SIMDImplementationsTest, ExpandCorrectness) {
    std::mt19937 rng(42);
    
    for (int dims : {1, 2, 3, 4, 5, 8}) {
        for (int i = 0; i < 100; i++) {
            std::vector<int32_t> target = createRandomMBR(dims, rng);
            std::vector<int32_t> source = createRandomMBR(dims, rng);
            std::vector<int32_t> original_target = target;
            
            simd_impl::expand_scalar(target.data(), source.data(), dims);
            
            // Verify expansion
            for (int d = 0; d < dims; d++) {
                int min_idx = d * 2;
                int max_idx = d * 2 + 1;
                
                EXPECT_LE(target[min_idx], original_target[min_idx])
                    << "Min not properly expanded at dim " << d;
                EXPECT_GE(target[max_idx], original_target[max_idx])
                    << "Max not properly expanded at dim " << d;
                
                EXPECT_LE(target[min_idx], source[min_idx])
                    << "Source min not included at dim " << d;
                EXPECT_GE(target[max_idx], source[max_idx])
                    << "Source max not included at dim " << d;
            }
        }
    }
}

// Test expand_point functionality
TEST_F(SIMDImplementationsTest, ExpandPointCorrectness) {
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist(-1000.0, 1000.0);
    
    for (int dims : {1, 2, 3, 4, 5}) {
        for (int i = 0; i < 100; i++) {
            // Create initial MBR
            std::vector<int32_t> mbr = createRandomMBR(dims, rng);
            std::vector<int32_t> original_mbr = mbr;
            
            // Create random point
            std::vector<double> point(dims);
            for (int d = 0; d < dims; d++) {
                point[d] = dist(rng);
            }
            
            simd_impl::expand_point_scalar(mbr.data(), point.data(), dims);
            
            // Verify expansion
            for (int d = 0; d < dims; d++) {
                int32_t point_sortable = floatToSortableInt((float)point[d]);
                int min_idx = d * 2;
                int max_idx = d * 2 + 1;
                
                EXPECT_LE(mbr[min_idx], original_mbr[min_idx])
                    << "Min improperly modified at dim " << d;
                EXPECT_GE(mbr[max_idx], original_mbr[max_idx])
                    << "Max improperly modified at dim " << d;
                
                EXPECT_LE(mbr[min_idx], point_sortable)
                    << "Point not included in min at dim " << d;
                EXPECT_GE(mbr[max_idx], point_sortable)
                    << "Point not included in max at dim " << d;
            }
        }
    }
}

// Test optimal function selection
TEST_F(SIMDImplementationsTest, OptimalFunctionSelection) {
    const auto& features = CPUFeatures::get();
    
    auto intersects_func = get_optimal_intersects_func();
    EXPECT_NE(intersects_func, nullptr);
    
    // Log which implementation was selected
    std::cout << "CPU Features:" << std::endl;
    std::cout << "  SSE2: " << (features.has_sse2 ? "yes" : "no") << std::endl;
    std::cout << "  AVX2: " << (features.has_avx2 ? "yes" : "no") << std::endl;
    std::cout << "  NEON: " << (features.has_neon ? "yes" : "no") << std::endl;
    
    #if defined(__aarch64__) || defined(__arm64__)
    // On Apple M3, we should get NEON
    if (features.has_neon) {
        std::cout << "Running on ARM with NEON support (Apple M3)" << std::endl;
    }
    #endif
}

// Performance comparison test (optional, can be disabled in CI)
TEST_F(SIMDImplementationsTest, DISABLED_PerformanceComparison) {
    std::mt19937 rng(42);
    const int iterations = 1000000;
    const int dims = 4;  // Common case
    
    // Generate test data
    std::vector<std::pair<std::vector<int32_t>, std::vector<int32_t>>> test_data;
    for (int i = 0; i < 1000; i++) {
        test_data.push_back({createRandomMBR(dims, rng), createRandomMBR(dims, rng)});
    }
    
    // Test scalar performance
    auto start = std::chrono::high_resolution_clock::now();
    int scalar_matches = 0;
    for (int i = 0; i < iterations; i++) {
        const auto& [mbr1, mbr2] = test_data[i % test_data.size()];
        if (simd_impl::intersects_scalar(mbr1.data(), mbr2.data(), dims)) {
            scalar_matches++;
        }
    }
    auto scalar_time = std::chrono::high_resolution_clock::now() - start;
    
    // Test optimal performance
    start = std::chrono::high_resolution_clock::now();
    int optimal_matches = 0;
    auto optimal_func = get_optimal_intersects_func();
    for (int i = 0; i < iterations; i++) {
        const auto& [mbr1, mbr2] = test_data[i % test_data.size()];
        if (optimal_func(mbr1.data(), mbr2.data(), dims)) {
            optimal_matches++;
        }
    }
    auto optimal_time = std::chrono::high_resolution_clock::now() - start;
    
    EXPECT_EQ(scalar_matches, optimal_matches);
    
    std::cout << "Performance Results (" << iterations << " iterations):" << std::endl;
    std::cout << "  Scalar: " << std::chrono::duration_cast<std::chrono::microseconds>(scalar_time).count() << " us" << std::endl;
    std::cout << "  Optimal: " << std::chrono::duration_cast<std::chrono::microseconds>(optimal_time).count() << " us" << std::endl;
    std::cout << "  Speedup: " << (double)scalar_time.count() / optimal_time.count() << "x" << std::endl;
}

// Test special float values
TEST_F(SIMDImplementationsTest, SpecialFloatValues) {
    const int dims = 2;
    
    // Test with infinity
    {
        std::vector<int32_t> mbr1 = {
            floatToSortableInt(-INFINITY), floatToSortableInt(INFINITY),
            floatToSortableInt(0.0f), floatToSortableInt(100.0f)
        };
        std::vector<int32_t> mbr2 = {
            floatToSortableInt(-100.0f), floatToSortableInt(100.0f),
            floatToSortableInt(-100.0f), floatToSortableInt(100.0f)
        };
        
        bool scalar_result = simd_impl::intersects_scalar(mbr1.data(), mbr2.data(), dims);
        auto optimal_func = get_optimal_intersects_func();
        bool optimal_result = optimal_func(mbr1.data(), mbr2.data(), dims);
        
        EXPECT_EQ(scalar_result, optimal_result);
        EXPECT_TRUE(scalar_result);  // Should intersect
    }
    
    // Test with negative zero
    {
        std::vector<int32_t> mbr1 = {
            floatToSortableInt(-0.0f), floatToSortableInt(0.0f),
            floatToSortableInt(-0.0f), floatToSortableInt(0.0f)
        };
        std::vector<int32_t> mbr2 = mbr1;
        
        bool scalar_result = simd_impl::intersects_scalar(mbr1.data(), mbr2.data(), dims);
        auto optimal_func = get_optimal_intersects_func();
        bool optimal_result = optimal_func(mbr1.data(), mbr2.data(), dims);
        
        EXPECT_EQ(scalar_result, optimal_result);
        EXPECT_TRUE(scalar_result);  // Identical MBRs should intersect
    }
}

// Test expand_point implementations
TEST(SIMDImplementations, ExpandPointCorrectness) {
    using namespace xtree;
    
    const auto& features = CPUFeatures::get();
    
    // Test various dimensions
    std::vector<int> test_dims = {1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 16};
    
    for (int dims : test_dims) {
        // Test case 1: Expand box with positive point
        {
            std::vector<int32_t> box_scalar(dims * 2);
            std::vector<int32_t> box_test(dims * 2);
            std::vector<double> point(dims);
            
            // Initialize box with some bounds
            for (int d = 0; d < dims; d++) {
                float min_val = 10.0f + d * 5.0f;
                float max_val = 20.0f + d * 5.0f;
                box_scalar[d * 2] = floatToSortableInt(min_val);
                box_scalar[d * 2 + 1] = floatToSortableInt(max_val);
                
                // Point outside the box (should expand)
                point[d] = 25.0 + d * 5.0;
            }
            
            // Copy initial box
            std::memcpy(box_test.data(), box_scalar.data(), dims * 2 * sizeof(int32_t));
            
            // Run scalar version (reference)
            simd_impl::expand_point_scalar(box_scalar.data(), point.data(), dims);
            
            // Run optimal version
            auto optimal_func = get_optimal_expand_point_func();
            optimal_func(box_test.data(), point.data(), dims);
            
            // Compare results
            for (int i = 0; i < dims * 2; i++) {
                EXPECT_EQ(box_scalar[i], box_test[i]) 
                    << "Mismatch at dimension " << dims << ", index " << i;
            }
        }
        
        // Test case 2: Expand box with negative point
        {
            std::vector<int32_t> box_scalar(dims * 2);
            std::vector<int32_t> box_test(dims * 2);
            std::vector<double> point(dims);
            
            // Initialize box with bounds around zero
            for (int d = 0; d < dims; d++) {
                float min_val = -5.0f + d * 2.0f;
                float max_val = 5.0f + d * 2.0f;
                box_scalar[d * 2] = floatToSortableInt(min_val);
                box_scalar[d * 2 + 1] = floatToSortableInt(max_val);
                
                // Point outside the box (negative side)
                point[d] = -10.0 + d * 2.0;
            }
            
            // Copy initial box
            std::memcpy(box_test.data(), box_scalar.data(), dims * 2 * sizeof(int32_t));
            
            // Run both versions
            simd_impl::expand_point_scalar(box_scalar.data(), point.data(), dims);
            auto optimal_func = get_optimal_expand_point_func();
            optimal_func(box_test.data(), point.data(), dims);
            
            // Compare results
            for (int i = 0; i < dims * 2; i++) {
                EXPECT_EQ(box_scalar[i], box_test[i]) 
                    << "Mismatch at dimension " << dims << ", index " << i;
            }
        }
        
        // Test case 3: Point inside box (no expansion)
        {
            std::vector<int32_t> box_original(dims * 2);
            std::vector<int32_t> box_test(dims * 2);
            std::vector<double> point(dims);
            
            // Initialize box
            for (int d = 0; d < dims; d++) {
                float min_val = -10.0f;
                float max_val = 10.0f;
                box_original[d * 2] = floatToSortableInt(min_val);
                box_original[d * 2 + 1] = floatToSortableInt(max_val);
                
                // Point inside the box
                point[d] = 0.0;
            }
            
            // Copy box
            std::memcpy(box_test.data(), box_original.data(), dims * 2 * sizeof(int32_t));
            
            // Run optimal version
            auto optimal_func = get_optimal_expand_point_func();
            optimal_func(box_test.data(), point.data(), dims);
            
            // Verify box didn't change
            for (int i = 0; i < dims * 2; i++) {
                EXPECT_EQ(box_original[i], box_test[i]) 
                    << "Box changed when point was inside at dimension " << dims << ", index " << i;
            }
        }
    }
}

// Test expand_point with special values
TEST(SIMDImplementations, ExpandPointSpecialValues) {
    using namespace xtree;
    
    int dims = 4;
    std::vector<int32_t> box_scalar(dims * 2);
    std::vector<int32_t> box_test(dims * 2);
    std::vector<double> point(dims);
    
    // Initialize with finite bounds
    for (int d = 0; d < dims; d++) {
        box_scalar[d * 2] = floatToSortableInt(0.0f);
        box_scalar[d * 2 + 1] = floatToSortableInt(1.0f);
    }
    
    // Test with special values
    point[0] = -0.0;  // Negative zero
    point[1] = std::numeric_limits<double>::infinity();
    point[2] = -std::numeric_limits<double>::infinity();
    point[3] = 0.5;  // Normal value
    
    std::memcpy(box_test.data(), box_scalar.data(), dims * 2 * sizeof(int32_t));
    
    // Run both versions
    simd_impl::expand_point_scalar(box_scalar.data(), point.data(), dims);
    auto optimal_func = get_optimal_expand_point_func();
    optimal_func(box_test.data(), point.data(), dims);
    
    // Compare results
    for (int i = 0; i < dims * 2; i++) {
        EXPECT_EQ(box_scalar[i], box_test[i]) 
            << "Special value mismatch at index " << i;
    }
}

// Test expand_point with mixed positive/negative values
TEST(SIMDImplementations, ExpandPointMixedSigns) {
    using namespace xtree;
    
    int dims = 8;
    std::vector<int32_t> box_scalar(dims * 2);
    std::vector<int32_t> box_test(dims * 2);
    std::vector<double> point(dims);
    
    // Initialize box with varying bounds
    for (int d = 0; d < dims; d++) {
        float center = (d - 4) * 10.0f;
        box_scalar[d * 2] = floatToSortableInt(center - 5.0f);
        box_scalar[d * 2 + 1] = floatToSortableInt(center + 5.0f);
    }
    
    // Create points with alternating signs
    for (int d = 0; d < dims; d++) {
        point[d] = (d % 2 == 0) ? -20.0 - d : 20.0 + d;
    }
    
    std::memcpy(box_test.data(), box_scalar.data(), dims * 2 * sizeof(int32_t));
    
    // Run both versions
    simd_impl::expand_point_scalar(box_scalar.data(), point.data(), dims);
    auto optimal_func = get_optimal_expand_point_func();
    optimal_func(box_test.data(), point.data(), dims);
    
    // Compare results
    for (int i = 0; i < dims * 2; i++) {
        EXPECT_EQ(box_scalar[i], box_test[i]) 
            << "Mixed sign mismatch at index " << i;
    }
}