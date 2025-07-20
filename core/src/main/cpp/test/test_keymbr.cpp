/*
 * SPDX-License-Identifier: SSPL-1.0
 *
 * The Lucenia project is source-available software: you can
 * redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 *
 * As per the terms of the Server Side Public License, if you
 * make the functionality of this program or a modified version
 * available over a network, you must make the source code
 * available for download.
 *
 * The full text of the Server Side Public License, version 1,
 * can be found at:
 * https://www.mongodb.com/licensing/server-side-public-license
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <limits>
#include <vector>
#include <random>
#include <chrono>
#include "../src/keymbr.h"
#include "../src/util.h"

using namespace xtree;
using namespace std;

// Test fixture for KeyMBR tests
class KeyMBRTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Common setup if needed
    }
    
    void TearDown() override {
        // Common cleanup if needed
    }
};

// KeyMBR Creation Tests
TEST_F(KeyMBRTest, Creation2D) {
    KeyMBR mbr2d(2, 32);
    EXPECT_EQ(mbr2d.getDimensionCount(), 2);
}

TEST_F(KeyMBRTest, Creation3D) {
    KeyMBR mbr3d(3, 32);
    EXPECT_EQ(mbr3d.getDimensionCount(), 3);
}

TEST_F(KeyMBRTest, CreationHighDimensional) {
    KeyMBR mbr10d(10, 32);
    EXPECT_EQ(mbr10d.getDimensionCount(), 10);
}

// KeyMBR Bounds Tests
TEST_F(KeyMBRTest, InitialBounds) {
    KeyMBR mbr(2, 32);
    
    // Initial bounds should be max/min float values
    EXPECT_EQ(mbr.getMin(0), numeric_limits<float>::max());
    EXPECT_EQ(mbr.getMax(0), -numeric_limits<float>::max());
    EXPECT_EQ(mbr.getMin(1), numeric_limits<float>::max());
    EXPECT_EQ(mbr.getMax(1), -numeric_limits<float>::max());
}

TEST_F(KeyMBRTest, MemoryUsage) {
    KeyMBR mbr(2, 32);
    long memUsed = mbr.getMemoryUsed();
    EXPECT_GT(memUsed, 0);
}

// KeyMBR Expansion Tests
TEST_F(KeyMBRTest, Expansion) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    mbr1.expand(mbr2);
    // After expansion, mbr1 should still have its initial state since mbr2 is also uninitialized
    EXPECT_EQ(mbr1.getMin(0), numeric_limits<float>::max());
}

TEST_F(KeyMBRTest, Reset) {
    KeyMBR mbr(2, 32);
    
    // Add a point first
    vector<double> point = {5.0, 5.0};
    mbr.expandWithPoint(&point);
    
    // Then reset
    mbr.reset();
    
    EXPECT_EQ(mbr.getMin(0), numeric_limits<float>::max());
    EXPECT_EQ(mbr.getMax(0), -numeric_limits<float>::max());
}

// KeyMBR Intersection Tests
TEST_F(KeyMBRTest, NonIntersectingMBRs) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    // Set up mbr1: box from (0,0) to (10,10)
    vector<double> p1_min = {0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (20,20) to (30,30)
    vector<double> p2_min = {20.0, 20.0};
    vector<double> p2_max = {30.0, 30.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    EXPECT_FALSE(mbr1.intersects(mbr2));
    EXPECT_FALSE(mbr2.intersects(mbr1));
}

TEST_F(KeyMBRTest, IntersectingMBRs) {
    KeyMBR mbr1(2, 32);
    KeyMBR mbr2(2, 32);
    
    // Set up mbr1: box from (0,0) to (10,10)
    vector<double> p1_min = {0.0, 0.0};
    vector<double> p1_max = {10.0, 10.0};
    mbr1.expandWithPoint(&p1_min);
    mbr1.expandWithPoint(&p1_max);
    
    // Set up mbr2: box from (5,5) to (15,15) - overlaps with mbr1
    vector<double> p2_min = {5.0, 5.0};
    vector<double> p2_max = {15.0, 15.0};
    mbr2.expandWithPoint(&p2_min);
    mbr2.expandWithPoint(&p2_max);
    
    EXPECT_TRUE(mbr1.intersects(mbr2));
    EXPECT_TRUE(mbr2.intersects(mbr1));
}

// KeyMBR Area and Edge Deltas Tests
TEST_F(KeyMBRTest, Area) {
    KeyMBR mbr(2, 32);
    double area = mbr.area();
    // Initial area could be infinity due to inverted bounds
    EXPECT_TRUE(!isnan(area));
}

TEST_F(KeyMBRTest, EdgeDeltas) {
    KeyMBR mbr(2, 32);
    double deltas = mbr.edgeDeltas();
    EXPECT_TRUE(!isnan(deltas));
}

// Utility Functions Tests
TEST(UtilityTest, MemoryFunctions) {
    size_t totalMem = getTotalSystemMemory();
    size_t availMem = getAvailableSystemMemory();
    
    EXPECT_GT(totalMem, 0);
    EXPECT_GT(availMem, 0);
    EXPECT_LE(availMem, totalMem);
}

TEST(UtilityTest, TimeMeasurement) {
    unsigned long time1 = GetTimeMicro64();
    // Small delay
    for(int i = 0; i < 1000000; i++) { /* spin */ }
    unsigned long time2 = GetTimeMicro64();
    
    EXPECT_GE(time2, time1);
}

// Dimensional Scaling Tests
TEST(DimensionalScalingTest, MemoryScaling) {
    vector<int> dimensions = {1, 2, 3, 5, 10, 20, 50};
    
    long prevMemUsed = 0;
    for (int dim : dimensions) {
        KeyMBR mbr(dim, 32);
        long memUsed = mbr.getMemoryUsed();
        
        // Memory should increase with dimensions
        if (prevMemUsed > 0) {
            EXPECT_GT(memUsed, prevMemUsed);
        }
        prevMemUsed = memUsed;
    }
}

// Performance Tests
TEST(PerformanceTest, MBRCreationPerformance) {
    const int NUM_ITERATIONS = 10000;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        KeyMBR mbr(2, 32);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    double avgTime = static_cast<double>(duration.count()) / NUM_ITERATIONS;
    
    // Just verify it completes in reasonable time (e.g., less than 10μs per creation)
    EXPECT_LT(avgTime, 10.0);
}

// Stress Tests
TEST(StressTest, ManyMBRs) {
    vector<KeyMBR*> mbrs;
    const int NUM_MBRS = 1000;
    
    // Should not throw or crash
    EXPECT_NO_THROW({
        for (int i = 0; i < NUM_MBRS; i++) {
            mbrs.push_back(new KeyMBR(2, 32));
        }
        
        // Clean up
        for (auto mbr : mbrs) {
            delete mbr;
        }
    });
}