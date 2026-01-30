/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Test to verify CRC32C hardware acceleration
 */

#include <gtest/gtest.h>
#include <vector>
#include <chrono>
#include <iostream>
#include <iomanip>
#include "persistence/checksums.h"

using namespace xtree::persist;
using namespace std::chrono;

class CRC32CHardwareTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CRC32CHardwareTest, DetectHardwareSupport) {
    std::cout << "\n=== CRC32C Hardware Detection ===" << std::endl;
    
    // Platform detection
    std::cout << "Platform: ";
#if defined(__x86_64__) || defined(_M_X64)
    std::cout << "x86_64";
    #ifdef __SSE4_2__
    std::cout << " (SSE4.2 available at compile time)";
    #endif
#elif defined(__aarch64__) || defined(_M_ARM64)
    std::cout << "ARM64";
    #ifdef __ARM_FEATURE_CRC32
    std::cout << " (CRC32 feature available at compile time)";
    #endif
#else
    std::cout << "Unknown";
#endif
    std::cout << std::endl;
    
    // Runtime detection
    std::cout << "Hardware CRC32: ";
    bool hw_available = false;
#if defined(__x86_64__) || defined(_M_X64)
    if (CRC32C::has_sse42()) {
        std::cout << "SSE4.2 CRC32 instructions ENABLED" << std::endl;
        hw_available = true;
    } else {
        std::cout << "Using software implementation (no SSE4.2)" << std::endl;
    }
#elif defined(__aarch64__) || defined(_M_ARM64)
    if (CRC32C::has_crc32()) {
        std::cout << "ARMv8 CRC32 instructions ENABLED" << std::endl;
        hw_available = true;
    } else {
        std::cout << "Using software implementation (no ARM CRC32)" << std::endl;
    }
#else
    std::cout << "Using software implementation" << std::endl;
#endif
    
    // On Apple Silicon, we expect hardware support
#ifdef __APPLE__
    #if defined(__aarch64__) || defined(__arm64__)
    EXPECT_TRUE(hw_available) << "Apple Silicon should have CRC32 hardware support";
    #endif
#endif
}

TEST_F(CRC32CHardwareTest, ComparePerformance) {
    const size_t data_size = 1024 * 1024; // 1MB
    std::vector<uint8_t> data(data_size);
    for (size_t i = 0; i < data_size; i++) {
        data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    
    // Software implementation
    auto start_sw = high_resolution_clock::now();
    uint32_t sw_result = CRC32C::software_crc32c(~0u, data.data(), data.size()) ^ 0xFFFFFFFF;
    auto end_sw = high_resolution_clock::now();
    auto sw_duration = duration_cast<microseconds>(end_sw - start_sw).count();
    
    // Hardware implementation (if available)
    uint32_t hw_result = 0;
    int64_t hw_duration = -1;
    
#if defined(__x86_64__) || defined(_M_X64)
    if (CRC32C::has_sse42()) {
        auto start_hw = high_resolution_clock::now();
        hw_result = CRC32C::hardware_crc32c(~0u, data.data(), data.size()) ^ 0xFFFFFFFF;
        auto end_hw = high_resolution_clock::now();
        hw_duration = duration_cast<microseconds>(end_hw - start_hw).count();
    }
#elif defined(__aarch64__) || defined(_M_ARM64)
    if (CRC32C::has_crc32()) {
        auto start_hw = high_resolution_clock::now();
        hw_result = CRC32C::hardware_crc32c_arm(~0u, data.data(), data.size()) ^ 0xFFFFFFFF;
        auto end_hw = high_resolution_clock::now();
        hw_duration = duration_cast<microseconds>(end_hw - start_hw).count();
    }
#endif
    
    std::cout << "\n=== Performance Comparison (1MB data) ===" << std::endl;
    std::cout << "Software: " << sw_duration << " µs (CRC: 0x" 
              << std::hex << sw_result << std::dec << ")" << std::endl;
    
    if (hw_duration >= 0) {
        std::cout << "Hardware: " << hw_duration << " µs (CRC: 0x" 
                  << std::hex << hw_result << std::dec << ")" << std::endl;
        
        if (hw_duration > 0) {
            double speedup = static_cast<double>(sw_duration) / hw_duration;
            std::cout << "Speedup: " << std::fixed << std::setprecision(2) 
                      << speedup << "x faster" << std::endl;
        }
        
        // Verify results match
        EXPECT_EQ(sw_result, hw_result) << "Hardware and software CRC32C should match";
    } else {
        std::cout << "Hardware: Not available" << std::endl;
    }
}

TEST_F(CRC32CHardwareTest, BenchmarkThroughput) {
    std::cout << "\n=== Throughput Benchmark ===" << std::endl;
    
    struct TestCase {
        size_t size;
        const char* label;
    };
    
    TestCase sizes[] = {
        {64, "64B"},
        {1024, "1KB"},
        {16384, "16KB"},
        {65536, "64KB"},
        {1048576, "1MB"},
    };
    
    for (const auto& tc : sizes) {
        std::vector<uint8_t> data(tc.size);
        for (size_t i = 0; i < tc.size; i++) {
            data[i] = static_cast<uint8_t>(i & 0xFF);
        }
        
        // Run multiple iterations for better accuracy
        int iterations = tc.size < 1024 ? 100000 : (tc.size < 65536 ? 10000 : 1000);
        
        auto start = high_resolution_clock::now();
        uint32_t crc = 0;
        for (int i = 0; i < iterations; i++) {
            crc = CRC32C::compute(data.data(), data.size());
        }
        auto end = high_resolution_clock::now();
        
        auto duration = duration_cast<microseconds>(end - start).count();
        double throughput = (tc.size * iterations * 1000000.0) / (duration * 1024.0 * 1024.0);
        
        std::cout << std::setw(8) << tc.label << ": " 
                  << std::fixed << std::setprecision(2) << std::setw(10) 
                  << throughput << " MB/s"
                  << " (CRC: 0x" << std::hex << crc << std::dec << ")"
                  << std::endl;
    }
}