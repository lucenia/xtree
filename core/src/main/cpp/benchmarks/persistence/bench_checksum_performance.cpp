/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Comprehensive Checksum Performance Benchmarks
 * Tests all checksum implementations in the persistence layer
 */

#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <random>
#include "../../src/persistence/checksums.h"

using namespace xtree::persist;
using namespace std::chrono;

class ChecksumBenchmark : public ::testing::Test {
protected:
    std::vector<std::vector<uint8_t>> test_data_;
    
    void SetUp() override {
        // Pre-generate test data of various sizes
        std::mt19937 rng(42);
        std::uniform_int_distribution<> byte_dist(0, 255);
        
        size_t sizes[] = {64, 256, 1024, 4096, 16384, 65536, 262144, 1048576};
        for (size_t size : sizes) {
            std::vector<uint8_t> data(size);
            for (size_t i = 0; i < size; i++) {
                data[i] = static_cast<uint8_t>(byte_dist(rng));
            }
            test_data_.push_back(std::move(data));
        }
    }
    
    void printSeparator(const std::string& title) {
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "  " << title << "\n";
        std::cout << std::string(70, '=') << "\n";
    }
    
    template<typename ChecksumType>
    double benchmark_checksum(const std::vector<uint8_t>& data, int iterations, 
                             const std::string& name) {
        // Warm up
        for (int i = 0; i < 100; i++) {
            ChecksumType::compute(data);
        }
        
        // Benchmark
        auto start = high_resolution_clock::now();
        typename ChecksumType::value_type checksum = 0;
        for (int i = 0; i < iterations; i++) {
            checksum = ChecksumType::compute(data);
        }
        auto end = high_resolution_clock::now();
        
        auto duration = duration_cast<microseconds>(end - start).count();
        double throughput = (data.size() * iterations * 1000000.0) / 
                           (duration * 1024.0 * 1024.0);
        
        std::cout << std::setw(12) << name << " | "
                  << std::setw(10) << data.size() << " bytes | "
                  << std::setw(8) << iterations << " iters | "
                  << std::setw(10) << duration << " µs | "
                  << std::fixed << std::setprecision(2) 
                  << std::setw(10) << throughput << " MB/s\n";
        
        return throughput;
    }
};

TEST_F(ChecksumBenchmark, CRC32C_Performance) {
    printSeparator("CRC32C Performance (Hardware Accelerated)");
    
    // Platform detection
    std::cout << "\nPlatform: ";
#if defined(__x86_64__) || defined(_M_X64)
    std::cout << "x86_64";
    if (CRC32C::has_sse42()) {
        std::cout << " (SSE4.2 CRC32 ENABLED)";
    } else {
        std::cout << " (Software fallback)";
    }
#elif defined(__aarch64__) || defined(_M_ARM64)
    std::cout << "ARM64";
    if (CRC32C::has_crc32()) {
        std::cout << " (ARM CRC32 ENABLED)";
    } else {
        std::cout << " (Software fallback)";
    }
#else
    std::cout << "Unknown (Software implementation)";
#endif
    std::cout << "\n\n";
    
    std::cout << "Algorithm    | Size       | Iterations | Time       | Throughput\n";
    std::cout << "-------------|------------|------------|------------|------------\n";
    
    struct TestCase {
        size_t size;
        int iterations;
    };
    
    TestCase cases[] = {
        {64, 1000000},
        {256, 1000000},
        {1024, 500000},
        {4096, 200000},
        {16384, 50000},
        {65536, 10000},
        {262144, 2500},
        {1048576, 500}
    };
    
    for (const auto& tc : cases) {
        // Find matching test data
        for (const auto& data : test_data_) {
            if (data.size() == tc.size) {
                benchmark_checksum<CRC32C>(data, tc.iterations, "CRC32C");
                break;
            }
        }
    }
}

TEST_F(ChecksumBenchmark, XXHash64_Performance) {
    printSeparator("XXHash64 Performance");
    
    std::cout << "\nXXHash64 - Fast non-cryptographic hash\n";
    std::cout << "Optimized for speed over collision resistance\n\n";
    
    std::cout << "Algorithm    | Size       | Iterations | Time       | Throughput\n";
    std::cout << "-------------|------------|------------|------------|------------\n";
    
    struct TestCase {
        size_t size;
        int iterations;
    };
    
    TestCase cases[] = {
        {64, 1000000},
        {256, 1000000},
        {1024, 500000},
        {4096, 200000},
        {16384, 50000},
        {65536, 10000},
        {262144, 2500},
        {1048576, 500}
    };
    
    for (const auto& tc : cases) {
        for (const auto& data : test_data_) {
            if (data.size() == tc.size) {
                benchmark_checksum<XXHash64>(data, tc.iterations, "XXHash64");
                break;
            }
        }
    }
}

TEST_F(ChecksumBenchmark, CRC64_Performance) {
    printSeparator("CRC64 Performance");
    
    std::cout << "\nCRC64 - ECMA-182 polynomial\n";
    std::cout << "Better error detection for large data blocks\n\n";
    
    std::cout << "Algorithm    | Size       | Iterations | Time       | Throughput\n";
    std::cout << "-------------|------------|------------|------------|------------\n";
    
    struct TestCase {
        size_t size;
        int iterations;
    };
    
    TestCase cases[] = {
        {64, 500000},
        {256, 500000},
        {1024, 250000},
        {4096, 100000},
        {16384, 25000},
        {65536, 5000},
        {262144, 1250},
        {1048576, 250}
    };
    
    for (const auto& tc : cases) {
        for (const auto& data : test_data_) {
            if (data.size() == tc.size) {
                benchmark_checksum<CRC64>(data, tc.iterations, "CRC64");
                break;
            }
        }
    }
}

TEST_F(ChecksumBenchmark, Adler32_Performance) {
    printSeparator("Adler32 Performance");
    
    std::cout << "\nAdler32 - Simple rolling checksum\n";
    std::cout << "Fast but weaker error detection\n\n";
    
    std::cout << "Algorithm    | Size       | Iterations | Time       | Throughput\n";
    std::cout << "-------------|------------|------------|------------|------------\n";
    
    struct TestCase {
        size_t size;
        int iterations;
    };
    
    TestCase cases[] = {
        {64, 2000000},
        {256, 2000000},
        {1024, 1000000},
        {4096, 400000},
        {16384, 100000},
        {65536, 20000},
        {262144, 5000},
        {1048576, 1000}
    };
    
    for (const auto& tc : cases) {
        for (const auto& data : test_data_) {
            if (data.size() == tc.size) {
                benchmark_checksum<Adler32>(data, tc.iterations, "Adler32");
                break;
            }
        }
    }
}

TEST_F(ChecksumBenchmark, AlgorithmComparison) {
    printSeparator("Checksum Algorithm Comparison");
    
    std::cout << "\nComparing all algorithms on 4KB blocks (typical page size):\n\n";
    
    const size_t BLOCK_SIZE = 4096;
    const int ITERATIONS = 100000;
    
    // Find 4KB test data
    const std::vector<uint8_t>* test_block = nullptr;
    for (const auto& data : test_data_) {
        if (data.size() == BLOCK_SIZE) {
            test_block = &data;
            break;
        }
    }
    
    if (!test_block) {
        FAIL() << "No 4KB test data found";
    }
    
    std::cout << "Algorithm    | Throughput  | Relative Speed | Use Case\n";
    std::cout << "-------------|-------------|----------------|--------------------\n";
    
    // Benchmark each algorithm
    double crc32c_tp = benchmark_checksum<CRC32C>(*test_block, ITERATIONS, "CRC32C");
    double xxhash_tp = benchmark_checksum<XXHash64>(*test_block, ITERATIONS, "XXHash64");
    double crc64_tp = benchmark_checksum<CRC64>(*test_block, ITERATIONS, "CRC64");
    double adler_tp = benchmark_checksum<Adler32>(*test_block, ITERATIONS, "Adler32");
    
    // Find fastest for relative speed calculation
    double max_tp = std::max({crc32c_tp, xxhash_tp, crc64_tp, adler_tp});
    
    std::cout << "\nSummary:\n";
    std::cout << std::fixed << std::setprecision(2);
    
    auto print_summary = [max_tp](const std::string& name, double tp, 
                                  const std::string& use_case) {
        std::cout << std::setw(12) << name << " | "
                  << std::setw(9) << tp << " MB/s | "
                  << std::setw(14) << (tp / max_tp * 100) << "% | "
                  << use_case << "\n";
    };
    
    print_summary("CRC32C", crc32c_tp, "WAL, critical data paths");
    print_summary("XXHash64", xxhash_tp, "Non-critical hashing");
    print_summary("CRC64", crc64_tp, "Large file integrity");
    print_summary("Adler32", adler_tp, "Legacy compatibility");
}

TEST_F(ChecksumBenchmark, CRC32C_HardwareVsSoftware) {
    printSeparator("CRC32C Hardware vs Software");
    
#if (defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__) || defined(_M_ARM64))
    const size_t TEST_SIZE = 65536; // 64KB
    const int ITERATIONS = 10000;
    
    // Find matching test data
    const std::vector<uint8_t>* test_data = nullptr;
    for (const auto& data : test_data_) {
        if (data.size() == TEST_SIZE) {
            test_data = &data;
            break;
        }
    }
    
    if (!test_data) {
        FAIL() << "No test data of size " << TEST_SIZE;
    }
    
    std::cout << "\nComparing hardware-accelerated vs software CRC32C:\n";
    std::cout << "Test size: " << TEST_SIZE << " bytes, " << ITERATIONS << " iterations\n\n";
    
    // Software benchmark
    auto start_sw = high_resolution_clock::now();
    uint32_t sw_result = 0;
    for (int i = 0; i < ITERATIONS; i++) {
        sw_result = CRC32C::software_crc32c(~0u, test_data->data(), 
                                            test_data->size()) ^ 0xFFFFFFFF;
    }
    auto end_sw = high_resolution_clock::now();
    auto sw_duration = duration_cast<microseconds>(end_sw - start_sw).count();
    
    // Hardware benchmark (if available)
    uint32_t hw_result = 0;
    auto hw_duration = sw_duration;
    bool hw_available = false;
    
#if defined(__x86_64__) || defined(_M_X64)
    if (CRC32C::has_sse42()) {
        hw_available = true;
        auto start_hw = high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; i++) {
            hw_result = CRC32C::hardware_crc32c(~0u, test_data->data(), 
                                                test_data->size()) ^ 0xFFFFFFFF;
        }
        auto end_hw = high_resolution_clock::now();
        hw_duration = duration_cast<microseconds>(end_hw - start_hw).count();
    }
#elif defined(__aarch64__) || defined(_M_ARM64)
    if (CRC32C::has_crc32()) {
        hw_available = true;
        auto start_hw = high_resolution_clock::now();
        for (int i = 0; i < ITERATIONS; i++) {
            hw_result = CRC32C::hardware_crc32c_arm(~0u, test_data->data(), 
                                                    test_data->size()) ^ 0xFFFFFFFF;
        }
        auto end_hw = high_resolution_clock::now();
        hw_duration = duration_cast<microseconds>(end_hw - start_hw).count();
    }
#endif
    
    double sw_throughput = (TEST_SIZE * ITERATIONS * 1000000.0) / 
                          (sw_duration * 1024.0 * 1024.0);
    std::cout << "Software implementation:\n";
    std::cout << "  Time: " << sw_duration << " µs\n";
    std::cout << "  Throughput: " << std::fixed << std::setprecision(2) 
              << sw_throughput << " MB/s\n";
    std::cout << "  Result: 0x" << std::hex << sw_result << std::dec << "\n";
    
    if (hw_available) {
        double hw_throughput = (TEST_SIZE * ITERATIONS * 1000000.0) / 
                              (hw_duration * 1024.0 * 1024.0);
        std::cout << "\nHardware implementation:\n";
        std::cout << "  Time: " << hw_duration << " µs\n";
        std::cout << "  Throughput: " << std::fixed << std::setprecision(2) 
                  << hw_throughput << " MB/s\n";
        std::cout << "  Result: 0x" << std::hex << hw_result << std::dec << "\n";
        
        double speedup = static_cast<double>(sw_duration) / hw_duration;
        std::cout << "\nHardware speedup: " << std::fixed << std::setprecision(2) 
                  << speedup << "x faster\n";
        
        // Verify results match
        EXPECT_EQ(sw_result, hw_result) << "Hardware and software results should match";
        
        // Expect significant speedup with hardware acceleration
        EXPECT_GT(speedup, 2.0) << "Hardware should be at least 2x faster";
    } else {
        std::cout << "\nHardware acceleration not available on this system\n";
    }
#else
    std::cout << "\nPlatform not supported for hardware acceleration comparison\n";
#endif
}

TEST_F(ChecksumBenchmark, StreamingPerformance) {
    printSeparator("Streaming Checksum Performance");
    
    std::cout << "\nTesting incremental update performance (1MB total, 4KB chunks):\n\n";
    
    const size_t TOTAL_SIZE = 1024 * 1024;  // 1MB
    const size_t CHUNK_SIZE = 4096;         // 4KB chunks
    const int ITERATIONS = 100;
    
    // Generate test data
    std::vector<uint8_t> data(TOTAL_SIZE);
    std::mt19937 rng(42);
    std::uniform_int_distribution<> byte_dist(0, 255);
    for (size_t i = 0; i < TOTAL_SIZE; i++) {
        data[i] = static_cast<uint8_t>(byte_dist(rng));
    }
    
    std::cout << "Algorithm    | Time (µs)  | Throughput  | Overhead vs One-shot\n";
    std::cout << "-------------|------------|-------------|--------------------\n";
    
    // CRC32C streaming
    {
        auto start = high_resolution_clock::now();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            CRC32C crc;
            for (size_t offset = 0; offset < TOTAL_SIZE; offset += CHUNK_SIZE) {
                crc.update(data.data() + offset, CHUNK_SIZE);
            }
            auto result = crc.finalize();
            (void)result;
        }
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        // Compare with one-shot
        auto oneshot_start = high_resolution_clock::now();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            auto result = CRC32C::compute(data);
            (void)result;
        }
        auto oneshot_end = high_resolution_clock::now();
        auto oneshot_duration = duration_cast<microseconds>(oneshot_end - oneshot_start).count();
        
        double throughput = (TOTAL_SIZE * ITERATIONS * 1000000.0) / 
                           (duration * 1024.0 * 1024.0);
        double overhead = ((double)duration / oneshot_duration - 1.0) * 100;
        
        std::cout << std::setw(12) << "CRC32C" << " | "
                  << std::setw(10) << duration << " | "
                  << std::fixed << std::setprecision(2) 
                  << std::setw(9) << throughput << " MB/s | "
                  << std::setw(17) << overhead << "%\n";
    }
    
    // XXHash64 streaming
    {
        auto start = high_resolution_clock::now();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            XXHash64 hash;
            for (size_t offset = 0; offset < TOTAL_SIZE; offset += CHUNK_SIZE) {
                hash.update(data.data() + offset, CHUNK_SIZE);
            }
            auto result = hash.finalize();
            (void)result;
        }
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start).count();
        
        // Compare with one-shot
        auto oneshot_start = high_resolution_clock::now();
        for (int iter = 0; iter < ITERATIONS; iter++) {
            auto result = XXHash64::compute(data);
            (void)result;
        }
        auto oneshot_end = high_resolution_clock::now();
        auto oneshot_duration = duration_cast<microseconds>(oneshot_end - oneshot_start).count();
        
        double throughput = (TOTAL_SIZE * ITERATIONS * 1000000.0) / 
                           (duration * 1024.0 * 1024.0);
        double overhead = ((double)duration / oneshot_duration - 1.0) * 100;
        
        std::cout << std::setw(12) << "XXHash64" << " | "
                  << std::setw(10) << duration << " | "
                  << std::fixed << std::setprecision(2) 
                  << std::setw(9) << throughput << " MB/s | "
                  << std::setw(17) << overhead << "%\n";
    }
}

TEST_F(ChecksumBenchmark, Summary) {
    printSeparator("Checksum Performance Summary");
    
    std::cout << "\n📊 Actual Measured Performance:\n\n";
    
    // Test on multiple data sizes to get real numbers
    struct TestSize {
        size_t size;
        const char* description;
        int iterations;
    };
    
    TestSize test_sizes[] = {
        {4096, "4KB (page size)", 100000},
        {65536, "64KB", 10000},
        {1048576, "1MB", 500}
    };
    
    for (const auto& ts : test_sizes) {
        std::cout << "Data size: " << ts.description << "\n";
        std::cout << "Algorithm    | Throughput  | Relative | Notes\n";
        std::cout << "-------------|-------------|----------|------------------------\n";
        
        // Find matching test data
        const std::vector<uint8_t>* test_data = nullptr;
        for (const auto& data : test_data_) {
            if (data.size() == ts.size) {
                test_data = &data;
                break;
            }
        }
        
        if (!test_data) {
            continue;
        }
        
        // Benchmark each algorithm
        double crc32c_tp = benchmark_checksum<CRC32C>(*test_data, ts.iterations, "CRC32C");
        double xxhash_tp = benchmark_checksum<XXHash64>(*test_data, ts.iterations, "XXHash64");
        double crc64_tp = benchmark_checksum<CRC64>(*test_data, ts.iterations / 4, "CRC64");
        double adler_tp = benchmark_checksum<Adler32>(*test_data, ts.iterations * 2, "Adler32");
        
        // Find fastest
        double max_tp = std::max({crc32c_tp, xxhash_tp, crc64_tp, adler_tp});
        
        // Print results with analysis
        auto print_result = [max_tp](const std::string& name, double tp, 
                                     const std::string& notes) {
            std::cout << std::setw(12) << name << " | "
                      << std::fixed << std::setprecision(1) 
                      << std::setw(9) << tp << " MB/s | "
                      << std::setw(7) << (int)(tp / max_tp * 100) << "% | "
                      << notes << "\n";
        };
        
        std::string crc32c_notes = "Hardware: ";
#if defined(__x86_64__) || defined(_M_X64)
        crc32c_notes += CRC32C::has_sse42() ? "SSE4.2" : "Software";
#elif defined(__aarch64__) || defined(_M_ARM64)
        crc32c_notes += CRC32C::has_crc32() ? "ARM CRC32" : "Software";
#else
        crc32c_notes += "Software";
#endif
        
        print_result("CRC32C", crc32c_tp, crc32c_notes);
        print_result("XXHash64", xxhash_tp, "Non-cryptographic");
        print_result("CRC64", crc64_tp, "Strong detection");
        print_result("Adler32", adler_tp, "Weak detection");
        
        std::cout << "\n";
    }
    
    std::cout << "🎯 Performance-Based Recommendations:\n";
    std::cout << "  1. XXHash64 is fastest overall - use for non-critical hashing\n";
    std::cout << "  2. CRC32C with hardware is best for persistence (good speed + reliability)\n";
    std::cout << "  3. CRC64 only when you need maximum error detection\n";
    std::cout << "  4. Avoid Adler32 - weak detection not worth the speed\n";
    
    // Show actual targets vs achieved
    std::cout << "\n✓ Performance Targets:\n";
    
    // Get 4KB performance for targets
    const std::vector<uint8_t>* page_data = nullptr;
    for (const auto& data : test_data_) {
        if (data.size() == 4096) {
            page_data = &data;
            break;
        }
    }
    
    if (page_data) {
        double crc32c_4k = benchmark_checksum<CRC32C>(*page_data, 10000, "CRC32C");
        std::cout << "  Target: CRC32C >1000 MB/s    Actual: " 
                  << std::fixed << std::setprecision(0) << crc32c_4k << " MB/s "
                  << (crc32c_4k > 1000 ? "✓" : "✗") << "\n";
                  
        double xxhash_4k = benchmark_checksum<XXHash64>(*page_data, 10000, "XXHash64");
        std::cout << "  Target: XXHash64 >5000 MB/s  Actual: " 
                  << std::fixed << std::setprecision(0) << xxhash_4k << " MB/s "
                  << (xxhash_4k > 5000 ? "✓" : "✗") << "\n";
    }
    
    std::cout << "\n" << std::string(70, '=') << "\n\n";
}