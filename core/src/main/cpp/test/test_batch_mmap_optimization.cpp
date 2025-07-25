#include "../src/fileio/cow_mmap_manager.hpp"
#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <memory>
#include <iomanip>
#include <functional>

using namespace xtree;
using namespace std::chrono;

// Test data generator
class TestDataGenerator {
private:
    std::mt19937 rng_;
    std::uniform_int_distribution<size_t> size_dist_;
    std::uniform_int_distribution<char> data_dist_;
    
public:
    TestDataGenerator() : rng_(std::random_device{}()), size_dist_(512, 8192), data_dist_(0, 255) {}
    
    std::vector<std::pair<size_t, std::pair<std::unique_ptr<char[]>, size_t>>> 
    generate_regions(size_t num_regions, size_t file_size) {
        
        std::vector<std::pair<size_t, std::pair<std::unique_ptr<char[]>, size_t>>> regions;
        regions.reserve(num_regions);
        
        for (size_t i = 0; i < num_regions; ++i) {
            size_t region_size = size_dist_(rng_);
            size_t offset = (i * file_size) / num_regions;
            
            auto data = std::make_unique<char[]>(region_size);
            for (size_t j = 0; j < region_size; ++j) {
                data[j] = static_cast<char>(data_dist_(rng_));
            }
            
            regions.emplace_back(offset, std::make_pair(std::move(data), region_size));
        }
        
        return regions;
    }
};

// Performance test suite
class BatchMMapPerformanceTest {
private:
    TestDataGenerator generator_;
    
    // Convert unique_ptr regions to raw pointer format for API
    std::vector<std::pair<size_t, std::pair<const void*, size_t>>>
    convert_to_raw_regions(const std::vector<std::pair<size_t, std::pair<std::unique_ptr<char[]>, size_t>>>& regions) {
        
        std::vector<std::pair<size_t, std::pair<const void*, size_t>>> raw_regions;
        raw_regions.reserve(regions.size());
        
        for (const auto& region : regions) {
            raw_regions.emplace_back(region.first, 
                                   std::make_pair(region.second.first.get(), region.second.second));
        }
        
        return raw_regions;
    }
    
public:
    struct BenchmarkResult {
        double duration_ms;
        double throughput_mbps;
        size_t operations;
        size_t bytes_written;
        std::string method_name;
        
        void print() const {
            std::cout << method_name << ":\n";
            std::cout << "  Duration: " << duration_ms << " ms\n";
            std::cout << "  Throughput: " << throughput_mbps << " MB/s\n";
            std::cout << "  Operations: " << operations << "\n";
            std::cout << "  Bytes: " << (bytes_written / 1024) << " KB\n\n";
        }
    };
    
    BenchmarkResult benchmark_method(
        const std::string& filename,
        const std::vector<std::pair<size_t, std::pair<std::unique_ptr<char[]>, size_t>>>& regions,
        const std::string& method_name,
        std::function<bool(COWMemoryMappedFile*, const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>&)> method) {
        
        // Calculate total size
        size_t total_size = 0;
        for (const auto& region : regions) {
            total_size = std::max(total_size, region.first + region.second.second);
        }
        
        size_t total_bytes = 0;
        for (const auto& region : regions) {
            total_bytes += region.second.second;
        }
        
        // Create memory-mapped file
        COWMemoryMappedFile mmap_file(filename, total_size + 1024 * 1024, false);
        if (!mmap_file.map()) {
            std::cerr << "Failed to create memory-mapped file for " << method_name << std::endl;
            return {"Failed", 0.0, 0, 0, method_name};
        }
        
        // Convert to raw regions
        auto raw_regions = convert_to_raw_regions(regions);
        
        // Benchmark the method
        auto start = high_resolution_clock::now();
        
        bool success = method(&mmap_file, raw_regions);
        
        auto end = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(end - start);
        
        if (!success) {
            std::cerr << "Method " << method_name << " failed!" << std::endl;
            return {"Failed", 0.0, 0, 0, method_name};
        }
        
        double duration_ms = duration.count() / 1000.0;
        double throughput_mbps = (total_bytes / 1024.0 / 1024.0) / (duration.count() / 1000000.0);
        
        // Clean up
        mmap_file.unmap();
        
        return {duration_ms, throughput_mbps, regions.size(), total_bytes, method_name};
    }
    
    void run_comprehensive_benchmark() {
        std::cout << "=== Batch MMap Optimization Performance Test ===\n\n";
        
        // Test configurations
        std::vector<std::pair<size_t, std::string>> test_configs = {
            {50, "Small batch (50 regions)"},
            {200, "Medium batch (200 regions)"},
            {500, "Large batch (500 regions)"}
        };
        
        for (const auto& [num_regions, config_name] : test_configs) {
            std::cout << "Testing " << config_name << ":\n";
            std::cout << std::string(40, '-') << "\n";
            
            // Generate test data
            auto regions = generator_.generate_regions(num_regions, 64 * 1024 * 1024); // 64MB file
            
            // Test different methods
            std::vector<BenchmarkResult> results;
            
            // 1. Original batch method
            results.push_back(benchmark_method(
                "test_original_batch.tmp",
                regions,
                "Original Batch",
                [](COWMemoryMappedFile* file, const auto& regions) {
                    return file->write_regions_batch(regions);
                }
            ));
            
            // 2. Vectorized method
            results.push_back(benchmark_method(
                "test_vectorized.tmp",
                regions,
                "Vectorized I/O",
                [](COWMemoryMappedFile* file, const auto& regions) {
                    return file->write_regions_vectorized(regions);
                }
            ));
            
            // 3. Optimized batch method
            results.push_back(benchmark_method(
                "test_optimized_batch.tmp", 
                regions,
                "Optimized Batch",
                [](COWMemoryMappedFile* file, const auto& regions) {
                    return file->write_regions_batch_optimized(regions);
                }
            ));
            
            // Print results
            for (const auto& result : results) {
                result.print();
            }
            
            // Calculate improvements
            if (results.size() >= 3 && results[0].duration_ms > 0) {
                double vectorized_improvement = (results[0].throughput_mbps / results[1].throughput_mbps) * 100 - 100;
                double optimized_improvement = (results[0].throughput_mbps / results[2].throughput_mbps) * 100 - 100;
                
                std::cout << "Performance Improvements:\n";
                std::cout << "  Vectorized I/O: " << std::fixed << std::setprecision(1) << vectorized_improvement << "%";
#ifdef _WIN32
                std::cout << " (Windows: falls back to batch)";
#endif
                std::cout << "\n";
                std::cout << "  Optimized Batch: " << std::fixed << std::setprecision(1) << optimized_improvement << "%\n\n";
            }
            
            std::cout << std::string(60, '=') << "\n\n";
        }
    }
};

int main() {
    try {
        BatchMMapPerformanceTest test;
        test.run_comprehensive_benchmark();
        
        std::cout << "Benchmark completed successfully!\n";
        std::cout << "This demonstrates how batch mmap optimization improves\n";
        std::cout << "performance through region merging and vectorized I/O.\n";
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}