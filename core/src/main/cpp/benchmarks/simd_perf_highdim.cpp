#include <gtest/gtest.h>
#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <iomanip>
#include <algorithm>
#include "../src/util/cpu_features.h"
#include "../src/util/float_utils.h"

// Forward declarations
namespace xtree {
    typedef bool (*intersects_func_t)(const int32_t*, const int32_t*, int);
    typedef void (*expand_func_t)(int32_t*, const int32_t*, int);
    typedef void (*expand_point_func_t)(int32_t*, const double*, int);
    
    intersects_func_t get_optimal_intersects_func();
    expand_func_t get_optimal_expand_func();
    expand_point_func_t get_optimal_expand_point_func();
    
    namespace simd_impl {
        // Scalar implementations
        bool intersects_scalar(const int32_t* box1, const int32_t* box2, int dimensions);
        void expand_scalar(int32_t* target, const int32_t* source, int dimensions);
        void expand_point_scalar(int32_t* box, const double* point, int dimensions);
        
        // SSE2 implementations
        bool intersects_sse2(const int32_t* box1, const int32_t* box2, int dimensions);
        void expand_sse2(int32_t* target, const int32_t* source, int dimensions);
        void expand_point_sse2(int32_t* box, const double* point, int dimensions);
        
        // AVX2 implementations
        bool intersects_avx2(const int32_t* box1, const int32_t* box2, int dimensions);
        void expand_avx2(int32_t* target, const int32_t* source, int dimensions);
        void expand_point_avx2(int32_t* box, const double* point, int dimensions);
    }
}

struct PerfResult {
    double scalar_time;
    double simd_time;
    double speedup;
    std::string simd_type;
};

class SIMDHighDimBenchmark : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SIMDHighDimBenchmark, PerformanceAnalysis) {
    using namespace xtree;
    
    // Get CPU features
    const auto& features = CPUFeatures::get();
    std::cout << "CPU Features: SSE2=" << features.has_sse2 
              << " AVX2=" << features.has_avx2 << std::endl << std::endl;
    
    // Test with a comprehensive range of dimensions
    std::vector<int> dimensions_to_test = {1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 16, 20, 24, 32, 48, 64, 96, 128};
    const int iterations = 100000;
    const int num_test_cases = 1000;
    
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> real_dist(-1000.0, 1000.0);
    
    // Get optimal functions
    auto optimal_intersects = get_optimal_intersects_func();
    auto optimal_expand = get_optimal_expand_func();
    auto optimal_expand_point = get_optimal_expand_point_func();
    
    // Determine SIMD type being used
    std::string simd_type = "Scalar";
    if (features.has_avx2 && optimal_intersects == simd_impl::intersects_avx2) {
        simd_type = "AVX2";
    } else if (features.has_sse2 && optimal_intersects == simd_impl::intersects_sse2) {
        simd_type = "SSE2";
    }
    
    std::cout << "Using SIMD: " << simd_type << std::endl << std::endl;
    
    // Print header
    std::cout << std::setw(10) << "Dimensions" 
              << std::setw(20) << "Operation"
              << std::setw(15) << "Scalar (μs)"
              << std::setw(15) << simd_type + " (μs)"
              << std::setw(12) << "Speedup"
              << std::setw(15) << "Recommendation" << std::endl;
    std::cout << std::string(92, '-') << std::endl;
    
    for (int dimensions : dimensions_to_test) {
        // Generate test data for intersects/expand
        std::vector<std::vector<int32_t>> boxes1(num_test_cases);
        std::vector<std::vector<int32_t>> boxes2(num_test_cases);
        std::vector<std::vector<double>> points(num_test_cases);
        
        for (int i = 0; i < num_test_cases; i++) {
            boxes1[i].resize(dimensions * 2);
            boxes2[i].resize(dimensions * 2);
            points[i].resize(dimensions);
            
            for (int d = 0; d < dimensions; d++) {
                // Generate boxes
                float min1 = real_dist(rng);
                float max1 = min1 + std::abs(real_dist(rng));
                boxes1[i][d * 2] = floatToSortableInt(min1);
                boxes1[i][d * 2 + 1] = floatToSortableInt(max1);
                
                float min2 = real_dist(rng);
                float max2 = min2 + std::abs(real_dist(rng));
                boxes2[i][d * 2] = floatToSortableInt(min2);
                boxes2[i][d * 2 + 1] = floatToSortableInt(max2);
                
                // Generate point
                points[i][d] = real_dist(rng);
            }
        }
        
        // Test INTERSECTS
        {
            // Scalar
            auto start = std::chrono::high_resolution_clock::now();
            int scalar_matches = 0;
            for (int i = 0; i < iterations; i++) {
                const auto& box1 = boxes1[i % num_test_cases];
                const auto& box2 = boxes2[i % num_test_cases];
                if (simd_impl::intersects_scalar(box1.data(), box2.data(), dimensions)) {
                    scalar_matches++;
                }
            }
            auto scalar_time = std::chrono::high_resolution_clock::now() - start;
            
            // SIMD
            start = std::chrono::high_resolution_clock::now();
            int simd_matches = 0;
            for (int i = 0; i < iterations; i++) {
                const auto& box1 = boxes1[i % num_test_cases];
                const auto& box2 = boxes2[i % num_test_cases];
                if (optimal_intersects(box1.data(), box2.data(), dimensions)) {
                    simd_matches++;
                }
            }
            auto simd_time = std::chrono::high_resolution_clock::now() - start;
            
            double scalar_us = std::chrono::duration_cast<std::chrono::microseconds>(scalar_time).count();
            double simd_us = std::chrono::duration_cast<std::chrono::microseconds>(simd_time).count();
            double speedup = scalar_us / simd_us;
            
            std::cout << std::setw(10) << dimensions
                      << std::setw(20) << "intersects"
                      << std::setw(15) << std::fixed << std::setprecision(0) << scalar_us
                      << std::setw(15) << simd_us
                      << std::setw(12) << std::fixed << std::setprecision(2) << speedup << "x"
                      << std::setw(15) << (speedup > 1.1 ? "Use SIMD" : "Use Scalar") << std::endl;
            
            if (scalar_matches != simd_matches) {
                std::cerr << "ERROR: Intersects results don't match!" << std::endl;
                FAIL() << "Intersects results don't match!";
            }
        }
        
        // Test EXPAND
        {
            // Make copies for testing
            std::vector<std::vector<int32_t>> target_scalar(num_test_cases);
            std::vector<std::vector<int32_t>> target_simd(num_test_cases);
            for (int i = 0; i < num_test_cases; i++) {
                target_scalar[i] = boxes1[i];
                target_simd[i] = boxes1[i];
            }
            
            // Scalar
            auto start = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < iterations; i++) {
                int idx = i % num_test_cases;
                simd_impl::expand_scalar(target_scalar[idx].data(), boxes2[idx].data(), dimensions);
            }
            auto scalar_time = std::chrono::high_resolution_clock::now() - start;
            
            // SIMD
            start = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < iterations; i++) {
                int idx = i % num_test_cases;
                optimal_expand(target_simd[idx].data(), boxes2[idx].data(), dimensions);
            }
            auto simd_time = std::chrono::high_resolution_clock::now() - start;
            
            double scalar_us = std::chrono::duration_cast<std::chrono::microseconds>(scalar_time).count();
            double simd_us = std::chrono::duration_cast<std::chrono::microseconds>(simd_time).count();
            double speedup = scalar_us / simd_us;
            
            std::cout << std::setw(10) << ""
                      << std::setw(20) << "expand"
                      << std::setw(15) << std::fixed << std::setprecision(0) << scalar_us
                      << std::setw(15) << simd_us
                      << std::setw(12) << std::fixed << std::setprecision(2) << speedup << "x"
                      << std::setw(15) << (speedup > 1.1 ? "Use SIMD" : "Use Scalar") << std::endl;
        }
        
        // Test EXPAND_POINT
        {
            // Make copies for testing
            std::vector<std::vector<int32_t>> box_scalar(num_test_cases);
            std::vector<std::vector<int32_t>> box_simd(num_test_cases);
            for (int i = 0; i < num_test_cases; i++) {
                box_scalar[i] = boxes1[i];
                box_simd[i] = boxes1[i];
            }
            
            // Scalar
            auto start = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < iterations; i++) {
                int idx = i % num_test_cases;
                simd_impl::expand_point_scalar(box_scalar[idx].data(), points[idx].data(), dimensions);
            }
            auto scalar_time = std::chrono::high_resolution_clock::now() - start;
            
            // SIMD
            start = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < iterations; i++) {
                int idx = i % num_test_cases;
                optimal_expand_point(box_simd[idx].data(), points[idx].data(), dimensions);
            }
            auto simd_time = std::chrono::high_resolution_clock::now() - start;
            
            double scalar_us = std::chrono::duration_cast<std::chrono::microseconds>(scalar_time).count();
            double simd_us = std::chrono::duration_cast<std::chrono::microseconds>(simd_time).count();
            double speedup = scalar_us / simd_us;
            
            std::cout << std::setw(10) << ""
                      << std::setw(20) << "expand_point"
                      << std::setw(15) << std::fixed << std::setprecision(0) << scalar_us
                      << std::setw(15) << simd_us
                      << std::setw(12) << std::fixed << std::setprecision(2) << speedup << "x"
                      << std::setw(15) << (speedup > 1.1 ? "Use SIMD" : "Use Scalar") << std::endl;
        }
        
        std::cout << std::endl;
    }
    
    // Track crossover points
    struct CrossoverInfo {
        int first_beneficial = -1;  // First dimension where SIMD is beneficial
        int last_beneficial = -1;   // Last dimension where SIMD is beneficial
        double best_speedup = 0.0;
        int best_dimension = 0;
        std::vector<std::pair<int, double>> all_speedups;
    };
    
    CrossoverInfo intersects_info, expand_info, expand_point_info;
    
    // Re-run tests to collect crossover data
    for (int dimensions : dimensions_to_test) {
        // Generate test data
        std::vector<std::vector<int32_t>> boxes1(num_test_cases);
        std::vector<std::vector<int32_t>> boxes2(num_test_cases);
        std::vector<std::vector<double>> points(num_test_cases);
        
        for (int i = 0; i < num_test_cases; i++) {
            boxes1[i].resize(dimensions * 2);
            boxes2[i].resize(dimensions * 2);
            points[i].resize(dimensions);
            
            for (int d = 0; d < dimensions; d++) {
                float min1 = real_dist(rng);
                float max1 = min1 + std::abs(real_dist(rng));
                boxes1[i][d * 2] = floatToSortableInt(min1);
                boxes1[i][d * 2 + 1] = floatToSortableInt(max1);
                
                float min2 = real_dist(rng);
                float max2 = min2 + std::abs(real_dist(rng));
                boxes2[i][d * 2] = floatToSortableInt(min2);
                boxes2[i][d * 2 + 1] = floatToSortableInt(max2);
                
                points[i][d] = real_dist(rng);
            }
        }
        
        // Test each operation
        auto test_operation = [&](auto scalar_func, auto simd_func, CrossoverInfo& info, const std::string& name) {
            if (name == "intersects") {
                auto start = std::chrono::high_resolution_clock::now();
                for (int i = 0; i < iterations; i++) {
                    scalar_func(boxes1[i % num_test_cases].data(), boxes2[i % num_test_cases].data(), dimensions);
                }
                auto scalar_time = std::chrono::high_resolution_clock::now() - start;
                
                start = std::chrono::high_resolution_clock::now();
                for (int i = 0; i < iterations; i++) {
                    simd_func(boxes1[i % num_test_cases].data(), boxes2[i % num_test_cases].data(), dimensions);
                }
                auto simd_time = std::chrono::high_resolution_clock::now() - start;
                
                double speedup = (double)scalar_time.count() / simd_time.count();
                info.all_speedups.push_back({dimensions, speedup});
                
                if (speedup > 1.05) {  // 5% improvement threshold
                    if (info.first_beneficial == -1) info.first_beneficial = dimensions;
                    info.last_beneficial = dimensions;
                    if (speedup > info.best_speedup) {
                        info.best_speedup = speedup;
                        info.best_dimension = dimensions;
                    }
                }
            }
        };
        
        test_operation(simd_impl::intersects_scalar, optimal_intersects, intersects_info, "intersects");
        
        // For expand operations, we need copies
        std::vector<std::vector<int32_t>> target_scalar(num_test_cases);
        std::vector<std::vector<int32_t>> target_simd(num_test_cases);
        for (int i = 0; i < num_test_cases; i++) {
            target_scalar[i] = boxes1[i];
            target_simd[i] = boxes1[i];
        }
        
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            int idx = i % num_test_cases;
            simd_impl::expand_scalar(target_scalar[idx].data(), boxes2[idx].data(), dimensions);
        }
        auto scalar_time = std::chrono::high_resolution_clock::now() - start;
        
        start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            int idx = i % num_test_cases;
            optimal_expand(target_simd[idx].data(), boxes2[idx].data(), dimensions);
        }
        auto simd_time = std::chrono::high_resolution_clock::now() - start;
        
        double expand_speedup = (double)scalar_time.count() / simd_time.count();
        expand_info.all_speedups.push_back({dimensions, expand_speedup});
        if (expand_speedup > 1.05) {
            if (expand_info.first_beneficial == -1) expand_info.first_beneficial = dimensions;
            expand_info.last_beneficial = dimensions;
            if (expand_speedup > expand_info.best_speedup) {
                expand_info.best_speedup = expand_speedup;
                expand_info.best_dimension = dimensions;
            }
        }
        
        // For expand_point
        std::vector<std::vector<int32_t>> box_scalar(num_test_cases);
        std::vector<std::vector<int32_t>> box_simd(num_test_cases);
        for (int i = 0; i < num_test_cases; i++) {
            box_scalar[i] = boxes1[i];
            box_simd[i] = boxes1[i];
        }
        
        start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            int idx = i % num_test_cases;
            simd_impl::expand_point_scalar(box_scalar[idx].data(), points[idx].data(), dimensions);
        }
        scalar_time = std::chrono::high_resolution_clock::now() - start;
        
        start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            int idx = i % num_test_cases;
            optimal_expand_point(box_simd[idx].data(), points[idx].data(), dimensions);
        }
        simd_time = std::chrono::high_resolution_clock::now() - start;
        
        double expand_point_speedup = (double)scalar_time.count() / simd_time.count();
        expand_point_info.all_speedups.push_back({dimensions, expand_point_speedup});
        if (expand_point_speedup > 1.05) {
            if (expand_point_info.first_beneficial == -1) expand_point_info.first_beneficial = dimensions;
            expand_point_info.last_beneficial = dimensions;
            if (expand_point_speedup > expand_point_info.best_speedup) {
                expand_point_info.best_speedup = expand_point_speedup;
                expand_point_info.best_dimension = dimensions;
            }
        }
    }
    
    // Summary of crossover points
    std::cout << "\nSUMMARY - Analysis for " << simd_type << ":" << std::endl;
    std::cout << "==========================================" << std::endl;
    
    auto print_analysis = [](const std::string& op_name, const CrossoverInfo& info) {
        std::cout << "\n" << op_name << ":" << std::endl;
        if (info.first_beneficial == -1) {
            std::cout << "  ❌ SIMD is NEVER beneficial (always use scalar)" << std::endl;
        } else {
            std::cout << "  ✓ SIMD beneficial for dimensions: " << info.first_beneficial 
                      << " to " << info.last_beneficial << std::endl;
            std::cout << "  Best speedup: " << std::fixed << std::setprecision(2) 
                      << info.best_speedup << "x at " << info.best_dimension << " dimensions" << std::endl;
            
            // Find ranges where SIMD is beneficial
            std::cout << "  Recommended ranges: ";
            bool in_range = false;
            int range_start = -1;
            for (size_t i = 0; i < info.all_speedups.size(); i++) {
                if (info.all_speedups[i].second > 1.05) {
                    if (!in_range) {
                        range_start = info.all_speedups[i].first;
                        in_range = true;
                    }
                } else if (in_range) {
                    std::cout << "[" << range_start << "-" << info.all_speedups[i-1].first << "] ";
                    in_range = false;
                }
            }
            if (in_range) {
                std::cout << "[" << range_start << "-" << info.all_speedups.back().first << "]";
            }
            std::cout << std::endl;
        }
    };
    
    print_analysis("intersects", intersects_info);
    print_analysis("expand", expand_info);
    print_analysis("expand_point", expand_point_info);
    
    std::cout << "\nRECOMMENDED IMPLEMENTATION:" << std::endl;
    std::cout << "===========================" << std::endl;
    
    // For intersects
    if (intersects_info.first_beneficial == -1) {
        std::cout << "intersects: Always use scalar (no SIMD benefit detected)" << std::endl;
    } else {
        std::cout << "intersects: Use SIMD for dimensions in ranges: ";
        bool in_range = false;
        int range_start = -1;
        for (size_t i = 0; i < intersects_info.all_speedups.size(); i++) {
            if (intersects_info.all_speedups[i].second > 1.05) {
                if (!in_range) {
                    range_start = intersects_info.all_speedups[i].first;
                    in_range = true;
                }
            } else if (in_range) {
                std::cout << "[" << range_start << "-" << intersects_info.all_speedups[i-1].first << "] ";
                in_range = false;
            }
        }
        if (in_range) {
            std::cout << "[" << range_start << "+]";
        }
        std::cout << std::endl;
    }
    
    // Similar for expand and expand_point
    if (expand_info.first_beneficial != -1) {
        std::cout << "expand: Use SIMD for dimensions " << expand_info.first_beneficial 
                  << "-" << expand_info.last_beneficial << std::endl;
    } else {
        std::cout << "expand: Always use scalar" << std::endl;
    }
    
    if (expand_point_info.first_beneficial != -1) {
        std::cout << "expand_point: Use SIMD for dimensions " << expand_point_info.first_beneficial 
                  << "-" << expand_point_info.last_beneficial << std::endl;
    } else {
        std::cout << "expand_point: Always use scalar" << std::endl;
    }
    
}