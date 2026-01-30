/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Runtime CPU feature detection for SIMD optimizations
 */

#pragma once

#include <cstdint>

namespace xtree {

// CPU feature flags
struct CPUFeatures {
    bool has_sse2 = false;
    bool has_sse42 = false;
    bool has_avx = false;
    bool has_avx2 = false;
    bool has_neon = false;
    
    static const CPUFeatures& get();
    
private:
    CPUFeatures();
    void detect_features();
};

// Function pointer types for different implementations
typedef bool (*intersects_func_t)(const int32_t* box1, const int32_t* box2, int dimensions);
typedef void (*expand_func_t)(int32_t* target, const int32_t* source, int dimensions);
typedef void (*expand_point_func_t)(int32_t* box, const double* point, int dimensions);

// Get optimal function pointers based on CPU features
intersects_func_t get_optimal_intersects_func();
expand_func_t get_optimal_expand_func();
expand_point_func_t get_optimal_expand_point_func();

} // namespace xtree