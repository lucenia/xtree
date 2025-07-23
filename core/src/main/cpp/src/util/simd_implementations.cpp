/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * SIMD implementations for MBR operations
 */

#include "cpu_features.h"
#include "../keymbr.h"
#include "float_utils.h"
#include <cstring>
#include <algorithm>
#include <iostream>

// Include SIMD headers if available
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
    #include <immintrin.h>
#elif defined(__aarch64__) || defined(__arm64__)
    #include <arm_neon.h>
#endif

namespace xtree {

// Function pointer typedefs
typedef bool (*intersects_func_t)(const int32_t*, const int32_t*, int);
typedef void (*expand_func_t)(int32_t*, const int32_t*, int);
typedef void (*expand_point_func_t)(int32_t*, const double*, int);

// Function declarations
intersects_func_t get_optimal_intersects_func();
expand_func_t get_optimal_expand_func();
expand_point_func_t get_optimal_expand_point_func();

// Forward declarations of implementations
namespace simd_impl {

// Scalar implementations (always available)
bool intersects_scalar(const int32_t* box1, const int32_t* box2, int dimensions) {
    for (int d = 0; d < dimensions * 2; d += 2) {
        if (box1[d+1] < box2[d] || box2[d+1] < box1[d]) {
            return false;
        }
    }
    return true;
}

void expand_scalar(int32_t* target, const int32_t* source, int dimensions) {
    for (int d = 0; d < dimensions * 2; d += 2) {
        target[d] = std::min(target[d], source[d]);
        target[d+1] = std::max(target[d+1], source[d+1]);
    }
}

void expand_point_scalar(int32_t* box, const double* point, int dimensions) {
    for (int d = 0; d < dimensions; d++) {
        int32_t sortableValue = floatToSortableInt((float)point[d]);
        unsigned short idx = d * 2;
        box[idx] = std::min(box[idx], sortableValue);
        box[idx+1] = std::max(box[idx+1], sortableValue);
    }
}

#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)

// Platform-specific alignment and optimization attributes
#ifdef _MSC_VER
    #define SIMD_ALIGN(x) __declspec(align(x))
    #define SIMD_RESTRICT __restrict
#else
    #define SIMD_ALIGN(x) __attribute__((aligned(x)))
    #define SIMD_RESTRICT __restrict
    // Note: We'll only use target attributes when actually using SIMD instructions
    // to avoid issues with stub functions under Rosetta
    #ifdef __clang__
        #define SIMD_TARGET_SSE2 __attribute__((target("sse2")))
        #define SIMD_TARGET_AVX2 __attribute__((target("avx2")))
    #else
        #define SIMD_TARGET_SSE2 __attribute__((target("sse2")))
        #define SIMD_TARGET_AVX2 __attribute__((target("avx2")))
    #endif
#endif

// SSE2 implementation  
#ifndef _MSC_VER
#ifndef DISABLE_SIMD_ATTRIBUTES
SIMD_TARGET_SSE2
#endif
#endif
bool intersects_sse2(const int32_t* SIMD_RESTRICT box1, const int32_t* SIMD_RESTRICT box2, int dimensions) {
    // Use scalar for dimensions where SIMD overhead isn't worth it
    // Based on tight-loop benchmarking:
    // - SSE2 is faster for 2-4 dimensions (~27-29% speedup)
    // - SSE2 is marginally faster for 8 dimensions (~3.5% speedup)
    // - SSE2 becomes slower for 16+ dimensions
    // However, in real-world usage with function pointer overhead,
    // the crossover might be different. Let's use SSE2 for 2-8 dimensions.
    if (dimensions == 1 || dimensions > 8) {
        return intersects_scalar(box1, box2, dimensions);
    }
    
    int d = 0;
    
    // Process 2 dimensions at a time with SSE2 (4 int32_t values)
    for (; d + 3 < dimensions * 2; d += 4) {
        // Load 4 values [min0, max0, min1, max1]
        __m128i a = _mm_loadu_si128((const __m128i*)(box1 + d));
        __m128i b = _mm_loadu_si128((const __m128i*)(box2 + d));
        
        // For MBR intersection test, boxes DON'T overlap if:
        // box1.max < box2.min OR box2.max < box1.min (for any dimension)
        
        // Extract min and max pairs correctly
        __m128i a_min = _mm_shuffle_epi32(a, _MM_SHUFFLE(2, 0, 2, 0)); // [min0, min0, min1, min1]
        __m128i a_max = _mm_shuffle_epi32(a, _MM_SHUFFLE(3, 1, 3, 1)); // [max0, max0, max1, max1]
        __m128i b_min = _mm_shuffle_epi32(b, _MM_SHUFFLE(2, 0, 2, 0)); // [min0, min0, min1, min1]
        __m128i b_max = _mm_shuffle_epi32(b, _MM_SHUFFLE(3, 1, 3, 1)); // [max0, max0, max1, max1]
        
        // Compare: a_max < b_min (box1.max < box2.min)
        __m128i cmp1 = _mm_cmplt_epi32(a_max, b_min);
        // Compare: b_max < a_min (box2.max < box1.min)
        __m128i cmp2 = _mm_cmplt_epi32(b_max, a_min);
        
        // Combine the two: any overlap failure in any dim
        __m128i fail = _mm_or_si128(cmp1, cmp2);
        
        // Check if any bits are set (indicating non-overlap)
        if (_mm_movemask_epi8(fail)) {
            return false;
        }
    }
    
    // Handle remaining dimensions with scalar code
    for (; d < dimensions * 2; d += 2) {
        if (box1[d+1] < box2[d] || box2[d+1] < box1[d]) {
            return false;
        }
    }
    
    return true;
}

// SSE2 implementation for expand
#ifndef _MSC_VER
#ifndef DISABLE_SIMD_ATTRIBUTES
SIMD_TARGET_SSE2
#endif
#endif
void expand_sse2(int32_t* target, const int32_t* source, int dimensions) {
    // Use same dimension threshold as intersects
    if (dimensions < 2 || dimensions > 8) {
        return expand_scalar(target, source, dimensions);
    }
    
    int d = 0;
    for (; d + 3 < dimensions * 2; d += 4) {
        __m128i t = _mm_loadu_si128((__m128i*)(target + d));
        __m128i s = _mm_loadu_si128((__m128i*)(source + d));

        // t = [min0, max0, min1, max1]
        // Extract actual pairs
        __m128i t_min = _mm_shuffle_epi32(t, _MM_SHUFFLE(2, 0, 2, 0));  // [min1, min0, min1, min0]
        __m128i t_max = _mm_shuffle_epi32(t, _MM_SHUFFLE(3, 1, 3, 1));  // [max1, max0, max1, max0]
        __m128i s_min = _mm_shuffle_epi32(s, _MM_SHUFFLE(2, 0, 2, 0));
        __m128i s_max = _mm_shuffle_epi32(s, _MM_SHUFFLE(3, 1, 3, 1));

        __m128i new_min = _mm_min_epi32(t_min, s_min);
        __m128i new_max = _mm_max_epi32(t_max, s_max);

        // Reverse the shuffle to get [min0, max0, min1, max1]
        __m128i result = _mm_setzero_si128();
        result = _mm_unpacklo_epi32(new_min, new_max); // [min0, max0, min1, max1]
        _mm_storeu_si128((__m128i*)(target + d), result);
    }

    for (; d < dimensions * 2; d += 2) {
        target[d] = std::min(target[d], source[d]);
        target[d + 1] = std::max(target[d + 1], source[d + 1]);
    }
}

// SSE2 implementation for expand_point
#ifndef _MSC_VER
#ifndef DISABLE_SIMD_ATTRIBUTES
SIMD_TARGET_SSE2
#endif
#endif
void expand_point_sse2(int32_t* box, const double* point, int dimensions) {
    // For small dimensions, scalar might be faster due to conversion overhead
    if (dimensions < 2) {
        return expand_point_scalar(box, point, dimensions);
    }
    
    int d = 0;
    for (; d + 1 < dimensions; d += 2) {
        // Load 2 doubles and convert to float, then to sortable int
        __m128d point_d = _mm_loadu_pd(point + d);
        __m128 point_f = _mm_cvtpd_ps(point_d);
        
        // Convert float to sortable int
        __m128i bits = _mm_castps_si128(point_f);
        __m128i mask = _mm_srai_epi32(bits, 31);
        __m128i sortable = _mm_xor_si128(bits, _mm_and_si128(mask, _mm_set1_epi32(0x7fffffff)));
        
        // Now sortable contains [s0, s1, ?, ?]
        // Duplicate to [s0, s0, s1, s1]
        __m128i pt_interleaved = _mm_shuffle_epi32(sortable, _MM_SHUFFLE(1, 1, 0, 0));
        
        // Load box [min0, max0, min1, max1]
        __m128i box_vals = _mm_loadu_si128((__m128i*)(box + d * 2));
        
        // Apply min to positions 0,2 and max to positions 1,3
        // We need to be selective about which operations to apply
        // Create masks
        __m128i min_mask = _mm_set_epi32(0, -1, 0, -1);  // positions 0,2
        __m128i max_mask = _mm_set_epi32(-1, 0, -1, 0);  // positions 1,3
        
        __m128i new_min = _mm_min_epi32(box_vals, pt_interleaved);
        __m128i new_max = _mm_max_epi32(box_vals, pt_interleaved);
        
        // Combine results using masks
        __m128i result = _mm_or_si128(
            _mm_and_si128(min_mask, new_min),
            _mm_and_si128(max_mask, new_max)
        );
        
        _mm_storeu_si128((__m128i*)(box + d * 2), result);
    }

    // Handle any remaining dimensions with scalar code
    for (; d < dimensions; ++d) {
        int32_t sortableValue = floatToSortableInt((float)point[d]);
        box[d * 2]     = std::min(box[d * 2], sortableValue);
        box[d * 2 + 1] = std::max(box[d * 2 + 1], sortableValue);
    }
}

// AVX2 implementation
#ifndef _MSC_VER
#ifndef DISABLE_SIMD_ATTRIBUTES
SIMD_TARGET_AVX2
#endif
#endif
bool intersects_avx2(const int32_t* box1, const int32_t* box2, int dimensions) {
    // AVX2 processes 4 dimensions at once, so it needs at least 4 dimensions
    // Based on SSE2 benchmarking showing benefits for 2-8 dimensions,
    // AVX2 should theoretically be good for 4-16 dimensions
    // For now, let's use AVX2 for 4-16 dimensions
    if (dimensions < 4 || dimensions > 16) {
        // Fall back to SSE2 for 2-3 dimensions, scalar for 1 or 17+ dimensions
        if (dimensions >= 2 && dimensions <= 8) {
            return intersects_sse2(box1, box2, dimensions);
        }
        return intersects_scalar(box1, box2, dimensions);
    }
    
    int d = 0;
    
    // Process 4 dimensions at a time with AVX2 (8 int32_t values)
    for (; d + 7 < dimensions * 2; d += 8) {
        // Load 8 values [min0, max0, min1, max1, min2, max2, min3, max3]
        __m256i a = _mm256_loadu_si256((const __m256i*)(box1 + d));
        __m256i b = _mm256_loadu_si256((const __m256i*)(box2 + d));
        
        // For MBR intersection test, boxes DON'T overlap if:
        // box1.max < box2.min OR box2.max < box1.min (for any dimension)
        
        // Extract min and max pairs correctly using permutevar8x32
        // Permute indices: 0, 2, 4, 6 = mins, 1, 3, 5, 7 = maxes
        __m256i min_indices = _mm256_set_epi32(6, 4, 2, 0, 6, 4, 2, 0);
        __m256i max_indices = _mm256_set_epi32(7, 5, 3, 1, 7, 5, 3, 1);
        
        // Use AVX2 gather-permute (fast + correct order)
        __m256i a_min = _mm256_permutevar8x32_epi32(a, min_indices); // [min0, min1, min2, min3, min0, min1, min2, min3]
        __m256i a_max = _mm256_permutevar8x32_epi32(a, max_indices); // [max0, max1, max2, max3, max0, max1, max2, max3]
        __m256i b_min = _mm256_permutevar8x32_epi32(b, min_indices);
        __m256i b_max = _mm256_permutevar8x32_epi32(b, max_indices);
        
        // Compare: a_max < b_min (box1.max < box2.min)
        __m256i cmp1 = _mm256_cmpgt_epi32(b_min, a_max);  // Use gt instead of lt for cleaner logic
        // Compare: b_max < a_min (box2.max < box1.min)
        __m256i cmp2 = _mm256_cmpgt_epi32(a_min, b_max);
        
        // Combine the two: any overlap failure in any dim
        __m256i fail = _mm256_or_si256(cmp1, cmp2);
        
        // Check if any bits are set (indicating non-overlap)
        if (_mm256_movemask_epi8(fail)) {
            return false;
        }
    }
    
    // Handle remaining dimensions with scalar code
    for (; d < dimensions * 2; d += 2) {
        if (box1[d+1] < box2[d] || box2[d+1] < box1[d]) {
            return false;
        }
    }
    
    return true;
}

// AVX2 implementation for expand
#ifndef _MSC_VER
#ifndef DISABLE_SIMD_ATTRIBUTES
SIMD_TARGET_AVX2
#endif
#endif
void expand_avx2(int32_t* target, const int32_t* source, int dimensions) {
    // Use same dimension threshold as intersects
    if (dimensions < 4 || dimensions > 16) {
        // Fall back to SSE2 for 2-3 dimensions
        if (dimensions >= 2 && dimensions <= 8) {
            return expand_sse2(target, source, dimensions);
        }
        return expand_scalar(target, source, dimensions);
    }
    
    int d = 0;
    for (; d + 7 < dimensions * 2; d += 8) {
        __m256i t = _mm256_loadu_si256((__m256i*)(target + d));
        __m256i s = _mm256_loadu_si256((__m256i*)(source + d));

        // Deinterleave
        __m256i t_min = _mm256_shuffle_epi32(t, _MM_SHUFFLE(2, 0, 2, 0)); // same lane-wise, not global
        __m256i t_max = _mm256_shuffle_epi32(t, _MM_SHUFFLE(3, 1, 3, 1));
        __m256i s_min = _mm256_shuffle_epi32(s, _MM_SHUFFLE(2, 0, 2, 0));
        __m256i s_max = _mm256_shuffle_epi32(s, _MM_SHUFFLE(3, 1, 3, 1));

        __m256i new_min = _mm256_min_epi32(t_min, s_min);
        __m256i new_max = _mm256_max_epi32(t_max, s_max);

        __m256i result = _mm256_unpacklo_epi32(new_min, new_max); // interleave min0,max0,...

        // Actually, because AVX2 shuffle operates within 128-bit lanes, you need to permute 128-bit lanes
        __m256i interleave_lo = _mm256_unpacklo_epi32(new_min, new_max); // min0,max0,...
        __m256i interleave_hi = _mm256_unpackhi_epi32(new_min, new_max);
        __m256i result_full = _mm256_permute2x128_si256(interleave_lo, interleave_hi, 0x20);

        _mm256_storeu_si256((__m256i*)(target + d), result_full);
    }

    for (; d < dimensions * 2; d += 2) {
        target[d] = std::min(target[d], source[d]);
        target[d + 1] = std::max(target[d + 1], source[d + 1]);
    }
}

// AVX2 implementation for expand_point
#ifndef _MSC_VER
#ifndef DISABLE_SIMD_ATTRIBUTES
SIMD_TARGET_AVX2
#endif
#endif
void expand_point_avx2(int32_t* box, const double* point, int dimensions) {
    // For small dimensions, use SSE2 or scalar
    if (dimensions < 4) {
        return expand_point_sse2(box, point, dimensions);
    }
    
    int d = 0;
    
    // Process 4 dimensions at a time
    for (; d + 3 < dimensions; d += 4) {
        // Load 4 doubles
        __m256d point_d = _mm256_loadu_pd(point + d);
        
        // Convert to floats (double -> float)
        __m128 point_f = _mm256_cvtpd_ps(point_d);
        
        // Get bit representation and apply sortable conversion
        __m128i bits = _mm_castps_si128(point_f);
        __m128i mask = _mm_srai_epi32(bits, 31);  // Sign mask
        __m128i sortable = _mm_xor_si128(bits, _mm_and_si128(mask, _mm_set1_epi32(0x7fffffff)));
        
        // Now we have 4 sortable ints: [s0, s1, s2, s3]
        // We need to expand to [s0, s0, s1, s1, s2, s2, s3, s3]
        // Use AVX2 permute to duplicate each element
        __m256i sortable_256 = _mm256_castsi128_si256(sortable);
        sortable_256 = _mm256_inserti128_si256(sortable_256, sortable, 1);  // Duplicate to upper lane
        __m256i expanded = _mm256_permutevar8x32_epi32(sortable_256, 
            _mm256_set_epi32(3, 3, 2, 2, 1, 1, 0, 0));
        
        // Load current box values [min0, max0, min1, max1, min2, max2, min3, max3]
        __m256i box_vals = _mm256_loadu_si256((__m256i*)(box + d * 2));
        
        // Create masks for even/odd positions
        __m256i min_mask = _mm256_set_epi32(0, -1, 0, -1, 0, -1, 0, -1);
        __m256i max_mask = _mm256_set_epi32(-1, 0, -1, 0, -1, 0, -1, 0);
        
        // Apply min to even positions
        __m256i new_mins = _mm256_min_epi32(box_vals, expanded);
        // Apply max to odd positions  
        __m256i new_maxs = _mm256_max_epi32(box_vals, expanded);
        
        // Combine using masks
        __m256i result = _mm256_or_si256(
            _mm256_and_si256(min_mask, new_mins),
            _mm256_and_si256(max_mask, new_maxs)
        );
        
        _mm256_storeu_si256((__m256i*)(box + d * 2), result);
    }
    
    // Handle remaining dimensions with SSE2
    for (; d + 1 < dimensions; d += 2) {
        __m128d point_d = _mm_loadu_pd(point + d);
        __m128 point_f = _mm_cvtpd_ps(point_d);
        __m128i bits = _mm_castps_si128(point_f);
        __m128i mask = _mm_srai_epi32(bits, 31);
        __m128i sortable = _mm_xor_si128(bits, _mm_and_si128(mask, _mm_set1_epi32(0x7fffffff)));
        __m128i expanded = _mm_shuffle_epi32(sortable, _MM_SHUFFLE(1, 1, 0, 0));
        __m128i box_vals = _mm_loadu_si128((__m128i*)(box + d * 2));
        __m128i min_mask = _mm_set_epi32(0, -1, 0, -1);
        __m128i max_mask = _mm_set_epi32(-1, 0, -1, 0);
        __m128i new_mins = _mm_min_epi32(box_vals, expanded);
        __m128i new_maxs = _mm_max_epi32(box_vals, expanded);
        __m128i result = _mm_or_si128(
            _mm_and_si128(min_mask, new_mins),
            _mm_and_si128(max_mask, new_maxs)
        );
        _mm_storeu_si128((__m128i*)(box + d * 2), result);
    }
    
    // Handle last dimension with scalar
    for (; d < dimensions; d++) {
        int32_t sortableValue = floatToSortableInt((float)point[d]);
        unsigned short idx = d * 2;
        box[idx] = std::min(box[idx], sortableValue);
        box[idx+1] = std::max(box[idx+1], sortableValue);
    }
}

#endif // x86 SIMD

#if defined(__aarch64__) || defined(__arm64__)
#if defined(__ARM_NEON)

// Helper function to deinterleave min/max pairs
inline void deinterleave_minmax(int32x4_t input, int32x4_t& min_out, int32x4_t& max_out) {
    int32x4x2_t unzip = vuzpq_s32(input, input);
    min_out = unzip.val[0];
    max_out = unzip.val[1];
}

// NEON implementation for ARM
bool intersects_neon(const int32_t* box1, const int32_t* box2, int dimensions) {
    // Based on our benchmarking, NEON provides benefits for 2-16 dimensions  
    if (dimensions < 2 || dimensions > 16) {
        return intersects_scalar(box1, box2, dimensions);
    }
    
    int d = 0;
    
    // Process up to 4 dimensions at a time with NEON (8 int32_t values)
    for (; d + 7 < dimensions * 2; d += 8) {
        // Load 8 values [min0, max0, min1, max1, min2, max2, min3, max3]
        int32x4_t a1 = vld1q_s32(box1 + d);     // [min0, max0, min1, max1]
        int32x4_t a2 = vld1q_s32(box1 + d + 4); // [min2, max2, min3, max3]
        int32x4_t b1 = vld1q_s32(box2 + d);     // [min0, max0, min1, max1]
        int32x4_t b2 = vld1q_s32(box2 + d + 4); // [min2, max2, min3, max3]
        
        // Deinterleave min/max pairs for first 2 dimensions
        int32x4_t a1_min, a1_max, b1_min, b1_max;
        deinterleave_minmax(a1, a1_min, a1_max);
        deinterleave_minmax(b1, b1_min, b1_max);
        
        // Deinterleave min/max pairs for next 2 dimensions
        int32x4_t a2_min, a2_max, b2_min, b2_max;
        deinterleave_minmax(a2, a2_min, a2_max);
        deinterleave_minmax(b2, b2_min, b2_max);
        
        // Check b_min > a_max OR a_min > b_max for first 2 dimensions
        uint32x4_t cmp1_1 = vcgtq_s32(b1_min, a1_max);
        uint32x4_t cmp2_1 = vcgtq_s32(a1_min, b1_max);
        uint32x4_t fail1 = vorrq_u32(cmp1_1, cmp2_1);
        
        // Check b_min > a_max OR a_min > b_max for next 2 dimensions
        uint32x4_t cmp1_2 = vcgtq_s32(b2_min, a2_max);
        uint32x4_t cmp2_2 = vcgtq_s32(a2_min, b2_max);
        uint32x4_t fail2 = vorrq_u32(cmp1_2, cmp2_2);
        
        // Check if any failure bit is set in either vector
        // Use vmaxvq_u32 for efficient reduction (requires ARMv8)
        if (vmaxvq_u32(fail1) != 0 || vmaxvq_u32(fail2) != 0) {
            return false;
        }
    }
    
    // Process 2 dimensions at a time for remaining
    for (; d + 3 < dimensions * 2; d += 4) {
        int32x4_t a = vld1q_s32(box1 + d);
        int32x4_t b = vld1q_s32(box2 + d);
        
        int32x4_t a_min, a_max, b_min, b_max;
        deinterleave_minmax(a, a_min, a_max);
        deinterleave_minmax(b, b_min, b_max);
        
        uint32x4_t cmp1 = vcgtq_s32(b_min, a_max);
        uint32x4_t cmp2 = vcgtq_s32(a_min, b_max);
        uint32x4_t fail = vorrq_u32(cmp1, cmp2);
        
        // Check all lanes since we might have 2 valid dimensions
        if (vmaxvq_u32(fail) != 0) {
            return false;
        }
    }
    
    // Handle remaining dimensions with scalar code
    for (; d < dimensions * 2; d += 2) {
        if (box1[d+1] < box2[d] || box2[d+1] < box1[d]) {
            return false;
        }
    }
    
    return true;
}

// NEON implementation for expand
void expand_neon(int32_t* target, const int32_t* source, int dimensions) {
    // Use NEON for same range as intersects
    if (dimensions < 2 || dimensions > 16) {
        return expand_scalar(target, source, dimensions);
    }
    
    int d = 0;
    
    // Process 4 dimensions at a time
    for (; d + 7 < dimensions * 2; d += 8) {
        // Load target and source values
        int32x4_t t1 = vld1q_s32(target + d);     // [min0, max0, min1, max1]
        int32x4_t t2 = vld1q_s32(target + d + 4); // [min2, max2, min3, max3]
        int32x4_t s1 = vld1q_s32(source + d);
        int32x4_t s2 = vld1q_s32(source + d + 4);
        
        // Deinterleave min/max pairs
        int32x4_t t1_min, t1_max, s1_min, s1_max;
        int32x4_t t2_min, t2_max, s2_min, s2_max;
        deinterleave_minmax(t1, t1_min, t1_max);
        deinterleave_minmax(s1, s1_min, s1_max);
        deinterleave_minmax(t2, t2_min, t2_max);
        deinterleave_minmax(s2, s2_min, s2_max);
        
        // Compute new min (minimum of both) and max (maximum of both)
        int32x4_t new_min1 = vminq_s32(t1_min, s1_min);
        int32x4_t new_max1 = vmaxq_s32(t1_max, s1_max);
        int32x4_t new_min2 = vminq_s32(t2_min, s2_min);
        int32x4_t new_max2 = vmaxq_s32(t2_max, s2_max);
        
        // Interleave back and store
        // vzipq_s32 is the inverse of vuzpq_s32
        int32x4x2_t zip1 = vzipq_s32(new_min1, new_max1);
        int32x4x2_t zip2 = vzipq_s32(new_min2, new_max2);
        
        vst1q_s32(target + d, zip1.val[0]);
        vst1q_s32(target + d + 4, zip2.val[0]);
    }
    
    // Process 2 dimensions at a time
    for (; d + 3 < dimensions * 2; d += 4) {
        int32x4_t t = vld1q_s32(target + d);
        int32x4_t s = vld1q_s32(source + d);
        
        int32x4_t t_min, t_max, s_min, s_max;
        deinterleave_minmax(t, t_min, t_max);
        deinterleave_minmax(s, s_min, s_max);
        
        int32x4_t new_min = vminq_s32(t_min, s_min);
        int32x4_t new_max = vmaxq_s32(t_max, s_max);
        
        int32x4x2_t zip = vzipq_s32(new_min, new_max);
        vst1q_s32(target + d, zip.val[0]);
    }
    
    // Handle remaining dimensions
    for (; d < dimensions * 2; d += 2) {
        target[d] = std::min(target[d], source[d]);
        target[d+1] = std::max(target[d+1], source[d+1]);
    }
}

// NEON implementation for expand_point
void expand_point_neon(int32_t* box, const double* point, int dimensions) {
    // For small dimensions, scalar might be faster
    if (dimensions < 2) {
        return expand_point_scalar(box, point, dimensions);
    }
    
    int d = 0;
    
    // Process 2 dimensions at a time
    for (; d + 1 < dimensions; d += 2) {
        // Load 2 doubles
        float64x2_t point_d = vld1q_f64(point + d);
        
        // Convert to floats (double -> float)
        float32x2_t point_f = vcvt_f32_f64(point_d);
        
        // Get bit representation
        int32x2_t bits = vreinterpret_s32_f32(point_f);
        
        // Apply sortable conversion: bits ^ ((bits >> 31) & 0x7fffffff)
        int32x2_t sign_mask = vshr_n_s32(bits, 31);
        int32x2_t sortable = veor_s32(bits, vand_s32(sign_mask, vdup_n_s32(0x7fffffff)));
        
        // Now sortable contains [s0, s1]
        // We need to expand to [s0, s0, s1, s1]
        int32_t s0 = vget_lane_s32(sortable, 0);
        int32_t s1 = vget_lane_s32(sortable, 1);
        int32x4_t expanded = {s0, s0, s1, s1};
        
        // Load current box values [min0, max0, min1, max1]
        int32x4_t box_vals = vld1q_s32(box + d * 2);
        
        // Apply min to positions 0,2 and max to positions 1,3
        // We'll use a mask-based approach similar to SSE2
        // Create the expanded values for selective min/max
        int32x4_t new_mins = vminq_s32(box_vals, expanded);
        int32x4_t new_maxs = vmaxq_s32(box_vals, expanded);
        
        // Manual selection: take min at positions 0,2 and max at positions 1,3
        int32_t result_vals[4] = {
            vgetq_lane_s32(new_mins, 0),  // min0
            vgetq_lane_s32(new_maxs, 1),  // max0
            vgetq_lane_s32(new_mins, 2),  // min1
            vgetq_lane_s32(new_maxs, 3)   // max1
        };
        
        vst1q_s32(box + d * 2, vld1q_s32(result_vals));
    }
    
    // Handle remaining dimension
    for (; d < dimensions; d++) {
        int32_t sortableValue = floatToSortableInt((float)point[d]);
        unsigned short idx = d * 2;
        box[idx] = std::min(box[idx], sortableValue);
        box[idx+1] = std::max(box[idx+1], sortableValue);
    }
}

#endif // __ARM_NEON
#endif // ARM64

} // namespace simd_impl

// Clean up macros
#undef SIMD_ALIGN
#undef SIMD_RESTRICT
#undef SIMD_TARGET_SSE2

// Function to get optimal intersects implementation
intersects_func_t get_optimal_intersects_func() {
    const auto& features = CPUFeatures::get();
    
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
    if (features.has_avx2) {
        return simd_impl::intersects_avx2;
    } else if (features.has_sse2) {
        return simd_impl::intersects_sse2;
    }
#elif defined(__aarch64__) || defined(__arm64__)
    if (features.has_neon) {
        return simd_impl::intersects_neon;
    }
#endif
    
    return simd_impl::intersects_scalar;
}

// Function to get optimal expand implementation
expand_func_t get_optimal_expand_func() {
    const auto& features = CPUFeatures::get();
    
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
    if (features.has_avx2) {
        return simd_impl::expand_avx2;
    } else if (features.has_sse2) {
        return simd_impl::expand_sse2;
    }
#elif defined(__aarch64__) || defined(__arm64__)
#if defined(__ARM_NEON)
    if (features.has_neon) {
        return simd_impl::expand_neon;
    }
#endif
#endif
    
    return simd_impl::expand_scalar;
}

// Function to get optimal expand_point implementation
expand_point_func_t get_optimal_expand_point_func() {
    const auto& features = CPUFeatures::get();
    
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
    if (features.has_avx2) {
        return simd_impl::expand_point_avx2;
    } else if (features.has_sse2) {
        return simd_impl::expand_point_sse2;
    }
#elif defined(__aarch64__) || defined(__arm64__)
#if defined(__ARM_NEON)
    if (features.has_neon) {
        return simd_impl::expand_point_neon;
    }
#endif
#endif
    
    return simd_impl::expand_point_scalar;
}

} // namespace xtree