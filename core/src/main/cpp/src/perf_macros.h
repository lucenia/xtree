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

/*
 * Performance macros for XTree
 * These macros provide optimal performance for hot paths
 */

#pragma once

#include <cstdint>
#include <cstring>
#include "util/float_utils.h"

// Compiler hints for branch prediction
#ifdef __GNUC__
#define LIKELY(x)   __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#define PREFETCH(addr, rw, locality) ((void)0)
#endif

// Branchless MIN/MAX for integers
#define BRANCHLESS_MIN(a, b) ((a) ^ (((a) ^ (b)) & -((a) > (b))))
#define BRANCHLESS_MAX(a, b) ((b) ^ (((a) ^ (b)) & -((a) > (b))))

// Fast 2D intersects check (most common case)
#define FAST_INTERSECTS_2D(box1, box2) \
    (!((box1)[1] < (box2)[0] || \
       (box2)[1] < (box1)[0] || \
       (box1)[3] < (box2)[2] || \
       (box2)[3] < (box1)[2]))

// Direct sortable box access macros (use public methods)
#define SORTABLE_MIN(mbr, axis) ((mbr)->getSortableMin(axis))
#define SORTABLE_MAX(mbr, axis) ((mbr)->getSortableMax(axis))
#define SORTABLE_BOX(mbr, idx) ((mbr)->getSortableBoxVal(idx))

// Note: Prefetching requires access to internal data structure
// Use from within KeyMBR member functions only

// Memory alignment for better cache performance
#define CACHE_ALIGN __attribute__((aligned(64)))

// Force inline for critical functions
#ifdef __GNUC__
#define FORCE_INLINE __attribute__((always_inline)) inline
#else
#define FORCE_INLINE inline
#endif

// Note: Direct 2D expand optimization requires friend access or 
// implementation within KeyMBR class methods

// Batch conversion macro for SIMD-friendly code
#define CONVERT_FLOATS_TO_SORTABLE_BATCH(floats, sortables, count) do { \
    for (size_t _i = 0; _i < (count); ++_i) { \
        int32_t _bits; \
        memcpy(&_bits, &(floats)[_i], sizeof(float)); \
        (sortables)[_i] = _bits ^ ((_bits >> 31) & 0x7fffffff); \
    } \
} while(0)

namespace xtree {

// Optimized structure for 2D points (most common case)
struct Point2D {
    int32_t x;
    int32_t y;
    
    FORCE_INLINE Point2D(float fx, float fy) {
        x = floatToSortableInt(fx);
        y = floatToSortableInt(fy);
    }
};

// Performance statistics for profiling
struct PerfStats {
    uint64_t intersectCalls = 0;
    uint64_t expandCalls = 0;
    uint64_t areaCalls = 0;
    uint64_t cacheHits = 0;
    uint64_t cacheMisses = 0;
};

} // namespace xtree