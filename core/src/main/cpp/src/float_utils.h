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

#pragma once

#include <cstdint>
#include <cstring>
#include <limits>

namespace xtree {

using namespace std;

/**
 * Utility functions for converting between floats and sortable integers.
 * This allows us to use integer comparisons for spatial indexing while
 * maintaining the correct ordering of floating-point values.
 */

/**
 * Convert a float to a sortable 32-bit integer.
 * Negative floats are converted such that they sort before positive floats.
 * The conversion maintains the relative ordering of all float values.
 */
inline int32_t floatToSortableInt(float value) {
    int32_t bits;
    // Get the bit representation of the float
    std::memcpy(&bits, &value, sizeof(float));
    
    // Use the same optimized bit manipulation as Java/Lucene:
    // bits ^ ((bits >> 31) & 0x7fffffff)
    // This flips the sign bit for positive numbers and all non-sign bits for negative numbers
    return bits ^ ((bits >> 31) & 0x7fffffff);
}

/**
 * Convert a sortable 32-bit integer back to a float.
 * This reverses the floatToSortableInt operation.
 */
inline float sortableIntToFloat(int32_t sortableBits) {
    // The reverse transformation is the same operation!
    // Applying the XOR operation twice returns the original value
    int32_t bits = sortableBits ^ ((sortableBits >> 31) & 0x7fffffff);
    
    float value;
    std::memcpy(&value, &bits, sizeof(float));
    return value;
}

/**
 * Compare two sortable integers representing floats.
 * Returns true if a < b in floating-point ordering.
 */
inline bool sortableIntLess(int32_t a, int32_t b) {
    return a < b;
}

/**
 * Compare two sortable integers representing floats.
 * Returns true if a <= b in floating-point ordering.
 */
inline bool sortableIntLessEqual(int32_t a, int32_t b) {
    return a <= b;
}

// Pre-computed sortable int values for common float bounds
// These are compile-time constants to avoid runtime computation
// numeric_limits<float>::max() converts to 0x7f7fffff (2139095039)
// -numeric_limits<float>::max() converts to 0x80800000 (-2139095040)
static const int32_t SORTABLE_FLOAT_MAX = 2139095039;
static const int32_t SORTABLE_FLOAT_MIN = -2139095040;

} // namespace xtree