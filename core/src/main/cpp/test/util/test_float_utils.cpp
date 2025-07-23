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
#include <algorithm>
#include <limits>
#include <cmath>
#include "../../src/util/float_utils.h"

using namespace xtree;
using namespace std;

TEST(FloatUtilsTest, BasicConversion) {
    // Test basic positive and negative values
    float values[] = {0.0f, 1.0f, -1.0f, 100.0f, -100.0f, 3.14159f, -3.14159f};
    
    for (float value : values) {
        int32_t sortable = floatToSortableInt(value);
        float converted = sortableIntToFloat(sortable);
        EXPECT_FLOAT_EQ(value, converted) << "Failed for value: " << value;
    }
}

TEST(FloatUtilsTest, SpecialValues) {
    // Test special float values
    float specialValues[] = {
        numeric_limits<float>::max(),
        numeric_limits<float>::min(),
        numeric_limits<float>::lowest(),
        numeric_limits<float>::epsilon(),
        -numeric_limits<float>::epsilon(),
        numeric_limits<float>::infinity(),
        -numeric_limits<float>::infinity(),
        0.0f,
        -0.0f
    };
    
    for (float value : specialValues) {
        // Skip NaN as it doesn't equal itself
        if (!isnan(value)) {
            int32_t sortable = floatToSortableInt(value);
            float converted = sortableIntToFloat(sortable);
            if (isinf(value)) {
                EXPECT_TRUE(isinf(converted));
                EXPECT_EQ(signbit(value), signbit(converted));
            } else {
                EXPECT_FLOAT_EQ(value, converted) << "Failed for special value: " << value;
            }
        }
    }
}

TEST(FloatUtilsTest, SortingOrder) {
    // Test that sorting order is preserved
    vector<float> floats = {
        -numeric_limits<float>::infinity(),
        -1000.0f,
        -100.0f,
        -10.0f,
        -1.0f,
        -0.1f,
        -numeric_limits<float>::epsilon(),
        -0.0f,
        0.0f,
        numeric_limits<float>::epsilon(),
        0.1f,
        1.0f,
        10.0f,
        100.0f,
        1000.0f,
        numeric_limits<float>::infinity()
    };
    
    // Convert to sortable integers
    vector<int32_t> sortableInts;
    for (float f : floats) {
        sortableInts.push_back(floatToSortableInt(f));
    }
    
    // Verify the integers maintain the same order
    for (size_t i = 0; i < sortableInts.size() - 1; i++) {
        EXPECT_LT(sortableInts[i], sortableInts[i + 1]) 
            << "Order not preserved at index " << i 
            << ": " << floats[i] << " -> " << sortableInts[i]
            << " vs " << floats[i + 1] << " -> " << sortableInts[i + 1];
    }
}

TEST(FloatUtilsTest, RandomSorting) {
    // Test with random values
    vector<float> randomFloats;
    srand(42); // Fixed seed for reproducibility
    
    // Generate random floats including negative values
    for (int i = 0; i < 1000; i++) {
        float value = (rand() / (float)RAND_MAX) * 2000.0f - 1000.0f;
        randomFloats.push_back(value);
    }
    
    // Sort using float comparison
    vector<float> floatSorted = randomFloats;
    sort(floatSorted.begin(), floatSorted.end());
    
    // Convert to sortable ints and sort
    vector<pair<int32_t, float>> intSorted;
    for (float f : randomFloats) {
        intSorted.push_back({floatToSortableInt(f), f});
    }
    sort(intSorted.begin(), intSorted.end(), 
         [](const auto& a, const auto& b) { return a.first < b.first; });
    
    // Verify the sorting produces the same order
    for (size_t i = 0; i < floatSorted.size(); i++) {
        EXPECT_FLOAT_EQ(floatSorted[i], intSorted[i].second)
            << "Mismatch at position " << i;
    }
}

TEST(FloatUtilsTest, ComparisonFunctions) {
    float a = -10.5f;
    float b = 10.5f;
    float c = 10.5f;
    
    int32_t sortA = floatToSortableInt(a);
    int32_t sortB = floatToSortableInt(b);
    int32_t sortC = floatToSortableInt(c);
    
    // Test less than
    EXPECT_TRUE(sortableIntLess(sortA, sortB));
    EXPECT_FALSE(sortableIntLess(sortB, sortA));
    EXPECT_FALSE(sortableIntLess(sortB, sortC));
    
    // Test less than or equal
    EXPECT_TRUE(sortableIntLessEqual(sortA, sortB));
    EXPECT_FALSE(sortableIntLessEqual(sortB, sortA));
    EXPECT_TRUE(sortableIntLessEqual(sortB, sortC));
    EXPECT_TRUE(sortableIntLessEqual(sortC, sortB));
}