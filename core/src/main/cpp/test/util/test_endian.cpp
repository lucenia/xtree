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
#include "util/endian.hpp"
#include <cstring>
#include <vector>

using namespace xtree::util;

class EndianTest : public ::testing::Test {
protected:
    // Test data buffers
    uint8_t buffer[16];
    
    void SetUp() override {
        std::memset(buffer, 0, sizeof(buffer));
    }
};

// Test 16-bit conversions
TEST_F(EndianTest, Store16BitLittleEndian) {
    uint16_t value = 0x1234;
    store_le16(buffer, value);
    
    // Little-endian format: least significant byte first
    EXPECT_EQ(buffer[0], 0x34);
    EXPECT_EQ(buffer[1], 0x12);
}

TEST_F(EndianTest, Load16BitLittleEndian) {
    buffer[0] = 0x34;
    buffer[1] = 0x12;
    
    uint16_t value = load_le16(buffer);
    EXPECT_EQ(value, 0x1234);
}

TEST_F(EndianTest, RoundTrip16Bit) {
    std::vector<uint16_t> test_values = {
        0x0000, 0x0001, 0x00FF, 0x0100, 0xFF00, 0xFFFF,
        0x1234, 0xABCD, 0x8000, 0x7FFF
    };
    
    for (uint16_t val : test_values) {
        store_le16(buffer, val);
        uint16_t loaded = load_le16(buffer);
        EXPECT_EQ(loaded, val) << "Failed for value: 0x" << std::hex << val;
    }
}

// Test 32-bit conversions
TEST_F(EndianTest, Store32BitLittleEndian) {
    uint32_t value = 0x12345678;
    store_le32(buffer, value);
    
    EXPECT_EQ(buffer[0], 0x78);
    EXPECT_EQ(buffer[1], 0x56);
    EXPECT_EQ(buffer[2], 0x34);
    EXPECT_EQ(buffer[3], 0x12);
}

TEST_F(EndianTest, Load32BitLittleEndian) {
    buffer[0] = 0x78;
    buffer[1] = 0x56;
    buffer[2] = 0x34;
    buffer[3] = 0x12;
    
    uint32_t value = load_le32(buffer);
    EXPECT_EQ(value, 0x12345678);
}

TEST_F(EndianTest, RoundTrip32Bit) {
    std::vector<uint32_t> test_values = {
        0x00000000, 0x00000001, 0x000000FF, 0x00000100,
        0x0000FF00, 0x00010000, 0x00FF0000, 0x01000000,
        0xFF000000, 0xFFFFFFFF, 0x12345678, 0xABCDEF01,
        0x80000000, 0x7FFFFFFF
    };
    
    for (uint32_t val : test_values) {
        store_le32(buffer, val);
        uint32_t loaded = load_le32(buffer);
        EXPECT_EQ(loaded, val) << "Failed for value: 0x" << std::hex << val;
    }
}

// Test 64-bit conversions
TEST_F(EndianTest, Store64BitLittleEndian) {
    uint64_t value = 0x123456789ABCDEF0ULL;
    store_le64(buffer, value);
    
    EXPECT_EQ(buffer[0], 0xF0);
    EXPECT_EQ(buffer[1], 0xDE);
    EXPECT_EQ(buffer[2], 0xBC);
    EXPECT_EQ(buffer[3], 0x9A);
    EXPECT_EQ(buffer[4], 0x78);
    EXPECT_EQ(buffer[5], 0x56);
    EXPECT_EQ(buffer[6], 0x34);
    EXPECT_EQ(buffer[7], 0x12);
}

TEST_F(EndianTest, Load64BitLittleEndian) {
    buffer[0] = 0xF0;
    buffer[1] = 0xDE;
    buffer[2] = 0xBC;
    buffer[3] = 0x9A;
    buffer[4] = 0x78;
    buffer[5] = 0x56;
    buffer[6] = 0x34;
    buffer[7] = 0x12;
    
    uint64_t value = load_le64(buffer);
    EXPECT_EQ(value, 0x123456789ABCDEF0ULL);
}

TEST_F(EndianTest, RoundTrip64Bit) {
    std::vector<uint64_t> test_values = {
        0x0000000000000000ULL, 0x0000000000000001ULL,
        0x00000000000000FFULL, 0x0000000000000100ULL,
        0x000000000000FF00ULL, 0x0000000000010000ULL,
        0x00000000FF000000ULL, 0x0000000100000000ULL,
        0x000000FF00000000ULL, 0x0001000000000000ULL,
        0x00FF000000000000ULL, 0x0100000000000000ULL,
        0xFF00000000000000ULL, 0xFFFFFFFFFFFFFFFFULL,
        0x123456789ABCDEF0ULL, 0xFEDCBA9876543210ULL,
        0x8000000000000000ULL, 0x7FFFFFFFFFFFFFFFULL
    };
    
    for (uint64_t val : test_values) {
        store_le64(buffer, val);
        uint64_t loaded = load_le64(buffer);
        EXPECT_EQ(loaded, val) << "Failed for value: 0x" << std::hex << val;
    }
}

// Test safe (unaligned) versions
TEST_F(EndianTest, SafeStore16Unaligned) {
    // Test storing at unaligned address
    uint16_t value = 0x1234;
    store_le16_safe(buffer + 1, value);  // Unaligned address
    
    EXPECT_EQ(buffer[1], 0x34);
    EXPECT_EQ(buffer[2], 0x12);
}

TEST_F(EndianTest, SafeLoad16Unaligned) {
    buffer[1] = 0x34;
    buffer[2] = 0x12;
    
    uint16_t value = load_le16_safe(buffer + 1);  // Unaligned address
    EXPECT_EQ(value, 0x1234);
}

TEST_F(EndianTest, SafeStore32Unaligned) {
    uint32_t value = 0x12345678;
    store_le32_safe(buffer + 1, value);  // Unaligned address
    
    EXPECT_EQ(buffer[1], 0x78);
    EXPECT_EQ(buffer[2], 0x56);
    EXPECT_EQ(buffer[3], 0x34);
    EXPECT_EQ(buffer[4], 0x12);
}

TEST_F(EndianTest, SafeLoad32Unaligned) {
    buffer[1] = 0x78;
    buffer[2] = 0x56;
    buffer[3] = 0x34;
    buffer[4] = 0x12;
    
    uint32_t value = load_le32_safe(buffer + 1);  // Unaligned address
    EXPECT_EQ(value, 0x12345678);
}

TEST_F(EndianTest, SafeStore64Unaligned) {
    uint64_t value = 0x123456789ABCDEF0ULL;
    store_le64_safe(buffer + 1, value);  // Unaligned address
    
    EXPECT_EQ(buffer[1], 0xF0);
    EXPECT_EQ(buffer[2], 0xDE);
    EXPECT_EQ(buffer[3], 0xBC);
    EXPECT_EQ(buffer[4], 0x9A);
    EXPECT_EQ(buffer[5], 0x78);
    EXPECT_EQ(buffer[6], 0x56);
    EXPECT_EQ(buffer[7], 0x34);
    EXPECT_EQ(buffer[8], 0x12);
}

TEST_F(EndianTest, SafeLoad64Unaligned) {
    buffer[1] = 0xF0;
    buffer[2] = 0xDE;
    buffer[3] = 0xBC;
    buffer[4] = 0x9A;
    buffer[5] = 0x78;
    buffer[6] = 0x56;
    buffer[7] = 0x34;
    buffer[8] = 0x12;
    
    uint64_t value = load_le64_safe(buffer + 1);  // Unaligned address
    EXPECT_EQ(value, 0x123456789ABCDEF0ULL);
}

// Test boundary values
TEST_F(EndianTest, BoundaryValues) {
    // Test minimum values
    store_le16(buffer, 0);
    EXPECT_EQ(load_le16(buffer), 0);
    
    store_le32(buffer, 0);
    EXPECT_EQ(load_le32(buffer), 0);
    
    store_le64(buffer, 0);
    EXPECT_EQ(load_le64(buffer), 0);
    
    // Test maximum values
    store_le16(buffer, UINT16_MAX);
    EXPECT_EQ(load_le16(buffer), UINT16_MAX);
    
    store_le32(buffer, UINT32_MAX);
    EXPECT_EQ(load_le32(buffer), UINT32_MAX);
    
    store_le64(buffer, UINT64_MAX);
    EXPECT_EQ(load_le64(buffer), UINT64_MAX);
}

// Test that conversions don't affect adjacent bytes
TEST_F(EndianTest, NoBufferOverrun) {
    // Fill buffer with sentinel values
    std::memset(buffer, 0xAA, sizeof(buffer));
    
    // Store 16-bit value
    store_le16(buffer + 2, 0x1234);
    EXPECT_EQ(buffer[0], 0xAA);  // Before: unchanged
    EXPECT_EQ(buffer[1], 0xAA);  // Before: unchanged
    EXPECT_EQ(buffer[2], 0x34);  // Changed
    EXPECT_EQ(buffer[3], 0x12);  // Changed
    EXPECT_EQ(buffer[4], 0xAA);  // After: unchanged
    
    // Reset and test 32-bit
    std::memset(buffer, 0xBB, sizeof(buffer));
    store_le32(buffer + 2, 0x12345678);
    EXPECT_EQ(buffer[0], 0xBB);  // Before: unchanged
    EXPECT_EQ(buffer[1], 0xBB);  // Before: unchanged
    EXPECT_EQ(buffer[6], 0xBB);  // After: unchanged
    
    // Reset and test 64-bit
    std::memset(buffer, 0xCC, sizeof(buffer));
    store_le64(buffer + 2, 0x123456789ABCDEF0ULL);
    EXPECT_EQ(buffer[0], 0xCC);  // Before: unchanged
    EXPECT_EQ(buffer[1], 0xCC);  // Before: unchanged
    EXPECT_EQ(buffer[10], 0xCC); // After: unchanged
}

// Test sequential reads/writes (simulating wire format usage)
TEST_F(EndianTest, SequentialWireFormat) {
    uint8_t* ptr = buffer;
    
    // Write various types sequentially
    store_le16(ptr, 0x1234); ptr += 2;
    store_le32(ptr, 0x56789ABC); ptr += 4;
    store_le64(ptr, 0xDEF0123456789ABCULL); ptr += 8;
    store_le16(ptr, 0xCDEF); ptr += 2;
    
    // Read them back
    ptr = buffer;
    EXPECT_EQ(load_le16(ptr), 0x1234); ptr += 2;
    EXPECT_EQ(load_le32(ptr), 0x56789ABC); ptr += 4;
    EXPECT_EQ(load_le64(ptr), 0xDEF0123456789ABCULL); ptr += 8;
    EXPECT_EQ(load_le16(ptr), 0xCDEF);
}

// Test that the format is truly portable (always little-endian regardless of host)
TEST_F(EndianTest, PortableWireFormat) {
    // These are the expected wire bytes for known values
    // This ensures the format doesn't change across platforms
    
    // 0x1234 in little-endian
    store_le16(buffer, 0x1234);
    EXPECT_EQ(buffer[0], 0x34);
    EXPECT_EQ(buffer[1], 0x12);
    
    // 0x12345678 in little-endian
    store_le32(buffer, 0x12345678);
    EXPECT_EQ(buffer[0], 0x78);
    EXPECT_EQ(buffer[1], 0x56);
    EXPECT_EQ(buffer[2], 0x34);
    EXPECT_EQ(buffer[3], 0x12);
    
    // 0x0123456789ABCDEF in little-endian
    store_le64(buffer, 0x0123456789ABCDEFULL);
    EXPECT_EQ(buffer[0], 0xEF);
    EXPECT_EQ(buffer[1], 0xCD);
    EXPECT_EQ(buffer[2], 0xAB);
    EXPECT_EQ(buffer[3], 0x89);
    EXPECT_EQ(buffer[4], 0x67);
    EXPECT_EQ(buffer[5], 0x45);
    EXPECT_EQ(buffer[6], 0x23);
    EXPECT_EQ(buffer[7], 0x01);
}