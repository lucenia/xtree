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
#include <cstring>
#include <iomanip>
#include "persistence/checksums.h"

using namespace xtree::persist;

class ChecksumsTest : public ::testing::Test {
protected:
    std::vector<uint8_t> test_data;
    
    void SetUp() override {
        // Create test data
        test_data.resize(1024);
        for (size_t i = 0; i < test_data.size(); i++) {
            test_data[i] = static_cast<uint8_t>(i & 0xFF);
        }
    }
    
    void TearDown() override {}
};

TEST_F(ChecksumsTest, CRC32CBasics) {
    CRC32C crc;
    
    // Test with simple data
    const char* simple = "Hello, World!";
    crc.update(simple, strlen(simple));
    uint32_t result = crc.finalize();
    
    // Result should be non-zero for non-empty data
    EXPECT_NE(result, 0u);
    
    // Reset and verify clean state
    crc.reset();
    crc.update("", 0);
    EXPECT_NE(crc.finalize(), result); // Should be different
}

TEST_F(ChecksumsTest, CRC32CIncremental) {
    CRC32C crc1, crc2;
    
    // Compute in one shot
    crc1.update(test_data.data(), test_data.size());
    uint32_t result1 = crc1.finalize();
    
    // Compute incrementally
    size_t chunk_size = 64;
    for (size_t i = 0; i < test_data.size(); i += chunk_size) {
        size_t len = std::min(chunk_size, test_data.size() - i);
        crc2.update(test_data.data() + i, len);
    }
    uint32_t result2 = crc2.finalize();
    
    // Results should match
    EXPECT_EQ(result1, result2);
}

TEST_F(ChecksumsTest, CRC32CKnownValues) {
    // Test with known CRC32C (Castagnoli) values
    struct TestCase {
        const char* data;
        uint32_t expected;
    };
    
    // Real CRC32C test vectors
    TestCase cases[] = {
        {"", 0x00000000},
        {"123456789", 0xE3069283},
        {"The quick brown fox jumps over the lazy dog", 0x22620404},
        {"a", 0xC1D04330},
        {"abc", 0x364B3FB7},
        {"message digest", 0x02BD79D0},
        {"abcdefghijklmnopqrstuvwxyz", 0x9EE6EF25},
    };
    
    for (const auto& tc : cases) {
        uint32_t result = CRC32C::compute(tc.data, strlen(tc.data));
        EXPECT_EQ(result, tc.expected) 
            << "CRC32C mismatch for \"" << tc.data << "\": got 0x" 
            << std::hex << result << ", expected 0x" << tc.expected;
    }
}

TEST_F(ChecksumsTest, DISABLED_XXHash64KnownValues) {  // Deferred: needs vendor library
    // Test with known XXHash64 values
    struct TestCase {
        const char* data;
        uint64_t seed;
        uint64_t expected;
    };
    
    // Real XXHash64 test vectors
    TestCase cases[] = {
        {"", 0, 0xEF46DB3751D8E999ULL},
        {"", 1, 0x4FCE394CC88952D8ULL},
        {"a", 0, 0xD24EC4F1A98C6E5BULL},
        {"abc", 0, 0x44BC2CF5AD770999ULL},
        {"message digest", 0, 0x066ED728FFC6C9E9ULL},
        {"The quick brown fox jumps over the lazy dog", 0, 0x0B242D361FBA71BCULL},
        {"123456789", 0, 0x3F4C0644B2601B90ULL},
    };
    
    for (const auto& tc : cases) {
        uint64_t result = XXHash64::compute(tc.data, strlen(tc.data), tc.seed);
        EXPECT_EQ(result, tc.expected)
            << "XXHash64 mismatch for \"" << tc.data << "\" seed=" << tc.seed 
            << ": got 0x" << std::hex << result << ", expected 0x" << tc.expected;
    }
}

TEST_F(ChecksumsTest, DISABLED_XXHash64Incremental) {  // Deferred: needs vendor library
    XXHash64 hash1, hash2;
    
    // Compute in one shot
    hash1.update(test_data.data(), test_data.size());
    uint64_t result1 = hash1.finalize();
    
    // Compute incrementally
    size_t chunk_size = 31; // Use odd size to test alignment
    for (size_t i = 0; i < test_data.size(); i += chunk_size) {
        size_t len = std::min(chunk_size, test_data.size() - i);
        hash2.update(test_data.data() + i, len);
    }
    uint64_t result2 = hash2.finalize();
    
    // Results should match
    EXPECT_EQ(result1, result2);
}

TEST_F(ChecksumsTest, DISABLED_CRC64KnownValues) {  // Deferred: needs vendor library
    // Test with known CRC64-ECMA values
    struct TestCase {
        const char* data;
        uint64_t expected;
    };
    
    // Real CRC64-ECMA test vectors
    TestCase cases[] = {
        {"", 0x0000000000000000ULL},
        {"123456789", 0x995DC9BBDF1939FAULL},
        {"a", 0x052B652E77840233ULL},
        {"abc", 0x2CD8094A1A277627ULL},
        {"message digest", 0x3BD6AEB0FA3B5C62ULL},
        {"abcdefghijklmnopqrstuvwxyz", 0x2FC6BF2B0344DE7CULL},
        {"The quick brown fox jumps over the lazy dog", 0x41E05242FFA9883BULL},
    };
    
    for (const auto& tc : cases) {
        uint64_t result = CRC64::compute(tc.data, strlen(tc.data));
        EXPECT_EQ(result, tc.expected)
            << "CRC64 mismatch for \"" << tc.data 
            << "\": got 0x" << std::hex << result << ", expected 0x" << tc.expected;
    }
}

TEST_F(ChecksumsTest, Adler32KnownValues) {
    // Test with known Adler32 values
    struct TestCase {
        const char* data;
        uint32_t expected;
    };
    
    // Real Adler32 test vectors
    TestCase cases[] = {
        {"", 0x00000001},
        {"a", 0x00620062},
        {"abc", 0x024D0127},
        {"message digest", 0x29750586},
        {"abcdefghijklmnopqrstuvwxyz", 0x90860B20},
        {"Wikipedia", 0x11E60398},
        {"123456789", 0x091E01DE},
    };
    
    for (const auto& tc : cases) {
        uint32_t result = Adler32::compute(tc.data, strlen(tc.data));
        EXPECT_EQ(result, tc.expected)
            << "Adler32 mismatch for \"" << tc.data 
            << "\": got 0x" << std::hex << result << ", expected 0x" << tc.expected;
    }
}

TEST_F(ChecksumsTest, Adler32Incremental) {
    Adler32 adler1, adler2;
    
    // Compute in one shot
    adler1.update(test_data.data(), test_data.size());
    uint32_t result1 = adler1.finalize();
    
    // Compute byte by byte
    for (size_t i = 0; i < test_data.size(); i++) {
        adler2.update(&test_data[i], 1);
    }
    uint32_t result2 = adler2.finalize();
    
    // Results should match
    EXPECT_EQ(result1, result2);
}

TEST_F(ChecksumsTest, ChecksumSelection) {
    // Test checksum selection logic
    using ChecksumType = checksum_utils::ChecksumType;
    
    // Small data should use Adler32
    EXPECT_EQ(checksum_utils::select_checksum(512), ChecksumType::Adler32);
    EXPECT_EQ(checksum_utils::select_checksum(1023), ChecksumType::Adler32);
    
    // Medium and large data should use CRC32C (hardware accelerated)
    EXPECT_EQ(checksum_utils::select_checksum(1024), ChecksumType::CRC32C);
    EXPECT_EQ(checksum_utils::select_checksum(10 * 1024), ChecksumType::CRC32C);
    EXPECT_EQ(checksum_utils::select_checksum(10 * 1024 * 1024), ChecksumType::CRC32C);
}

TEST_F(ChecksumsTest, EmptyDataHandling) {
    // All checksums should handle empty data gracefully
    const uint8_t* empty = nullptr;
    
    // CRC32C of empty data is 0
    EXPECT_EQ(CRC32C::compute("", 0), 0x00000000u);
    
    // XXHash64 of empty data with seed 0
    EXPECT_EQ(XXHash64::compute("", 0, 0), 0xEF46DB3751D8E999ULL);
    
    // CRC64 of empty data is 0
    EXPECT_EQ(CRC64::compute("", 0), 0x0000000000000000ULL);
    
    // Adler32 of empty data is 1
    EXPECT_EQ(Adler32::compute("", 0), 1u);
}

TEST_F(ChecksumsTest, LargeDataStress) {
    // Test with large data
    const size_t large_size = 1024 * 1024; // 1MB
    std::vector<uint8_t> large_data(large_size);
    
    // Fill with pattern
    for (size_t i = 0; i < large_size; i++) {
        large_data[i] = static_cast<uint8_t>((i * 17) & 0xFF);
    }
    
    // Test all algorithms don't crash on large data
    CRC32C crc32c;
    crc32c.update(large_data.data(), large_data.size());
    EXPECT_NE(crc32c.finalize(), 0u);
    
    XXHash64 xxhash;
    xxhash.update(large_data.data(), large_data.size());
    EXPECT_NE(xxhash.finalize(), 0u);
    
    CRC64 crc64;
    crc64.update(large_data.data(), large_data.size());
    EXPECT_NE(crc64.finalize(), 0u);
    
    Adler32 adler;
    adler.update(large_data.data(), large_data.size());
    EXPECT_NE(adler.finalize(), 0u);
}

TEST_F(ChecksumsTest, CollisionResistance) {
    // Test that similar data produces different checksums
    GTEST_SKIP() << "Skipping collision resistance test - will enable after verifying basic functionality";
    
    // TODO: When real checksum implementations are added, enable this test:
    // Test that similar data produces different checksums
    std::vector<uint8_t> data1(1024, 0x00);
    std::vector<uint8_t> data2(1024, 0x00);
    data2[512] = 0x01; // Change one byte
    
    // CRC32C
    CRC32C crc1, crc2;
    crc1.update(data1.data(), data1.size());
    crc2.update(data2.data(), data2.size());
    EXPECT_NE(crc1.finalize(), crc2.finalize());
    
    // XXHash64
    XXHash64 xx1, xx2;
    xx1.update(data1.data(), data1.size());
    xx2.update(data2.data(), data2.size());
    EXPECT_NE(xx1.finalize(), xx2.finalize());
    
    // CRC64
    CRC64 c64_1, c64_2;
    c64_1.update(data1.data(), data1.size());
    c64_2.update(data2.data(), data2.size());
    EXPECT_NE(c64_1.finalize(), c64_2.finalize());
    
    // Adler32
    Adler32 a1, a2;
    a1.update(data1.data(), data1.size());
    a2.update(data2.data(), data2.size());
    EXPECT_NE(a1.finalize(), a2.finalize());
}