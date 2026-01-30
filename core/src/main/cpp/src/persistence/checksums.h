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

#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include <mutex>

namespace xtree {
namespace persist {

// CRC32C (Castagnoli) - optimized for modern CPUs with hardware support
class CRC32C {
public:
    using value_type = uint32_t;
    
    CRC32C() : value_(~0u) {}
    
    // Update CRC with more data
    void update(const void* data, size_t len);
    void update(const uint8_t* data, size_t len) {
        update(static_cast<const void*>(data), len);
    }
    void update(const std::vector<uint8_t>& data) {
        update(data.data(), data.size());
    }
    
    // Get final CRC value
    uint32_t finalize() const { return value_ ^ 0xFFFFFFFF; }
    
    // Reset to initial state
    void reset() { value_ = ~0u; }
    
    // One-shot computation
    static uint32_t compute(const void* data, size_t len);
    static uint32_t compute(const uint8_t* data, size_t len) {
        return compute(static_cast<const void*>(data), len);
    }
    static uint32_t compute(const std::vector<uint8_t>& data) {
        return compute(data.data(), data.size());
    }
    
    // Combine two CRCs (useful for parallel computation)
    static uint32_t combine(uint32_t crc1, uint32_t crc2, size_t len2);
    
    // Public for benchmarking
    static uint32_t software_crc32c(uint32_t crc, const uint8_t* data, size_t len);
    
    #if defined(__x86_64__) || defined(_M_X64)
    static bool has_sse42();
    static uint32_t hardware_crc32c(uint32_t crc, const uint8_t* data, size_t len);
    #endif
    
    #if defined(__aarch64__) || defined(_M_ARM64)
    static bool has_crc32();
    static uint32_t hardware_crc32c_arm(uint32_t crc, const uint8_t* data, size_t len);
    #endif
    
private:
    uint32_t value_;
    
    // CRC32C polynomial (Castagnoli)
    static constexpr uint32_t kPolynomial = 0x82F63B78;
    
    // Tables for software implementation (slicing-by-8)
    static uint32_t table8_[8][256];
    static std::once_flag init_once_;
    static void init_tables();
    
    static inline uint32_t crc32c_byte(uint32_t crc, uint8_t b) {
        return (crc >> 8) ^ table8_[0][(crc ^ b) & 0xFF];
    }
};

// XXHash64 - fast non-cryptographic hash
class XXHash64 {
public:
    using value_type = uint64_t;
    
    XXHash64(uint64_t seed = 0);
    
    // Update hash with more data
    void update(const void* data, size_t len);
    void update(const uint8_t* data, size_t len) {
        update(static_cast<const void*>(data), len);
    }
    void update(const std::vector<uint8_t>& data) {
        update(data.data(), data.size());
    }
    
    // Get final hash value
    uint64_t finalize();
    
    // Reset with new seed
    void reset(uint64_t seed = 0);
    
    // One-shot computation
    static uint64_t compute(const void* data, size_t len, uint64_t seed = 0);
    static uint64_t compute(const uint8_t* data, size_t len, uint64_t seed = 0) {
        return compute(static_cast<const void*>(data), len, seed);
    }
    static uint64_t compute(const std::vector<uint8_t>& data, uint64_t seed = 0) {
        return compute(data.data(), data.size(), seed);
    }
    
private:
    uint64_t seed_;
    uint64_t v1_, v2_, v3_, v4_;
    uint64_t total_len_;
    size_t memsize_;
    uint8_t memory_[32];
    
    static constexpr uint64_t kPrime1 = 11400714785074694791ULL;
    static constexpr uint64_t kPrime2 = 14029467366897019727ULL;
    static constexpr uint64_t kPrime3 = 1609587929392839161ULL;
    static constexpr uint64_t kPrime4 = 9650029242287828579ULL;
    static constexpr uint64_t kPrime5 = 2870177450012600261ULL;
    
    static uint64_t rotate_left(uint64_t x, int r) {
        return (x << r) | (x >> (64 - r));
    }
    
    static uint64_t round(uint64_t acc, uint64_t input);
    static uint64_t merge_round(uint64_t acc, uint64_t val);
};

// CRC64 - ECMA-182 polynomial for larger data integrity checks
class CRC64 {
public:
    using value_type = uint64_t;
    
    CRC64() : value_(~0ULL) {}
    
    void update(const void* data, size_t len);
    void update(const uint8_t* data, size_t len) {
        update(static_cast<const void*>(data), len);
    }
    void update(const std::vector<uint8_t>& data) {
        update(data.data(), data.size());
    }
    
    uint64_t finalize() const { return value_ ^ 0xFFFFFFFFFFFFFFFFULL; }
    void reset() { value_ = ~0ULL; }
    
    static uint64_t compute(const void* data, size_t len);
    static uint64_t compute(const uint8_t* data, size_t len) {
        return compute(static_cast<const void*>(data), len);
    }
    static uint64_t compute(const std::vector<uint8_t>& data) {
        return compute(data.data(), data.size());
    }
    
    static uint64_t combine(uint64_t crc1, uint64_t crc2, size_t len2);
    
private:
    uint64_t value_;
    
    // ECMA-182 polynomial
    static constexpr uint64_t kPolynomial = 0xC96C5795D7870F42ULL;
    
    static uint64_t kTable[256];
    static std::once_flag init_once_;
    static void init_table();
};

// Adler-32 checksum
class Adler32 {
public:
    using value_type = uint32_t;
    
    Adler32() : a_(1), b_(0) {}
    
    void update(const void* data, size_t len);
    void update(const uint8_t* data, size_t len) {
        update(static_cast<const void*>(data), len);
    }
    void update(const std::vector<uint8_t>& data) {
        update(data.data(), data.size());
    }
    
    uint32_t finalize() const { return (b_ << 16) | a_; }
    void reset() { a_ = 1; b_ = 0; }
    
    static uint32_t compute(const void* data, size_t len);
    static uint32_t compute(const uint8_t* data, size_t len) {
        return compute(static_cast<const void*>(data), len);
    }
    static uint32_t compute(const std::vector<uint8_t>& data) {
        return compute(data.data(), data.size());
    }
    
private:
    uint32_t a_, b_;
    
    static constexpr uint32_t kBase = 65521; // Largest prime < 65536
    static constexpr uint32_t NMAX = 5552;    // Chunk size to avoid overflow
};

// Convenience functions for common use cases
inline uint32_t crc32c(const uint8_t* data, size_t len) {
    return CRC32C::compute(data, len);
}

inline uint32_t crc32c(const void* data, size_t len) {
    return CRC32C::compute(data, len);
}

// Utility functions
namespace checksum_utils {
    // Generic checksum verification
    template<typename ChecksumType, typename Expected>
    bool verify(const void* data, size_t len, Expected expected) {
        return ChecksumType::compute(data, len) == static_cast<Expected>(expected);
    }
    
    // Select best checksum for given data size
    enum class ChecksumType {
        Adler32,   // Fast, good for small data
        CRC32C,    // Hardware accelerated, good balance
        XXHash64,  // Very fast, good distribution
        CRC64      // Strong error detection
    };
    
    ChecksumType select_checksum(size_t data_size, bool need_crypto_strength = false);
}

} // namespace persist
} // namespace xtree