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

#include "checksums.h"
#include <cstring>
#include <cassert>
#include <cstdlib>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#ifdef _MSC_VER
#include <intrin.h>
#endif
#endif

namespace xtree {
namespace persist {

// ============================================================================
// CRC32C Implementation (Castagnoli polynomial)
// ============================================================================

// Static member definitions
uint32_t CRC32C::table8_[8][256];
std::once_flag CRC32C::init_once_;

void CRC32C::init_tables() {
    // Generate main table for Castagnoli polynomial (0x82F63B78 - reflected)
    constexpr uint32_t poly = 0x82F63B78u;
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t r = i;
        for (int k = 0; k < 8; ++k) {
            r = (r >> 1) ^ (-(int)(r & 1) & poly);
        }
        table8_[0][i] = r;
    }
    
    // Generate tables 1..7 for slicing-by-8
    for (int t = 1; t < 8; ++t) {
        for (uint32_t i = 0; i < 256; ++i) {
            table8_[t][i] = (table8_[t-1][i] >> 8) ^ table8_[0][table8_[t-1][i] & 0xFF];
        }
    }
}

void CRC32C::update(const void* data, size_t len) {
    std::call_once(init_once_, &CRC32C::init_tables);
    
    if (len == 0 || data == nullptr) return;
    
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t crc = value_;
    
#if defined(__x86_64__) || defined(_M_X64)
    if (has_sse42()) {
        // Hardware CRC32C using SSE4.2 intrinsics
        crc = hardware_crc32c(crc, p, len);
    } else
#elif defined(__aarch64__) || defined(_M_ARM64)
    if (has_crc32()) {
        // Hardware CRC32C using ARMv8 CRC32 instructions
        crc = hardware_crc32c_arm(crc, p, len);
    } else
#endif
    {
        // Software fallback: slicing-by-8
        // Align to 8-byte boundary
        while (len > 0 && ((uintptr_t)p & 7) != 0) {
            crc = crc32c_byte(crc, *p++);
            len--;
        }
        
        // Process 8 bytes at a time
        while (len >= 8) {
            uint64_t v;
            memcpy(&v, p, 8);  // Safe unaligned access
            uint32_t c = crc ^ (uint32_t)v;
            uint32_t d = (uint32_t)(v >> 32);
            
            crc = table8_[7][(c      ) & 0xFF] ^
                  table8_[6][(c >>  8) & 0xFF] ^
                  table8_[5][(c >> 16) & 0xFF] ^
                  table8_[4][(c >> 24) & 0xFF] ^
                  table8_[3][(d      ) & 0xFF] ^
                  table8_[2][(d >>  8) & 0xFF] ^
                  table8_[1][(d >> 16) & 0xFF] ^
                  table8_[0][(d >> 24) & 0xFF];
            
            p += 8;
            len -= 8;
        }
        
        // Process remaining bytes
        while (len--) {
            crc = crc32c_byte(crc, *p++);
        }
    }
    
    value_ = crc;
}

uint32_t CRC32C::compute(const void* data, size_t len) {
    CRC32C crc;
    crc.update(data, len);
    return crc.finalize();
}

uint32_t CRC32C::combine(uint32_t crc1, uint32_t crc2, size_t len2) {
    // TODO: Implement proper GF(2) matrix exponentiation for combine
    // This requires polynomial arithmetic in GF(2) which is non-trivial
    (void)crc1;
    (void)crc2;
    (void)len2;
    assert(false && "CRC32C::combine not implemented - needs GF(2) matrix math");
    abort();  // Fail hard in production too
}

uint32_t CRC32C::software_crc32c(uint32_t crc, const uint8_t* data, size_t len) {
    std::call_once(init_once_, &CRC32C::init_tables);
    
    for (size_t i = 0; i < len; i++) {
        crc = crc32c_byte(crc, data[i]);
    }
    return crc;
}

#if defined(__x86_64__) || defined(_M_X64)
bool CRC32C::has_sse42() {
    // Simple CPUID check for SSE4.2
    #ifdef _MSC_VER
        int cpuInfo[4];
        __cpuid(cpuInfo, 1);
        return (cpuInfo[2] & (1 << 20)) != 0;  // SSE4.2 bit
    #else
        uint32_t ecx;
        __asm__ ("cpuid" : "=c"(ecx) : "a"(1) : "ebx", "edx");
        return (ecx & (1 << 20)) != 0;
    #endif
}

#ifdef __GNUC__
__attribute__((target("sse4.2")))
#endif
uint32_t CRC32C::hardware_crc32c(uint32_t crc, const uint8_t* data, size_t len) {
    // Use SSE4.2 CRC32 instruction
    while (len >= 8) {
        uint64_t v;
        memcpy(&v, data, 8);
        crc = _mm_crc32_u64(crc, v);
        data += 8;
        len -= 8;
    }
    
    while (len >= 4) {
        uint32_t v;
        memcpy(&v, data, 4);
        crc = _mm_crc32_u32(crc, v);
        data += 4;
        len -= 4;
    }
    
    while (len--) {
        crc = _mm_crc32_u8(crc, *data++);
    }
    
    return crc;
}
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#ifdef __ARM_FEATURE_CRC32
#include <arm_acle.h>
#endif

bool CRC32C::has_crc32() {
#ifdef __ARM_FEATURE_CRC32
    // ARMv8 with CRC32 extension - available on all Apple Silicon
    return true;
#else
    // Older ARM or CRC32 not available at compile time
    return false;
#endif
}

uint32_t CRC32C::hardware_crc32c_arm(uint32_t crc, const uint8_t* data, size_t len) {
#ifdef __ARM_FEATURE_CRC32
    // Use ARMv8 CRC32C instructions (Castagnoli polynomial)
    // These are available on all Apple Silicon Macs
    
    // Process 8 bytes at a time
    while (len >= 8) {
        uint64_t v;
        memcpy(&v, data, 8);
        crc = __crc32cd(crc, v);
        data += 8;
        len -= 8;
    }
    
    // Process 4 bytes
    if (len >= 4) {
        uint32_t v;
        memcpy(&v, data, 4);
        crc = __crc32cw(crc, v);
        data += 4;
        len -= 4;
    }
    
    // Process 2 bytes
    if (len >= 2) {
        uint16_t v;
        memcpy(&v, data, 2);
        crc = __crc32ch(crc, v);
        data += 2;
        len -= 2;
    }
    
    // Process 1 byte
    if (len >= 1) {
        crc = __crc32cb(crc, *data);
    }
    
    return crc;
#else
    // Fallback to software implementation
    return software_crc32c(crc, data, len);
#endif
}
#endif

// ============================================================================
// CRC64 Implementation (ECMA-182 polynomial)
// ============================================================================

uint64_t CRC64::kTable[256];
std::once_flag CRC64::init_once_;

void CRC64::init_table() {
    // ECMA-182 polynomial (reflected form)
    constexpr uint64_t poly = 0xC96C5795D7870F42ULL;
    
    for (uint64_t i = 0; i < 256; ++i) {
        uint64_t r = i;
        for (int k = 0; k < 8; ++k) {
            if (r & 1ULL) {
                r = (r >> 1) ^ poly;
            } else {
                r >>= 1;
            }
        }
        kTable[i] = r;
    }
}

void CRC64::update(const void* data, size_t len) {
    std::call_once(init_once_, &CRC64::init_table);
    
    if (len == 0 || data == nullptr) return;
    
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint64_t crc = value_;
    
    while (len--) {
        crc = (crc >> 8) ^ kTable[(crc ^ *p++) & 0xFF];
    }
    
    value_ = crc;
}

uint64_t CRC64::compute(const void* data, size_t len) {
    CRC64 crc;
    crc.update(data, len);
    return crc.finalize();
}

uint64_t CRC64::combine(uint64_t crc1, uint64_t crc2, size_t len2) {
    // TODO: Implement proper GF(2) matrix exponentiation for combine
    // This requires polynomial arithmetic in GF(2) which is non-trivial
    (void)crc1;
    (void)crc2;
    (void)len2;
    assert(false && "CRC64::combine not implemented - needs GF(2) matrix math");
    abort();  // Fail hard in production too
}

// ============================================================================
// Adler-32 Implementation
// ============================================================================

void Adler32::update(const void* data, size_t len) {
    if (len == 0 || data == nullptr) return;
    
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint32_t a = a_;
    uint32_t b = b_;
    
    // Process in chunks to avoid overflow
    while (len > 0) {
        size_t n = (len < NMAX) ? len : NMAX;
        len -= n;
        
        // Unroll loop for better performance
        while (n >= 16) {
            for (int i = 0; i < 16; ++i) {
                a += p[i];
                b += a;
            }
            p += 16;
            n -= 16;
        }
        
        // Handle remaining bytes
        while (n--) {
            a += *p++;
            b += a;
        }
        
        // Modulo to prevent overflow
        a %= kBase;
        b %= kBase;
    }
    
    a_ = a;
    b_ = b;
}

uint32_t Adler32::compute(const void* data, size_t len) {
    Adler32 adler;
    adler.update(data, len);
    return adler.finalize();
}

// ============================================================================
// XXHash64 Implementation (simplified - recommend using official xxHash)
// ============================================================================

XXHash64::XXHash64(uint64_t seed) 
    : seed_(seed), total_len_(0), memsize_(0) {
    v1_ = seed + kPrime1 + kPrime2;
    v2_ = seed + kPrime2;
    v3_ = seed;
    v4_ = seed - kPrime1;
}

void XXHash64::update(const void* data, size_t len) {
    if (len == 0 || data == nullptr) return;
    
    const uint8_t* p = static_cast<const uint8_t*>(data);
    total_len_ += len;
    
    // Handle buffered data
    if (memsize_ + len < 32) {
        memcpy(memory_ + memsize_, p, len);
        memsize_ += len;
        return;
    }
    
    if (memsize_ > 0) {
        size_t to_copy = 32 - memsize_;
        memcpy(memory_ + memsize_, p, to_copy);
        
        // Process the 32-byte buffer
        const uint64_t* p64 = reinterpret_cast<const uint64_t*>(memory_);
        v1_ = round(v1_, p64[0]);
        v2_ = round(v2_, p64[1]);
        v3_ = round(v3_, p64[2]);
        v4_ = round(v4_, p64[3]);
        
        p += to_copy;
        len -= to_copy;
        memsize_ = 0;
    }
    
    // Process 32-byte blocks
    const uint8_t* limit = p + len - 32;
    while (p <= limit) {
        const uint64_t* p64 = reinterpret_cast<const uint64_t*>(p);
        v1_ = round(v1_, p64[0]);
        v2_ = round(v2_, p64[1]);
        v3_ = round(v3_, p64[2]);
        v4_ = round(v4_, p64[3]);
        p += 32;
    }
    
    // Buffer remaining bytes
    size_t remaining = len & 31;
    if (remaining > 0) {
        memsize_ = remaining;
        memcpy(memory_, p, memsize_);
    }
}

uint64_t XXHash64::finalize() {
    uint64_t h64;
    
    if (total_len_ >= 32) {
        h64 = rotate_left(v1_, 1) + rotate_left(v2_, 7) + 
              rotate_left(v3_, 12) + rotate_left(v4_, 18);
        h64 = merge_round(h64, v1_);
        h64 = merge_round(h64, v2_);
        h64 = merge_round(h64, v3_);
        h64 = merge_round(h64, v4_);
    } else {
        h64 = seed_ + kPrime5;
    }
    
    h64 += total_len_;
    
    // Process remaining bytes
    const uint8_t* p = memory_;
    const uint8_t* end = p + memsize_;
    
    while (p + 8 <= end) {
        uint64_t k1;
        memcpy(&k1, p, 8);
        k1 *= kPrime2;
        k1 = rotate_left(k1, 31);
        k1 *= kPrime1;
        h64 ^= k1;
        h64 = rotate_left(h64, 27) * kPrime1 + kPrime4;
        p += 8;
    }
    
    if (p + 4 <= end) {
        uint32_t k1;
        memcpy(&k1, p, 4);
        h64 ^= k1 * kPrime1;
        h64 = rotate_left(h64, 23) * kPrime2 + kPrime3;
        p += 4;
    }
    
    while (p < end) {
        h64 ^= (*p++) * kPrime5;
        h64 = rotate_left(h64, 11) * kPrime1;
    }
    
    // Final avalanche
    h64 ^= h64 >> 33;
    h64 *= kPrime2;
    h64 ^= h64 >> 29;
    h64 *= kPrime3;
    h64 ^= h64 >> 32;
    
    return h64;
}

void XXHash64::reset(uint64_t seed) {
    seed_ = seed;
    total_len_ = 0;
    memsize_ = 0;
    v1_ = seed + kPrime1 + kPrime2;
    v2_ = seed + kPrime2;
    v3_ = seed;
    v4_ = seed - kPrime1;
}

uint64_t XXHash64::compute(const void* data, size_t len, uint64_t seed) {
    XXHash64 hash(seed);
    hash.update(data, len);
    return hash.finalize();
}

uint64_t XXHash64::round(uint64_t acc, uint64_t input) {
    acc += input * kPrime2;
    acc = rotate_left(acc, 31);
    acc *= kPrime1;
    return acc;
}

uint64_t XXHash64::merge_round(uint64_t acc, uint64_t val) {
    val = round(0, val);
    acc ^= val;
    acc = acc * kPrime1 + kPrime4;
    return acc;
}

// ============================================================================
// Utility Functions
// ============================================================================

namespace checksum_utils {

ChecksumType select_checksum(size_t data_size, bool need_crypto_strength) {
    (void)need_crypto_strength;  // TODO: Add crypto checksums if needed
    
    // Only use fully implemented algorithms
    // XXHash64 and CRC64 are shelved until proper vendor libraries are added
    if (data_size < 1024) {
        return ChecksumType::Adler32;  // Fast for very small data
    } else {
        return ChecksumType::CRC32C;   // Hardware accelerated, good for all sizes
    }
}

} // namespace checksum_utils

} // namespace persist
} // namespace xtree