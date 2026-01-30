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
#include <cstring>

namespace xtree {
namespace util {

/**
 * Endianness conversion utilities for portable wire formats.
 * 
 * These functions convert between host byte order and little-endian
 * wire format. On little-endian hosts (x86, ARM in LE mode), these
 * are essentially no-ops. On big-endian hosts, they perform byte swapping.
 * 
 * The wire format is always little-endian for portability.
 */

// Store functions - convert from host to little-endian wire format

inline void store_le16(uint8_t* buf, uint16_t val) {
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
}

inline void store_le32(uint8_t* buf, uint32_t val) {
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
    buf[2] = static_cast<uint8_t>(val >> 16);
    buf[3] = static_cast<uint8_t>(val >> 24);
}

inline void store_le64(uint8_t* buf, uint64_t val) {
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
    buf[2] = static_cast<uint8_t>(val >> 16);
    buf[3] = static_cast<uint8_t>(val >> 24);
    buf[4] = static_cast<uint8_t>(val >> 32);
    buf[5] = static_cast<uint8_t>(val >> 40);
    buf[6] = static_cast<uint8_t>(val >> 48);
    buf[7] = static_cast<uint8_t>(val >> 56);
}

// Load functions - convert from little-endian wire format to host

inline uint16_t load_le16(const uint8_t* buf) {
    return static_cast<uint16_t>(buf[0]) |
           (static_cast<uint16_t>(buf[1]) << 8);
}

inline uint32_t load_le32(const uint8_t* buf) {
    return static_cast<uint32_t>(buf[0]) |
           (static_cast<uint32_t>(buf[1]) << 8) |
           (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) << 24);
}

inline uint64_t load_le64(const uint8_t* buf) {
    return static_cast<uint64_t>(buf[0]) |
           (static_cast<uint64_t>(buf[1]) << 8) |
           (static_cast<uint64_t>(buf[2]) << 16) |
           (static_cast<uint64_t>(buf[3]) << 24) |
           (static_cast<uint64_t>(buf[4]) << 32) |
           (static_cast<uint64_t>(buf[5]) << 40) |
           (static_cast<uint64_t>(buf[6]) << 48) |
           (static_cast<uint64_t>(buf[7]) << 56);
}

// Float support - IEEE 754 single precision (32-bit)
// Treats float bits as uint32 for endian conversion

inline void store_lef32(uint8_t* buf, float val) {
    uint32_t bits;
    std::memcpy(&bits, &val, sizeof(float));
    store_le32(buf, bits);
}

inline float load_lef32(const uint8_t* buf) {
    uint32_t bits = load_le32(buf);
    float val;
    std::memcpy(&val, &bits, sizeof(float));
    return val;
}

// Double support - IEEE 754 double precision (64-bit)
// Treats double bits as uint64 for endian conversion

inline void store_lef64(uint8_t* buf, double val) {
    uint64_t bits;
    std::memcpy(&bits, &val, sizeof(double));
    store_le64(buf, bits);
}

inline double load_lef64(const uint8_t* buf) {
    uint64_t bits = load_le64(buf);
    double val;
    std::memcpy(&val, &bits, sizeof(double));
    return val;
}

// Convenience functions using memcpy for safer unaligned access
// These avoid undefined behavior when reading/writing to unaligned addresses

inline void store_le16_safe(void* buf, uint16_t val) {
    uint8_t bytes[2];
    store_le16(bytes, val);
    std::memcpy(buf, bytes, 2);
}

inline void store_le32_safe(void* buf, uint32_t val) {
    uint8_t bytes[4];
    store_le32(bytes, val);
    std::memcpy(buf, bytes, 4);
}

inline void store_le64_safe(void* buf, uint64_t val) {
    uint8_t bytes[8];
    store_le64(bytes, val);
    std::memcpy(buf, bytes, 8);
}

inline uint16_t load_le16_safe(const void* buf) {
    uint8_t bytes[2];
    std::memcpy(bytes, buf, 2);
    return load_le16(bytes);
}

inline uint32_t load_le32_safe(const void* buf) {
    uint8_t bytes[4];
    std::memcpy(bytes, buf, 4);
    return load_le32(bytes);
}

inline uint64_t load_le64_safe(const void* buf) {
    uint8_t bytes[8];
    std::memcpy(bytes, buf, 8);
    return load_le64(bytes);
}

// Host endianness detection (compile-time when possible)
#if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__)
    #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        constexpr bool is_little_endian_host = true;
    #else
        constexpr bool is_little_endian_host = false;
    #endif
#elif defined(_WIN32) || defined(_WIN64)
    // Windows on x86/x64 is always little-endian
    constexpr bool is_little_endian_host = true;
#else
    // Runtime detection fallback
    inline bool is_little_endian_host() {
        union {
            uint32_t i;
            uint8_t c[4];
        } test = {0x01020304};
        return test.c[0] == 0x04;
    }
#endif

} // namespace util
} // namespace xtree