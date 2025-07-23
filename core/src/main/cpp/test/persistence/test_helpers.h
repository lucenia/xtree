/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Common test helpers for persistence tests
 */

#pragma once

#include <string>
#include <filesystem>
#include <fstream>
#include <random>
#include <vector>
#include <cstring>

namespace xtree::persist::test {

// Create a temporary directory for testing
inline std::string create_temp_dir(const std::string& prefix) {
    std::filesystem::path temp_path = std::filesystem::temp_directory_path();
    
    // Generate random suffix
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(10000, 99999);
    
    std::string dir_name = prefix + "_" + std::to_string(dis(gen));
    std::filesystem::path test_dir = temp_path / dir_name;
    
    std::filesystem::create_directories(test_dir);
    return test_dir.string();
}

// Generate test data with a pattern
inline std::vector<uint8_t> generate_test_data(size_t size, uint8_t pattern = 0) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; i++) {
        data[i] = pattern + (i % 256);
    }
    return data;
}

// Verify data matches expected pattern
inline bool verify_test_data(const void* ptr, size_t size, uint8_t pattern) {
    const uint8_t* data = static_cast<const uint8_t*>(ptr);
    for (size_t i = 0; i < size; i++) {
        if (data[i] != uint8_t(pattern + (i % 256))) {
            return false;
        }
    }
    return true;
}

// Corrupt a file at a specific offset
inline void corrupt_file(const std::string& path, size_t offset, size_t len) {
    std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) return;
    
    file.seekp(offset);
    std::vector<uint8_t> garbage(len, 0xFF);
    file.write(reinterpret_cast<char*>(garbage.data()), len);
}

// Truncate file to simulate torn write
inline void truncate_file(const std::string& path, size_t new_size) {
    std::filesystem::resize_file(path, new_size);
}

// Helper to fill buffer with pattern
inline void fill_pattern(void* buf, size_t len, uint32_t pattern) {
    uint8_t* ptr = static_cast<uint8_t*>(buf);
    for (size_t i = 0; i < len; i += 4) {
        if (i + 4 <= len) {
            std::memcpy(ptr + i, &pattern, 4);
        } else {
            std::memcpy(ptr + i, &pattern, len - i);
        }
    }
}

} // namespace xtree::persist::test