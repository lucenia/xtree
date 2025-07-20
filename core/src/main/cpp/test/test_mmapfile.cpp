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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <thread>
#include <chrono>
#include <set>
#include <unistd.h>
#include "../src/mmapfile.h"

using namespace xtree;
using namespace std;

class MMapFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a unique test directory
        test_dir_ = filesystem::temp_directory_path() / 
                   ("xtree_mmap_test_" + to_string(getpid()) + "_" + to_string(time(nullptr)));
        filesystem::create_directories(test_dir_);
        
        test_file_ = test_dir_ / "test.mmap";
        large_test_file_ = test_dir_ / "large_test.mmap";
    }
    
    void TearDown() override {
        // Clean up test files
        filesystem::remove_all(test_dir_);
    }
    
    filesystem::path test_dir_;
    filesystem::path test_file_;
    filesystem::path large_test_file_;
    
    // Helper to create a file with specific content
    void create_test_file(const filesystem::path& path, const string& content) {
        ofstream file(path, ios::binary);
        file.write(content.c_str(), content.size());
    }
    
    // Helper to read file content
    string read_file_content(const filesystem::path& path) {
        ifstream file(path, ios::binary);
        return string((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
    }
};

// Test creating a new memory-mapped file
TEST_F(MMapFileTest, CreateNewFile) {
    const size_t initial_size = 1024 * 1024; // 1MB
    
    {
        MMapFile mmap(test_file_.string(), initial_size);
        
        // File should exist and have correct size
        EXPECT_TRUE(filesystem::exists(test_file_));
        EXPECT_GE(filesystem::file_size(test_file_), initial_size);
        EXPECT_EQ(mmap.size(), filesystem::file_size(test_file_));
        EXPECT_GE(mmap.mapped_size(), initial_size);
        
        // Should be able to get valid pointers
        void* ptr = mmap.getPointer(0);
        EXPECT_NE(ptr, nullptr);
        
        // Test writing to mapped memory
        char* char_ptr = static_cast<char*>(ptr);
        strcpy(char_ptr + 100, "Hello, MMap!");
        
        // Force sync to ensure data is written
        mmap.sync();
    }
    
    // Verify data persisted after file is closed
    ifstream file(test_file_, ios::binary);
    file.seekg(100);
    string content(12, '\0');
    file.read(&content[0], 12);
    EXPECT_EQ(content, "Hello, MMap!");
}

// Test opening an existing XTree binary file
TEST_F(MMapFileTest, OpenExistingBinaryFile) {
    // First create a valid XTree binary file
    size_t data_offset = 0;
    {
        MMapFile mmap(test_file_.string(), 1024*1024);
        
        // Allocate some data
        data_offset = mmap.allocate(128);
        EXPECT_GT(data_offset, 0);
        
        // Write some test data
        char* ptr = static_cast<char*>(mmap.getPointer(data_offset));
        ASSERT_NE(ptr, nullptr);
        strcpy(ptr, "Test data in binary format");
    }
    
    // Now open the existing file
    {
        MMapFile mmap(test_file_.string());
        
        EXPECT_GT(mmap.size(), 0);
        
        // Should be able to read the data we wrote at the correct offset
        char* ptr = static_cast<char*>(mmap.getPointer(data_offset));
        ASSERT_NE(ptr, nullptr);
        
        string content(ptr);
        EXPECT_EQ(content, "Test data in binary format");
    }
}

// Test that opening a plain text file fails
TEST_F(MMapFileTest, OpenPlainTextFileFails) {
    // Create a file without the proper binary header
    ofstream file(test_file_, ios::binary);
    file << "Plain text file content without proper header";
    file.close();
    
    // Should throw when trying to open a non-XTree file
    EXPECT_THROW({
        MMapFile mmap(test_file_.string());
    }, std::runtime_error);
}

// Test allocation functionality
TEST_F(MMapFileTest, AllocationTest) {
    const size_t initial_size = 4096; // 4KB
    MMapFile mmap(test_file_.string(), initial_size);
    
    // Test multiple allocations
    vector<size_t> offsets;
    vector<size_t> sizes = {64, 128, 256, 512, 1024};
    
    for (size_t size : sizes) {
        size_t offset = mmap.allocate(size);
        EXPECT_GT(offset, 0); // Should not allocate at offset 0 (reserved for header)
        EXPECT_LT(offset + size, mmap.size());
        offsets.push_back(offset);
        
        // Write unique data to each allocation
        char* ptr = static_cast<char*>(mmap.getPointer(offset));
        string test_data = "Data block " + to_string(size);
        strcpy(ptr, test_data.c_str());
    }
    
    // Verify all allocated blocks have correct data
    for (size_t i = 0; i < offsets.size(); ++i) {
        char* ptr = static_cast<char*>(mmap.getPointer(offsets[i]));
        string expected = "Data block " + to_string(sizes[i]);
        EXPECT_EQ(string(ptr), expected);
    }
}

// Test file expansion
TEST_F(MMapFileTest, FileExpansion) {
    const size_t initial_size = 1024; // 1KB
    MMapFile mmap(test_file_.string(), initial_size);
    
    EXPECT_EQ(mmap.size(), initial_size);
    
    // Expand file
    const size_t new_size = 8192; // 8KB
    EXPECT_TRUE(mmap.expand(new_size));
    EXPECT_GE(mmap.size(), new_size);
    EXPECT_GE(mmap.mapped_size(), new_size);
    
    // Should be able to allocate in expanded region
    size_t offset = mmap.allocate(4096); // 4KB allocation
    EXPECT_GT(offset, 0);
    EXPECT_LT(offset + 4096, mmap.size());
    
    // Write to expanded region
    char* ptr = static_cast<char*>(mmap.getPointer(offset));
    strcpy(ptr, "Data in expanded region");
    mmap.sync();
    
    // Verify data persisted
    EXPECT_EQ(string(ptr), "Data in expanded region");
}

// Test root offset management
TEST_F(MMapFileTest, RootOffsetManagement) {
    const size_t root_offset = 1024;
    
    {
        MMapFile mmap(test_file_.string(), 4096);
        
        // Initially should be 0 (no root set)
        EXPECT_EQ(mmap.get_root_offset(), 0);
        
        // Set root offset
        mmap.set_root_offset(root_offset);
        EXPECT_EQ(mmap.get_root_offset(), root_offset);
        
        // Force sync to ensure header is written
        mmap.sync();
    }
    
    // Root offset should persist after reopening
    {
        MMapFile mmap2(test_file_.string());
        EXPECT_EQ(mmap2.get_root_offset(), root_offset);
    }
}

// Test memory locking (mlock/munlock)
TEST_F(MMapFileTest, MemoryLocking) {
    const size_t file_size = 8192; // 8KB
    MMapFile mmap(test_file_.string(), file_size);
    
    // Allocate a block to lock
    size_t offset = mmap.allocate(1024);
    EXPECT_GT(offset, 0);
    
    // Lock the region (may fail if not running as root, that's ok)
    bool lock_result = mmap.mlock_region(offset, 1024);
    // Don't assert on lock result since it may fail due to permissions
    
    if (lock_result) {
        // If locking succeeded, unlocking should also succeed
        EXPECT_TRUE(mmap.munlock_region(offset, 1024));
    }
    
    // Test locking invalid regions
    EXPECT_FALSE(mmap.mlock_region(file_size + 1000, 1024)); // Beyond file
    EXPECT_FALSE(mmap.munlock_region(file_size + 1000, 1024)); // Beyond file
}

// Test read-only mode
TEST_F(MMapFileTest, ReadOnlyMode) {
    // First create a valid binary file with data
    size_t data_offset = 0;
    const string test_content = "Read-only test content";
    
    {
        MMapFile mmap(test_file_.string(), 4096);
        data_offset = mmap.allocate(test_content.size() + 1);
        char* ptr = static_cast<char*>(mmap.getPointer(data_offset));
        strcpy(ptr, test_content.c_str());
        mmap.sync();
    }
    
    // Now open in read-only mode
    {
        MMapFile mmap(test_file_.string(), 0, true); // Read-only mode
        
        // Should be able to read
        char* ptr = static_cast<char*>(mmap.getPointer(data_offset));
        EXPECT_NE(ptr, nullptr);
        
        string read_content(ptr);
        EXPECT_EQ(read_content, test_content);
        
        // Allocation should fail in read-only mode
        EXPECT_EQ(mmap.allocate(1024), 0);
    }
}

// Test error conditions
TEST_F(MMapFileTest, ErrorConditions) {
    // Test invalid file path
    EXPECT_THROW({
        MMapFile mmap("/invalid/path/file.mmap");
    }, std::runtime_error);
    
    // Test getting pointer beyond file size
    {
        MMapFile mmap(test_file_.string(), 1024);
        EXPECT_EQ(mmap.getPointer(2048), nullptr); // Beyond file size
    }
}

// Test concurrent access simulation
TEST_F(MMapFileTest, ConcurrentAccess) {
    const size_t file_size = 1024 * 1024; // 1MB
    MMapFile mmap(test_file_.string(), file_size);
    
    // Simulate concurrent allocations
    vector<thread> threads;
    vector<size_t> thread_offsets(4);
    
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&, i]() {
            size_t offset = mmap.allocate(1024);
            thread_offsets[i] = offset;
            
            if (offset > 0) {
                char* ptr = static_cast<char*>(mmap.getPointer(offset));
                string data = "Thread " + to_string(i) + " data";
                strcpy(ptr, data.c_str());
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Verify each thread got a unique allocation
    set<size_t> unique_offsets(thread_offsets.begin(), thread_offsets.end());
    EXPECT_EQ(unique_offsets.size(), 4);
    
    // Verify data integrity
    for (int i = 0; i < 4; ++i) {
        if (thread_offsets[i] > 0) {
            char* ptr = static_cast<char*>(mmap.getPointer(thread_offsets[i]));
            string expected = "Thread " + to_string(i) + " data";
            EXPECT_EQ(string(ptr), expected);
        }
    }
}

// Performance test for large files
TEST_F(MMapFileTest, LargeFilePerformance) {
    const size_t large_size = 100 * 1024 * 1024; // 100MB
    
    auto start = chrono::high_resolution_clock::now();
    
    {
        MMapFile mmap(large_test_file_.string(), large_size);
        
        // Test random access patterns
        random_device rd;
        mt19937 gen(rd());
        uniform_int_distribution<size_t> dist(0, large_size - 1024);
        
        for (int i = 0; i < 1000; ++i) {
            size_t offset = dist(gen);
            char* ptr = static_cast<char*>(mmap.getPointer(offset));
            EXPECT_NE(ptr, nullptr);
            
            // Write some data
            *reinterpret_cast<uint64_t*>(ptr) = i;
        }
        
        mmap.sync();
    }
    
    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
    
    // Should complete reasonably quickly (less than 5 seconds)
    EXPECT_LT(duration.count(), 5000);
    
    // Verify file was created with correct size
    EXPECT_GE(filesystem::file_size(large_test_file_), large_size);
}

// Test data integrity across file close/reopen cycles
TEST_F(MMapFileTest, DataIntegrityAcrossReopens) {
    const size_t num_blocks = 10; // Reduced for test efficiency
    vector<pair<size_t, string>> test_data;
    
    // Phase 1: Create file and write data
    {
        MMapFile mmap(test_file_.string(), 64 * 1024); // 64KB
        
        for (size_t i = 0; i < num_blocks; ++i) {
            size_t offset = mmap.allocate(256);
            EXPECT_GT(offset, 0);
            
            string data = "Block " + to_string(i) + " data content";
            char* ptr = static_cast<char*>(mmap.getPointer(offset));
            ASSERT_NE(ptr, nullptr);
            strcpy(ptr, data.c_str());
            
            test_data.emplace_back(offset, data);
        }
        
        mmap.sync();
    }
    
    // Phase 2: Reopen and verify all data
    {
        MMapFile mmap(test_file_.string());
        
        for (const auto& [offset, expected_data] : test_data) {
            char* ptr = static_cast<char*>(mmap.getPointer(offset));
            ASSERT_NE(ptr, nullptr);
            EXPECT_EQ(string(ptr), expected_data);
        }
    }
    
    // Phase 3: Reopen again and add more data
    {
        MMapFile mmap(test_file_.string());
        
        // Verify existing data still intact
        for (const auto& [offset, expected_data] : test_data) {
            char* ptr = static_cast<char*>(mmap.getPointer(offset));
            ASSERT_NE(ptr, nullptr);
            EXPECT_EQ(string(ptr), expected_data);
        }
        
        // Add new data
        size_t new_offset = mmap.allocate(256);
        EXPECT_GT(new_offset, 0);
        
        char* ptr = static_cast<char*>(mmap.getPointer(new_offset));
        ASSERT_NE(ptr, nullptr);
        strcpy(ptr, "New data after reopen");
        mmap.sync();
        
        // Verify new data
        EXPECT_EQ(string(ptr), "New data after reopen");
    }
}