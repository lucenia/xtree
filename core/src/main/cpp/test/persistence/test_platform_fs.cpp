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
#include <fstream>
#include <cstring>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif
#include "persistence/platform_fs.h"

using namespace xtree::persist;

class PlatformFSTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::string test_file;
    
    void SetUp() override {
        // Create a unique test directory
        test_dir = "/tmp/xtree_platform_fs_test_" + std::to_string(getpid());
        test_file = test_dir + "/test.dat";
        
        // Create test directory
        #ifdef _WIN32
            CreateDirectoryA(test_dir.c_str(), nullptr);
        #else
            mkdir(test_dir.c_str(), 0755);
        #endif
    }
    
    void TearDown() override {
        // Clean up test files and directory
        #ifdef _WIN32
            DeleteFileA(test_file.c_str());
            RemoveDirectoryA(test_dir.c_str());
        #else
            unlink(test_file.c_str());
            rmdir(test_dir.c_str());
        #endif
    }
    
    void CreateTestFile(size_t size) {
        std::ofstream ofs(test_file, std::ios::binary);
        std::vector<char> data(size);
        for (size_t i = 0; i < size; i++) {
            data[i] = static_cast<char>(i % 256);
        }
        ofs.write(data.data(), size);
        ofs.close();
    }
};

TEST_F(PlatformFSTest, MapFileReadOnly) {
    const size_t file_size = 4096;
    CreateTestFile(file_size);
    
    MappedRegion region;
    FSResult result = PlatformFS::map_file(test_file, 0, file_size, MapMode::ReadOnly, &region);
    
    EXPECT_TRUE(result.ok);
    EXPECT_NE(region.addr, nullptr);
    EXPECT_EQ(region.size, file_size);
    EXPECT_NE(region.file_handle, 0);
    
    // Verify data
    const char* data = static_cast<const char*>(region.addr);
    for (size_t i = 0; i < 256; i++) {
        EXPECT_EQ(data[i], static_cast<char>(i % 256));
    }
    
    // Unmap
    result = PlatformFS::unmap(region);
    EXPECT_TRUE(result.ok);
}

TEST_F(PlatformFSTest, MapFileReadWrite) {
    const size_t file_size = 8192;
    CreateTestFile(file_size);
    
    MappedRegion region;
    FSResult result = PlatformFS::map_file(test_file, 0, file_size, MapMode::ReadWrite, &region);
    
    EXPECT_TRUE(result.ok);
    EXPECT_NE(region.addr, nullptr);
    
    // Modify data
    char* data = static_cast<char*>(region.addr);
    for (size_t i = 0; i < 100; i++) {
        data[i] = 'X';
    }
    
    // Flush changes
    result = PlatformFS::flush_view(region.addr, 100);
    EXPECT_TRUE(result.ok);
    
    // Unmap
    result = PlatformFS::unmap(region);
    EXPECT_TRUE(result.ok);
    
    // Verify changes persisted
    std::ifstream ifs(test_file, std::ios::binary);
    char buffer[100];
    ifs.read(buffer, 100);
    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(buffer[i], 'X');
    }
}

TEST_F(PlatformFSTest, MapPartialFile) {
    const size_t file_size = 16384;
    const size_t map_offset = 4096;
    const size_t map_size = 8192;
    
    CreateTestFile(file_size);
    
    MappedRegion region;
    FSResult result = PlatformFS::map_file(test_file, map_offset, map_size, MapMode::ReadOnly, &region);
    
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(region.size, map_size);
    
    // Verify we're reading from the correct offset
    const char* data = static_cast<const char*>(region.addr);
    for (size_t i = 0; i < 256; i++) {
        EXPECT_EQ(data[i], static_cast<char>((map_offset + i) % 256));
    }
    
    result = PlatformFS::unmap(region);
    EXPECT_TRUE(result.ok);
}

TEST_F(PlatformFSTest, FileSizeQuery) {
    const size_t expected_size = 12345;
    CreateTestFile(expected_size);
    
    auto [result, size] = PlatformFS::file_size(test_file);
    
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(size, expected_size);
}

TEST_F(PlatformFSTest, PreallocateFile) {
    const size_t preallocate_size = 1024 * 1024; // 1MB
    
    FSResult result = PlatformFS::preallocate(test_file, preallocate_size);
    EXPECT_TRUE(result.ok);
    
    // Verify file exists and has correct size
    auto [size_result, size] = PlatformFS::file_size(test_file);
    EXPECT_TRUE(size_result.ok);
    EXPECT_GE(size, preallocate_size);
}

TEST_F(PlatformFSTest, AtomicReplace) {
    // Create original file
    std::string original = test_file;
    std::string temp = test_file + ".tmp";
    
    std::ofstream ofs(original);
    ofs << "original content";
    ofs.close();
    
    // Create temp file
    ofs.open(temp);
    ofs << "new content";
    ofs.close();
    
    // Atomic replace
    FSResult result = PlatformFS::atomic_replace(temp, original);
    EXPECT_TRUE(result.ok);
    
    // Verify content was replaced
    std::ifstream ifs(original);
    std::string content;
    std::getline(ifs, content);
    EXPECT_EQ(content, "new content");
    
    // Temp file should be gone
    EXPECT_FALSE(std::ifstream(temp).good());
}

TEST_F(PlatformFSTest, FlushFile) {
    CreateTestFile(4096);
    
    MappedRegion region;
    FSResult result = PlatformFS::map_file(test_file, 0, 4096, MapMode::ReadWrite, &region);
    EXPECT_TRUE(result.ok);
    
    // Modify and flush
    char* data = static_cast<char*>(region.addr);
    data[0] = 'Z';
    
    result = PlatformFS::flush_file(region.file_handle);
    EXPECT_TRUE(result.ok);
    
    PlatformFS::unmap(region);
}

TEST_F(PlatformFSTest, AdviseWillNeed) {
    const size_t file_size = 64 * 1024; // 64KB
    CreateTestFile(file_size);
    
    MappedRegion region;
    FSResult result = PlatformFS::map_file(test_file, 0, file_size, MapMode::ReadOnly, &region);
    EXPECT_TRUE(result.ok);
    
    // Advise that we'll need the data soon
    result = PlatformFS::advise_willneed(region.file_handle, 0, file_size);
    // This might not be supported on all platforms, so just check it doesn't crash
    
    PlatformFS::unmap(region);
}

TEST_F(PlatformFSTest, Prefetch) {
    const size_t file_size = 16384;
    CreateTestFile(file_size);
    
    MappedRegion region;
    FSResult result = PlatformFS::map_file(test_file, 0, file_size, MapMode::ReadOnly, &region);
    EXPECT_TRUE(result.ok);
    
    // Try to prefetch some data
    result = PlatformFS::prefetch(region.addr, 4096);
    // This might not be supported on all platforms, so just check it doesn't crash
    
    PlatformFS::unmap(region);
}

TEST_F(PlatformFSTest, MapNonExistentFile) {
    MappedRegion region;
    FSResult result = PlatformFS::map_file("/tmp/non_existent_file_xyz123", 0, 4096, MapMode::ReadOnly, &region);
    
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.err, 0);
}

TEST_F(PlatformFSTest, UnmapInvalidRegion) {
    MappedRegion invalid_region{};
    FSResult result = PlatformFS::unmap(invalid_region);
    
    // Should handle gracefully
    EXPECT_FALSE(result.ok);
}