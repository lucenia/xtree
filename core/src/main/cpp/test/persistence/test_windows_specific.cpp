/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Windows-specific tests for the persistence layer.
 * Tests Windows file handling, directory operations, and memory mapping.
 */

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <vector>
#include <thread>
#include <random>

#ifdef _WIN32
#include <windows.h>
#include <memoryapi.h>
#endif

#include "../../src/persistence/platform_fs.h"
#include "../../src/persistence/ot_delta_log.h"
#include "../../src/persistence/superblock.hpp"
#include "../../src/persistence/manifest.h"

using namespace xtree::persist;
namespace fs = std::filesystem;

class WindowsSpecificTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = fs::temp_directory_path() / 
                    ("windows_test_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }
    
    fs::path test_dir_;
};

#ifdef _WIN32

// Test Windows file creation with proper flags
TEST_F(WindowsSpecificTest, FileCreationFlags) {
    const auto test_file = test_dir_ / "test_flags.dat";
    
    // Test with FILE_FLAG_WRITE_THROUGH
    {
        HANDLE h = CreateFileW(
            test_file.wstring().c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ,
            nullptr,
            CREATE_ALWAYS,
            FILE_FLAG_WRITE_THROUGH | FILE_FLAG_SEQUENTIAL_SCAN,
            nullptr
        );
        
        ASSERT_NE(h, INVALID_HANDLE_VALUE) << "Failed to create file with WRITE_THROUGH";
        
        // Write test data
        const char data[] = "test data";
        DWORD written;
        BOOL result = WriteFile(h, data, sizeof(data), &written, nullptr);
        EXPECT_TRUE(result);
        EXPECT_EQ(written, sizeof(data));
        
        // Flush file buffers
        EXPECT_TRUE(FlushFileBuffers(h));
        
        CloseHandle(h);
    }
    
    // Verify file exists and has correct data
    EXPECT_TRUE(fs::exists(test_file));
    std::ifstream ifs(test_file, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "test data");
}

// Test directory operations with BACKUP_SEMANTICS
TEST_F(WindowsSpecificTest, DirectoryOperations) {
    const auto sub_dir = test_dir_ / "subdir";
    fs::create_directories(sub_dir);
    
    // Open directory with BACKUP_SEMANTICS for fsync
    HANDLE h = CreateFileW(
        test_dir_.wstring().c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,  // Required for directories
        nullptr
    );
    
    ASSERT_NE(h, INVALID_HANDLE_VALUE) << "Failed to open directory";
    
    // Directory fsync (metadata flush)
    EXPECT_TRUE(FlushFileBuffers(h));
    
    CloseHandle(h);
}

// Test memory-mapped file operations
TEST_F(WindowsSpecificTest, MemoryMappedFile) {
    const auto test_file = test_dir_ / "mmap_test.dat";
    const size_t file_size = 64 * 1024;  // 64KB
    
    // Create and size the file
    {
        HANDLE h = CreateFileW(
            test_file.wstring().c_str(),
            GENERIC_READ | GENERIC_WRITE,
            0,
            nullptr,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            nullptr
        );
        
        ASSERT_NE(h, INVALID_HANDLE_VALUE);
        
        // Set file size
        LARGE_INTEGER size;
        size.QuadPart = file_size;
        EXPECT_TRUE(SetFilePointerEx(h, size, nullptr, FILE_BEGIN));
        EXPECT_TRUE(SetEndOfFile(h));
        
        CloseHandle(h);
    }
    
    // Map the file
    {
        HANDLE hFile = CreateFileW(
            test_file.wstring().c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ,
            nullptr,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            nullptr
        );
        
        ASSERT_NE(hFile, INVALID_HANDLE_VALUE);
        
        HANDLE hMap = CreateFileMappingW(
            hFile,
            nullptr,
            PAGE_READWRITE,
            0, 0,  // Use file size
            nullptr
        );
        
        ASSERT_NE(hMap, nullptr);
        
        void* addr = MapViewOfFile(
            hMap,
            FILE_MAP_ALL_ACCESS,
            0, 0,  // Map from beginning
            0      // Map entire file
        );
        
        ASSERT_NE(addr, nullptr);
        
        // Write through mapped memory
        memset(addr, 0xAB, 1024);
        
        // Flush view
        EXPECT_TRUE(FlushViewOfFile(addr, 1024));
        
        // Unmap
        EXPECT_TRUE(UnmapViewOfFile(addr));
        CloseHandle(hMap);
        CloseHandle(hFile);
    }
    
    // Verify data persisted
    std::ifstream ifs(test_file, std::ios::binary);
    std::vector<char> buffer(1024);
    ifs.read(buffer.data(), buffer.size());
    EXPECT_TRUE(std::all_of(buffer.begin(), buffer.end(), [](char c) { return c == (char)0xAB; }));
}

// Test atomic file operations
TEST_F(WindowsSpecificTest, AtomicFileOperations) {
    const auto temp_file = test_dir_ / "temp.tmp";
    const auto final_file = test_dir_ / "final.dat";
    
    // Create temp file
    {
        std::ofstream ofs(temp_file);
        ofs << "temporary data";
    }
    
    // Atomic rename
    BOOL result = MoveFileExW(
        temp_file.wstring().c_str(),
        final_file.wstring().c_str(),
        MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH
    );
    
    EXPECT_TRUE(result) << "Atomic rename failed";
    EXPECT_FALSE(fs::exists(temp_file));
    EXPECT_TRUE(fs::exists(final_file));
    
    // Verify content
    std::ifstream ifs(final_file);
    std::string content;
    std::getline(ifs, content);
    EXPECT_EQ(content, "temporary data");
}

// Test concurrent file access with sharing
TEST_F(WindowsSpecificTest, ConcurrentFileAccess) {
    const auto test_file = test_dir_ / "concurrent.dat";
    
    // Create file with sharing enabled
    HANDLE h1 = CreateFileW(
        test_file.wstring().c_str(),
        GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE,  // Allow concurrent access
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    
    ASSERT_NE(h1, INVALID_HANDLE_VALUE);
    
    // Write from first handle
    const char data1[] = "handle1";
    DWORD written;
    WriteFile(h1, data1, sizeof(data1), &written, nullptr);
    
    // Open second handle for reading
    HANDLE h2 = CreateFileW(
        test_file.wstring().c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    
    ASSERT_NE(h2, INVALID_HANDLE_VALUE);
    
    // Read from second handle
    char buffer[100];
    DWORD read;
    SetFilePointer(h2, 0, nullptr, FILE_BEGIN);
    ReadFile(h2, buffer, sizeof(buffer), &read, nullptr);
    
    EXPECT_EQ(read, sizeof(data1));
    EXPECT_EQ(memcmp(buffer, data1, sizeof(data1)), 0);
    
    CloseHandle(h2);
    CloseHandle(h1);
}

// Test large file support
TEST_F(WindowsSpecificTest, LargeFileSupport) {
    const auto test_file = test_dir_ / "large.dat";
    
    HANDLE h = CreateFileW(
        test_file.wstring().c_str(),
        GENERIC_READ | GENERIC_WRITE,
        0,
        nullptr,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    
    ASSERT_NE(h, INVALID_HANDLE_VALUE);
    
    // Seek to 4GB position
    LARGE_INTEGER pos;
    pos.QuadPart = 4LL * 1024 * 1024 * 1024;  // 4GB
    LARGE_INTEGER new_pos;
    
    BOOL result = SetFilePointerEx(h, pos, &new_pos, FILE_BEGIN);
    EXPECT_TRUE(result);
    EXPECT_EQ(new_pos.QuadPart, pos.QuadPart);
    
    // Write at 4GB position
    const char data[] = "data at 4GB";
    DWORD written;
    WriteFile(h, data, sizeof(data), &written, nullptr);
    
    // Get file size
    LARGE_INTEGER size;
    GetFileSizeEx(h, &size);
    EXPECT_GT(size.QuadPart, pos.QuadPart);
    
    CloseHandle(h);
    
    // Clean up large file
    fs::remove(test_file);
}

#else

// Placeholder test for non-Windows platforms
TEST_F(WindowsSpecificTest, NotWindows) {
    GTEST_SKIP() << "Windows-specific tests skipped on non-Windows platform";
}

#endif  // _WIN32

// Cross-platform tests that validate Windows compatibility
TEST_F(WindowsSpecificTest, PlatformFSWindowsCompatibility) {
    const auto test_file = test_dir_ / "platform_test.dat";
    
    // Test file creation
    {
        std::ofstream ofs(test_file);
        ofs << "test content";
    }
    
    // Test file size
    auto [result, size] = PlatformFS::file_size(test_file.string());
    EXPECT_TRUE(result.ok);
    EXPECT_GT(size, 0);
    
    // Test directory fsync (should work on Windows)
    auto dir_result = PlatformFS::fsync_directory(test_dir_.string());
    EXPECT_TRUE(dir_result.ok);
    
    // Test atomic replace
    const auto temp_file = test_dir_ / "temp.tmp";
    const auto final_file = test_dir_ / "final.dat";
    
    {
        std::ofstream ofs(temp_file);
        ofs << "new content";
    }
    
    auto replace_result = PlatformFS::atomic_replace(temp_file.string(), final_file.string());
    EXPECT_TRUE(replace_result.ok);
    EXPECT_FALSE(fs::exists(temp_file));
    EXPECT_TRUE(fs::exists(final_file));
}

// Test OTDeltaLog on Windows
TEST_F(WindowsSpecificTest, DeltaLogWindowsHandling) {
    const auto log_path = test_dir_ / "test.wal";
    
    OTDeltaLog log(log_path.string());
    ASSERT_TRUE(log.open_for_append());
    
    // Test concurrent appends (Windows file sharing)
    std::vector<std::thread> threads;
    std::atomic<int> successful_appends{0};
    
    for (int t = 0; t < 4; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 100; i++) {
                OTDeltaRec rec;
                rec.handle_idx = t * 100 + i;
                rec.birth_epoch = i;
                rec.retire_epoch = UINT64_MAX;
                
                try {
                    log.append({rec});
                    successful_appends.fetch_add(1);
                } catch (...) {
                    // Should not happen
                }
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_EQ(successful_appends.load(), 400);
    
    // Test sync
    log.sync();
    
    // Test close and reopen
    log.close();
    ASSERT_TRUE(log.open_for_append());
    
    // Verify data persisted
    EXPECT_GT(log.get_end_offset(), 0);
}