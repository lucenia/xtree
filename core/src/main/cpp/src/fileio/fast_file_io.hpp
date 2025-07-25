/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * High-Performance File I/O Wrapper for Windows Optimization
 * 
 * This implementation provides Linux/macOS-level file I/O performance on Windows
 * by using optimized Windows APIs, async operations, and large buffer management.
 */

#ifndef FAST_FILE_IO_HPP
#define FAST_FILE_IO_HPP

#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <chrono>
#include <atomic>
#include <mutex>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <io.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#endif

namespace xtree {

// Forward declarations
class FastFileReader;
class FastFileWriter;
class MemoryMappedFile;

// File I/O performance statistics
struct FileIOStats {
    std::atomic<uint64_t> bytes_read{0};
    std::atomic<uint64_t> bytes_written{0};
    std::atomic<uint64_t> read_operations{0};
    std::atomic<uint64_t> write_operations{0};
    std::atomic<uint64_t> sync_operations{0};
    std::atomic<uint64_t> total_read_time_us{0};
    std::atomic<uint64_t> total_write_time_us{0};
    
    double get_read_throughput_mbps() const {
        uint64_t time = total_read_time_us.load();
        uint64_t bytes = bytes_read.load();
        return time > 0 ? (bytes / 1024.0 / 1024.0) / (time / 1000000.0) : 0.0;
    }
    
    double get_write_throughput_mbps() const {
        uint64_t time = total_write_time_us.load();
        uint64_t bytes = bytes_written.load();
        return time > 0 ? (bytes / 1024.0 / 1024.0) / (time / 1000000.0) : 0.0;
    }
};

// Global file I/O statistics
extern FileIOStats g_file_io_stats;

// High-performance file writer with Windows optimizations
class FastFileWriter {
private:
#ifdef _WIN32
    HANDLE file_handle_;
    OVERLAPPED overlapped_;
    bool async_mode_;
#else
    int file_descriptor_;
#endif
    
    std::vector<char> write_buffer_;
    size_t buffer_size_;
    size_t buffer_pos_;
    std::string filename_;
    bool is_open_;
    
    // Performance optimization settings
    static constexpr size_t DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1MB buffer
    static constexpr size_t LARGE_BUFFER_SIZE = 8 * 1024 * 1024; // 8MB for large files
    
public:
    explicit FastFileWriter(const std::string& filename, 
                           size_t buffer_size = DEFAULT_BUFFER_SIZE,
                           bool use_async = true);
    ~FastFileWriter();
    
    // Non-copyable but movable
    FastFileWriter(const FastFileWriter&) = delete;
    FastFileWriter& operator=(const FastFileWriter&) = delete;
    FastFileWriter(FastFileWriter&&) noexcept;
    FastFileWriter& operator=(FastFileWriter&&) noexcept;
    
    bool open();
    void close();
    bool is_open() const { return is_open_; }
    
    // High-performance write methods
    bool write(const void* data, size_t size);
    bool write(const std::vector<char>& data);
    bool write(const std::string& data);
    
    // Batch write for maximum performance
    bool write_batch(const std::vector<std::pair<const void*, size_t>>& chunks);
    
    // Pre-allocate file space to avoid fragmentation (Windows optimization for COW snapshots)
    bool preallocate_space(size_t expected_size);
    
    // Force flush buffer to disk
    bool flush();
    bool sync(); // Ensures data is written to physical storage
    
    // File positioning
    bool seek(int64_t offset, int whence = 0); // 0=SEEK_SET, 1=SEEK_CUR, 2=SEEK_END
    int64_t tell() const;
    
    // Performance tuning
    void set_buffer_size(size_t size);
    void enable_async_mode(bool enable);
    void set_large_file_mode(bool enable); // Uses 8MB buffer + optimizations
    
    // Statistics
    size_t get_bytes_written() const;
    double get_write_throughput_mbps() const;
    
private:
    bool flush_buffer();
    bool write_direct(const void* data, size_t size);
    void optimize_for_large_files();
};

// High-performance file reader with Windows optimizations  
class FastFileReader {
private:
#ifdef _WIN32
    HANDLE file_handle_;
    OVERLAPPED overlapped_;
    bool async_mode_;
#else
    int file_descriptor_;
#endif
    
    std::vector<char> read_buffer_;
    size_t buffer_size_;
    size_t buffer_pos_;
    size_t buffer_valid_;
    std::string filename_;
    bool is_open_;
    int64_t file_size_;
    
    static constexpr size_t DEFAULT_BUFFER_SIZE = 1024 * 1024; // 1MB buffer
    static constexpr size_t LARGE_BUFFER_SIZE = 8 * 1024 * 1024; // 8MB for large files
    
public:
    explicit FastFileReader(const std::string& filename,
                           size_t buffer_size = DEFAULT_BUFFER_SIZE,
                           bool use_async = true);
    ~FastFileReader();
    
    // Non-copyable but movable
    FastFileReader(const FastFileReader&) = delete;
    FastFileReader& operator=(const FastFileReader&) = delete;
    FastFileReader(FastFileReader&&) noexcept;
    FastFileReader& operator=(FastFileReader&&) noexcept;
    
    bool open();
    void close();
    bool is_open() const { return is_open_; }
    
    // High-performance read methods
    bool read(void* data, size_t size);
    bool read(std::vector<char>& data, size_t size);
    std::vector<char> read_all();
    
    // Batch read for maximum performance
    bool read_batch(const std::vector<std::pair<void*, size_t>>& chunks);
    
    // File positioning and info
    bool seek(int64_t offset, int whence = 0);
    int64_t tell() const;
    int64_t size() const { return file_size_; }
    bool eof() const;
    
    // Performance tuning
    void set_buffer_size(size_t size);
    void enable_async_mode(bool enable);
    void set_large_file_mode(bool enable);
    void prefetch(size_t bytes); // Prefetch data into buffer
    
    // Statistics  
    size_t get_bytes_read() const;
    double get_read_throughput_mbps() const;
    
private:
    bool fill_buffer();
    bool read_direct(void* data, size_t size);
    void optimize_for_large_files();
    int64_t get_file_size();
};

// Memory-mapped file for ultra-high performance
class MemoryMappedFile {
private:
#ifdef _WIN32
    HANDLE file_handle_;
    HANDLE mapping_handle_;
#else
    int file_descriptor_;
#endif
    
    void* mapped_data_;
    size_t file_size_;
    std::string filename_;
    bool read_only_;
    bool is_mapped_;
    
public:
    explicit MemoryMappedFile(const std::string& filename, bool read_only = true);
    ~MemoryMappedFile();
    
    // Non-copyable but movable
    MemoryMappedFile(const MemoryMappedFile&) = delete;
    MemoryMappedFile& operator=(const MemoryMappedFile&) = delete;
    MemoryMappedFile(MemoryMappedFile&&) noexcept;
    MemoryMappedFile& operator=(MemoryMappedFile&&) noexcept;
    
    bool map();
    void unmap();
    bool is_mapped() const { return is_mapped_; }
    
    // Direct memory access (fastest possible I/O)
    const void* data() const { return mapped_data_; }
    void* data() { return read_only_ ? nullptr : mapped_data_; }
    size_t size() const { return file_size_; }
    
    // Typed access helpers
    template<typename T>
    const T* as() const { return static_cast<const T*>(mapped_data_); }
    
    template<typename T>
    T* as() { return read_only_ ? nullptr : static_cast<T*>(mapped_data_); }
    
    // Memory management
    bool flush(); // Sync memory-mapped changes to disk
    bool advise_sequential(); // Hint for sequential access
    bool advise_random(); // Hint for random access
    
private:
    size_t get_file_size();
};

// Utility functions for optimized file operations
namespace FileUtils {
    
    // Fast file copy with optimized buffer sizes
    bool fast_copy(const std::string& src, const std::string& dst, 
                   size_t buffer_size = 8 * 1024 * 1024);
    
    // Batch file operations
    bool batch_delete(const std::vector<std::string>& filenames);
    bool batch_create(const std::vector<std::string>& filenames, size_t initial_size = 0);
    
    // File system optimization
    bool optimize_for_large_files(const std::string& filename);
    bool disable_file_indexing(const std::string& filename); // Windows Search optimization
    bool set_large_cache_hint(const std::string& filename);
    
    // Performance measurement
    struct IOBenchmark {
        std::string operation;
        double throughput_mbps;
        uint64_t operations_per_sec;
        uint64_t total_time_us;
    };
    
    IOBenchmark benchmark_sequential_write(const std::string& filename, 
                                          size_t file_size, size_t buffer_size);
    IOBenchmark benchmark_sequential_read(const std::string& filename, 
                                         size_t buffer_size);
    IOBenchmark benchmark_random_io(const std::string& filename, 
                                   size_t num_operations, size_t block_size);
    
    // System information
    struct FileSystemInfo {
        std::string filesystem_type;
        size_t cluster_size;
        size_t sector_size;
        bool supports_async_io;
        bool supports_memory_mapping;
    };
    
    FileSystemInfo get_filesystem_info(const std::string& path);
    
    // Global performance statistics
    void reset_global_stats();
    FileIOStats get_global_stats();
    void print_performance_report();
}

} // namespace xtree

#endif // FAST_FILE_IO_HPP