/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Platform-Agnostic High-Performance File I/O
 * 
 * This header provides a unified interface that automatically selects the
 * optimal file I/O implementation for each platform:
 * - Windows: Uses optimized FastFileWriter/FastFileReader with Windows-specific optimizations
 * - Linux/macOS: Uses standard optimized implementations that preserve existing performance
 * 
 * The API is identical across platforms, ensuring zero impact on existing Linux/macOS code.
 */

#ifndef PLATFORM_FILE_IO_HPP
#define PLATFORM_FILE_IO_HPP

#include <string>
#include <vector>
#include <memory>
#include <fstream>

#ifdef _WIN32
#include "fast_file_io.hpp"
#endif

namespace xtree {

// Forward declarations for platform abstraction
class PlatformFileWriter;
class PlatformFileReader;
class PlatformMemoryMappedFile;

//==============================================================================
// Platform-Agnostic File Writer
//==============================================================================

class PlatformFileWriter {
private:
#ifdef _WIN32
    std::unique_ptr<FastFileWriter> windows_writer_;
#else
    std::unique_ptr<std::ofstream> posix_writer_;
    std::vector<char> buffer_;
    size_t buffer_size_;
    size_t buffer_pos_;
#endif
    
    std::string filename_;
    bool is_open_;
    
public:
    // Constructor with platform-specific optimization hints
    explicit PlatformFileWriter(const std::string& filename, 
                               size_t buffer_size = 0,  // 0 = auto-detect optimal size
                               bool use_async = true);   // Ignored on Linux/macOS if not beneficial
    
    ~PlatformFileWriter();
    
    // Non-copyable but movable
    PlatformFileWriter(const PlatformFileWriter&) = delete;
    PlatformFileWriter& operator=(const PlatformFileWriter&) = delete;
    PlatformFileWriter(PlatformFileWriter&&) noexcept;
    PlatformFileWriter& operator=(PlatformFileWriter&&) noexcept;
    
    // Unified API - works identically on all platforms
    bool open();
    void close();
    bool is_open() const { return is_open_; }
    
    // Write operations - automatically optimized per platform
    bool write(const void* data, size_t size);
    bool write(const std::vector<char>& data);
    bool write(const std::string& data);
    
    // Advanced operations (no-op on platforms that don't benefit)
    bool write_batch(const std::vector<std::pair<const void*, size_t>>& chunks);
    bool preallocate_space(size_t expected_size); // COW snapshot optimization
    bool flush();
    bool sync();
    
    // Performance tuning (automatically ignored if not beneficial)
    void set_large_file_mode(bool enable);
    void enable_async_mode(bool enable);
    
    // Statistics (available on all platforms)
    size_t get_bytes_written() const;
    double get_write_throughput_mbps() const;
    
private:
    void init_platform_writer(size_t buffer_size, bool use_async);
    size_t get_optimal_buffer_size() const;
    
#ifndef _WIN32
    bool flush_posix_buffer();
#endif
};

//==============================================================================
// Platform-Agnostic File Reader  
//==============================================================================

class PlatformFileReader {
private:
#ifdef _WIN32
    std::unique_ptr<FastFileReader> windows_reader_;
#else
    std::unique_ptr<std::ifstream> posix_reader_;
    std::vector<char> buffer_;
    size_t buffer_size_;
    size_t buffer_pos_;
    size_t buffer_valid_;
    int64_t file_size_;
#endif
    
    std::string filename_;
    bool is_open_;
    
public:
    explicit PlatformFileReader(const std::string& filename,
                               size_t buffer_size = 0,
                               bool use_async = true);
    
    ~PlatformFileReader();
    
    // Non-copyable but movable
    PlatformFileReader(const PlatformFileReader&) = delete;
    PlatformFileReader& operator=(const PlatformFileReader&) = delete;
    PlatformFileReader(PlatformFileReader&&) noexcept;
    PlatformFileReader& operator=(PlatformFileReader&&) noexcept;
    
    // Unified API
    bool open();
    void close();
    bool is_open() const { return is_open_; }
    
    // Read operations
    bool read(void* data, size_t size);
    bool read(std::vector<char>& data, size_t size);
    std::vector<char> read_all();
    
    // Advanced operations
    bool read_batch(const std::vector<std::pair<void*, size_t>>& chunks);
    void prefetch(size_t bytes);
    
    // File information
    bool seek(int64_t offset, int whence = 0);
    int64_t tell() const;
    int64_t size() const;
    bool eof() const;
    
    // Performance tuning  
    void set_large_file_mode(bool enable);
    void enable_async_mode(bool enable);
    
    // Statistics
    size_t get_bytes_read() const;
    double get_read_throughput_mbps() const;
    size_t get_optimal_buffer_size() const;
    
private:
    void init_platform_reader(size_t buffer_size, bool use_async);
    int64_t get_file_size();
    
#ifndef _WIN32
    bool fill_posix_buffer();
#endif
};

//==============================================================================
// Platform-Agnostic Memory-Mapped File
//==============================================================================

class PlatformMemoryMappedFile {
private:
#ifdef _WIN32
    std::unique_ptr<MemoryMappedFile> windows_mmap_;
#else
    int file_descriptor_;
    void* mapped_data_;
    size_t file_size_;
    bool read_only_;
#endif
    
    std::string filename_;
    bool is_mapped_;
    
public:
    explicit PlatformMemoryMappedFile(const std::string& filename, bool read_only = true);
    ~PlatformMemoryMappedFile();
    
    // Non-copyable but movable
    PlatformMemoryMappedFile(const PlatformMemoryMappedFile&) = delete;
    PlatformMemoryMappedFile& operator=(const PlatformMemoryMappedFile&) = delete;
    PlatformMemoryMappedFile(PlatformMemoryMappedFile&&) noexcept;
    PlatformMemoryMappedFile& operator=(PlatformMemoryMappedFile&&) noexcept;
    
    // Unified API
    bool map();
    void unmap();
    bool is_mapped() const { return is_mapped_; }
    
    // Direct memory access
    const void* data() const;
    void* data(); // Returns nullptr if read_only
    size_t size() const;
    
    // Typed access helpers
    template<typename T>
    const T* as() const { return static_cast<const T*>(data()); }
    
    template<typename T>
    T* as() { return static_cast<T*>(data()); }
    
    // Memory management
    bool flush();
    bool advise_sequential();
    bool advise_random();
    
private:
    void init_platform_mmap();
    
#ifndef _WIN32
    size_t get_file_size_posix();
#endif
};

//==============================================================================
// Platform-Agnostic Utility Functions
//==============================================================================

namespace PlatformFileUtils {
    
    // Automatically optimized file operations
    bool fast_copy(const std::string& src, const std::string& dst, 
                   size_t buffer_size = 0); // 0 = auto-detect optimal size
    
    bool batch_delete(const std::vector<std::string>& filenames);
    bool batch_create(const std::vector<std::string>& filenames, size_t initial_size = 0);
    
    // Performance optimization (no-op on platforms that don't benefit)
    bool optimize_for_large_files(const std::string& filename);
    bool set_sequential_access_hint(const std::string& filename);
    
    // Cross-platform performance measurement
    struct IOBenchmark {
        std::string operation;
        double throughput_mbps;
        uint64_t operations_per_sec;
        uint64_t total_time_us;
        std::string platform_info;
    };
    
    IOBenchmark benchmark_write_performance(const std::string& filename, 
                                           size_t file_size, size_t buffer_size = 0);
    IOBenchmark benchmark_read_performance(const std::string& filename, 
                                          size_t buffer_size = 0);
    
    // System information
    struct PlatformInfo {
        std::string platform_name;
        std::string filesystem_type;
        size_t optimal_buffer_size;
        bool supports_async_io;
        bool supports_memory_mapping;
        bool uses_optimized_implementation;
    };
    
    PlatformInfo get_platform_info(const std::string& path = ".");
    
    // Global performance statistics (unified across platforms)
    void reset_global_stats();
    FileIOStats get_global_stats();
    void print_performance_report();
}

//==============================================================================
// Drop-in Replacement for Standard File Operations
//==============================================================================

// These functions provide drop-in replacements for standard file operations
// with automatic platform optimization:

namespace StandardFileReplacement {
    
    // Drop-in replacement for std::ofstream - automatically optimized
    class OptimizedOfstream {
    private:
        PlatformFileWriter writer_;
        
    public:
        explicit OptimizedOfstream(const std::string& filename, 
                                  std::ios::openmode mode = std::ios::out)
            : writer_(filename) {
            if (mode & std::ios::binary) {
                // Binary mode optimizations
                writer_.set_large_file_mode(true);
            }
            writer_.open();
        }
        
        // Standard stream interface
        OptimizedOfstream& write(const char* data, std::streamsize size) {
            writer_.write(data, static_cast<size_t>(size));
            return *this;
        }
        
        bool is_open() const { return writer_.is_open(); }
        void close() { writer_.close(); }
        bool good() const { return writer_.is_open(); }
        
        // Automatic flush on destruction
        ~OptimizedOfstream() { writer_.sync(); }
    };
    
    // Drop-in replacement for std::ifstream - automatically optimized
    class OptimizedIfstream {
    private:
        PlatformFileReader reader_;
        
    public:
        explicit OptimizedIfstream(const std::string& filename,
                                  std::ios::openmode mode = std::ios::in)
            : reader_(filename) {
            if (mode & std::ios::binary) {
                reader_.set_large_file_mode(true);
            }
            reader_.open();
        }
        
        // Standard stream interface
        OptimizedIfstream& read(char* data, std::streamsize size) {
            reader_.read(data, static_cast<size_t>(size));
            return *this;
        }
        
        bool is_open() const { return reader_.is_open(); }
        void close() { reader_.close(); }
        bool good() const { return reader_.is_open() && !reader_.eof(); }
        bool eof() const { return reader_.eof(); }
        
        std::streampos tellg() { return static_cast<std::streampos>(reader_.tell()); }
        OptimizedIfstream& seekg(std::streampos pos) {
            reader_.seek(static_cast<int64_t>(pos), 0);
            return *this;
        }
    };
}

//==============================================================================
// Integration Macros for Seamless Adoption
//==============================================================================

// These macros allow seamless integration without changing existing code:

#ifdef XTREE_USE_OPTIMIZED_FILE_IO
    // Replace standard file operations with optimized versions
    #define std_ofstream xtree::StandardFileReplacement::OptimizedOfstream
    #define std_ifstream xtree::StandardFileReplacement::OptimizedIfstream
    
    // Platform file utilities
    #define platform_file_copy xtree::PlatformFileUtils::fast_copy
    #define platform_file_optimize xtree::PlatformFileUtils::optimize_for_large_files
#else
    // Use standard implementations (no change to existing code)
    #define std_ofstream std::ofstream
    #define std_ifstream std::ifstream
    #define platform_file_copy std::filesystem::copy_file
    #define platform_file_optimize(x) true
#endif

} // namespace xtree

#endif // PLATFORM_FILE_IO_HPP