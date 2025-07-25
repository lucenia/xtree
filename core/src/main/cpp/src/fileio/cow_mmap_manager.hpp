/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * High-Performance Memory-Mapped File Manager for COW Snapshots
 * 
 * This provides ultra-fast persistence for XTree COW operations by using
 * memory-mapped files instead of traditional file I/O. Key benefits:
 * 
 * - Bypasses Windows file creation bottleneck (~2.6K files/sec → memory speed)
 * - Direct memory access - no system call overhead
 * - OS-managed dirty page flushing for optimal performance
 * - Large file support with automatic growing
 * - Thread-safe operations for concurrent access
 */

#ifndef COW_MMAP_MANAGER_HPP
#define COW_MMAP_MANAGER_HPP

#include "fast_file_io.hpp"
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <vector>
#include <string>
#include <unordered_map>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#endif

namespace xtree {

// Forward declarations
class COWMemoryMappedFile;
class COWMMapManager;

//==============================================================================
// COW-Optimized Memory Mapped File
//==============================================================================

class COWMemoryMappedFile {
private:
#ifdef _WIN32
    HANDLE file_handle_;
    HANDLE mapping_handle_;
    std::vector<void*> view_handles_; // Multiple views for large files
#else
    int file_descriptor_;
#endif
    
    void* base_address_;
    size_t current_size_;
    size_t max_size_;
    size_t grow_increment_;
    std::string filename_;
    bool read_only_;
    bool is_mapped_;
    
    // Performance tracking
    std::atomic<uint64_t> total_writes_{0};
    std::atomic<uint64_t> total_reads_{0};
    std::atomic<uint64_t> bytes_written_{0};
    std::atomic<uint64_t> bytes_read_{0};
    
    mutable std::mutex resize_mutex_;
    
public:
    static constexpr size_t DEFAULT_GROW_SIZE = 64 * 1024 * 1024; // 64MB increments
    static constexpr size_t MAX_FILE_SIZE = 4ULL * 1024 * 1024 * 1024; // 4GB max
    
    explicit COWMemoryMappedFile(const std::string& filename, 
                                size_t initial_size = DEFAULT_GROW_SIZE,
                                bool read_only = false);
    ~COWMemoryMappedFile();
    
    // Non-copyable but movable
    COWMemoryMappedFile(const COWMemoryMappedFile&) = delete;
    COWMemoryMappedFile& operator=(const COWMemoryMappedFile&) = delete;
    COWMemoryMappedFile(COWMemoryMappedFile&&) noexcept;
    COWMemoryMappedFile& operator=(COWMemoryMappedFile&&) noexcept;
    
    // Core operations
    bool map();
    void unmap();
    bool is_mapped() const { return is_mapped_; }
    
    // High-performance memory access
    void* get_write_pointer(size_t offset = 0);
    const void* get_read_pointer(size_t offset = 0) const;
    
    // Automatic growing for COW snapshots
    bool ensure_capacity(size_t required_size);
    bool grow_file(size_t new_size);
    
    // Direct memory operations (fastest possible)
    bool write_direct(size_t offset, const void* data, size_t size);
    bool read_direct(size_t offset, void* data, size_t size) const;
    
    // Batch operations for COW region writes
    bool write_regions_batch(const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>& regions);
    
    // Advanced batch operations with region merging and vectorized I/O
    bool write_regions_vectorized(const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>& regions);
    bool write_regions_batch_optimized(const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>& regions);
    
    // Memory management
    bool flush_to_disk(size_t offset = 0, size_t size = 0); // 0 = entire file
    bool sync_async(); // Non-blocking sync
    
    // File information
    size_t size() const { return current_size_; }
    size_t capacity() const { return max_size_; }
    const std::string& filename() const { return filename_; }
    
    // Performance statistics
    uint64_t get_write_count() const { return total_writes_.load(); }
    uint64_t get_read_count() const { return total_reads_.load(); }
    uint64_t get_bytes_written() const { return bytes_written_.load(); }
    uint64_t get_bytes_read() const { return bytes_read_.load(); }
    double get_write_throughput_mbps() const;
    double get_read_throughput_mbps() const;
    
private:
    bool create_mapping(size_t size);
    void cleanup_mapping();
    
#ifdef _WIN32
    bool create_windows_mapping(size_t size);
    void* create_additional_view(size_t offset, size_t size);
#else
    bool create_posix_mapping(size_t size);
#endif
};

//==============================================================================
// COW Memory-Mapped File Manager
//==============================================================================

class COWMMapManager {
private:
    std::unordered_map<std::string, std::unique_ptr<COWMemoryMappedFile>> active_files_;
    mutable std::shared_mutex files_mutex_;
    
    // Global settings
    size_t default_file_size_;
    size_t max_open_files_;
    bool enable_auto_sync_;
    
    // Performance tracking
    std::atomic<uint64_t> total_file_operations_{0};
    std::atomic<uint64_t> cache_hits_{0};
    std::atomic<uint64_t> cache_misses_{0};
    
public:
    explicit COWMMapManager(size_t default_size = COWMemoryMappedFile::DEFAULT_GROW_SIZE,
                           size_t max_files = 100);
    ~COWMMapManager();
    
    // File management
    COWMemoryMappedFile* get_or_create_file(const std::string& filename, 
                                          size_t initial_size = 0);
    bool close_file(const std::string& filename);
    void close_all_files();
    
    // COW snapshot operations
    bool write_cow_snapshot(const std::string& filename,
                           const std::vector<std::pair<const void*, size_t>>& memory_regions);
    bool read_cow_snapshot(const std::string& filename,
                          std::vector<std::pair<void*, size_t>>& memory_regions);
    
    // Batch operations for high performance
    bool write_multiple_snapshots(const std::vector<std::pair<std::string, 
                                  std::vector<std::pair<const void*, size_t>>>>& snapshots);
    
    // Advanced batch region mapping and optimization
    bool batch_map_regions(const std::vector<std::pair<std::string, size_t>>& files_and_sizes);
    bool write_regions_batch_merged(const std::string& filename,
                                   const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>& regions);
    
    // Memory management
    bool sync_all_files(bool async = true);
    bool flush_all_files();
    void trim_unused_files(); // Close files not accessed recently
    
    // Configuration
    void set_auto_sync(bool enable) { enable_auto_sync_ = enable; }
    void set_max_open_files(size_t max_files) { max_open_files_ = max_files; }
    
    // Statistics and monitoring
    size_t get_active_file_count() const;
    uint64_t get_total_operations() const { return total_file_operations_.load(); }
    double get_cache_hit_rate() const;
    
    // Performance analysis
    struct PerformanceStats {
        uint64_t total_operations;
        uint64_t cache_hits;
        uint64_t cache_misses;
        double cache_hit_rate;
        size_t active_files;
        uint64_t total_bytes_written;
        uint64_t total_bytes_read;
        double avg_write_throughput_mbps;
        double avg_read_throughput_mbps;
    };
    
    PerformanceStats get_performance_stats() const;
    void reset_performance_stats();
    
private:
    void cleanup_old_files();
    bool ensure_file_capacity();
};

//==============================================================================
// Utility Functions for COW Integration
//==============================================================================

namespace COWMMapUtils {
    
    // Batch optimization data structures
    struct MergedRegion {
        size_t start_offset;
        size_t total_size;
        std::vector<std::pair<size_t, std::pair<const void*, size_t>>> constituent_regions;
        
        MergedRegion(size_t start, size_t size) : start_offset(start), total_size(size) {}
    };
    
    struct BatchWriteRequest {
        std::vector<MergedRegion> merged_regions;
        size_t total_write_size;
        bool use_vectorized_io;
        
        BatchWriteRequest() : total_write_size(0), use_vectorized_io(true) {}
    };
    
    // Region merging and batch optimization functions
    std::vector<MergedRegion> merge_contiguous_regions(
        const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>& regions,
        size_t merge_threshold = 4096);
    
    BatchWriteRequest optimize_batch_write(
        const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>& regions);
    
    // Calculate optimal file size based on memory usage patterns
    size_t calculate_optimal_snapshot_size(size_t total_memory_tracked,
                                         size_t num_regions,
                                         double growth_factor = 1.5);
    
    // Validate memory-mapped file integrity
    bool validate_snapshot_file(const std::string& filename);
    
    // Performance benchmarking
    struct BenchmarkResult {
        double write_throughput_mbps;
        double read_throughput_mbps;
        uint64_t operations_per_sec;
        uint64_t avg_latency_us;
    };
    
    BenchmarkResult benchmark_mmap_performance(size_t test_size_mb = 100);
    
    // Platform-specific optimizations
    void optimize_for_sequential_access(COWMemoryMappedFile* file);
    void optimize_for_random_access(COWMemoryMappedFile* file);
    
    // Error handling and diagnostics
    std::string get_last_mmap_error();
    bool is_system_mmap_capable(size_t required_size);
}

} // namespace xtree

#endif // COW_MMAP_MANAGER_HPP