/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Optimized COW Memory Manager with High-Performance File I/O
 * 
 * This is an enhanced version of cow_memmgr.hpp that integrates our optimized
 * file I/O operations to achieve Linux/macOS-level performance on Windows.
 */

#ifndef COW_MEMMGR_OPTIMIZED_HPP
#define COW_MEMMGR_OPTIMIZED_HPP

#include "cow_memmgr.hpp"
#include "../fileio/fast_file_io.hpp"
#include <chrono>
#include <iostream>

namespace xtree {

// Enhanced performance statistics
struct COWPerformanceStats {
    std::atomic<uint64_t> snapshot_count{0};
    std::atomic<uint64_t> total_snapshot_time_us{0};
    std::atomic<uint64_t> total_bytes_written{0};
    std::atomic<uint64_t> total_bytes_read{0};
    std::atomic<uint64_t> file_io_time_us{0};
    std::atomic<uint64_t> memory_copy_time_us{0};
    
    double get_average_snapshot_time_ms() const {
        uint64_t count = snapshot_count.load();
        uint64_t time = total_snapshot_time_us.load();
        return count > 0 ? (time / 1000.0) / count : 0.0;
    }
    
    double get_snapshot_throughput_mbps() const {
        uint64_t time = total_snapshot_time_us.load();
        uint64_t bytes = total_bytes_written.load();
        return time > 0 ? (bytes / 1024.0 / 1024.0) / (time / 1000000.0) : 0.0;
    }
    
    void print_stats() const {
        std::cout << "\n=== COW Performance Statistics ===" << std::endl;
        std::cout << "Snapshots created: " << snapshot_count.load() << std::endl;
        std::cout << "Average snapshot time: " << get_average_snapshot_time_ms() << " ms" << std::endl; 
        std::cout << "Snapshot throughput: " << get_snapshot_throughput_mbps() << " MB/sec" << std::endl;
        std::cout << "Total bytes written: " << (total_bytes_written.load() / 1024 / 1024) << " MB" << std::endl;
        std::cout << "File I/O time: " << (file_io_time_us.load() / 1000) << " ms" << std::endl;
        std::cout << "Memory copy time: " << (memory_copy_time_us.load() / 1000) << " ms" << std::endl;
    }
};

// Global performance statistics
extern COWPerformanceStats g_cow_performance_stats;

template<typename RecordType>
class OptimizedDirectMemoryCOWManager : public DirectMemoryCOWManager<RecordType> {
private:
    using BaseClass = DirectMemoryCOWManager<RecordType>;
    
    // Enhanced configuration for Windows optimization
    struct OptimizationConfig {
        size_t file_buffer_size = 8 * 1024 * 1024; // 8MB buffer for optimal Windows performance
        bool use_async_io = true;
        bool use_memory_mapping = false; // Can be enabled for read operations
        bool use_batch_writes = true;
        bool compress_snapshots = false; // Future enhancement
        size_t write_parallelism = 1; // Number of parallel write threads
        
        // File system optimization hints
        bool disable_indexing = true; // Disable Windows Search indexing
        bool use_write_through = false; // Bypass system cache for large files
        bool enable_prefetching = true; // Prefetch for read operations
    };
    
    OptimizationConfig optimization_config_;
    mutable std::mutex stats_mutex_;
    
public:
    explicit OptimizedDirectMemoryCOWManager(IndexDetails<RecordType>* index_details, 
                                           const std::string& persist_file,
                                           const OptimizationConfig& config = OptimizationConfig{})
        : BaseClass(index_details, persist_file)
        , optimization_config_(config)
    {
        // Configure file I/O optimizations based on system capabilities
        configure_optimizations();
    }
    
    // Enhanced snapshot creation with optimized file I/O
    void persist_memory_snapshot() override {
        auto snapshot_start = std::chrono::high_resolution_clock::now();
        
        std::string temp_file = this->persist_file_ + ".tmp";
        
        // Use our optimized file writer
        FastFileWriter writer(temp_file, optimization_config_.file_buffer_size, 
                             optimization_config_.use_async_io);
        
        if (!writer.open()) {
            throw std::runtime_error("Failed to create optimized memory snapshot file");
        }
        
        // Configure writer for large file optimization
        writer.set_large_file_mode(true);
        
        // Write snapshot header
        MemorySnapshotHeader header;
        prepare_snapshot_header(header);
        
        auto io_start = std::chrono::high_resolution_clock::now();
        
        if (!writer.write(&header, sizeof(header))) {
            throw std::runtime_error("Failed to write snapshot header");
        }
        
        // Optimized memory region writing
        if (optimization_config_.use_batch_writes) {
            write_memory_regions_batch(writer);
        } else {
            write_memory_regions_sequential(writer);
        }
        
        // Ensure all data is written to disk
        writer.sync();
        writer.close();
        
        auto io_end = std::chrono::high_resolution_clock::now();
        
        // Atomic file replacement
        if (!replace_file_atomic(temp_file, this->persist_file_)) {
            throw std::runtime_error("Failed to replace snapshot file");
        }
        
        auto snapshot_end = std::chrono::high_resolution_clock::now();
        
        // Update performance statistics
        update_performance_stats(snapshot_start, snapshot_end, io_start, io_end, 
                                sizeof(header) + get_total_memory_size());
        
        this->operations_since_snapshot_ = 0;
    }
    
    // Enhanced snapshot loading with optimized file I/O
    bool load_memory_snapshot() {
        if (!std::filesystem::exists(this->persist_file_)) {
            return false;
        }
        
        auto load_start = std::chrono::high_resolution_clock::now();
        
        if (optimization_config_.use_memory_mapping) {
            return load_memory_snapshot_mapped();
        } else {
            return load_memory_snapshot_buffered();
        }
    }
    
    // Performance monitoring
    COWPerformanceStats get_performance_stats() const {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        return g_cow_performance_stats;
    }
    
    void reset_performance_stats() {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        g_cow_performance_stats = COWPerformanceStats{};
    }
    
    void print_performance_report() const {
        get_performance_stats().print_stats();
        
        // Also print global file I/O stats
        std::cout << "\n=== Global File I/O Statistics ===" << std::endl;
        auto file_stats = FileUtils::get_global_stats();
        std::cout << "Read throughput: " << file_stats.get_read_throughput_mbps() << " MB/sec" << std::endl;
        std::cout << "Write throughput: " << file_stats.get_write_throughput_mbps() << " MB/sec" << std::endl;
        std::cout << "Total read operations: " << file_stats.read_operations.load() << std::endl;
        std::cout << "Total write operations: " << file_stats.write_operations.load() << std::endl;
        std::cout << "Total sync operations: " << file_stats.sync_operations.load() << std::endl;
    }
    
    // Configuration management
    void set_optimization_config(const OptimizationConfig& config) {
        optimization_config_ = config;
        configure_optimizations();
    }
    
    const OptimizationConfig& get_optimization_config() const {
        return optimization_config_;
    }
    
private:
    void configure_optimizations() {
        // Auto-detect optimal buffer size based on available memory and file system
        auto fs_info = FileUtils::get_filesystem_info(this->persist_file_);
        
        // Adjust buffer size based on cluster size for optimal performance
        if (fs_info.cluster_size > 0) {
            size_t optimal_buffer = ((optimization_config_.file_buffer_size + fs_info.cluster_size - 1) 
                                   / fs_info.cluster_size) * fs_info.cluster_size;
            optimization_config_.file_buffer_size = optimal_buffer;
        }
        
        // Enable memory mapping only if supported and beneficial
        optimization_config_.use_memory_mapping = fs_info.supports_memory_mapping && 
                                                  get_total_memory_size() > 100 * 1024 * 1024; // 100MB+
        
        // Configure async I/O based on system support
        optimization_config_.use_async_io = fs_info.supports_async_io;
    }
    
    void prepare_snapshot_header(MemorySnapshotHeader& header) {
        {
            std::shared_lock<std::shared_mutex> lock(this->memory_tracker_.regions_lock_);
            header.total_regions = this->memory_tracker_.tracked_regions_.size();
            header.total_size = this->memory_tracker_.get_total_tracked_bytes();
        }
        
        // Set index details if available
        if (this->index_details_) {
            header.dimension = this->index_details_->getDimensionCount();
            header.precision = this->index_details_->getPrecision();
            header.root_address = this->index_details_->getRootAddress();
        } else {
            header.dimension = 0;
            header.precision = 0;
            header.root_address = 0;
        }
        
        header.snapshot_time = std::chrono::system_clock::now();
        header.magic = COW_SNAPSHOT_MAGIC;
        header.version = COW_SNAPSHOT_VERSION;
    }
    
    void write_memory_regions_batch(FastFileWriter& writer) {
        // Copy all memory regions while holding the lock to minimize lock time
        std::vector<std::pair<PageAlignedMemoryTracker::MemoryRegion, std::vector<char>>> region_copies;
        
        auto copy_start = std::chrono::high_resolution_clock::now();
        
        {
            std::shared_lock<std::shared_mutex> lock(this->memory_tracker_.regions_lock_);
            
            for (const auto& [addr, region] : this->memory_tracker_.tracked_regions_) {
                std::vector<char> data(region.size);
                std::memcpy(data.data(), region.start_addr, region.size);
                region_copies.emplace_back(region, std::move(data));
            }
        }
        
        auto copy_end = std::chrono::high_resolution_clock::now();
        auto copy_time = std::chrono::duration_cast<std::chrono::microseconds>(copy_end - copy_start);
        g_cow_performance_stats.memory_copy_time_us += copy_time.count();
        
        // Prepare batch write operations
        std::vector<std::pair<const void*, size_t>> write_chunks;
        
        // Write region headers first
        std::vector<RegionHeader> headers;
        size_t data_offset = sizeof(MemorySnapshotHeader) + (region_copies.size() * sizeof(RegionHeader));
        
        for (const auto& [region, data] : region_copies) {
            RegionHeader reg_header;
            reg_header.original_addr = region.start_addr;
            reg_header.size = region.size;
            reg_header.offset_in_file = data_offset;
            headers.push_back(reg_header);
            
            data_offset += region.size;
        }
        
        // Batch write headers
        if (!headers.empty()) {
            write_chunks.emplace_back(headers.data(), headers.size() * sizeof(RegionHeader));
        }
        
        // Batch write all region data
        for (const auto& [region, data] : region_copies) {
            write_chunks.emplace_back(data.data(), data.size());
        }
        
        // Perform optimized batch write
        if (!writer.write_batch(write_chunks)) {
            throw std::runtime_error("Failed to write memory regions in batch");
        }
    }
    
    void write_memory_regions_sequential(FastFileWriter& writer) {
        // Fallback to sequential writing for compatibility
        this->write_memory_regions_to_file_optimized(writer);
    }
    
    bool load_memory_snapshot_mapped() {
        MemoryMappedFile mapped_file(this->persist_file_, true);
        
        if (!mapped_file.map()) {
            return false;
        }
        
        // Advise sequential access for better performance
        mapped_file.advise_sequential();
        
        const char* data = static_cast<const char*>(mapped_file.data());
        size_t pos = 0;
        
        // Read and validate header
        if (mapped_file.size() < sizeof(MemorySnapshotHeader)) {
            return false;
        }
        
        const MemorySnapshotHeader* header = reinterpret_cast<const MemorySnapshotHeader*>(data);
        pos += sizeof(MemorySnapshotHeader);
        
        if (!validate_snapshot_header(*header)) {
            return false;
        }
        
        // Process memory regions from mapped data
        return process_memory_regions_from_mapped_data(data, pos, header->total_regions);
    }
    
    bool load_memory_snapshot_buffered() {
        FastFileReader reader(this->persist_file_, optimization_config_.file_buffer_size, 
                             optimization_config_.use_async_io);
        
        if (!reader.open()) {
            return false;
        }
        
        reader.set_large_file_mode(true);
        
        // Enable prefetching for better read performance
        if (optimization_config_.enable_prefetching) {
            reader.prefetch(optimization_config_.file_buffer_size * 2);
        }
        
        // Read and validate header
        MemorySnapshotHeader header;
        if (!reader.read(&header, sizeof(header))) {
            return false;
        }
        
        if (!validate_snapshot_header(header)) {
            return false;
        }
        
        // Process memory regions
        return process_memory_regions_from_buffered_reader(reader, header.total_regions);
    }
    
    bool replace_file_atomic(const std::string& temp_file, const std::string& target_file) {
#ifdef _WIN32
        // Use MoveFileEx with MOVEFILE_REPLACE_EXISTING for atomic replacement
        return MoveFileExA(temp_file.c_str(), target_file.c_str(), MOVEFILE_REPLACE_EXISTING) != 0;
#else
        // Use rename for atomic replacement on POSIX systems
        return rename(temp_file.c_str(), target_file.c_str()) == 0;
#endif
    }
    
    size_t get_total_memory_size() const {
        std::shared_lock<std::shared_mutex> lock(this->memory_tracker_.regions_lock_);
        return this->memory_tracker_.get_total_tracked_bytes();
    }
    
    void update_performance_stats(const std::chrono::high_resolution_clock::time_point& snapshot_start,
                                 const std::chrono::high_resolution_clock::time_point& snapshot_end,
                                 const std::chrono::high_resolution_clock::time_point& io_start,
                                 const std::chrono::high_resolution_clock::time_point& io_end,
                                 size_t bytes_written) {
        
        auto total_time = std::chrono::duration_cast<std::chrono::microseconds>(snapshot_end - snapshot_start);
        auto io_time = std::chrono::duration_cast<std::chrono::microseconds>(io_end - io_start);
        
        std::lock_guard<std::mutex> lock(stats_mutex_);
        g_cow_performance_stats.snapshot_count++;
        g_cow_performance_stats.total_snapshot_time_us += total_time.count();
        g_cow_performance_stats.total_bytes_written += bytes_written;
        g_cow_performance_stats.file_io_time_us += io_time.count();
    }
    
    bool validate_snapshot_header(const MemorySnapshotHeader& header) const {
        return header.magic == COW_SNAPSHOT_MAGIC && 
               header.version == COW_SNAPSHOT_VERSION &&
               header.total_regions > 0 &&
               header.total_size > 0;
    }
    
    // Additional helper methods for memory region processing
    bool process_memory_regions_from_mapped_data(const char* data, size_t& pos, size_t region_count) {
        // Implementation would process regions directly from mapped memory
        // This is a performance optimization for large snapshots
        return true; // Placeholder
    }
    
    bool process_memory_regions_from_buffered_reader(FastFileReader& reader, size_t region_count) {
        // Implementation would read regions using buffered reader
        // This provides better compatibility and error handling
        return true; // Placeholder
    }
    
    // Helper struct for region headers
    struct RegionHeader {
        void* original_addr;
        size_t size;
        size_t offset_in_file;
    };
};

// Template specialization factory function
template<typename RecordType>
std::unique_ptr<OptimizedDirectMemoryCOWManager<RecordType>> 
create_optimized_cow_manager(IndexDetails<RecordType>* index_details, 
                           const std::string& persist_file) {
    
    using OptConfig = typename OptimizedDirectMemoryCOWManager<RecordType>::OptimizationConfig;
    
    OptConfig config;
    
    // Auto-configure based on system capabilities
#ifdef _WIN32
    config.file_buffer_size = 8 * 1024 * 1024; // 8MB optimal for Windows
    config.use_async_io = true;
    config.disable_indexing = true;
    config.use_write_through = false; // Let our buffering handle it
#else
    config.file_buffer_size = 4 * 1024 * 1024; // 4MB optimal for Linux
    config.use_async_io = false; // Standard I/O is efficient on Linux
    config.disable_indexing = false; // Not applicable
#endif
    
    return std::make_unique<OptimizedDirectMemoryCOWManager<RecordType>>(
        index_details, persist_file, config);
}

} // namespace xtree

#endif // COW_MEMMGR_OPTIMIZED_HPP