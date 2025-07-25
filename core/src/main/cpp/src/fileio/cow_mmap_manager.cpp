/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * High-Performance Memory-Mapped File Manager Implementation
 */

#include "cow_mmap_manager.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>

#ifndef _WIN32
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#endif

namespace xtree {

//==============================================================================
// COWMemoryMappedFile Implementation
//==============================================================================

COWMemoryMappedFile::COWMemoryMappedFile(const std::string& filename, 
                                       size_t initial_size, 
                                       bool read_only)
    : filename_(filename)
    , read_only_(read_only)
    , base_address_(nullptr)
    , current_size_(initial_size)
    , max_size_(initial_size * 2) // Start with 2x capacity for growth
    , grow_increment_(initial_size)
    , is_mapped_(false)
#ifdef _WIN32
    , file_handle_(INVALID_HANDLE_VALUE)
    , mapping_handle_(nullptr)
#else
    , file_descriptor_(-1)
#endif
{
    // Ensure minimum size
    if (current_size_ < DEFAULT_GROW_SIZE) {
        current_size_ = DEFAULT_GROW_SIZE;
        max_size_ = DEFAULT_GROW_SIZE * 2;
        grow_increment_ = DEFAULT_GROW_SIZE;
    }
}

COWMemoryMappedFile::~COWMemoryMappedFile() {
    if (is_mapped_) {
        unmap();
    }
}

COWMemoryMappedFile::COWMemoryMappedFile(COWMemoryMappedFile&& other) noexcept
    : filename_(std::move(other.filename_))
    , read_only_(other.read_only_)
    , base_address_(other.base_address_)
    , current_size_(other.current_size_)
    , max_size_(other.max_size_)
    , grow_increment_(other.grow_increment_)
    , is_mapped_(other.is_mapped_)
    , total_writes_(other.total_writes_.load())
    , total_reads_(other.total_reads_.load())
    , bytes_written_(other.bytes_written_.load())
    , bytes_read_(other.bytes_read_.load())
#ifdef _WIN32
    , file_handle_(other.file_handle_)
    , mapping_handle_(other.mapping_handle_)
    , view_handles_(std::move(other.view_handles_))
#else
    , file_descriptor_(other.file_descriptor_)
#endif
{
    // Reset other object
    other.base_address_ = nullptr;
    other.is_mapped_ = false;
#ifdef _WIN32
    other.file_handle_ = INVALID_HANDLE_VALUE;
    other.mapping_handle_ = nullptr;
#else
    other.file_descriptor_ = -1;
#endif
}

bool COWMemoryMappedFile::map() {
    if (is_mapped_) {
        return true;
    }
    
    return create_mapping(max_size_);
}

void COWMemoryMappedFile::unmap() {
    if (!is_mapped_) {
        return;
    }
    
    // Flush any pending writes before unmapping
    if (!read_only_) {
        flush_to_disk();
    }
    
    cleanup_mapping();
    is_mapped_ = false;
}

bool COWMemoryMappedFile::create_mapping(size_t size) {
#ifdef _WIN32
    return create_windows_mapping(size);
#else
    return create_posix_mapping(size);
#endif
}

#ifdef _WIN32
bool COWMemoryMappedFile::create_windows_mapping(size_t size) {
    // Create or open the file
    DWORD access = read_only_ ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE);
    DWORD creation = read_only_ ? OPEN_EXISTING : OPEN_ALWAYS;
    
    // Use optimized flags for COW snapshots
    DWORD flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN;
    if (!read_only_) {
        flags |= FILE_FLAG_WRITE_THROUGH; // Ensure data reaches disk faster
    }
    
    file_handle_ = CreateFileA(
        filename_.c_str(),
        access,
        FILE_SHARE_READ,
        nullptr,
        creation,
        flags,
        nullptr
    );
    
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        return false;
    }
    
    // Set file size if not read-only
    if (!read_only_) {
        LARGE_INTEGER file_size;
        file_size.QuadPart = static_cast<LONGLONG>(size);
        
        if (!SetFilePointerEx(file_handle_, file_size, nullptr, FILE_BEGIN) ||
            !SetEndOfFile(file_handle_)) {
            CloseHandle(file_handle_);
            file_handle_ = INVALID_HANDLE_VALUE;
            return false;
        }
        
        // Reset file pointer
        file_size.QuadPart = 0;
        SetFilePointerEx(file_handle_, file_size, nullptr, FILE_BEGIN);
    } else {
        // Get actual file size for read-only files
        LARGE_INTEGER file_size;
        if (GetFileSizeEx(file_handle_, &file_size)) {
            current_size_ = std::min(static_cast<size_t>(file_size.QuadPart), size);
        }
    }
    
    // Create file mapping
    DWORD protect = read_only_ ? PAGE_READONLY : PAGE_READWRITE;
    DWORD size_high = static_cast<DWORD>(size >> 32);
    DWORD size_low = static_cast<DWORD>(size & 0xFFFFFFFF);
    
    mapping_handle_ = CreateFileMappingA(
        file_handle_,
        nullptr,
        protect,
        size_high,
        size_low,
        nullptr
    );
    
    if (!mapping_handle_) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
        return false;
    }
    
    // Map view of file
    DWORD map_access = read_only_ ? FILE_MAP_READ : FILE_MAP_WRITE;
    base_address_ = MapViewOfFile(
        mapping_handle_,
        map_access,
        0, 0, // Offset
        size  // Size
    );
    
    if (!base_address_) {
        CloseHandle(mapping_handle_);
        CloseHandle(file_handle_);
        mapping_handle_ = nullptr;
        file_handle_ = INVALID_HANDLE_VALUE;
        return false;
    }
    
    is_mapped_ = true;
    return true;
}

void COWMemoryMappedFile::cleanup_mapping() {
    if (base_address_) {
        UnmapViewOfFile(base_address_);
        base_address_ = nullptr;
    }
    
    // Clean up additional views
    for (void* view : view_handles_) {
        if (view) {
            UnmapViewOfFile(view);
        }
    }
    view_handles_.clear();
    
    if (mapping_handle_) {
        CloseHandle(mapping_handle_);
        mapping_handle_ = nullptr;
    }
    
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
    }
}

#else
bool COWMemoryMappedFile::create_posix_mapping(size_t size) {
    // Open or create file
    int flags = read_only_ ? O_RDONLY : (O_RDWR | O_CREAT);
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    
    file_descriptor_ = open(filename_.c_str(), flags, mode);
    
    if (file_descriptor_ == -1) {
        return false;
    }
    
    // Set file size if not read-only
    if (!read_only_) {
        if (ftruncate(file_descriptor_, size) != 0) {
            close(file_descriptor_);
            file_descriptor_ = -1;
            return false;
        }
    } else {
        // Get actual file size for read-only files
        struct stat st;
        if (fstat(file_descriptor_, &st) == 0) {
            current_size_ = std::min(static_cast<size_t>(st.st_size), size);
        }
    }
    
    // Create memory mapping
    int prot = read_only_ ? PROT_READ : (PROT_READ | PROT_WRITE);
    int map_flags = MAP_SHARED;
    
    base_address_ = mmap(nullptr, size, prot, map_flags, file_descriptor_, 0);
    
    if (base_address_ == MAP_FAILED) {
        close(file_descriptor_);
        file_descriptor_ = -1;
        base_address_ = nullptr;
        return false;
    }
    
    // Advise kernel about usage pattern
    madvise(base_address_, size, MADV_SEQUENTIAL);
    
    is_mapped_ = true;
    return true;
}

void COWMemoryMappedFile::cleanup_mapping() {
    if (base_address_) {
        munmap(base_address_, max_size_);
        base_address_ = nullptr;
    }
    
    if (file_descriptor_ != -1) {
        close(file_descriptor_);
        file_descriptor_ = -1;
    }
}
#endif

void* COWMemoryMappedFile::get_write_pointer(size_t offset) {
    if (!is_mapped_ || read_only_ || offset >= current_size_) {
        return nullptr;
    }
    
    return static_cast<char*>(base_address_) + offset;
}

const void* COWMemoryMappedFile::get_read_pointer(size_t offset) const {
    if (!is_mapped_ || offset >= current_size_) {
        return nullptr;
    }
    
    return static_cast<const char*>(base_address_) + offset;
}

bool COWMemoryMappedFile::write_direct(size_t offset, const void* data, size_t size) {
    if (!is_mapped_ || read_only_ || offset + size > current_size_) {
        return false;
    }
    
    void* dest = get_write_pointer(offset);
    if (!dest) {
        return false;
    }
    
    std::memcpy(dest, data, size);
    
    // Update statistics
    total_writes_.fetch_add(1);
    bytes_written_.fetch_add(size);
    
    return true;
}

bool COWMemoryMappedFile::read_direct(size_t offset, void* data, size_t size) const {
    if (!is_mapped_ || offset + size > current_size_) {
        return false;
    }
    
    const void* src = get_read_pointer(offset);
    if (!src) {
        return false;
    }
    
    std::memcpy(data, src, size);
    
    // Update statistics (const_cast is safe for atomic operations)
    const_cast<COWMemoryMappedFile*>(this)->total_reads_.fetch_add(1);
    const_cast<COWMemoryMappedFile*>(this)->bytes_read_.fetch_add(size);
    
    return true;
}

bool COWMemoryMappedFile::write_regions_batch(
    const std::vector<std::pair<size_t, std::pair<const void*, size_t>>>& regions) {
    
    if (!is_mapped_ || read_only_) {
        return false;
    }
    
    // Batch write for optimal performance
    for (const auto& region : regions) {
        size_t offset = region.first;
        const void* data = region.second.first;
        size_t size = region.second.second;
        
        if (!write_direct(offset, data, size)) {
            return false;
        }
    }
    
    return true;
}

bool COWMemoryMappedFile::ensure_capacity(size_t required_size) {
    if (required_size <= max_size_) {
        return true;
    }
    
    std::lock_guard<std::mutex> lock(resize_mutex_);
    
    // Check again after acquiring lock
    if (required_size <= max_size_) {
        return true;
    }
    
    // Calculate new size with growth factor
    size_t new_size = max_size_;
    while (new_size < required_size) {
        new_size += grow_increment_;
        if (new_size > MAX_FILE_SIZE) {
            return false; // File too large
        }
    }
    
    return grow_file(new_size);
}

bool COWMemoryMappedFile::grow_file(size_t new_size) {
    if (new_size <= max_size_) {
        return true;
    }
    
    // For Windows, we need to unmap, resize, and remap
    // For POSIX, we can use mremap if available
    
    if (is_mapped_) {
        unmap();
    }
    
    max_size_ = new_size;
    current_size_ = new_size;
    
    return map();
}

bool COWMemoryMappedFile::flush_to_disk(size_t offset, size_t size) {
    if (!is_mapped_ || read_only_) {
        return false;
    }
    
    if (size == 0) {
        size = current_size_;
    }
    
#ifdef _WIN32
    void* flush_addr = static_cast<char*>(base_address_) + offset;
    return FlushViewOfFile(flush_addr, size) != 0;
#else
    void* flush_addr = static_cast<char*>(base_address_) + offset;
    return msync(flush_addr, size, MS_SYNC) == 0;
#endif
}

bool COWMemoryMappedFile::sync_async() {
    if (!is_mapped_ || read_only_) {
        return false;
    }
    
#ifdef _WIN32
    return FlushViewOfFile(base_address_, 0) != 0;
#else
    return msync(base_address_, current_size_, MS_ASYNC) == 0;
#endif
}

double COWMemoryMappedFile::get_write_throughput_mbps() const {
    uint64_t writes = total_writes_.load();
    uint64_t bytes = bytes_written_.load();
    
    if (writes == 0) return 0.0;
    
    // Estimate based on operations (rough approximation)
    double estimated_time_sec = writes * 0.001; // Assume 1ms per operation
    return bytes > 0 ? (bytes / 1024.0 / 1024.0) / estimated_time_sec : 0.0;
}

double COWMemoryMappedFile::get_read_throughput_mbps() const {
    uint64_t reads = total_reads_.load();
    uint64_t bytes = bytes_read_.load();
    
    if (reads == 0) return 0.0;
    
    // Estimate based on operations (rough approximation)
    double estimated_time_sec = reads * 0.0005; // Assume 0.5ms per read operation
    return bytes > 0 ? (bytes / 1024.0 / 1024.0) / estimated_time_sec : 0.0;
}

//==============================================================================
// COWMMapManager Implementation
//==============================================================================

COWMMapManager::COWMMapManager(size_t default_size, size_t max_files)
    : default_file_size_(default_size)
    , max_open_files_(max_files)
    , enable_auto_sync_(true) {
}

COWMMapManager::~COWMMapManager() {
    close_all_files();
}

COWMemoryMappedFile* COWMMapManager::get_or_create_file(const std::string& filename, 
                                                       size_t initial_size) {
    if (initial_size == 0) {
        initial_size = default_file_size_;
    }
    
    std::unique_lock<std::shared_mutex> lock(files_mutex_);
    
    auto it = active_files_.find(filename);
    if (it != active_files_.end()) {
        cache_hits_.fetch_add(1);
        return it->second.get();
    }
    
    cache_misses_.fetch_add(1);
    
    // Check if we need to clean up old files
    if (active_files_.size() >= max_open_files_) {
        // Remove oldest file (simple LRU simulation)
        auto oldest = active_files_.begin();
        active_files_.erase(oldest);
    }
    
    // Create new file
    auto file = std::make_unique<COWMemoryMappedFile>(filename, initial_size, false);
    if (!file->map()) {
        return nullptr;
    }
    
    COWMemoryMappedFile* file_ptr = file.get();
    active_files_[filename] = std::move(file);
    
    total_file_operations_.fetch_add(1);
    
    return file_ptr;
}

bool COWMMapManager::close_file(const std::string& filename) {
    std::unique_lock<std::shared_mutex> lock(files_mutex_);
    
    auto it = active_files_.find(filename);
    if (it != active_files_.end()) {
        active_files_.erase(it);
        return true;
    }
    
    return false;
}

void COWMMapManager::close_all_files() {
    std::unique_lock<std::shared_mutex> lock(files_mutex_);
    active_files_.clear();
}

bool COWMMapManager::write_cow_snapshot(const std::string& filename,
                                       const std::vector<std::pair<const void*, size_t>>& memory_regions) {
    
    // Calculate total size needed
    size_t total_size = 0;
    for (const auto& region : memory_regions) {
        total_size += region.second;
    }
    
    COWMemoryMappedFile* file = get_or_create_file(filename, total_size + 1024); // Add header space
    if (!file) {
        return false;
    }
    
    if (!file->ensure_capacity(total_size + 1024)) {
        return false;
    }
    
    // Write regions sequentially
    size_t offset = 1024; // Reserve space for header
    std::vector<std::pair<size_t, std::pair<const void*, size_t>>> batch_regions;
    
    for (const auto& region : memory_regions) {
        batch_regions.emplace_back(offset, region);
        offset += region.second;
    }
    
    bool success = file->write_regions_batch(batch_regions);
    
    if (success && enable_auto_sync_) {
        file->sync_async();
    }
    
    return success;
}

size_t COWMMapManager::get_active_file_count() const {
    std::shared_lock<std::shared_mutex> lock(files_mutex_);
    return active_files_.size();
}

double COWMMapManager::get_cache_hit_rate() const {
    uint64_t hits = cache_hits_.load();
    uint64_t misses = cache_misses_.load();
    uint64_t total = hits + misses;
    
    return total > 0 ? static_cast<double>(hits) / total : 0.0;
}

COWMMapManager::PerformanceStats COWMMapManager::get_performance_stats() const {
    PerformanceStats stats{};
    stats.total_operations = total_file_operations_.load();
    stats.cache_hits = cache_hits_.load();
    stats.cache_misses = cache_misses_.load();
    stats.cache_hit_rate = get_cache_hit_rate();
    stats.active_files = get_active_file_count();
    
    // Aggregate stats from all files
    std::shared_lock<std::shared_mutex> lock(files_mutex_);
    for (const auto& [filename, file] : active_files_) {
        stats.total_bytes_written += file->get_bytes_written();
        stats.total_bytes_read += file->get_bytes_read();
    }
    
    return stats;
}

//==============================================================================
// Utility Functions
//==============================================================================

namespace COWMMapUtils {

size_t calculate_optimal_snapshot_size(size_t total_memory_tracked,
                                     size_t num_regions,
                                     double growth_factor) {
    
    // Base size: tracked memory + region headers + safety margin
    size_t base_size = total_memory_tracked + (num_regions * 64) + (1024 * 1024); // 1MB margin
    
    // Apply growth factor for future expansions
    size_t optimal_size = static_cast<size_t>(base_size * growth_factor);
    
    // Align to page boundaries for optimal performance
    size_t page_size = 4096; // Standard page size
    optimal_size = ((optimal_size + page_size - 1) / page_size) * page_size;
    
    return optimal_size;
}

bool validate_snapshot_file(const std::string& filename) {
    // Basic validation - check if file exists and is readable
    try {
        COWMemoryMappedFile test_file(filename, 0, true); // Read-only
        return test_file.map();
    } catch (...) {
        return false;
    }
}

COWMMapUtils::BenchmarkResult benchmark_mmap_performance(size_t test_size_mb) {
    BenchmarkResult result{};
    
    const size_t test_size = test_size_mb * 1024 * 1024;
    const size_t chunk_size = 64 * 1024; // 64KB chunks
    const size_t num_operations = test_size / chunk_size;
    
    std::string test_filename = "mmap_benchmark_test.tmp";
    
    try {
        COWMemoryMappedFile test_file(test_filename, test_size, false);
        if (!test_file.map()) {
            return result;
        }
        
        // Prepare test data
        std::vector<char> test_data(chunk_size, 'A');
        
        // Write performance test
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (size_t i = 0; i < num_operations; i++) {
            size_t offset = i * chunk_size;
            if (!test_file.write_direct(offset, test_data.data(), chunk_size)) {
                break;
            }
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        double duration_sec = duration.count() / 1000000.0;
        result.write_throughput_mbps = (test_size_mb / duration_sec);
        result.operations_per_sec = static_cast<uint64_t>(num_operations / duration_sec);
        result.avg_latency_us = duration.count() / num_operations;
        
        // Read performance test
        std::vector<char> read_buffer(chunk_size);
        
        start_time = std::chrono::high_resolution_clock::now();
        
        for (size_t i = 0; i < num_operations; i++) {
            size_t offset = i * chunk_size;
            if (!test_file.read_direct(offset, read_buffer.data(), chunk_size)) {
                break;
            }
        }
        
        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        duration_sec = duration.count() / 1000000.0;
        result.read_throughput_mbps = (test_size_mb / duration_sec);
        
    } catch (...) {
        // Return empty result on error
    }
    
    return result;
}

std::string get_last_mmap_error() {
#ifdef _WIN32
    DWORD error = GetLastError();
    LPSTR message = nullptr;
    FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        nullptr, error, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&message, 0, nullptr
    );
    
    std::string result = message ? message : "Unknown error";
    LocalFree(message);
    return result;
#else
    return std::string(strerror(errno));
#endif
}

} // namespace COWMMapUtils

} // namespace xtree