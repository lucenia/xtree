/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * High-Performance File I/O Implementation for Windows Optimization
 */

#include "fast_file_io.hpp"
#include <cstring>
#include <algorithm>
#include <chrono>
#include <iostream>

#ifdef _WIN32
#include <winioctl.h>
#else
#include <sys/types.h>
#include <sys/uio.h>
#endif

namespace xtree {

// Global file I/O statistics
FileIOStats g_file_io_stats;

//==============================================================================
// FastFileWriter Implementation
//==============================================================================

FastFileWriter::FastFileWriter(const std::string& filename, size_t buffer_size, bool use_async)
    : filename_(filename)
    , buffer_size_(buffer_size)
    , buffer_pos_(0)
    , is_open_(false)
#ifdef _WIN32
    , file_handle_(INVALID_HANDLE_VALUE)
    , async_mode_(use_async)
#else
    , file_descriptor_(-1)
#endif
{
    write_buffer_.resize(buffer_size_);
    
#ifdef _WIN32
    memset(&overlapped_, 0, sizeof(overlapped_));
#endif
}

FastFileWriter::~FastFileWriter() {
    if (is_open_) {
        close();
    }
}

FastFileWriter::FastFileWriter(FastFileWriter&& other) noexcept
    : filename_(std::move(other.filename_))
    , write_buffer_(std::move(other.write_buffer_))
    , buffer_size_(other.buffer_size_)
    , buffer_pos_(other.buffer_pos_)
    , is_open_(other.is_open_)
#ifdef _WIN32
    , file_handle_(other.file_handle_)
    , overlapped_(other.overlapped_)
    , async_mode_(other.async_mode_)
#else
    , file_descriptor_(other.file_descriptor_)
#endif
{
#ifdef _WIN32
    other.file_handle_ = INVALID_HANDLE_VALUE;
#else
    other.file_descriptor_ = -1;
#endif
    other.is_open_ = false;
}

FastFileWriter& FastFileWriter::operator=(FastFileWriter&& other) noexcept {
    if (this != &other) {
        if (is_open_) {
            close();
        }
        
        filename_ = std::move(other.filename_);
        write_buffer_ = std::move(other.write_buffer_);
        buffer_size_ = other.buffer_size_;
        buffer_pos_ = other.buffer_pos_;
        is_open_ = other.is_open_;
        
#ifdef _WIN32
        file_handle_ = other.file_handle_;
        overlapped_ = other.overlapped_;
        async_mode_ = other.async_mode_;
        other.file_handle_ = INVALID_HANDLE_VALUE;
#else
        file_descriptor_ = other.file_descriptor_;
        other.file_descriptor_ = -1;
#endif
        other.is_open_ = false;
    }
    return *this;
}

bool FastFileWriter::open() {
    if (is_open_) {
        return true;
    }
    
#ifdef _WIN32
    DWORD flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN;
    if (async_mode_) {
        flags |= FILE_FLAG_OVERLAPPED;
    }
    
    // Use larger system buffer for better performance
    if (buffer_size_ >= LARGE_BUFFER_SIZE) {
        flags |= FILE_FLAG_NO_BUFFERING; // We handle buffering ourselves for large files
    }
    
    // Optimize file creation for COW temporary files
    DWORD creation_disposition = CREATE_ALWAYS;
    
    // Use CREATE_NEW for temporary files (faster when we know file doesn't exist)
    if (filename_.find(".tmp") != std::string::npos) {
        creation_disposition = CREATE_NEW;
        flags |= FILE_ATTRIBUTE_TEMPORARY; // Hint that this is a temporary file
    }
    
    file_handle_ = CreateFileA(
        filename_.c_str(),
        GENERIC_WRITE,
        FILE_SHARE_READ,
        nullptr,
        creation_disposition,
        flags,
        nullptr
    );
    
    // If CREATE_NEW failed because file exists, fallback to CREATE_ALWAYS
    if (file_handle_ == INVALID_HANDLE_VALUE && creation_disposition == CREATE_NEW) {
        DWORD error = GetLastError();
        if (error == ERROR_FILE_EXISTS || error == ERROR_ALREADY_EXISTS) {
            file_handle_ = CreateFileA(
                filename_.c_str(),
                GENERIC_WRITE,
                FILE_SHARE_READ,
                nullptr,
                CREATE_ALWAYS,
                flags,
                nullptr
            );
        }
    }
    
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        return false;
    }
    
    // Optimize for large files
    if (buffer_size_ >= LARGE_BUFFER_SIZE) {
        optimize_for_large_files();
    }
    
#else
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    
    file_descriptor_ = ::open(filename_.c_str(), flags, 0644);
    if (file_descriptor_ == -1) {
        return false;
    }
    
    // Advise kernel about our access pattern
    if (buffer_size_ >= LARGE_BUFFER_SIZE) {
        posix_fadvise(file_descriptor_, 0, 0, POSIX_FADV_SEQUENTIAL);
    }
#endif
    
    is_open_ = true;
    buffer_pos_ = 0;
    return true;
}

void FastFileWriter::close() {
    if (!is_open_) {
        return;
    }
    
    // Flush any remaining buffered data
    flush_buffer();
    
#ifdef _WIN32
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (file_descriptor_ != -1) {
        ::close(file_descriptor_);
        file_descriptor_ = -1;
    }
#endif
    
    is_open_ = false;
}

bool FastFileWriter::write(const void* data, size_t size) {
    if (!is_open_ || !data || size == 0) {
        return false;
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    const char* ptr = static_cast<const char*>(data);
    size_t remaining = size;
    
    while (remaining > 0) {
        size_t space_in_buffer = buffer_size_ - buffer_pos_;
        
        if (space_in_buffer == 0) {
            // Buffer is full, flush it
            if (!flush_buffer()) {
                return false;
            }
            space_in_buffer = buffer_size_;
        }
        
        size_t to_copy = std::min(remaining, space_in_buffer);
        
        // If the data is larger than our buffer, write directly
        if (remaining >= buffer_size_ && buffer_pos_ == 0) {
            if (!write_direct(ptr, remaining)) {
                return false;
            }
            break;
        }
        
        // Copy to buffer
        memcpy(write_buffer_.data() + buffer_pos_, ptr, to_copy);
        buffer_pos_ += to_copy;
        ptr += to_copy;
        remaining -= to_copy;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    g_file_io_stats.bytes_written += size;
    g_file_io_stats.write_operations++;
    g_file_io_stats.total_write_time_us += duration.count();
    
    return true;
}

bool FastFileWriter::write_batch(const std::vector<std::pair<const void*, size_t>>& chunks) {
    if (!is_open_ || chunks.empty()) {
        return false;
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
#ifdef _WIN32
    // Use vectored I/O for maximum performance on Windows
    if (chunks.size() > 1 && buffer_pos_ == 0) {
        // Flush buffer first if needed
        if (buffer_pos_ > 0 && !flush_buffer()) {
            return false;
        }
        
        // Prepare WSABUF structures for WriteFileGather
        std::vector<FILE_SEGMENT_ELEMENT> segments;
        size_t total_size = 0;
        
        for (const auto& chunk : chunks) {
            FILE_SEGMENT_ELEMENT segment;
            segment.Buffer = const_cast<void*>(chunk.first);
            segments.push_back(segment);
            total_size += chunk.second;
        }
        
        // Final null segment
        FILE_SEGMENT_ELEMENT null_segment;
        null_segment.Buffer = nullptr; 
        segments.push_back(null_segment);
        
        DWORD bytes_written;
        BOOL result = WriteFileGather(file_handle_, segments.data(), 
                                     static_cast<DWORD>(total_size), nullptr, &overlapped_);
        
        if (result || GetLastError() == ERROR_IO_PENDING) {
            if (async_mode_ && GetLastError() == ERROR_IO_PENDING) {
                // Wait for completion
                GetOverlappedResult(file_handle_, &overlapped_, &bytes_written, TRUE);
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            
            g_file_io_stats.bytes_written += total_size;
            g_file_io_stats.write_operations++;
            g_file_io_stats.total_write_time_us += duration.count();
            
            return true;
        }
    }
#endif
    
    // Fallback to sequential writes
    for (const auto& chunk : chunks) {
        if (!write(chunk.first, chunk.second)) {
            return false;
        }
    }
    
    return true;
}

bool FastFileWriter::flush() {
    return flush_buffer();
}

bool FastFileWriter::preallocate_space(size_t expected_size) {
    if (!is_open_) return false;
    
#ifdef _WIN32
    // Use SetFilePointerEx and SetEndOfFile to pre-allocate space
    LARGE_INTEGER file_size;
    file_size.QuadPart = static_cast<LONGLONG>(expected_size);
    
    // Get current file position
    LARGE_INTEGER old_pos;
    if (!SetFilePointerEx(file_handle_, {0}, &old_pos, FILE_CURRENT)) {
        return false;
    }
    
    // Move to desired end position
    if (!SetFilePointerEx(file_handle_, file_size, nullptr, FILE_BEGIN)) {
        return false;
    }
    
    // Set end of file to pre-allocate disk space (this avoids fragmentation)
    bool success = SetEndOfFile(file_handle_) != 0;
    
    // Restore original file position
    SetFilePointerEx(file_handle_, old_pos, nullptr, FILE_BEGIN);
    
    if (success) {
        g_file_io_stats.write_operations.fetch_add(1);
    }
    
    return success;
#else
    // Use posix_fallocate on Linux/POSIX systems for space pre-allocation
    if (file_descriptor_ != -1) {
        return posix_fallocate(file_descriptor_, 0, expected_size) == 0;
    }
    return false;
#endif
}

bool FastFileWriter::sync() {
    if (!flush_buffer()) {
        return false;
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
#ifdef _WIN32
    bool result = FlushFileBuffers(file_handle_) != 0;
#else
    bool result = fsync(file_descriptor_) == 0;
#endif
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    g_file_io_stats.sync_operations++;
    g_file_io_stats.total_write_time_us += duration.count();
    
    return result;
}

bool FastFileWriter::flush_buffer() {
    if (buffer_pos_ == 0) {
        return true;
    }
    
    bool result = write_direct(write_buffer_.data(), buffer_pos_);
    if (result) {
        buffer_pos_ = 0;
    }
    return result;
}

bool FastFileWriter::write_direct(const void* data, size_t size) {
#ifdef _WIN32
    DWORD bytes_written;
    BOOL result;
    
    if (async_mode_) {
        result = WriteFile(file_handle_, data, static_cast<DWORD>(size), 
                          &bytes_written, &overlapped_);
        
        if (!result && GetLastError() == ERROR_IO_PENDING) {
            // Wait for async operation to complete
            result = GetOverlappedResult(file_handle_, &overlapped_, &bytes_written, TRUE);
        }
    } else {
        result = WriteFile(file_handle_, data, static_cast<DWORD>(size), 
                          &bytes_written, nullptr);
    }
    
    return result && bytes_written == size;
#else
    ssize_t bytes_written = ::write(file_descriptor_, data, size);
    return bytes_written == static_cast<ssize_t>(size);
#endif
}

void FastFileWriter::optimize_for_large_files() {
#ifdef _WIN32
    // Set file to not be indexed by Windows Search
    DWORD attributes = GetFileAttributesA(filename_.c_str());
    if (attributes != INVALID_FILE_ATTRIBUTES) {
        SetFileAttributesA(filename_.c_str(), attributes | FILE_ATTRIBUTE_NOT_CONTENT_INDEXED);
    }
    
    // Hint that we'll be writing large amounts of data
    FILE_IO_PRIORITY_HINT_INFO priority_hint;
    priority_hint.PriorityHint = IoPriorityHintNormal;
    SetFileInformationByHandle(file_handle_, FileIoPriorityHintInfo, 
                              &priority_hint, sizeof(priority_hint));
#endif
}

//==============================================================================
// FastFileReader Implementation  
//==============================================================================

FastFileReader::FastFileReader(const std::string& filename, size_t buffer_size, bool use_async)
    : filename_(filename)
    , buffer_size_(buffer_size)
    , buffer_pos_(0)
    , buffer_valid_(0)
    , is_open_(false)
    , file_size_(0)
#ifdef _WIN32
    , file_handle_(INVALID_HANDLE_VALUE)
    , async_mode_(use_async)
#else
    , file_descriptor_(-1)
#endif
{
    read_buffer_.resize(buffer_size_);
    
#ifdef _WIN32
    memset(&overlapped_, 0, sizeof(overlapped_));
#endif
}

FastFileReader::~FastFileReader() {
    if (is_open_) {
        close();
    }
}

bool FastFileReader::open() {
    if (is_open_) {
        return true;
    }
    
#ifdef _WIN32
    DWORD flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN;
    if (async_mode_) {
        flags |= FILE_FLAG_OVERLAPPED;
    }
    
    file_handle_ = CreateFileA(
        filename_.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        nullptr,
        OPEN_EXISTING,
        flags,
        nullptr
    );
    
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        return false;
    }
    
    // Get file size
    LARGE_INTEGER size;
    if (!GetFileSizeEx(file_handle_, &size)) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
        return false;
    }
    file_size_ = size.QuadPart;
    
#else
    file_descriptor_ = ::open(filename_.c_str(), O_RDONLY);
    if (file_descriptor_ == -1) {
        return false;
    }
    
    file_size_ = get_file_size();
    if (file_size_ < 0) {
        ::close(file_descriptor_);
        file_descriptor_ = -1;
        return false;
    }
    
    // Advise kernel about access pattern
    if (buffer_size_ >= LARGE_BUFFER_SIZE) {
        posix_fadvise(file_descriptor_, 0, 0, POSIX_FADV_SEQUENTIAL);
    }
#endif
    
    is_open_ = true;
    buffer_pos_ = 0;
    buffer_valid_ = 0;
    return true;
}

void FastFileReader::close() {
    if (!is_open_) {
        return;
    }
    
#ifdef _WIN32
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (file_descriptor_ != -1) {
        ::close(file_descriptor_);
        file_descriptor_ = -1;
    }
#endif
    
    is_open_ = false;
}

bool FastFileReader::read(void* data, size_t size) {
    if (!is_open_ || !data || size == 0) {
        return false;
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    char* ptr = static_cast<char*>(data);
    size_t remaining = size;
    
    while (remaining > 0) {
        size_t available = buffer_valid_ - buffer_pos_;
        
        if (available == 0) {
            // Buffer is empty, fill it
            if (!fill_buffer()) {
                return false;
            }
            available = buffer_valid_ - buffer_pos_;
            
            if (available == 0) {
                // End of file
                return false;
            }
        }
        
        size_t to_copy = std::min(remaining, available);
        
        // If requesting more data than buffer size, read directly
        if (remaining >= buffer_size_ && buffer_pos_ == 0 && buffer_valid_ == 0) {
            if (!read_direct(ptr, remaining)) {
                return false;
            }
            break;
        }
        
        // Copy from buffer
        memcpy(ptr, read_buffer_.data() + buffer_pos_, to_copy);
        buffer_pos_ += to_copy;
        ptr += to_copy;
        remaining -= to_copy;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    g_file_io_stats.bytes_read += size;
    g_file_io_stats.read_operations++;
    g_file_io_stats.total_read_time_us += duration.count();
    
    return true;
}

std::vector<char> FastFileReader::read_all() {
    if (!is_open_ || file_size_ <= 0) {
        return {};
    }
    
    std::vector<char> result(static_cast<size_t>(file_size_));
    
    // Reset to beginning
    seek(0, 0);
    
    if (!read(result.data(), result.size())) {
        return {};
    }
    
    return result;
}

bool FastFileReader::fill_buffer() {
    buffer_pos_ = 0;
    buffer_valid_ = 0;
    
    return read_direct(read_buffer_.data(), buffer_size_);
}

bool FastFileReader::read_direct(void* data, size_t size) {
#ifdef _WIN32
    DWORD bytes_read;  
    BOOL result;
    
    if (async_mode_) {
        result = ReadFile(file_handle_, data, static_cast<DWORD>(size), 
                         &bytes_read, &overlapped_);
        
        if (!result && GetLastError() == ERROR_IO_PENDING) {
            result = GetOverlappedResult(file_handle_, &overlapped_, &bytes_read, TRUE);
        }
    } else {
        result = ReadFile(file_handle_, data, static_cast<DWORD>(size), 
                         &bytes_read, nullptr);
    }
    
    if (result) {
        if (data == read_buffer_.data()) {
            buffer_valid_ = bytes_read;
        }
        return bytes_read > 0;
    }
    return false;
    
#else
    ssize_t bytes_read = ::read(file_descriptor_, data, size);
    if (bytes_read >= 0) {
        if (data == read_buffer_.data()) {
            buffer_valid_ = static_cast<size_t>(bytes_read);
        }
        return bytes_read > 0;
    }
    return false;
#endif
}

int64_t FastFileReader::get_file_size() {
#ifndef _WIN32
    struct stat st;
    if (fstat(file_descriptor_, &st) == 0) {
        return st.st_size;
    }
#endif
    return -1;
}

//==============================================================================
// Memory-Mapped File Implementation
//==============================================================================

MemoryMappedFile::MemoryMappedFile(const std::string& filename, bool read_only)
    : filename_(filename)
    , read_only_(read_only)
    , mapped_data_(nullptr)
    , file_size_(0)
    , is_mapped_(false)
#ifdef _WIN32
    , file_handle_(INVALID_HANDLE_VALUE)
    , mapping_handle_(nullptr)
#else
    , file_descriptor_(-1)
#endif
{
}

MemoryMappedFile::~MemoryMappedFile() {
    if (is_mapped_) {
        unmap();
    }
}

bool MemoryMappedFile::map() {
    if (is_mapped_) {
        return true;
    }
    
#ifdef _WIN32
    DWORD access = read_only_ ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE);
    DWORD share = FILE_SHARE_READ | (read_only_ ? FILE_SHARE_WRITE : 0);
    
    file_handle_ = CreateFileA(
        filename_.c_str(),
        access,
        share,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        nullptr
    );
    
    if (file_handle_ == INVALID_HANDLE_VALUE) {
        return false;
    }
    
    // Get file size
    LARGE_INTEGER size;
    if (!GetFileSizeEx(file_handle_, &size)) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
        return false;
    }
    file_size_ = size.QuadPart;
    
    if (file_size_ == 0) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
        return false;
    }
    
    // Create mapping
    DWORD protect = read_only_ ? PAGE_READONLY : PAGE_READWRITE;
    mapping_handle_ = CreateFileMappingA(file_handle_, nullptr, protect, 0, 0, nullptr);
    
    if (!mapping_handle_) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
        return false;
    }
    
    // Map view
    DWORD map_access = read_only_ ? FILE_MAP_READ : FILE_MAP_WRITE;
    mapped_data_ = MapViewOfFile(mapping_handle_, map_access, 0, 0, 0);
    
    if (!mapped_data_) {
        CloseHandle(mapping_handle_);
        CloseHandle(file_handle_);
        mapping_handle_ = nullptr;
        file_handle_ = INVALID_HANDLE_VALUE;
        return false;
    }
    
#else
    int flags = read_only_ ? O_RDONLY : O_RDWR;
    file_descriptor_ = ::open(filename_.c_str(), flags);
    
    if (file_descriptor_ == -1) {
        return false;
    }
    
    file_size_ = get_file_size();
    if (file_size_ <= 0) {
        ::close(file_descriptor_);
        file_descriptor_ = -1;
        return false;
    }
    
    int prot = PROT_READ | (read_only_ ? 0 : PROT_WRITE);
    mapped_data_ = mmap(nullptr, file_size_, prot, MAP_SHARED, file_descriptor_, 0);
    
    if (mapped_data_ == MAP_FAILED) {
        ::close(file_descriptor_);
        file_descriptor_ = -1;
        mapped_data_ = nullptr;
        return false;
    }
#endif
    
    is_mapped_ = true;
    return true;
}

void MemoryMappedFile::unmap() {
    if (!is_mapped_) {
        return;
    }
    
#ifdef _WIN32
    if (mapped_data_) {
        UnmapViewOfFile(mapped_data_);
        mapped_data_ = nullptr;
    }
    
    if (mapping_handle_) {
        CloseHandle(mapping_handle_);
        mapping_handle_ = nullptr;
    }
    
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(file_handle_);
        file_handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (mapped_data_) {
        munmap(mapped_data_, file_size_);
        mapped_data_ = nullptr;
    }
    
    if (file_descriptor_ != -1) {
        ::close(file_descriptor_);
        file_descriptor_ = -1;
    }
#endif
    
    is_mapped_ = false;
}

bool MemoryMappedFile::flush() {
    if (!is_mapped_ || read_only_) {
        return false;
    }
    
#ifdef _WIN32
    return FlushViewOfFile(mapped_data_, 0) != 0;
#else
    return msync(mapped_data_, file_size_, MS_SYNC) == 0;
#endif
}

size_t MemoryMappedFile::get_file_size() {
#ifndef _WIN32
    struct stat st;
    if (fstat(file_descriptor_, &st) == 0) {
        return st.st_size;
    }
#endif
    return 0;
}

} // namespace xtree