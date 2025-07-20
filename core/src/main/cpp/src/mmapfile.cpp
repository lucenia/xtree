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

#include "mmapfile.h"
#include <stdexcept>
#include <cstring>
#include <iostream>
#include <cerrno>
#include "util/log.h"

// Platform-specific includes
#ifdef _WIN32
    #include <windows.h>
    #include <io.h>
#else
    #include <sys/mman.h>
    #include <sys/stat.h>
    #include <fcntl.h>
    #include <unistd.h>
#endif

namespace xtree {

MMapFile::MMapFile(const std::string& filename, size_t initial_size, bool read_only) 
    : filename_(filename), fd_(-1), mapped_memory_(nullptr), 
      file_size_(0), mapped_size_(0), 
      next_allocation_offset_(HEADER_SIZE), read_only_(read_only) {
    
    // Check if file exists
    bool exists = false;
    bool has_header = false;
#ifdef _WIN32
    exists = (_access(filename.c_str(), 0) == 0);
#else
    struct stat st;
    if (stat(filename.c_str(), &st) == 0) {
        // File exists
        exists = true;
    }
#endif
    
    if (exists && initial_size == 0) {
        // Open existing file
        if (!open_existing_file()) {
            throw std::runtime_error("Failed to open existing file: " + filename);
        }
        
        // Check if file has our header
        has_header = false;
        if (file_size_ >= sizeof(FileHeader)) {
            // Temporarily map to check header
            if (map_memory()) {
                FileHeader* header = get_header();
                has_header = (header->magic == FILE_MAGIC);
                unmap_memory();
            }
        }
        
        if (!has_header) {
            // Existing file without our header - this is an error
            // MMapFile should only be used with our binary format
            close(fd_);
            throw std::runtime_error("File exists but is not in XTree binary format: " + filename);
        }
    } else {
        // Create new file or resize existing
        if (!create_new_file(initial_size > 0 ? initial_size : 1024*1024)) {
            throw std::runtime_error("Failed to create file: " + filename);
        }
    }
    
    // Map the file into memory
    if (!map_memory()) {
        close(fd_);
        throw std::runtime_error("Failed to map file: " + filename);
    }
    
    // Initialize or read header
    if (!exists || (exists && initial_size > 0)) {
        // New file or resizing - initialize header
        FileHeader* header = get_header();
        header->magic = FILE_MAGIC;
        header->version = FILE_VERSION;
        header->root_offset = 0;
        header->next_free_offset = HEADER_SIZE;
        std::memset(header->reserved, 0, sizeof(header->reserved));
        sync();
    } else if (exists && initial_size == 0 && has_header) {
        // Existing file with header - validate it and read state
        FileHeader* header = get_header();
        if (header->version != FILE_VERSION) {
            unmap_memory();
            close(fd_);
            throw std::runtime_error("Unsupported file version");
        }
        next_allocation_offset_ = header->next_free_offset;
    }
}

MMapFile::~MMapFile() {
    if (mapped_memory_) {
        // Update header before closing (only if we have a header)
        if (!read_only_ && next_allocation_offset_ > 0) {
            FileHeader* header = get_header();
            if (header && header->magic == FILE_MAGIC) {
                header->next_free_offset = next_allocation_offset_;
            }
        }
        sync();
        unmap_memory();
    }
    
    if (fd_ >= 0) {
#ifdef _WIN32
        _close(fd_);
#else
        close(fd_);
#endif
    }
}

bool MMapFile::create_new_file(size_t initial_size) {
#ifdef _WIN32
    // Windows implementation
    fd_ = _open(filename_.c_str(), 
                _O_CREAT | _O_RDWR | _O_BINARY | _O_TRUNC,
                _S_IREAD | _S_IWRITE);
    if (fd_ < 0) {
        return false;
    }
    
    // Extend file to initial size
    if (_chsize_s(fd_, initial_size) != 0) {
        _close(fd_);
        fd_ = -1;
        return false;
    }
#else
    // POSIX implementation
    int flags = O_CREAT | O_RDWR;
    if (!read_only_) {
        flags |= O_TRUNC;
    }
    
    fd_ = open(filename_.c_str(), flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_ < 0) {
        return false;
    }
    
    // Extend file to initial size
    if (ftruncate(fd_, initial_size) != 0) {
        close(fd_);
        fd_ = -1;
        return false;
    }
#endif
    
    file_size_ = initial_size;
    return true;
}

bool MMapFile::open_existing_file() {
#ifdef _WIN32
    // Windows implementation
    int flags = _O_RDWR | _O_BINARY;
    if (read_only_) {
        flags = _O_RDONLY | _O_BINARY;
    }
    
    fd_ = _open(filename_.c_str(), flags);
    if (fd_ < 0) {
        return false;
    }
    
    // Get file size
    file_size_ = _lseeki64(fd_, 0, SEEK_END);
    _lseeki64(fd_, 0, SEEK_SET);
#else
    // POSIX implementation
    int flags = O_RDWR;
    if (read_only_) {
        flags = O_RDONLY;
    }
    
    fd_ = open(filename_.c_str(), flags);
    if (fd_ < 0) {
        return false;
    }
    
    // Get file size
    struct stat st;
    if (fstat(fd_, &st) != 0) {
        close(fd_);
        fd_ = -1;
        return false;
    }
    file_size_ = st.st_size;
#endif
    
    return true;
}

bool MMapFile::map_memory() {
    if (file_size_ == 0) {
        return false;
    }
    
#ifdef _WIN32
    // Windows implementation using CreateFileMapping
    DWORD protect = read_only_ ? PAGE_READONLY : PAGE_READWRITE;
    HANDLE hMapFile = CreateFileMapping(
        (HANDLE)_get_osfhandle(fd_),
        NULL,
        protect,
        (DWORD)(file_size_ >> 32),
        (DWORD)file_size_,
        NULL);
    
    if (hMapFile == NULL) {
        return false;
    }
    
    DWORD access = read_only_ ? FILE_MAP_READ : FILE_MAP_ALL_ACCESS;
    mapped_memory_ = MapViewOfFile(hMapFile, access, 0, 0, 0);
    CloseHandle(hMapFile);
    
    if (mapped_memory_ == NULL) {
        return false;
    }
#else
    // POSIX implementation using mmap
    int prot = PROT_READ;
    if (!read_only_) {
        prot |= PROT_WRITE;
    }
    
    mapped_memory_ = mmap(nullptr, file_size_, prot, MAP_SHARED, fd_, 0);
    if (mapped_memory_ == MAP_FAILED) {
        mapped_memory_ = nullptr;
        return false;
    }
#endif
    
    mapped_size_ = file_size_;
    return true;
}

void MMapFile::unmap_memory() {
    if (!mapped_memory_) {
        return;
    }
    
#ifdef _WIN32
    UnmapViewOfFile(mapped_memory_);
#else
    munmap(mapped_memory_, mapped_size_);
#endif
    
    mapped_memory_ = nullptr;
    mapped_size_ = 0;
}

void* MMapFile::getPointer(size_t offset) const {
    if (!mapped_memory_ || offset >= file_size_) {
        return nullptr;
    }
    return static_cast<char*>(mapped_memory_) + offset;
}

size_t MMapFile::allocate(size_t size) {
    if (read_only_) {
        // Can't allocate in read-only mode
        return 0;
    }
    
    // Align size to 8-byte boundary for better performance
    size = (size + 7) & ~7;
    
    // Check if we need to expand the file
    if (next_allocation_offset_ + size > file_size_) {
        // Calculate new size (grow by at least 50% or requested size)
        size_t growth = std::max(file_size_ / 2, size + 1024*1024);
        size_t new_size = file_size_ + growth;
        
        if (!expand(new_size)) {
            return 0;
        }
    }
    
    size_t result = next_allocation_offset_;
    next_allocation_offset_ += size;
    
    // Update header
    FileHeader* header = get_header();
    if (header && header->magic == FILE_MAGIC) {
        header->next_free_offset = next_allocation_offset_;
        sync();  // Ensure allocation state is persisted
    }
    
    return result;
}

bool MMapFile::expand(size_t new_size) {
    if (new_size <= file_size_) {
        return true;
    }
    
    // Unmap current memory
    unmap_memory();
    
    // Extend the file
#ifdef _WIN32
    if (_chsize_s(fd_, new_size) != 0) {
        // Try to remap at old size
        map_memory();
        return false;
    }
#else
    if (ftruncate(fd_, new_size) != 0) {
        // Try to remap at old size
        map_memory();
        return false;
    }
#endif
    
    file_size_ = new_size;
    
    // Remap with new size
    return map_memory();
}

void MMapFile::sync() {
    if (!mapped_memory_ || read_only_) {
        return;
    }
    
#ifdef _WIN32
    FlushViewOfFile(mapped_memory_, 0);
#else
    msync(mapped_memory_, mapped_size_, MS_SYNC);
#endif
}

bool MMapFile::mlock_region(size_t offset, size_t size) {
    if (!mapped_memory_ || offset + size > file_size_) {
        return false;
    }
    
#ifdef _WIN32
    // Windows: Use VirtualLock
    void* addr = static_cast<char*>(mapped_memory_) + offset;
    return VirtualLock(addr, size) != 0;
#else
    // POSIX: Use mlock
    void* addr = static_cast<char*>(mapped_memory_) + offset;
    return mlock(addr, size) == 0;
#endif
}

bool MMapFile::munlock_region(size_t offset, size_t size) {
    if (!mapped_memory_ || offset + size > file_size_) {
        return false;
    }
    
#ifdef _WIN32
    // Windows: Use VirtualUnlock
    void* addr = static_cast<char*>(mapped_memory_) + offset;
    return VirtualUnlock(addr, size) != 0;
#else
    // POSIX: Use munlock
    void* addr = static_cast<char*>(mapped_memory_) + offset;
    return munlock(addr, size) == 0;
#endif
}

size_t MMapFile::get_root_offset() const {
    const FileHeader* header = get_header();
    return header ? header->root_offset : 0;
}

void MMapFile::set_root_offset(size_t offset) {
    FileHeader* header = get_header();
    if (header && !read_only_) {
        header->root_offset = offset;
        sync();  // Ensure the change is persisted
    }
}

MMapFile::FileHeader* MMapFile::get_header() const {
    return mapped_memory_ ? static_cast<FileHeader*>(mapped_memory_) : nullptr;
}

} // namespace xtree