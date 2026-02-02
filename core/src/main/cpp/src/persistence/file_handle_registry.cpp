/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "file_handle_registry.h"
#include "platform_fs.h"
#include "util/logmanager.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdexcept>
#include <iostream>
#include <limits.h>
#include <stdlib.h>
#include <string_view>
#ifndef _WIN32
#include <sys/resource.h>
#endif

namespace xtree {
namespace persist {

FileHandleRegistry& FileHandleRegistry::global() {
    // Meyers' singleton - thread-safe lazy initialization (C++11)
    static FileHandleRegistry* instance = []() {
        // Default to 512 open files for the global registry
        auto* fhr = new FileHandleRegistry(512);
        return fhr;
    }();
    return *instance;
}

void FileHandle::close() {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

FileHandleRegistry::FileHandleRegistry(size_t max_open_files)
    : max_open_files_(max_open_files) {
    // Sanity check - leave some FDs for other uses
    if (max_open_files_ < 4) {
        max_open_files_ = 4;
    }
    
    // Check system limit
#ifndef _WIN32
    struct rlimit rlim;
    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        // Leave headroom for other FDs (stdin/stdout/stderr, sockets, etc)
        size_t safe_limit = rlim.rlim_cur > 64 ? rlim.rlim_cur - 64 : rlim.rlim_cur / 2;
        if (max_open_files_ > safe_limit) {
            max_open_files_ = safe_limit;
            info() << "[FileHandleRegistry] Capped max_open_files to " 
                      << max_open_files_ << " based on system limit";
        }
    }
#endif
}

FileHandleRegistry::~FileHandleRegistry() {
    std::lock_guard<std::mutex> lock(mu_);
    
    // Close all file handles
    for (auto& [path, fh] : table_) {
        if (fh && fh->fd >= 0) {
            fh->close();
        }
    }
    table_.clear();
}

std::shared_ptr<FileHandle> FileHandleRegistry::acquire(
    const std::string& path, bool writable, bool create) {
    
    // Canonicalize the path first (before taking lock)
    std::string canonical = canonicalize_path(path);
    
    std::lock_guard<std::mutex> lock(mu_);
    
    // Check if already open
    auto it = table_.find(canonical);
    if (it != table_.end() && it->second) {
        auto fh = it->second;
        
        // Check if we need to upgrade to writable
        if (writable && !fh->writable) {
            // Need to reopen as writable
            ::close(fh->fd);
            int flags = O_RDWR | (create ? O_CREAT : 0);
#ifdef O_CLOEXEC
            flags |= O_CLOEXEC;
#endif
            fh->fd = ::open(canonical.c_str(), flags, 0644);
            if (fh->fd < 0) {
                throw std::runtime_error("Failed to reopen file as writable: " + canonical);
            }
            fh->writable = true;
        }
        
        // Update LRU timestamp and pin count
        fh->update_last_use();
        fh->pins++;
        
        return fh;
    }
    
    // Need to open - check if we need to evict first
    evict_if_needed();
    
    // Open the file (using canonical path)
    auto fh = open_or_grow(canonical, writable, create);
    table_[canonical] = fh;
    total_opens_++;
    
    return fh;
}

void FileHandleRegistry::release(const std::shared_ptr<FileHandle>& fh) {
    if (!fh) return;
    
    std::lock_guard<std::mutex> lock(mu_);
    
    if (fh->pins > 0) {
        fh->pins--;
    }
    
    // Could trigger eviction here if needed, but typically we wait
    // until acquire() needs space
}

size_t FileHandleRegistry::open_file_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    
    size_t count = 0;
    for (const auto& [path, fh] : table_) {
        if (fh && fh->fd >= 0) {
            count++;
        }
    }
    return count;
}

size_t FileHandleRegistry::debug_open_file_count() const {
    return open_file_count();
}

size_t FileHandleRegistry::debug_open_file_count_for_path(const std::string& path) const {
    // Canonicalize the path for consistent lookups
    std::string canonical = canonicalize_path(path);
    
    std::lock_guard<std::mutex> lock(mu_);
    
    auto it = table_.find(canonical);
    if (it != table_.end() && it->second && it->second->fd >= 0) {
        return 1;
    }
    return 0;
}

void FileHandleRegistry::debug_evict_all_unpinned() {
    std::lock_guard<std::mutex> lock(mu_);
    
    std::vector<std::string> to_evict;
    for (const auto& [path, fh] : table_) {
        if (fh && fh->pins == 0 && fh->fd >= 0) {
            to_evict.push_back(path);
        }
    }
    
    for (const auto& path : to_evict) {
        auto it = table_.find(path);
        if (it != table_.end() && it->second) {
            it->second->close();
            table_.erase(it);
            total_evictions_++;
        }
    }
}

void FileHandleRegistry::evict_if_needed() {
    // Count open files
    size_t open_count = 0;
    for (const auto& [path, fh] : table_) {
        if (fh && fh->fd >= 0) {
            open_count++;
        }
    }
    
    // Need to evict?
    if (open_count >= max_open_files_) {
        size_t to_evict = (open_count - max_open_files_) + 1;  // Free at least one
        auto candidates = find_eviction_candidates(to_evict);
        
        for (const auto& path : candidates) {
            auto it = table_.find(path);
            if (it != table_.end() && it->second) {
                it->second->close();
                table_.erase(it);
                total_evictions_++;
            }
        }
        
        // If everything is pinned, allow going over cap temporarily.
        // (OS limit was pre-capped in the ctor; we won't exceed that.)
    }
}

std::vector<std::string> FileHandleRegistry::find_eviction_candidates(size_t count) {
    struct Candidate {
        std::string path;
        uint64_t last_use_ns;
    };
    
    std::vector<Candidate> candidates;
    
    // Find all unpinned files
    for (const auto& [path, fh] : table_) {
        if (fh && fh->fd >= 0 && fh->pins == 0) {
            candidates.push_back({path, fh->last_use_ns});
        }
    }
    
    // Sort by LRU (oldest first)
    std::sort(candidates.begin(), candidates.end(),
              [](const Candidate& a, const Candidate& b) {
                  return a.last_use_ns < b.last_use_ns;
              });
    
    // Take up to 'count' candidates
    std::vector<std::string> result;
    for (size_t i = 0; i < std::min(count, candidates.size()); i++) {
        result.push_back(candidates[i].path);
    }
    
    return result;
}

std::shared_ptr<FileHandle> FileHandleRegistry::open_or_grow(
    const std::string& path, bool writable, bool create) {
    
    // Open with O_CLOEXEC to prevent FD leaks to child processes
    int flags = writable ? O_RDWR : O_RDONLY;
    if (create) flags |= O_CREAT;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    
    int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to open file " + path + ": " + strerror(errno));
    }
    
    // Get current size
    struct stat st;
    if (fstat(fd, &st) != 0) {
        ::close(fd);
        throw std::runtime_error("Failed to stat file " + path + ": " + strerror(errno));
    }
    
    size_t current_size = st.st_size;
    
    auto fh = std::make_shared<FileHandle>(fd, path, current_size, writable);
    fh->pins = 1;  // Caller is pinning it
    return fh;
}

void FileHandleRegistry::pin(const std::shared_ptr<FileHandle>& fh) {
    if (!fh) return;
    
    std::lock_guard<std::mutex> lock(mu_);
    fh->pins++;
    fh->update_last_use();  // Update LRU on pin
}

void FileHandleRegistry::unpin(const std::shared_ptr<FileHandle>& fh) {
    if (!fh) return;
    
    std::lock_guard<std::mutex> lock(mu_);
    if (fh->pins > 0) {
        fh->pins--;
    }
}

bool FileHandleRegistry::ensure_size(const std::shared_ptr<FileHandle>& fh, size_t min_size) {
    if (!fh) return false;
    
    std::lock_guard<std::mutex> lock(mu_);
    
    if (min_size <= fh->size_bytes) {
        return false;
    }
    
#if defined(__linux__)
    // Preallocate without forcing a metadata sync right now
    int rc = posix_fallocate(fh->fd, 0, static_cast<off_t>(min_size));
    if (rc != 0) {
        throw std::runtime_error("posix_fallocate failed for " + fh->path + ": " + std::to_string(rc));
    }
#else
    if (ftruncate(fh->fd, static_cast<off_t>(min_size)) != 0) {
        throw std::runtime_error("Failed to grow file " + fh->path + 
                               " to size " + std::to_string(min_size) +
                               ": " + strerror(errno));
    }
    // Note: No fsync here - durability now handled by msync in MappingExtent::unmap()
#endif
    
    fh->size_bytes = min_size;
    fh->update_last_use();
    return true;
}

void FileHandleRegistry::ensure_writable(const std::shared_ptr<FileHandle>& fh, bool create) {
    if (!fh || fh->writable) return;
    
    std::lock_guard<std::mutex> lock(mu_);
    if (fh->writable) return;  // double-check under lock
    
    // Close the read-only FD
    ::close(fh->fd);
    
    // Reopen as writable
    int flags = O_RDWR | (create ? O_CREAT : 0);
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
    
    int fd = ::open(fh->path.c_str(), flags, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to reopen file as writable: " + fh->path + ": " + strerror(errno));
    }
    
    fh->fd = fd;
    fh->writable = true;
    fh->update_last_use();
}

std::string FileHandleRegistry::canonicalize_path(const std::string& path) const {
    if (path.empty()) return path;
    if (path == "/") return "/";

    // ---- NEW: collapse odd-but-legal ".//<absolute>" prefixes to "<absolute>" ----
    // Consume every leading "./" *only* when it is immediately followed by '/'.
    // Examples:
    //   ".//tmp/a"      -> "/tmp/a"
    //   "./././/tmp/a" -> "/tmp/a"
    //   "./foo"         (relative) is left alone.
    size_t i = 0;
    while (i + 2 < path.size() && path[i] == '.' && path[i+1] == '/' && path[i+2] == '/') {
        i += 2; // drop one "./" pair; the next char is still '/'
    }
    std::string_view pfx = (i > 0) ? std::string_view(path).substr(i) : std::string_view(path);
    // -----------------------------------------------------------------------------

    char resolved[PATH_MAX];

    // 1) Fast path: if the full path exists, realpath() gives canonical form.
    if (realpath(std::string(pfx).c_str(), resolved)) {
        return std::string(resolved);
    }

    // 2) Build absolute path without requiring the leaf to exist (CWD cached per-thread).
    static thread_local std::string tl_cwd = []{
        char buf[PATH_MAX];
        return getcwd(buf, sizeof(buf)) ? std::string(buf) : std::string("/");
    }();

    std::string abs;
    abs.reserve(pfx.size() + tl_cwd.size() + 2);
    if (!pfx.empty() && pfx.front() == '/') abs.assign(pfx);
    else { abs = tl_cwd; abs.push_back('/'); abs.append(pfx); }

    // 3) Single-pass lexical normalization (collapses //, ., ..).
    auto normalize = [](std::string_view p) {
        std::vector<std::string> parts;
        parts.reserve(16);
        const bool absolute = !p.empty() && p.front() == '/';

        size_t i = 0, n = p.size();
        while (i < n) {
            while (i < n && p[i] == '/') ++i;     // skip slashes
            size_t j = i;
            while (j < n && p[j] != '/') ++j;     // read token

            if (j > i) {
                std::string_view tok = p.substr(i, j - i);
                if (tok == ".") {
                    // skip
                } else if (tok == "..") {
                    if (!parts.empty() && parts.back() != "..") parts.pop_back();
                    else if (!absolute) parts.emplace_back("..");
                } else {
                    parts.emplace_back(tok);
                }
            }
            i = j;
        }

        std::string out;
        if (absolute) out.push_back('/');
        for (size_t k = 0; k < parts.size(); ++k) {
            if (k) out.push_back('/');
            out += parts[k];
        }
        if (out.empty()) out = absolute ? "/" : ".";
        return out;
    };

    // Strip trailing slashes (except root) so base isn't empty.
    while (abs.size() > 1 && abs.back() == '/') abs.pop_back();

    // Split into (dir, base).
    const size_t slash = abs.find_last_of('/');
    const std::string dir_raw = (slash == std::string::npos) ? "." : abs.substr(0, slash);
    const std::string base    = (slash == std::string::npos) ? abs : abs.substr(slash + 1);

    // Normalize dir, then try realpath(dir).
    const std::string dir_norm = normalize(dir_raw);

    if (realpath(dir_norm.c_str(), resolved)) {
        std::string candidate(resolved);
        if (!candidate.empty() && candidate.back() != '/') candidate.push_back('/');
        candidate += base;

        // If the leaf exists, unify with the fast path.
        if (realpath(candidate.c_str(), resolved)) {
            return std::string(resolved);
        }
        return candidate; // canonical parent + base (leaf may not exist yet)
    }

    // Parent doesn't exist; fully lexical normalize so variants coalesce.
    return normalize(abs);
}

} // namespace persist
} // namespace xtree