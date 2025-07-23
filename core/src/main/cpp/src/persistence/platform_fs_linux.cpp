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

#include "platform_fs.h"

#ifdef XTREE_LINUX
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <filesystem>
#include <cstring>

namespace xtree { 
    namespace persist {

        static int open_flags(MapMode m) { 
            return (m==MapMode::ReadOnly) ? O_RDONLY : O_RDWR | O_CREAT; 
        }

        FSResult PlatformFS::map_file(const std::string& path, size_t offset, size_t size,
                                                MapMode mode, MappedRegion* out) {
            // open file:
            int fd = ::open(path.c_str(), open_flags(mode), 0644);
            if (fd < 0) { 
                return {false, errno};
            }

            // mmap with PROT_READ/PROT_WRITE based on mode:
            int prot = (mode == MapMode::ReadOnly) ? PROT_READ : (PROT_READ|PROT_WRITE);
            int flags = MAP_SHARED;
            void* addr = ::mmap(nullptr, size, prot, flags, fd, off_t(offset));

            if (addr == MAP_FAILED) { 
                int e = errno; 
                ::close(fd); 
                return {false, e}; 
            }

            // store fd in file_handle
            out->addr = addr; 
            out->size = size;
            out->file_handle = fd; 
            
            return {true, 0};
        }

        FSResult PlatformFS::unmap(const MappedRegion& r) {
            // munmap, close fd if owned
            int rc = ::munmap(r.addr, r.size);
            int ec = (rc == 0) ? 0 : errno;  // Only read errno if munmap failed
            ::close((int)r.file_handle);

            return { rc == 0, ec };
        }

        FSResult PlatformFS::flush_view(const void* addr, size_t len) {
            // msync(MS_SYNC)
            int rc = ::msync(const_cast<void*>(addr), len, MS_SYNC);
            return { rc == 0, rc == 0 ? 0 : errno };
        }

        FSResult PlatformFS::flush_file(intptr_t file_handle) {
            // fdatasync((int)file_handle)
            int rc = ::fdatasync((int)file_handle);
            return { rc == 0, rc == 0 ? 0 : errno };
        }
        
        FSResult PlatformFS::fsync_directory(const std::string& dir_path) {
            // Open directory for reading
            int fd = ::open(dir_path.c_str(), O_RDONLY);
            if (fd < 0) {
                return {false, errno};
            }
            
            // Fsync the directory to ensure metadata changes are persisted
            int rc = ::fsync(fd);
            int saved_errno = errno;
            ::close(fd);
            
            return {rc == 0, rc == 0 ? 0 : saved_errno};
        }

        FSResult PlatformFS::atomic_replace(const std::string& src, const std::string& dst) {
            // Use atomic rename with directory fsync for durability
            int rc = ::rename(src.c_str(), dst.c_str());
            if (rc != 0) {
                return {false, errno};
            }
            
            // Get parent directory and fsync it
            std::filesystem::path dst_path(dst);
            std::string parent_dir = dst_path.parent_path().string();
            if (parent_dir.empty()) {
                parent_dir = ".";
            }
            
            return fsync_directory(parent_dir);
        }

        FSResult PlatformFS::preallocate(const std::string& path, size_t len) {
            int fd = ::open(path.c_str(), O_RDWR|O_CREAT, 0644);
            if (fd < 0) { 
                return {false, errno};
            }
            
#ifdef __linux__
            // Try fallocate first
            int rc = ::fallocate(fd, 0, 0, (off_t)len);
            if (rc == 0) {
                ::close(fd);
                return {true, 0};
            }
#endif
            
            // Fallback: use posix_fallocate
            int rc = ::posix_fallocate(fd, 0, (off_t)len);
            if (rc != 0) {
                // posix_fallocate returns error code directly, not via errno
                // If it fails, try ftruncate as last resort
                int truncate_rc = ::ftruncate(fd, (off_t)len);
                if (truncate_rc != 0) {
                    int ec = errno;  // ftruncate sets errno
                    ::close(fd);
                    return {false, ec};
                }
                ::close(fd);
                return {true, 0};
            }
            
            ::close(fd);
            return {true, 0};
        }

        FSResult PlatformFS::advise_willneed(intptr_t fh, size_t off, size_t len) {
            // posix_fadvise(POSIX_FADV_WILLNEED)
#ifdef POSIX_FADV_WILLNEED
            int rc = ::posix_fadvise((int)fh, off, len, POSIX_FADV_WILLNEED);
            return {rc==0, rc};
#else
            return {true,0};
#endif
        }

        FSResult PlatformFS::prefetch(void* addr, size_t len) {
            // madvise(MADV_WILLNEED)
#ifdef MADV_WILLNEED
            int rc = ::madvise(addr, len, MADV_WILLNEED);
            return { rc==0, rc == 0 ? 0 : errno };
#else
            return {true,0};
#endif
        }

        std::pair<FSResult, size_t> PlatformFS::file_size(const std::string& path) {
            // stat
            struct stat st{}; 
            int rc = ::stat(path.c_str(), &st);
            return { { rc == 0, rc == 0 ? 0 : errno }, rc == 0 ? (size_t)st.st_size : 0 };
        }

        FSResult PlatformFS::ensure_directory(const std::string& path) {
            try {
                std::filesystem::create_directories(path);
                return {true, 0};
            } catch (const std::filesystem::filesystem_error& e) {
                // Extract error code from exception
                int err_code = e.code().value();
                if (err_code == 0 && std::filesystem::is_directory(path)) {
                    // Directory already exists
                    return {true, 0};
                }
                return {false, err_code};
            } catch (...) {
                return {false, EIO};
            }
        }

        FSResult PlatformFS::truncate(const std::string& path, size_t size) {
            if (::truncate(path.c_str(), off_t(size)) == 0) {
                return {true, 0};
            }
            return {false, errno};
        }

    } // namespace persist
} // namespace xtree
#endif