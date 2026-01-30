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

#ifdef __APPLE__
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace xtree { 
    namespace persist {

        static int open_flags(MapMode m) { 
            return (m==MapMode::ReadOnly) ? O_RDONLY : O_RDWR | O_CREAT; 
        }

        // TODO: Implement using mmap, fsync + F_FULLFSYNC, F_PREALLOCATE, F_RDADVISE
        FSResult PlatformFS::map_file(
            const std::string& path, 
            size_t offset, 
            size_t size,
            MapMode mode, 
            MappedRegion* out
        ) {
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
            int fd = (int)file_handle;
            int rc1 = ::fsync(fd); // Ensure data is flushed to disk
            int rc2 = ::fcntl(fd, F_FULLFSYNC); // Ensure full sync on macOS
            return { rc1 == 0 && rc2 == 0, rc2 == 0 ? 0 : errno };
        }
        
        FSResult PlatformFS::fsync_directory(const std::string& dir_path) {
            // Open directory for reading
            int fd = ::open(dir_path.c_str(), O_RDONLY);
            if (fd < 0) {
                return {false, errno};
            }
            
            // Use F_FULLFSYNC on macOS for directory
            int rc = ::fcntl(fd, F_FULLFSYNC);
            if (rc != 0) {
                // Fall back to regular fsync if F_FULLFSYNC fails on directory
                rc = ::fsync(fd);
            }
            
            int saved_errno = errno;
            ::close(fd);
            
            return {rc == 0, rc == 0 ? 0 : saved_errno};
        }

        FSResult PlatformFS::atomic_replace(const std::string& src, const std::string& dst) { 
            int rc = ::rename(src.c_str(), dst.c_str());
            if (rc != 0) {
                return {false, errno};
            }
            
            // fsync parent directory for durability
            std::string parent = dst.substr(0, dst.find_last_of('/'));
            if (parent.empty()) {
                parent = ".";
            }
            return fsync_directory(parent);
        }

        FSResult PlatformFS::preallocate(const std::string& path, size_t len) { 
            int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
            if (fd < 0) {
                return {false, errno};
            }
            
            // Use F_PREALLOCATE on macOS
            fstore_t fst{};
            fst.fst_flags = F_ALLOCATECONTIG;
            fst.fst_posmode = F_PEOFPOSMODE;
            fst.fst_offset = 0;
            fst.fst_length = (off_t)len;
            
            int rc = ::fcntl(fd, F_PREALLOCATE, &fst);
            if (rc != 0) {
                // Try non-contiguous allocation
                fst.fst_flags = F_ALLOCATEALL;
                rc = ::fcntl(fd, F_PREALLOCATE, &fst);
            }
            
            // Ensure file is actually extended to the requested size
            if (rc == 0) {
                rc = ::ftruncate(fd, (off_t)len);
            }
            
            int ec = (rc == 0) ? 0 : errno;
            ::close(fd);
            return {rc == 0, ec};
        }

        FSResult PlatformFS::advise_willneed(intptr_t fh, size_t off, size_t len) { 
            // posix_fadvise(POSIX_FADV_WILLNEED)
#ifdef POSIX_FADV_WILLNEED
            int rc = ::posix_fadvise((int)fh, off, len, POSIX_FADV_WILLNEED);
            return { rc == 0, rc};
#else
            return {true, 0};
#endif
        }

        FSResult PlatformFS::prefetch(void* addr, size_t len) { 
            // madvise(MADV_WILLNEED)
#ifdef MADV_WILLNEED
            int rc = ::madvise(addr, len, MADV_WILLNEED);
            return { rc == 0, rc == 0 ? 0 : errno };
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
            struct stat st;
            if (stat(path.c_str(), &st) == 0) {
                // Path exists, check if it's a directory
                if (S_ISDIR(st.st_mode)) {
                    return {true, 0};
                } else {
                    return {false, ENOTDIR};
                }
            }
            
            // Directory doesn't exist, create it (with parent directories)
            size_t pos = 0;
            do {
                pos = path.find('/', pos + 1);
                std::string subpath = path.substr(0, pos);
                if (!subpath.empty() && subpath != "/") {
                    if (mkdir(subpath.c_str(), 0755) != 0 && errno != EEXIST) {
                        return {false, errno};
                    }
                }
            } while (pos != std::string::npos);
            
            // Create the final directory
            if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
                return {false, errno};
            }
            
            return {true, 0};
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