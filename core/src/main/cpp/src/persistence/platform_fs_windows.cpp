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
#ifdef _WIN32
#include <windows.h>

namespace xtree { 
    namespace persist {
        static DWORD access_mask(MapMode m) { 
            return (m==MapMode::ReadOnly)? GENERIC_READ : (GENERIC_READ|GENERIC_WRITE); 
        }

        // Implemented with CreateFile, CreateFileMapping, MapViewOfFile, FlushViewOfFile, FlushFileBuffers, MoveFileEx
        FSResult PlatformFS::map_file(
            const std::string& path, 
            size_t offset, 
            size_t size,
            MapMode mode, 
            MappedRegion* out
        ) {
            HANDLE fh = CreateFileA(
                path.c_str(), 
                access_mask(mode), 
                FILE_SHARE_READ|FILE_SHARE_WRITE, 
                NULL,
                (mode==MapMode::ReadOnly) ? OPEN_EXISTING : OPEN_ALWAYS,
                FILE_ATTRIBUTE_NORMAL, 
                NULL
            );

            if (fh==INVALID_HANDLE_VALUE) { 
                return { false, (int) GetLastError() };
            }

            HANDLE mh = CreateFileMappingA(
                fh, 
                NULL, 
                (mode == MapMode::ReadOnly) ? PAGE_READONLY : PAGE_READWRITE,
                0, 
                0, 
                NULL
            );

            if (!mh) { 
                DWORD e = GetLastError(); 
                CloseHandle(fh); 
                return { false, (int) e }; 
            }
            SIZE_T off_hi = (SIZE_T)(offset >> 32), off_lo = (SIZE_T)(offset & 0xFFFFFFFF);

            void* addr = MapViewOfFile(
                mh, 
                (mode == MapMode::ReadOnly) ? FILE_MAP_READ : FILE_MAP_WRITE,
                (DWORD) off_hi, 
                (DWORD) off_lo, 
                size
            );
            if (!addr) { 
                DWORD e=GetLastError(); 
                CloseHandle(mh); 
                CloseHandle(fh); 
                return { false, (int) e }; 
            }

            out->addr = addr; 
            out->size = size; 
            out->file_handle = (intptr_t) fh; 
            CloseHandle(mh);

            return { true, 0 };
        }
        
        FSResult PlatformFS::unmap(const MappedRegion& rgn) { 
            BOOL ok = UnmapViewOfFile(rgn.addr); 
            CloseHandle((HANDLE)rgn.file_handle); 
            return { ok, ok ? 0 : (int) GetLastError() }; 
        }
        
        FSResult PlatformFS::flush_view(const void* addr, size_t len) {
            BOOL ok = FlushViewOfFile(addr, len); 
            return { ok, ok ? 0 : (int) GetLastError() };
        }
        
        FSResult PlatformFS::flush_file(intptr_t fh) {
            BOOL ok = FlushFileBuffers((HANDLE)fh); 
            return { ok, ok ? 0 : (int) GetLastError() };
        }
        
        FSResult PlatformFS::fsync_directory(const std::string& dir_path) {
            // On Windows, directory metadata is automatically flushed
            // when files are created/renamed within it
            // We can open the directory and flush it for extra safety
            HANDLE hDir = CreateFileA(dir_path.c_str(),
                                      GENERIC_READ,
                                      FILE_SHARE_READ | FILE_SHARE_WRITE,
                                      NULL,
                                      OPEN_EXISTING,
                                      FILE_FLAG_BACKUP_SEMANTICS, // Required for directories
                                      NULL);
            if (hDir == INVALID_HANDLE_VALUE) {
                return {false, (int)GetLastError()};
            }
            
            BOOL ok = FlushFileBuffers(hDir);
            DWORD err = GetLastError();
            CloseHandle(hDir);
            
            return {ok, ok ? 0 : (int)err};
        }
        
        FSResult PlatformFS::atomic_replace(const std::string& src, const std::string& dst) {
            BOOL ok = MoveFileExA(src.c_str(), dst.c_str(), MOVEFILE_REPLACE_EXISTING|MOVEFILE_WRITE_THROUGH); 
            return { ok, ok ? 0 : (int) GetLastError() };
        }
        
        FSResult PlatformFS::preallocate(const std::string& path, size_t len) { 
            HANDLE h = CreateFileA(path.c_str(), 
                                   GENERIC_WRITE, 
                                   FILE_SHARE_READ | FILE_SHARE_WRITE,
                                   NULL, 
                                   OPEN_ALWAYS, 
                                   FILE_ATTRIBUTE_NORMAL, 
                                   NULL);
            if (h == INVALID_HANDLE_VALUE) {
                return {false, (int)GetLastError()};
            }
            
            LARGE_INTEGER li;
            li.QuadPart = (LONGLONG)len;
            BOOL ok = SetFilePointerEx(h, li, NULL, FILE_BEGIN) && 
                      SetEndOfFile(h) && 
                      FlushFileBuffers(h);
            
            DWORD err = GetLastError();
            CloseHandle(h);
            return {ok, ok ? 0 : (int)err};
        }
        
        FSResult PlatformFS::advise_willneed(intptr_t, size_t, size_t) { 
            return { true, 0 }; // Windows does not support posix_fadvise
        }

        FSResult PlatformFS::prefetch(void*, size_t) { 
            return { true, 0 }; // Windows does not support madvise
        }

        std::pair<FSResult, size_t> PlatformFS::file_size(const std::string& path) { 
            WIN32_FILE_ATTRIBUTE_DATA fad{}; 
            BOOL ok = GetFileAttributesExA(path.c_str(), GetFileExInfoStandard, &fad); 
            ULARGE_INTEGER s; 
            s.HighPart = fad.nFileSizeHigh; 
            s.LowPart = fad.nFileSizeLow; 
            return {{ok, ok ? 0 : (int) GetLastError()}, ok ? (size_t) s.QuadPart : 0};
        }

        FSResult PlatformFS::ensure_directory(const std::string& path) {
            // Check if path exists
            DWORD attr = GetFileAttributesA(path.c_str());
            if (attr != INVALID_FILE_ATTRIBUTES) {
                // Path exists, check if it's a directory
                if (attr & FILE_ATTRIBUTE_DIRECTORY) {
                    return {true, 0};
                } else {
                    return {false, ERROR_DIRECTORY};  
                }
            }
            
            // Directory doesn't exist, create it (with parent directories)
            // Windows needs backslash as separator for CreateDirectory
            std::string winpath = path;
            for (auto& c : winpath) {
                if (c == '/') c = '\\';
            }
            
            size_t pos = 0;
            do {
                pos = winpath.find('\\', pos + 1);
                std::string subpath = winpath.substr(0, pos);
                if (!subpath.empty() && subpath.find(':') != subpath.length() - 1) {
                    // Don't try to create drive letters like "C:"
                    if (!CreateDirectoryA(subpath.c_str(), NULL)) {
                        DWORD err = GetLastError();
                        if (err != ERROR_ALREADY_EXISTS) {
                            return {false, (int)err};
                        }
                    }
                }
            } while (pos != std::string::npos);
            
            // Create the final directory
            if (!CreateDirectoryA(winpath.c_str(), NULL)) {
                DWORD err = GetLastError();
                if (err != ERROR_ALREADY_EXISTS) {
                    return {false, (int)err};
                }
            }
            
            return {true, 0};
        }
        
        FSResult PlatformFS::truncate(const std::string& path, size_t size) {
            HANDLE hFile = CreateFileA(path.c_str(),
                                      GENERIC_WRITE,
                                      FILE_SHARE_READ,
                                      NULL,
                                      OPEN_EXISTING,
                                      FILE_ATTRIBUTE_NORMAL,
                                      NULL);
            if (hFile == INVALID_HANDLE_VALUE) {
                return {false, (int)GetLastError()};
            }
            
            LARGE_INTEGER liSize;
            liSize.QuadPart = size;
            
            BOOL result = SetFilePointerEx(hFile, liSize, NULL, FILE_BEGIN);
            if (result) {
                result = SetEndOfFile(hFile);
            }
            
            DWORD err = result ? 0 : GetLastError();
            CloseHandle(hFile);
            
            return {result != 0, (int)err};
        }
    }
} // namespace
#endif