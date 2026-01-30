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

#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
#include <utility>

namespace xtree { 
    namespace persist {

        // Cross-platform file + mapping abstraction.
        struct MappedRegion {
        void*    addr = nullptr;
        size_t   size = 0;
        intptr_t file_handle = 0; // fd on POSIX, HANDLE on Windows (cast)
        };

        enum class MapMode { 
            ReadOnly, 
            ReadWrite 
        };

        struct OpenFlags {
            bool sequential = false;
            bool random_access = true;
        };

        struct FSResult { 
            bool ok; 
            int err; 
        };

        class PlatformFS {
        public:
            static FSResult map_file(const std::string& path, size_t offset, size_t size,
                                MapMode mode, MappedRegion* out);
            static FSResult unmap(const MappedRegion& rgn);
            static FSResult flush_view(const void* addr, size_t len);
            static FSResult flush_file(intptr_t file_handle);
            static FSResult fsync_directory(const std::string& dir_path);

            static FSResult atomic_replace(const std::string& tmp, const std::string& final);
            static FSResult preallocate(const std::string& path, size_t len);

            static FSResult advise_willneed(intptr_t file_handle, size_t offset, size_t len);
            static FSResult prefetch(void* addr, size_t len);

            static std::pair<FSResult, size_t> file_size(const std::string& path);
            static FSResult ensure_directory(const std::string& path);
            static FSResult truncate(const std::string& path, size_t size);
        };

    }
} // namespace xtree::persist