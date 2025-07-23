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

#include "util.h"

#ifdef __GNUC__
#include <unistd.h>
#endif

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_host.h>
#endif

#ifdef WIN32
#include <windows.h>
#endif

namespace xtree {

#ifdef __GNUC__
    size_t getTotalSystemMemory() {
        long pages = sysconf(_SC_PHYS_PAGES);
        long page_size = sysconf(_SC_PAGE_SIZE);
        return pages * page_size;
    }

    size_t getAvailableSystemMemory() {
#ifdef __APPLE__
        // macOS doesn't have _SC_AVPHYS_PAGES, use vm_stat instead
        vm_size_t page_size;
        vm_statistics64_data_t vm_stat;
        mach_msg_type_number_t host_size = sizeof(vm_stat) / sizeof(natural_t);
        
        if (host_page_size(mach_host_self(), &page_size) != KERN_SUCCESS) {
            return 0;
        }
        
        if (host_statistics64(mach_host_self(), HOST_VM_INFO, 
                              (host_info64_t)&vm_stat, &host_size) != KERN_SUCCESS) {
            return 0;
        }
        
        // Calculate available memory = free + inactive
        return (vm_stat.free_count + vm_stat.inactive_count) * page_size;
#else
        // Linux and other POSIX systems
        long pages = sysconf(_SC_AVPHYS_PAGES);
        long page_size = sysconf(_SC_PAGE_SIZE);
        return pages * page_size;
#endif
    }
#endif

#ifdef WIN32
    size_t getTotalSystemMemory() {
        MEMORYSTATUSEX status;
        status.dwLength = sizeof(status);
        GlobalMemoryStatusEx(&status);
        return status.ullTotalPhys;
    }
    
    size_t getAvailableSystemMemory() {
        MEMORYSTATUSEX status;
        status.dwLength = sizeof(status);
        GlobalMemoryStatusEx(&status);
        return status.ullAvailPhys;
    }
#endif

    /* Returns the amount of microseconds elapsed since the UNIX epoch. Works on both
     * windows and linux. */
    unsigned long GetTimeMicro64() {
#ifdef _WIN32
        /* Windows */
        FILETIME ft;
        LARGE_INTEGER li;

        /* Get the current time as a FILETIME */
        GetSystemTimeAsFileTime(&ft);

        /* Convert FILETIME to LARGE_INTEGER for easier manipulation */
        li.LowPart = ft.dwLowDateTime;
        li.HighPart = ft.dwHighDateTime;

        /* FILETIME is in 100-nanosecond intervals since January 1, 1601 */
        /* Convert to microseconds since Unix epoch (January 1, 1970) */
        const unsigned long long EPOCH_DIFF = 11644473600000000ULL; /* Microseconds between 1601 and 1970 */
        unsigned long long microseconds = li.QuadPart / 10ULL - EPOCH_DIFF;

        return static_cast<unsigned long>(microseconds);
#else
        /* Linux and other POSIX systems */
        struct timeval tv;

        gettimeofday(&tv, NULL);

        unsigned long ret = tv.tv_usec;
        /* Convert from micro seconds (10^-6) to milliseconds (10^-3) */
        //ret /= 1000;

        /* Adds the seconds (10^0) after converting them to milliseconds (10^-3) */
        ret += (tv.tv_sec * 1000000);

        return ret;
#endif
    }

}
