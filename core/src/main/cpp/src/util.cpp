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

    /* Returns the amount of milliseconds elapsed since the UNIX epoch. Works on both
     * windows and linux. */
    unsigned long GetTimeMicro64() {
        /* Linux */
        struct timeval tv;

        gettimeofday(&tv, NULL);

        unsigned long ret = tv.tv_usec;
        /* Convert from micro seconds (10^-6) to milliseconds (10^-3) */
        //ret /= 1000;

        /* Adds the seconds (10^0) after converting them to milliseconds (10^-3) */
        ret += (tv.tv_sec * 1000000);

        return ret;
    }

}
