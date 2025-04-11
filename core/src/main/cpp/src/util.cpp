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

namespace xtree {

#ifdef __GNUC__
#include <unistd.h>
    size_t getTotalSystemMemory() {
        long pages = sysconf(_SC_PHYS_PAGES);
        long page_size = sysconf(_SC_PAGE_SIZE);
        return pages * page_size;
    }

    size_t getAvailableSystemMemory() {
        long pages = sysconf(_SC_AVPHYS_PAGES);
        long page_size = sysconf(_SC_PAGE_SIZE);
        return pages * page_size;
    }
#endif

#ifdef WIN32
#include <windows.h>
    size_t getTotalSystemMemory() {
        MEMORYSTATUSEX status;
        status.dwLength = sizeof(status);
        GlobalMemoryStatusEx(&status);
        return status.ullTotalPhys;
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
