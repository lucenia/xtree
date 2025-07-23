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

/*
 * This file provides symbol definitions for test linking on Windows
 */
#include "../src/util/log.h"
#include "../src/memmgr/cow_memmgr.hpp"

namespace xtree {
    // Define the static members that are causing linking issues
    boost::thread_specific_ptr<Logger> Logger::tsp;
    
    // Explicitly initialize RUNTIME_PAGE_SIZE for test environment
    // This ensures it gets the correct value in test context
    const size_t PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE = 4096; // Direct value for Windows
    
    // Initialize logging for tests - only enable when _DEBUG is defined
    struct TestLoggerInitializer {
        TestLoggerInitializer() {
#ifdef _DEBUG
            // In debug mode, log to a file instead of stderr for cleaner test output
            FILE* logFile = fopen("xtree_test_debug.log", "w");
            if (logFile) {
                Logger::setLogFile(logFile);
            } else {
                // Fallback to stderr if file creation fails
                Logger::setLogFile(stderr);
            }
#else
            // In release mode, use a null file to discard log output
#ifdef _WIN32
            FILE* nullFile = fopen("NUL", "w");
#else
            FILE* nullFile = fopen("/dev/null", "w");
#endif
            if (nullFile) {
                Logger::setLogFile(nullFile);
            } else {
                // Ultimate fallback to stderr
                Logger::setLogFile(stderr);
            }
#endif
        }
    };
    static TestLoggerInitializer test_logger_init;
}