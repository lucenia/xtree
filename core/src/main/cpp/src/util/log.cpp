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

#include "log.h"
#include "logmanager.h"

namespace xtree {

    ILogger iLogger;
    boost::mutex Logger::sm;
    FILE* Logger::logfile = nullptr;
    boost::thread_specific_ptr<Logger> Logger::tsp;
    
    // Global log level (defaults to WARNING for production performance)
    // Only WARNING, ERROR, and SEVERE messages are logged by default
    // Set LOG_LEVEL=INFO or LOG_LEVEL=DEBUG for more verbose output
    std::atomic<int> logLevel{LOG_WARNING};
    int tlogLevel = LOG_WARNING;
    const char * (*getcurns)() = []() -> const char* { return "xtree"; };
    
    // Static initializer to set log level from environment
    struct LogInitializer {
        LogInitializer() {
            initLoggingFromEnv();
        }
    };
    static LogInitializer log_init;

    const char* logLevelToString( LogLevel l ) {
        switch(l) {
        case LOG_TRACE:
            return "TRACE";
        case LOG_DEBUG:
            return "DEBUG";
        case LOG_INFO:
            return "INFO";
        case LOG_WARNING:
            return "WARN";
        case LOG_ERROR:
            return "ERROR";
        case LOG_SEVERE:
            return "SEVERE";
        default:
            return "UNKNOWN";
        }
    }

    void Logger::flush(Tee *t) {
        string msg = ss.str();
        string threadName = getThreadName();
        const char * type = logLevelToString(logLevel);

        ostringstream oss;
        oss << time_t_to_String();
        oss << " [" << getThreadName() << "] ";

        // Always show log level
        oss << "[" << type << "] ";
        
        for ( int i=0; i<indent; i++ )
            oss << '\t';

        oss << msg << '\n';  // Add newline for auto-flushed messages

        std::lock_guard<boost::mutex> lk(sm);
        string out = oss.str();

        if( t ) t->write(logLevel,out);

        if (logfile) {
            // Check if logfile is actually stderr
            if (logfile == stderr) {
                // It's stderr, just write to it
                fprintf(stderr, "%s", oss.str().c_str());
                fflush(stderr);
            } else {
                // Write ONLY to the log file when it's set
                if(fprintf(logfile, "%s", oss.str().c_str())>=0) {
                    fflush(logfile);
                }
                else {
                    int x = errno;
                    cerr << "Failed to write to logfile: " << errnoWithDescription(x) << ": " << out << endl;
                }
            }
        } else {
            // If no logfile is set, output to stderr
            fprintf(stderr, "%s", oss.str().c_str());
            fflush(stderr);
        }
        _init();
    }

    void Logger::setLogFile( FILE* f ) {
        std::lock_guard<boost::mutex> lk(sm);
        logfile = f;
    }
}
