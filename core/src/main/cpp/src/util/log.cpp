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
    FILE* Logger::logfile;
    thread_specific_ptr<Logger> Logger::tsp;

    const char* logLevelToString( LogLevel l ) {
        switch(l) {
        case DEBUG:
        case INFO:
            return "";
        case WARNING:
            return "warning";
        case ERROR:
            return "ERROR";
        case SEVERE:
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

        for ( int i=0; i<indent; i++ )
            oss << '\t';

        if ( type[0] ) {
            oss << type;
            oss << ": ";
        }

        oss << msg;

        boost::mutex::scoped_lock lk(sm);
        string out = oss.str();

        if( t ) t->write(logLevel,out);

        assert(logfile);
        if(fprintf(logfile, "%s", oss.str().c_str())>=0) {
            fflush(logfile);
        }
        else {
            int x = errno;
            cerr << "Failed to write to logfile: " << errnoWithDescription(x) << ": " << out << endl;
        }
        _init();
    }

    void Logger::setLogFile( FILE* f ) {
        boost::mutex::scoped_lock lk(sm);
        logfile = f;
    }
}
