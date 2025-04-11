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
