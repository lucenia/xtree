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

#include "log.h"
#ifndef _WIN32
#include <cxxabi.h>
#include <sys/file.h>
#endif

#//define BOOST_NO_CXX11_SCOPED_ENUMS
#include <boost/filesystem.hpp>
//#undef BOOST_NO_CXX11_SCOPED_ENUMS

namespace xtree {

    class LogManager {
    public:

        LogManager(string logpath="") : _enabled(0), _file(0) {
            cout << "all native output going to: ";
            string lp = "";
            if(!logpath.empty()) {
#ifdef _WIN32
            	lp = logpath + "\\xtree.log";
#else
            	lp = logpath + "/xtree.log";
#endif
            }
            else {
#ifdef _WIN32
            	lp = string(getenv("ACCUMULO_HOME")) + "\\logs\\xtree.log";
#else
            	lp = string(getenv("ACCUMULO_HOME")) + "/logs/xtree.log";
#endif
            }
            cout << lp << endl;

            initLogger(lp, true);
        }

        void initLogger( const string& lp, bool append ) {
           start(lp, append);
        }

        void time_t_to_Struct(time_t t, struct tm *buf, bool local=false) {
            if ( local )
                localtime_r(&t, buf);
            else
                gmtime_r(&t, buf);
        }

        string terseCurrentTime(bool colonsOk=true) {
            struct tm t;
            time_t_to_Struct( time(0), &t );

            const char* fmt = (colonsOk ? "%Y-%m-%dT%H:%M:%S" : "%Y-%m-%dT%H-%M-%S");
            char buf[32];
            assert(strftime(buf, sizeof(buf), fmt, &t) == 19);
            return buf;
        }

        void start( const string& lp, bool append) {
            _append = append;

            bool exists = boost::filesystem::exists(lp);

            FILE * test = fopen( lp.c_str() , _append ? "a" : "w" );
            if ( ! test ) {
                if (boost::filesystem::is_directory(lp)) {
                    cout << "logpath [" << lp << "] should be a file name not a directory" << endl;
                }
                else {
                    cout << "can't open [" << lp << "] for log file: " << errnoWithDescription() << endl;
                }
                assert( 0 );
            }

            if (append && exists){
                // two blank lines before and after
                const string msg = "\n\n***** SERVER RESTARTED *****\n\n\n";
                assert(fwrite(msg.data(), 1, msg.size(), test)>0);
            }

            fclose( test );

            _path = lp;
            _enabled = 1;
            rotate();
        }

        void rotate() {
            if( !_enabled ) {
                cerr << "LogManager not enabled" << endl;
                return;
            }

            if ( _file ) {

#ifdef POSIX_FADV_DONTNEED
                posix_fadvise(fileno(_file), 0, 0, POSIX_FADV_DONTNEED);
#endif

                // Rename the (open) existing log file to a timestamped name
                stringstream ss;
                ss << _path << "." << terseCurrentTime( false );
                string s = ss.str();
                cout << "renaming" << endl;
                rename( _path.c_str() , s.c_str() );
            }

            FILE* tmp = 0;  // The new file using the original logpath name

#if _WIN32
            // We rename an open log file (above, on next rotation) and the trick to getting Windows to do that is
            // to open the file with FILE_SHARE_DELETE.  So, we can't use the freopen() call that non-Windows
            // versions use because it would open the file without the FILE_SHARE_DELETE flag we need.
            //
            HANDLE newFileHandle = CreateFileA(
                    _path.c_str(),
                    GENERIC_WRITE,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    NULL,
                    OPEN_ALWAYS,
                    FILE_ATTRIBUTE_NORMAL,
                    NULL
            );
            if ( INVALID_HANDLE_VALUE != newFileHandle ) {
                int newFileDescriptor = _open_osfhandle( reinterpret_cast<intptr_t>(newFileHandle), _O_APPEND );
                tmp = _fdopen( newFileDescriptor, _append ? "a" : "w" );
            }
#else
            // this reopens the log file and redirects stdout to the logfile
            tmp = freopen(_path.c_str(), _append ? "a" : "w", stdout);
#endif
            if ( !tmp ) {
                cerr << "can't open: " << _path.c_str() << " for log file" << endl;
                assert( 0 );
            }

            // redirect stdout and stderr to log file
//            dup2( fileno( tmp ), 2 );   // stderr

            Logger::setLogFile(tmp); // after this point no thread will be using old file

#if _WIN32
            if ( _file )
                fclose( _file );  // In Windows, we still have the old file open, close it now
#endif

#if 0 // enable to test redirection
            cout << "written to cout" << endl;
            cerr << "written to cerr" << endl;
            log() << "written to log()" << endl;
#endif

            _file = tmp;    // Save new file for next rotation
        }

    private:
        bool _enabled;
        string _path;
        bool _append;
        FILE *_file;
        streambuf *logbuffer;

    };
}
