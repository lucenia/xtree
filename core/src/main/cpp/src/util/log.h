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

#include "../pch.h"

namespace xtree {

    class LogManager;

    enum LogLevel {
        LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERROR, LOG_SEVERE
    };

    class Tee {
    public:
        virtual ~Tee() {}
        virtual void write(LogLevel level, const string& str) = 0;
    };

    class ILogger {
    public:
        virtual ILogger& operator<<(Tee* tee) {
            return *this;
        }
        virtual ~ILogger() {};

        virtual ILogger& operator<<(const char*) {
            return *this;
        }
        virtual ILogger& operator<<(const string&) {
            return *this;
        }
        virtual ILogger& operator<<(char *) {
            return *this;
        }
        virtual ILogger& operator<<(char) {
            return *this;
        }
        virtual ILogger& operator<<(int) {
            return *this;
        }
        virtual ILogger& operator<<(unsigned long) {
            return *this;
        }
        virtual ILogger& operator<<(long) {
            return *this;
        }
        virtual ILogger& operator<<(unsigned) {
            return *this;
        }
        virtual ILogger& operator<<(unsigned short) {
            return *this;
        }
        virtual ILogger& operator<<(double) {
            return *this;
        }
        virtual ILogger& operator<<(void *) {
            return *this;
        }
        virtual ILogger& operator<<(const void *) {
            return *this;
        }
        virtual ILogger& operator<<(long long) {
            return *this;
        }
        virtual ILogger& operator<<(unsigned long long) {
            return *this;
        }
        virtual ILogger& operator<<(bool) {
            return *this;
        }
        template< class T >
        ILogger& operator<<(T *t) {
            return operator<<( static_cast<void*>(t) );
        }
        template< class T >
        ILogger& operator<<(const T *t) {
            return operator<<( static_cast<const void*>(t) );
        }
        template< class T >
        ILogger& operator<<(const boost::shared_ptr<T> p) {
            T *t = p.get();
            if(!t)
                *this << "null";
            else
                *this << *t;
            return *this;
        }
        virtual ILogger& operator<< (ostream& ( *endl )(ostream&)) {
            return *this;
        }
        virtual ILogger& operator<< (ios_base& (*hex)(ios_base&)) {
            return *this;
        }
        virtual void flush(Tee *t=0) {}

        /**
         * converts time_t to a string
         */
        inline string time_t_to_String(time_t t = time(0), bool local=false) {
            char buf[25];
#if defined(_WIN32)
            ctime_s(buf, sizeof(buf), &t);
#else
            ctime_r(&t, buf);
#endif
            buf[24] = 0; // don't want the \n
            return buf;
        }

    };
    extern ILogger iLogger;

    class Logger : public ILogger {
        static boost::mutex sm;
        stringstream ss;
        int indent;
        LogLevel logLevel;
        static FILE* logfile;
        static ofstream lfstream;
        static scoped_ptr<ostream> stream;
        string _threadName;
    public:

        friend class LogManager;
        /**
         * set the log file
         */
        static void setLogFile(FILE* f);

        void flush(Tee *t=0);

        inline string getThreadName() { return _threadName; }

        /**
         * set the log level
         */
        inline Logger& setLogLevel(LogLevel l) {
            logLevel = l;
            return *this;
        }

        Logger& operator<<(const char *x) { ss << x; return *this; }
        Logger& operator<<(const string& x) { ss << x; return *this; }
        Logger& operator<<(char *x)       { ss << x; return *this; }
        Logger& operator<<(char x)        { ss << x; return *this; }
        Logger& operator<<(int x)         { ss << x; return *this; }
        Logger& operator<<(long x)          { ss << x; return *this; }
        Logger& operator<<(unsigned long x) { ss << x; return *this; }
        Logger& operator<<(unsigned x)      { ss << x; return *this; }
        Logger& operator<<(unsigned short x){ ss << x; return *this; }
        Logger& operator<<(double x)        { ss << x; return *this; }
        Logger& operator<<(void *x)         { ss << x; return *this; }
        Logger& operator<<(const void *x)   { ss << x; return *this; }
        Logger& operator<<(long long x)     { ss << x; return *this; }
        Logger& operator<<(unsigned long long x) { ss << x; return *this; }
        Logger& operator<<(bool x)               { ss << x; return *this; }

        Logger& operator<<(Tee* tee) {
            ss << '\n';
            flush(tee);
            return *this;
        }

        Logger& operator<< (ostream& ( *_endl )(ostream&)) {
            ss << '\n';
            flush(0);
            return *this;
        }
        Logger& operator<< (ios_base& (*_hex)(ios_base&)) {
            ss << _hex;
            return *this;
        }

        Logger& prolog() {
            return *this;
        }

        void indentInc(){ indent++; }
        void indentDec(){ indent--; }
        int getIndent() const { return indent; }

    private:
        static boost::thread_specific_ptr<Logger> tsp;
        Logger() {
            indent = 0;
            _init();
        }
        void _init() {
            ss.str("");
            logLevel = LOG_INFO;
            _threadName = "XTREE_NATIVE";
        }
    public:
        static Logger& get() {
            Logger *p = tsp.get();
            if( p == 0 )
                tsp.reset( p = new Logger() );
            return *p;
        }
    };

    extern int logLevel;
    extern int tlogLevel;

    inline ILogger& out( int level = 0 ) {
        if ( level > logLevel )
            return iLogger;
        return Logger::get();
    }

    /* flush the log stream if the log level is
       at the specified level or higher. */
    inline void logflush(int level = 0) {
        if( level > logLevel )
            Logger::get().flush(0);
    }

    /* without prolog */
    inline ILogger& _log( int level = 0 ) {
        if ( level > logLevel )
            return iLogger;
        return Logger::get();
    }

    inline ILogger& log( int level ) {
        if ( level > logLevel )
            return iLogger;
        return Logger::get().prolog();
    }

    inline ILogger& log( LogLevel l ) {
        return Logger::get().prolog().setLogLevel( l );
    }

    inline ILogger& log() {
        return Logger::get().prolog();
    }

    inline ILogger& error() {
        return log( LOG_ERROR );
    }

    inline ILogger& warning() {
        return log( LOG_WARNING );
    }

    extern const char * (*getcurns)();

    inline ILogger& problem( int level = 0 ) {
        if ( level > logLevel )
            return iLogger;
        Logger& l = Logger::get().prolog();
        l << ' ' << getcurns() << ' ';
        return l;
    }

    inline string errnoWithDescription(int x = errno) {
        stringstream s;
        s << "errno:" << x << ' ';
        s << strerror(x);

        return s.str();
    }

    struct LogIndentLevel {
        LogIndentLevel() {
            Logger::get().indentInc();
        }
        ~LogIndentLevel() {
            Logger::get().indentDec();
        }
    };

}
