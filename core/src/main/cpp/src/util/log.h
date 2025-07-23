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
#include <cctype>
#include <cstdlib>
#include <algorithm>
#include <type_traits>
#include <atomic>
#include <boost/filesystem/path.hpp>

namespace xtree {

    class LogManager;

    enum LogLevel {
        LOG_TRACE,    // Support engineering: detailed tracing
        LOG_DEBUG,    // Developer: debug information  
        LOG_INFO,     // Production: normal operation
        LOG_WARNING,  // Production: warning conditions
        LOG_ERROR,    // Production: error conditions
        LOG_SEVERE    // Production: fatal errors
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
            char buf[26];
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
        
        // Support for boost::filesystem::path
        template<typename PathType>
        typename std::enable_if<
            std::is_same<PathType, boost::filesystem::path>::value,
            Logger&
        >::type operator<<(const PathType& p) {
            ss << p.string();
            return *this;
        }

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

    extern std::atomic<int> logLevel;
    extern int tlogLevel;

    inline ILogger& out( int level = 0 ) {
        if ( level < logLevel.load(std::memory_order_relaxed) )  // Fixed: lower value = more verbose
            return iLogger;
        return Logger::get();
    }

    /* flush the log stream if the log level is
       at the specified level or higher. */
    inline void logflush(int level = 0) {
        if( level < logLevel.load(std::memory_order_relaxed) )  // Fixed: lower value = more verbose
            Logger::get().flush(0);
    }

    /* without prolog */
    inline ILogger& _log( int level = 0 ) {
        if ( level < logLevel.load(std::memory_order_relaxed) )  // Fixed: lower value = more verbose
            return iLogger;
        return Logger::get();
    }

    inline ILogger& log( int level ) {
        if ( level < logLevel.load(std::memory_order_relaxed) )  // Fixed: lower value = more verbose
            return iLogger;
        return Logger::get().prolog();
    }

    // Auto-flushing wrapper for log messages
    class LoggerWrapper {
        Logger* logger_;
        bool should_flush_;
    public:
        // Make constructor inline and trivial for optimization
        __attribute__((always_inline))
        LoggerWrapper(Logger* logger, bool should_flush) 
            : logger_(logger), should_flush_(should_flush) {}
        
        ~LoggerWrapper() {
            // Only do work if we have a logger (filtered messages have nullptr)
            if (should_flush_ && logger_) {
                logger_->flush(0);
            }
        }
        
        template<typename T>
        __attribute__((always_inline))
        LoggerWrapper& operator<<(const T& value) {
            // Short-circuit for filtered messages
            if (logger_) {
                (*logger_) << value;
            }
            return *this;
        }
        
        LoggerWrapper& operator<<(ostream& (*endl)(ostream&)) {
            if (logger_) {
                (*logger_) << endl;
                should_flush_ = false; // endl already flushes
            }
            return *this;
        }
    };
    
    inline LoggerWrapper log( LogLevel l ) {
        if ( l < logLevel.load(std::memory_order_relaxed) )  // LogLevel enum: lower value = more verbose
            return LoggerWrapper(nullptr, false);   // Return no-op wrapper
        Logger& logger = Logger::get().prolog().setLogLevel( l );
        return LoggerWrapper(&logger, true);  // Auto-flush on destruction
    }

    inline ILogger& log() {
        return Logger::get().prolog();
    }

    __attribute__((always_inline))
    inline LoggerWrapper error() {
        if (LOG_ERROR < logLevel.load(std::memory_order_relaxed))
            return LoggerWrapper(nullptr, false);
        return LoggerWrapper(&Logger::get().prolog().setLogLevel(LOG_ERROR), true);
    }

    __attribute__((always_inline))
    inline LoggerWrapper warn() {
        if (LOG_WARNING < logLevel.load(std::memory_order_relaxed))
            return LoggerWrapper(nullptr, false);
        return LoggerWrapper(&Logger::get().prolog().setLogLevel(LOG_WARNING), true);
    }

    __attribute__((always_inline))
    inline LoggerWrapper warning() {
        if (LOG_WARNING < logLevel.load(std::memory_order_relaxed))
            return LoggerWrapper(nullptr, false);
        return LoggerWrapper(&Logger::get().prolog().setLogLevel(LOG_WARNING), true);
    }

    extern const char * (*getcurns)();

    inline ILogger& problem( int level = 0 ) {
        if ( level < logLevel.load(std::memory_order_relaxed) )  // Fixed: lower value = more verbose
            return iLogger;
        Logger& l = Logger::get().prolog();
        l << ' ' << getcurns() << ' ';
        return l;
    }

    inline string errnoWithDescription(int x = errno) {
        stringstream s;
        s << "errno:" << x << ' ';
#ifdef _WIN32
        char buffer[256];
        strerror_s(buffer, sizeof(buffer), x);
        s << buffer;
#else
        s << strerror(x);
#endif

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

    // Helper functions for specific log levels
    // These return lightweight wrappers that compiler can optimize away
    __attribute__((always_inline))
    inline LoggerWrapper trace() {
        if (LOG_TRACE < logLevel.load(std::memory_order_relaxed))
            return LoggerWrapper(nullptr, false);
        return LoggerWrapper(&Logger::get().prolog().setLogLevel(LOG_TRACE), true);
    }
    
    __attribute__((always_inline))
    inline LoggerWrapper debug() {
        if (LOG_DEBUG < logLevel.load(std::memory_order_relaxed))
            return LoggerWrapper(nullptr, false);
        return LoggerWrapper(&Logger::get().prolog().setLogLevel(LOG_DEBUG), true);
    }
    
    __attribute__((always_inline))
    inline LoggerWrapper info() {
        if (LOG_INFO < logLevel.load(std::memory_order_relaxed))
            return LoggerWrapper(nullptr, false);
        return LoggerWrapper(&Logger::get().prolog().setLogLevel(LOG_INFO), true);
    }
    
    __attribute__((always_inline))
    inline LoggerWrapper severe() {
        if (LOG_SEVERE < logLevel.load(std::memory_order_relaxed))
            return LoggerWrapper(nullptr, false);
        return LoggerWrapper(&Logger::get().prolog().setLogLevel(LOG_SEVERE), true);
    }

    // Set log level from string (for configuration)
    inline bool setLogLevelFromString(const std::string& level) {
        std::string upper = level;
        for (auto& c : upper) c = std::toupper(c);
        
        if (upper == "TRACE")   { logLevel.store(LOG_TRACE, std::memory_order_relaxed); return true; }
        if (upper == "DEBUG")   { logLevel.store(LOG_DEBUG, std::memory_order_relaxed); return true; }
        if (upper == "INFO")    { logLevel.store(LOG_INFO, std::memory_order_relaxed); return true; }
        if (upper == "WARNING" || upper == "WARN") { logLevel.store(LOG_WARNING, std::memory_order_relaxed); return true; }
        if (upper == "ERROR")   { logLevel.store(LOG_ERROR, std::memory_order_relaxed); return true; }
        if (upper == "SEVERE" || upper == "FATAL") { logLevel.store(LOG_SEVERE, std::memory_order_relaxed); return true; }
        
        return false;
    }
    
    // Initialize logging from environment variable
    inline void initLoggingFromEnv() {
        const char* env_level = std::getenv("LOG_LEVEL");
        if (env_level) {
            if (!setLogLevelFromString(env_level)) {
                std::cerr << "Warning: Invalid LOG_LEVEL '" << env_level 
                         << "'. Valid levels: TRACE, DEBUG, INFO, WARNING, ERROR, SEVERE\n";
            }
        }
    }

}
