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
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <memory>
#include <algorithm>
#ifndef _WIN32
#include <cxxabi.h>
#include <sys/file.h>
#else
#ifndef NOMINMAX
#define NOMINMAX  // Prevent Windows from defining min/max macros
#endif
#include <windows.h>
#include <io.h>
#include <fcntl.h>
#include <cstdio>  // for rename
#include <cstdlib> // for getenv
#endif

#include <cstdio>   // for rename on all platforms
#include <cstdlib>  // for getenv on all platforms

#//define BOOST_NO_CXX11_SCOPED_ENUMS
#include <boost/filesystem.hpp>
//#undef BOOST_NO_CXX11_SCOPED_ENUMS

namespace xtree {

    class LogManager {
    public:
        struct RotationConfig {
            size_t max_file_size;
            size_t max_files;
            std::chrono::hours max_age;
            bool enable_auto_rotation;
            
            RotationConfig() 
                : max_file_size(100 * 1024 * 1024)  // 100MB default
                , max_files(10)                      // Keep 10 old log files
                , max_age(24)                        // Rotate daily
                , enable_auto_rotation(true)         // Auto-rotate based on size/age
            {}
        };

        LogManager(string logpath="", const RotationConfig& config = RotationConfig()) 
            : _enabled(false), _file(nullptr), _rotation_config(config), _running(false) {
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
                // Priority: XTREE_LOG_DIR > XTREE_HOME/logs > ACCUMULO_HOME/logs > ./logs
                const char* logDir = getenv("XTREE_LOG_DIR");
                if (logDir) {
#ifdef _WIN32
                    lp = string(logDir) + "\\xtree.log";
#else
                    lp = string(logDir) + "/xtree.log";
#endif
                } else {
                    const char* xtreeHome = getenv("XTREE_HOME");
                    const char* accumuloHome = getenv("ACCUMULO_HOME");
                    const char* baseDir = xtreeHome ? xtreeHome : (accumuloHome ? accumuloHome : ".");
#ifdef _WIN32
                    lp = string(baseDir) + "\\logs\\xtree.log";
#else
                    lp = string(baseDir) + "/logs/xtree.log";
#endif
                }
            }
            cout << lp << endl;

            initLogger(lp, true);
        }

        void initLogger( const string& lp, bool append ) {
           start(lp, append);
        }

        void time_t_to_Struct(time_t t, struct tm *buf, bool local=false) {
#ifdef _WIN32
            if ( local )
                localtime_s(buf, &t);
            else
                gmtime_s(buf, &t);
#else
            if ( local )
                localtime_r(&t, buf);
            else
                gmtime_r(&t, buf);
#endif
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

#ifdef _WIN32
            FILE * test = nullptr;
            fopen_s(&test, lp.c_str(), _append ? "a" : "w");
#else
            FILE * test = fopen( lp.c_str() , _append ? "a" : "w" );
#endif
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
            
            // Start auto-rotation thread if enabled
            startAutoRotation();
        }

        void rotate() {
            if( !_enabled ) {
                cerr << "LogManager not enabled" << endl;
                return;
            }

            // If there's an existing log file (either open or on disk), rotate it
            if ( _file || boost::filesystem::exists(_path) ) {

#if defined(POSIX_FADV_DONTNEED) && !defined(_WIN32)
                if (_file) {
                    posix_fadvise(fileno(_file), 0, 0, POSIX_FADV_DONTNEED);
                }
#endif

                // Rename the existing log file to a timestamped name
                stringstream ss;
                ss << _path << "." << terseCurrentTime( false );
                string s = ss.str();
                
                // Only rename if the file exists on disk
                if (boost::filesystem::exists(_path)) {
                    cout << "renaming" << endl;
#ifdef _WIN32
                    // On Windows, rename fails if target exists
                    std::remove(s.c_str());  // Delete target if it exists
                    std::rename(_path.c_str(), s.c_str());
#else
                    rename(_path.c_str(), s.c_str());
#endif
                }
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
            // Open the log file without redirecting stdout (tests need stdout)
            tmp = fopen(_path.c_str(), _append ? "a" : "w");
#endif
            if ( !tmp ) {
                cerr << "can't open: " << _path.c_str() << " for log file" << endl;
                assert( 0 );
            }

            // redirect stdout and stderr to log file
//            dup2( fileno( tmp ), 2 );   // stderr

            Logger::setLogFile(tmp); // after this point no thread will be using old file

            // Close old file on all platforms to prevent FD leak
            if ( _file )
                fclose( _file );  // Close old file on all platforms

#if 0 // enable to test redirection
            cout << "written to cout" << endl;
            cerr << "written to cerr" << endl;
            log() << "written to log()" << endl;
#endif

            _file = tmp;    // Save new file for next rotation
        }

    private:
        std::atomic<bool> _enabled;
        string _path;
        bool _append;
        std::atomic<FILE*> _file;
        streambuf *logbuffer;
        
        // Auto-rotation members
        RotationConfig _rotation_config;
        std::atomic<bool> _running;
        std::thread _rotation_thread;
        std::chrono::steady_clock::time_point _last_rotation;
        std::chrono::steady_clock::time_point _file_created;
        
        void startAutoRotation() {
            if (!_rotation_config.enable_auto_rotation) {
                _running = false;  // Ensure it's false if auto-rotation is disabled
                return;
            }
            
            _running = true;
            _last_rotation = std::chrono::steady_clock::now();
            _file_created = _last_rotation;
            
            _rotation_thread = std::thread([this]() {
                while (_running.load()) {
                    // Check every minute, but wake up every 100ms to check if we should stop
                    // This ensures quick shutdown (max 100ms delay)
                    for (int i = 0; i < 600 && _running.load(); ++i) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
                    
                    if (!_running.load()) break;
                    
                    checkAndRotate();
                }
            });
            
            if (logLevel.load(std::memory_order_relaxed) <= LOG_INFO) {
                info() << "Auto-rotation enabled: max_size=" 
                       << (_rotation_config.max_file_size / (1024*1024)) << "MB, "
                       << "max_files=" << _rotation_config.max_files << ", "
                       << "max_age=" << _rotation_config.max_age.count() << "h";
            }
        }
        
        void checkAndRotate() {
            if (!_enabled.load(std::memory_order_relaxed) || !_file.load(std::memory_order_relaxed)) return;
            
            bool should_rotate = false;
            std::string reason;
            
            // Check file size
            try {
                if (boost::filesystem::exists(_path)) {
                    auto size = boost::filesystem::file_size(_path);
                    if (size >= _rotation_config.max_file_size) {
                        should_rotate = true;
                        reason = "size limit (" + std::to_string(size / (1024*1024)) + "MB)";
                    }
                }
            } catch (...) {
                // Ignore filesystem errors
            }
            
            // Check age
            auto now = std::chrono::steady_clock::now();
            auto age = now - _file_created;
            if (age >= _rotation_config.max_age) {
                should_rotate = true;
                reason = "age limit";
            }
            
            if (should_rotate) {
                // Skip the info log if we're at WARNING or higher to avoid lock contention
                if (logLevel.load(std::memory_order_relaxed) <= LOG_INFO) {
                    info() << "Auto-rotating log: " << reason;
                }
                rotate();
                _file_created = std::chrono::steady_clock::now();
                cleanupOldLogs();
            }
        }
        
        void cleanupOldLogs() {
            try {
                namespace fs = boost::filesystem;
                fs::path log_dir = fs::path(_path).parent_path();
                string log_name = fs::path(_path).filename().string();
                
                std::vector<fs::path> log_files;
                
                // Find all rotated log files (format: xtree.log.YYYY-MM-DDTHH-MM-SS)
                for (fs::directory_iterator it(log_dir); it != fs::directory_iterator(); ++it) {
                    if (fs::is_regular_file(it->status())) {
                        string filename = it->path().filename().string();
                        if (filename.find(log_name + ".") == 0) {
                            log_files.push_back(it->path());
                        }
                    }
                }
                
                // Sort by modification time (oldest first)
                std::sort(log_files.begin(), log_files.end(),
                    [](const fs::path& a, const fs::path& b) {
                        return fs::last_write_time(a) < fs::last_write_time(b);
                    });
                
                // Remove old files if we have too many
                while (log_files.size() > _rotation_config.max_files) {
                    trace() << "Removing old log: " << log_files.front();
                    fs::remove(log_files.front());
                    log_files.erase(log_files.begin());
                }
            } catch (const std::exception& e) {
                warning() << "Failed to cleanup old logs: " << e.what();
            }
        }
        
    public:
        ~LogManager() {
            // Shut down rotation thread if it's running
            if (_rotation_thread.joinable()) {
                _running = false;
                _rotation_thread.join();
            }
            FILE* f = _file.load(std::memory_order_acquire);
            if (f) {
                // Reset the logger to use stderr before closing our file
                Logger::setLogFile(nullptr);
                fclose(f);
                _file.store(nullptr, std::memory_order_release);
            }
        }

    };
    
    // Global singleton for easy file logging setup
    inline std::unique_ptr<LogManager>& getGlobalLogManager() {
        static std::unique_ptr<LogManager> global_log_manager;
        return global_log_manager;
    }
    
    inline LogManager* enableFileLogging(const std::string& log_dir = "") {
        static std::once_flag init_flag;
        
        std::call_once(init_flag, [&log_dir]() {
            // Configure from environment variables
            LogManager::RotationConfig config;
            
            if (const char* size = std::getenv("XTREE_LOG_MAX_SIZE_MB")) {
                config.max_file_size = std::stoull(size) * 1024 * 1024;
            }
            
            if (const char* files = std::getenv("XTREE_LOG_MAX_FILES")) {
                config.max_files = std::stoull(files);
            }
            
            if (const char* hours = std::getenv("XTREE_LOG_MAX_AGE_HOURS")) {
                config.max_age = std::chrono::hours(std::stoull(hours));
            }
            
            if (const char* auto_rotate = std::getenv("XTREE_LOG_AUTO_ROTATE")) {
                config.enable_auto_rotation = (std::string(auto_rotate) != "0");
            }
            
            // Determine log directory
            std::string dir;
            if (const char* env_dir = std::getenv("XTREE_LOG_DIR")) {
                dir = env_dir;
            } else if (!log_dir.empty()) {
                dir = log_dir;
            } else {
                // Use default from LogManager constructor
                dir = "";
            }
            
            getGlobalLogManager() = std::make_unique<LogManager>(dir, config);
        });
        
        return getGlobalLogManager().get();
    }
    
    // Function to explicitly shutdown the global LogManager
    inline void shutdownFileLogging() {
        getGlobalLogManager().reset();
    }
}
