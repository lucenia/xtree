/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "log.h"
#include <csignal>
#include <fstream>
#include <thread>
#include <atomic>
#include <filesystem>

namespace xtree {

/**
 * Runtime log level control for production systems.
 * 
 * Supports:
 * 1. Signal-based control (SIGUSR1/SIGUSR2)
 * 2. File-based control (/tmp/xtree_log_level)
 * 3. Programmatic control
 */
class LogControl {
private:
    static std::atomic<int> pending_action;  // Signal action to apply
    
public:
    // Async-signal-safe handlers that only set an atomic flag
    static void increaseLogLevel(int) {
        pending_action.store(1, std::memory_order_relaxed);
    }
    
    static void decreaseLogLevel(int) {
        pending_action.store(-1, std::memory_order_relaxed);
    }
    
    static void reloadLogLevel(int) {
        pending_action.store(2, std::memory_order_relaxed);
    }
    
    // Process pending signal actions (called from safe context)
    static void processPendingActions() {
        int action = pending_action.exchange(0, std::memory_order_relaxed);
        if (action == 1) {
            // Increase verbosity
            int current = logLevel.load(std::memory_order_relaxed);
            if (current > LOG_TRACE) {
                logLevel.store(current - 1, std::memory_order_relaxed);
                logMessage();
            }
        } else if (action == -1) {
            // Decrease verbosity
            int current = logLevel.load(std::memory_order_relaxed);
            if (current < LOG_SEVERE) {
                logLevel.store(current + 1, std::memory_order_relaxed);
                logMessage();
            }
        } else if (action == 2) {
            // Reload from environment
            initLoggingFromEnv();
            logMessage();
        }
    }
    
    // Install signal handlers for runtime control
    static void installSignalHandlers() {
#ifndef _WIN32
        std::signal(SIGUSR1, increaseLogLevel);  // Increase verbosity
        std::signal(SIGUSR2, decreaseLogLevel);  // Decrease verbosity
        std::signal(SIGHUP, reloadLogLevel);     // Reload from env
        
        if (logLevel.load(std::memory_order_relaxed) <= LOG_INFO) {
            info() << "Log control signals installed: "
                   << "SIGUSR1=increase verbosity, "
                   << "SIGUSR2=decrease verbosity, "
                   << "SIGHUP=reload from LOG_LEVEL env";
        }
#endif
    }
    
    // File-based control: Watch /tmp/xtree_log_level
    static std::atomic<bool> file_watcher_running;
    static std::thread* file_watcher_thread;
    static std::mutex file_watcher_mutex;
    
    static void startFileWatcher(const std::string& path = "/tmp/xtree_log_level") {
        std::lock_guard<std::mutex> lock(file_watcher_mutex);
        
        if (file_watcher_running.load()) {
            return; // Already running
        }
        
        file_watcher_running.store(true);
        file_watcher_thread = new std::thread([path]() {
            if (logLevel.load(std::memory_order_relaxed) <= LOG_INFO) {
                info() << "Log level file watcher started: " << path;
            }
            
            std::filesystem::path control_file(path);
            auto last_write = std::filesystem::file_time_type::min();
            
            while (file_watcher_running.load()) {
                try {
                    if (std::filesystem::exists(control_file)) {
                        auto current_write = std::filesystem::last_write_time(control_file);
                        if (current_write != last_write) {
                            last_write = current_write;
                            
                            std::ifstream file(path);
                            std::string level;
                            if (std::getline(file, level)) {
                                if (setLogLevelFromString(level)) {
                                    // Only log if we're at INFO or below
                                    if (logLevel.load(std::memory_order_relaxed) <= LOG_INFO) {
                                        info() << "Log level changed to " << level << " from file";
                                    }
                                }
                            }
                        }
                    }
                } catch (...) {
                    // Ignore filesystem errors
                }
                
                // Check for pending signal actions and shutdown
                for (int i = 0; i < 10 && file_watcher_running.load(); ++i) {
                    processPendingActions();  // Process any pending signal actions
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        });
    }
    
    // Stop file watcher gracefully
    static void stopFileWatcher() {
        std::lock_guard<std::mutex> lock(file_watcher_mutex);
        
        if (!file_watcher_running.load()) {
            return; // Not running
        }
        
        file_watcher_running.store(false);
        
        if (file_watcher_thread && file_watcher_thread->joinable()) {
            file_watcher_thread->join();
            delete file_watcher_thread;
            file_watcher_thread = nullptr;
        }
    }
    
    // Set log level and notify
    static void setLogLevel(LogLevel level) {
        logLevel.store(level, std::memory_order_relaxed);
        logMessage();
    }
    
    // Get current log level as string
    static std::string getCurrentLogLevelString() {
        switch(logLevel.load(std::memory_order_relaxed)) {
            case LOG_TRACE: return "TRACE";
            case LOG_DEBUG: return "DEBUG";
            case LOG_INFO: return "INFO";
            case LOG_WARNING: return "WARNING";
            case LOG_ERROR: return "ERROR";
            case LOG_SEVERE: return "SEVERE";
            default: return "UNKNOWN";
        }
    }
    
private:
    static void logMessage() {
        // Use cerr directly to ensure this message is always visible
        std::cerr << "[LogControl] Log level is now: " 
                  << getCurrentLogLevelString() 
                  << " (" << logLevel << ")" << std::endl;
    }
};

// Static member definitions
inline std::atomic<bool> LogControl::file_watcher_running{false};
inline std::thread* LogControl::file_watcher_thread = nullptr;
inline std::mutex LogControl::file_watcher_mutex;
inline std::atomic<int> LogControl::pending_action{0};

// Convenience function to enable all runtime controls
inline void enableLogControl() {
    LogControl::installSignalHandlers();
    LogControl::startFileWatcher();
}

// Cleanup function for graceful shutdown
inline void disableLogControl() {
    LogControl::stopFileWatcher();
}

} // namespace xtree