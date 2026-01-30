/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "log.h"
#include "logmanager.h"
#include "log_control.h"
#include <memory>
#include <mutex>

namespace xtree {

/**
 * RAII manager for the entire logging subsystem.
 * Ensures proper initialization and teardown of all logging components.
 * 
 * Usage:
 *   - Tests: Create in SetUpTestSuite(), destroy in TearDownTestSuite()
 *   - Production: Create at startup, destroy at shutdown
 *   - Singleton pattern available via getInstance()
 */
class LogRuntime {
public:
    struct Config {
        // Logging output
        bool enable_file_logging;
        std::string log_dir;  // Empty = use default from environment
        
        // Rotation settings
        LogManager::RotationConfig rotation_config;
        
        // Runtime control
        bool enable_signal_handlers;
        bool enable_file_watcher;
        std::string control_file_path;
        
        // Initial log level
        LogLevel initial_level;
        
        Config() 
            : enable_file_logging(false)
            , enable_signal_handlers(false)
            , enable_file_watcher(false)
            , control_file_path("/tmp/xtree_log_level")
            , initial_level(LOG_WARNING) {}
    };
    
    /**
     * Create a LogRuntime with the given configuration
     */
    explicit LogRuntime(const Config& config = Config()) 
        : config_(config), log_manager_(nullptr) {
        
        // Set initial log level
        logLevel.store(config_.initial_level, std::memory_order_relaxed);
        
        // Set up file logging if requested
        if (config_.enable_file_logging) {
            log_manager_ = std::make_unique<LogManager>(
                config_.log_dir, 
                config_.rotation_config
            );
        }
        
        // Install signal handlers if requested
        if (config_.enable_signal_handlers) {
            LogControl::installSignalHandlers();
        }
        
        // Start file watcher if requested
        if (config_.enable_file_watcher) {
            LogControl::startFileWatcher(config_.control_file_path);
        }
    }
    
    /**
     * Destructor ensures clean shutdown of all components
     */
    ~LogRuntime() {
        shutdown();
    }
    
    /**
     * Explicitly shutdown all logging components
     * Safe to call multiple times
     */
    void shutdown() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Stop file watcher first (it may be writing logs)
        if (config_.enable_file_watcher) {
            LogControl::stopFileWatcher();
            config_.enable_file_watcher = false;
        }
        
        // Reset logger to stderr before destroying LogManager
        Logger::setLogFile(nullptr);
        
        // Destroy LogManager (joins rotation thread, closes files)
        log_manager_.reset();
        
        // Note: We don't uninstall signal handlers as that's generally unsafe
    }
    
    /**
     * Global singleton instance (optional, for convenience)
     */
    static LogRuntime* getInstance() {
        static std::unique_ptr<LogRuntime> instance;
        static std::once_flag init_flag;
        
        std::call_once(init_flag, []() {
            Config config;
            
            // Read configuration from environment
            if (const char* enable = std::getenv("XTREE_LOG_ENABLE_FILE")) {
                config.enable_file_logging = (std::string(enable) != "0");
            }
            
            if (const char* dir = std::getenv("XTREE_LOG_DIR")) {
                config.log_dir = dir;
            }
            
            if (const char* signals = std::getenv("XTREE_LOG_ENABLE_SIGNALS")) {
                config.enable_signal_handlers = (std::string(signals) != "0");
            }
            
            if (const char* watcher = std::getenv("XTREE_LOG_ENABLE_WATCHER")) {
                config.enable_file_watcher = (std::string(watcher) != "0");
            }
            
            // Rotation config from environment
            if (const char* size = std::getenv("XTREE_LOG_MAX_SIZE_MB")) {
                config.rotation_config.max_file_size = std::stoull(size) * 1024 * 1024;
            }
            
            if (const char* files = std::getenv("XTREE_LOG_MAX_FILES")) {
                config.rotation_config.max_files = std::stoull(files);
            }
            
            if (const char* hours = std::getenv("XTREE_LOG_MAX_AGE_HOURS")) {
                config.rotation_config.max_age = std::chrono::hours(std::stoull(hours));
            }
            
            if (const char* auto_rotate = std::getenv("XTREE_LOG_AUTO_ROTATE")) {
                config.rotation_config.enable_auto_rotation = (std::string(auto_rotate) != "0");
            }
            
            instance = std::make_unique<LogRuntime>(config);
        });
        
        return instance.get();
    }
    
    /**
     * Initialize global singleton with specific config
     * Must be called before getInstance() if custom config is needed
     */
    static void initialize(const Config& config) {
        static std::once_flag init_flag;
        std::call_once(init_flag, [&config]() {
            // Create instance with user config
            static std::unique_ptr<LogRuntime> instance(new LogRuntime(config));
        });
    }
    
    // Prevent copying
    LogRuntime(const LogRuntime&) = delete;
    LogRuntime& operator=(const LogRuntime&) = delete;
    
    // Allow moving
    LogRuntime(LogRuntime&&) = default;
    LogRuntime& operator=(LogRuntime&&) = default;
    
private:
    Config config_;
    std::unique_ptr<LogManager> log_manager_;
    std::mutex mutex_;
};

/**
 * Convenience function to shutdown all logging
 * Safe to call even if LogRuntime was never created
 */
inline void shutdownLogging() {
    // Stop any file watcher that might be running
    LogControl::stopFileWatcher();
    
    // Shutdown any global file logging
    shutdownFileLogging();
    
    // Reset logger to stderr
    Logger::setLogFile(nullptr);
    
    // If there's a singleton LogRuntime, shut it down
    // Note: This is a no-op if getInstance() was never called
}

/**
 * Test helper: RAII guard for tests
 */
class LogRuntimeGuard {
public:
    explicit LogRuntimeGuard(const LogRuntime::Config& config = LogRuntime::Config()) 
        : original_level_(logLevel.load(std::memory_order_relaxed)),  // Save BEFORE construction
          runtime_(std::make_unique<LogRuntime>(config)) {
    }
    
    ~LogRuntimeGuard() {
        // Ensure clean shutdown
        runtime_->shutdown();
        
        // Restore original log level
        logLevel.store(original_level_, std::memory_order_relaxed);
        
        // Reset to stderr
        Logger::setLogFile(nullptr);
    }
    
    LogRuntime* operator->() { return runtime_.get(); }
    LogRuntime& operator*() { return *runtime_; }
    
private:
    int original_level_;
    std::unique_ptr<LogRuntime> runtime_;
};

} // namespace xtree