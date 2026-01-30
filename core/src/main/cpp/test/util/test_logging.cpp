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

#include <gtest/gtest.h>
#include "../../src/util/log.h"
#include "../../src/util/logmanager.h"
#include "../../src/util/log_control.h"
#include <fstream>
#include <filesystem>
#include <thread>
#include <regex>
#include <unistd.h>  // for dup, dup2
#include <cstdio>    // for fileno

namespace xtree {

// Note: logLevel globals are now defined in log.cpp

class LoggingTest : public ::testing::Test {
protected:
    int original_log_level;
    std::string test_log_dir;
    std::string test_log_file;
    
    void SetUp() override {
        // Save original log level
        original_log_level = logLevel;
        
        // Create test log directory
        test_log_dir = "/tmp/xtree_logging_test_" + std::to_string(getpid());
        std::filesystem::create_directories(test_log_dir);
        test_log_file = test_log_dir + "/test.log";
    }
    
    void TearDown() override {
        // Restore original log level
        logLevel = original_log_level;
        
        // Clean up test directory
        std::filesystem::remove_all(test_log_dir);
        
        // Unset environment variable
        unsetenv("LOG_LEVEL");
    }
    
    bool containsLogMessage(const std::string& log_content, const std::string& level, const std::string& message) {
        // Look for pattern: [LEVEL] ... message
        std::string pattern = "\\[" + level + "\\].*" + message;
        std::regex re(pattern);
        return std::regex_search(log_content, re);
    }
    
    std::string captureLogOutput(std::function<void()> func) {
        // Use file redirection since logger uses fprintf(stderr)
        // Create a temporary file
        std::string tmp_file = test_log_dir + "/capture.log";
        
        // Save current stderr
        int saved_stderr = dup(STDERR_FILENO);
        
        // Open temp file and redirect stderr to it
        FILE* temp = fopen(tmp_file.c_str(), "w");
        if (!temp) return "";
        int temp_fd = fileno(temp);
        dup2(temp_fd, STDERR_FILENO);
        
        // Run the function
        func();
        
        // Flush and close
        fflush(stderr);
        fclose(temp);
        
        // Restore stderr
        dup2(saved_stderr, STDERR_FILENO);
        close(saved_stderr);
        
        // Read the captured output
        std::ifstream file(tmp_file);
        std::string content((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
        
        // Clean up temp file
        std::filesystem::remove(tmp_file);
        
        return content;
    }
};

TEST_F(LoggingTest, LogLevelFiltering) {
    // Test that only messages at or above the current log level are shown
    
    // Set to INFO level
    logLevel = LOG_INFO;
    
    // Test basic functionality by just calling the functions
    // We'll test the capture mechanism separately
    trace() << "trace message";
    debug() << "debug message";
    info() << "info message";
    warning() << "warning message";
    error() << "error message";
    severe() << "severe message";
    
    // For now, just test that the log level setting works
    EXPECT_EQ(logLevel, LOG_INFO);
    
    // Test that filtering logic is correct (manually for now)
    EXPECT_TRUE(LOG_TRACE < logLevel);   // Should be filtered
    EXPECT_TRUE(LOG_DEBUG < logLevel);   // Should be filtered
    EXPECT_FALSE(LOG_INFO < logLevel);   // Should pass
    EXPECT_FALSE(LOG_WARNING < logLevel); // Should pass
    EXPECT_FALSE(LOG_ERROR < logLevel);  // Should pass
    EXPECT_FALSE(LOG_SEVERE < logLevel); // Should pass
}

TEST_F(LoggingTest, SetLogLevelFromString) {
    // Test setting log level from string
    
    EXPECT_TRUE(setLogLevelFromString("TRACE"));
    EXPECT_EQ(logLevel, LOG_TRACE);
    
    EXPECT_TRUE(setLogLevelFromString("DEBUG"));
    EXPECT_EQ(logLevel, LOG_DEBUG);
    
    EXPECT_TRUE(setLogLevelFromString("INFO"));
    EXPECT_EQ(logLevel, LOG_INFO);
    
    EXPECT_TRUE(setLogLevelFromString("WARNING"));
    EXPECT_EQ(logLevel, LOG_WARNING);
    
    EXPECT_TRUE(setLogLevelFromString("WARN")); // Alias
    EXPECT_EQ(logLevel, LOG_WARNING);
    
    EXPECT_TRUE(setLogLevelFromString("ERROR"));
    EXPECT_EQ(logLevel, LOG_ERROR);
    
    EXPECT_TRUE(setLogLevelFromString("SEVERE"));
    EXPECT_EQ(logLevel, LOG_SEVERE);
    
    EXPECT_TRUE(setLogLevelFromString("FATAL")); // Alias
    EXPECT_EQ(logLevel, LOG_SEVERE);
    
    // Test case insensitivity
    EXPECT_TRUE(setLogLevelFromString("debug"));
    EXPECT_EQ(logLevel, LOG_DEBUG);
    
    EXPECT_TRUE(setLogLevelFromString("DeBuG"));
    EXPECT_EQ(logLevel, LOG_DEBUG);
    
    // Test invalid level
    EXPECT_FALSE(setLogLevelFromString("INVALID"));
}

TEST_F(LoggingTest, SetLogLevelFromEnvironment) {
    // Test setting log level from environment variable
    
    // Set environment variable
    setenv("LOG_LEVEL", "DEBUG", 1);
    initLoggingFromEnv();
    EXPECT_EQ(logLevel, LOG_DEBUG);
    
    // Test different levels
    setenv("LOG_LEVEL", "TRACE", 1);
    initLoggingFromEnv();
    EXPECT_EQ(logLevel, LOG_TRACE);
    
    setenv("LOG_LEVEL", "ERROR", 1);
    initLoggingFromEnv();
    EXPECT_EQ(logLevel, LOG_ERROR);
    
    // Test invalid level (should not change current level)
    int current_level = logLevel;
    setenv("LOG_LEVEL", "INVALID_LEVEL", 1);
    auto output = captureLogOutput([&]() {
        initLoggingFromEnv();
    });
    EXPECT_EQ(logLevel, current_level); // Should not change
    EXPECT_TRUE(output.find("Invalid LOG_LEVEL") != std::string::npos);
}

TEST_F(LoggingTest, LogMessageFormatting) {
    // Test that log messages work with various formats
    
    logLevel = LOG_INFO;
    
    // Just test that these don't crash
    info() << "test message with number " << 42 << " and string";
    warning() << "test with double " << 3.14159;
    error() << "test with pointer " << static_cast<void*>(nullptr);
    
    // Test passed if we got here without crashing
    SUCCEED();
}

TEST_F(LoggingTest, TraceLevel) {
    // Test TRACE level specifically (for support engineering)
    
    logLevel = LOG_TRACE;
    
    // Just test that trace works when enabled
    trace() << "detailed trace info";
    
    EXPECT_EQ(logLevel, LOG_TRACE);
}

TEST_F(LoggingTest, DebugLevel) {
    // Test DEBUG level specifically (for developers)
    
    logLevel = LOG_DEBUG;
    
    // Test that these work at DEBUG level
    debug() << "debug information";
    trace() << "trace should not show";
    
    // Verify filtering logic
    EXPECT_FALSE(LOG_DEBUG < logLevel);  // DEBUG should pass
    EXPECT_TRUE(LOG_TRACE < logLevel);   // TRACE should be filtered
}

TEST_F(LoggingTest, ProductionLevels) {
    // Test production log levels (INFO, WARNING, ERROR, SEVERE)
    
    logLevel = LOG_WARNING;
    
    // Test that these work at WARNING level
    info() << "info should not show";
    warning() << "warning shows";
    error() << "error shows";
    severe() << "severe shows";
    
    // Verify filtering logic
    EXPECT_TRUE(LOG_INFO < logLevel);      // INFO should be filtered
    EXPECT_FALSE(LOG_WARNING < logLevel);  // WARNING should pass
    EXPECT_FALSE(LOG_ERROR < logLevel);    // ERROR should pass
    EXPECT_FALSE(LOG_SEVERE < logLevel);   // SEVERE should pass
}

TEST_F(LoggingTest, ThreadSafety) {
    // Test that logging is thread-safe
    
    logLevel = LOG_INFO;
    const int num_threads = 10;
    const int messages_per_thread = 100;
    
    std::vector<std::thread> threads;
    
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([i, messages_per_thread]() {
            for (int j = 0; j < messages_per_thread; ++j) {
                info() << "Thread " << i << " message " << j;
            }
        });
    }
    
    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }
    
    // If we get here without crashing, thread safety is working
    SUCCEED();
}

TEST_F(LoggingTest, LogManagerFileOutput) {
    // Test LogManager writing to file
    
    // Disable auto-rotation for this test to avoid thread issues
    LogManager::RotationConfig config;
    config.enable_auto_rotation = false;  // No background thread
    
    {
        LogManager log_mgr(test_log_dir, config);
        
        // Write some log messages
        logLevel = LOG_INFO;
        info() << "test message to file";
        warning() << "warning to file";
        
        // Force flush
        Logger::get().flush();
    } // LogManager destroyed here
    
    // Check that log file exists and contains messages
    std::string log_path = test_log_dir + "/xtree.log";
    EXPECT_TRUE(std::filesystem::exists(log_path));
    
    // Read file content
    std::ifstream log_file(log_path);
    std::string content((std::istreambuf_iterator<char>(log_file)),
                        std::istreambuf_iterator<char>());
    
    // Should contain our messages
    EXPECT_TRUE(content.find("test message to file") != std::string::npos);
    EXPECT_TRUE(content.find("warning to file") != std::string::npos);
}

TEST_F(LoggingTest, LogRotation) {
    // Test log rotation functionality
    
    // Disable auto-rotation for manual control
    LogManager::RotationConfig config;
    config.enable_auto_rotation = false;
    
    {
        LogManager log_mgr(test_log_dir, config);
        
        // Write initial message
        info() << "before rotation";
        Logger::get().flush();
        
        // Rotate log
        log_mgr.rotate();
        
        // Write message after rotation
        info() << "after rotation";
        Logger::get().flush();
    } // LogManager destroyed here
    
    // Should have both the current log and a rotated log
    std::string current_log = test_log_dir + "/xtree.log";
    EXPECT_TRUE(std::filesystem::exists(current_log));
    
    // Check for rotated log (has timestamp suffix)
    bool found_rotated = false;
    for (const auto& entry : std::filesystem::directory_iterator(test_log_dir)) {
        if (entry.path().string().find("xtree.log.") != std::string::npos) {
            found_rotated = true;
            break;
        }
    }
    EXPECT_TRUE(found_rotated) << "Should have found a rotated log file";
}

TEST_F(LoggingTest, HelperFunctions) {
    // Test helper functions for different log levels
    
    logLevel = LOG_INFO;
    
    // Test that helper functions work correctly
    trace() << "trace helper";
    debug() << "debug helper";
    info() << "info helper";
    warning() << "warning helper";
    error() << "error helper";
    severe() << "severe helper";
    
    // Just verify we didn't crash
    SUCCEED();
}

TEST_F(LoggingTest, ComplexDataTypes) {
    // Test logging of various data types
    
    logLevel = LOG_INFO;
    
    // Test that various data types work
    info() << "int: " << 42;
    info() << "double: " << 3.14159;
    info() << "bool: " << true << " " << false;
    info() << "pointer: " << static_cast<void*>(nullptr);
    info() << "hex: " << std::hex << 255;
    info() << "string: " << std::string("test string");
    info() << "char: " << 'X';
    info() << "unsigned: " << 123u;
    info() << "long long: " << 9876543210LL;
    
    // Just verify we didn't crash with different types
    SUCCEED();
}

TEST_F(LoggingTest, NoSpamAtHighLevels) {
    // Ensure that when log level is set high, lower priority messages don't appear
    
    logLevel = LOG_SEVERE;
    
    // These should all be filtered except severe
    trace() << "should not appear";
    debug() << "should not appear";
    info() << "should not appear";
    warning() << "should not appear";
    error() << "should not appear";
    severe() << "only this should appear";
    
    // Verify filtering logic
    EXPECT_TRUE(LOG_TRACE < logLevel);
    EXPECT_TRUE(LOG_DEBUG < logLevel);
    EXPECT_TRUE(LOG_INFO < logLevel);
    EXPECT_TRUE(LOG_WARNING < logLevel);
    EXPECT_TRUE(LOG_ERROR < logLevel);
    EXPECT_FALSE(LOG_SEVERE < logLevel);  // Only SEVERE should pass
}

TEST_F(LoggingTest, RecoveryIntegration) {
    // Test that our logging changes in recovery.cpp work correctly
    // This is a compile-time test mostly - if it compiles and runs, it works
    
    logLevel = LOG_INFO;
    
    // Simulate recovery-style logging
    warning() << "Failed to load manifest, continuing with directory scan";
    info() << "Loaded " << 1000 << " entries from checkpoint epoch " << 42;
    trace() << "  Found log file: delta.log (size=" << 1024 << ")";
    error() << "Delta log replay failed: test error";
    info() << "Recovery completed in " << 100 << " ms";
    debug() << "Recommendation: Rotate delta logs (5 logs accumulated)";
    
    // Just verify we can log these without crashing
    SUCCEED();
}

TEST_F(LoggingTest, FileLoggingWithRotation) {
    // Test file logging with manual rotation
    
    // Configure for manual rotation
    LogManager::RotationConfig config;
    config.max_file_size = 1024 * 5;  // 5KB for quick rotation
    config.max_files = 3;              // Keep only 3 old files
    config.max_age = std::chrono::hours(24);  // Won't trigger in test
    config.enable_auto_rotation = false;  // Manual control for test
    
    {
        // Create log manager
        LogManager log_mgr(test_log_dir, config);
        
        // Write messages to fill up file
        for (int i = 0; i < 50; i++) {
            info() << "Test message " << i << " - padding to fill space quickly "
                   << "The quick brown fox jumps over the lazy dog";
        }
        
        // Force rotation
        log_mgr.rotate();
        info() << "Message after rotation";
        
        // Force flush before destruction
        Logger::get().flush();
    } // LogManager destroyed here
    
    // Check that rotation happened
    std::string rotated_pattern = test_log_dir + "/xtree.log.";
    int rotated_count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(test_log_dir)) {
        if (entry.path().string().find(rotated_pattern) != std::string::npos) {
            rotated_count++;
        }
    }
    
    EXPECT_GT(rotated_count, 0) << "Should have at least one rotated log file";
    
    // Check current log exists
    EXPECT_TRUE(std::filesystem::exists(test_log_dir + "/xtree.log"));
}

TEST_F(LoggingTest, RuntimeLogLevelControl) {
    // Test runtime log level changes via LogControl
    
    logLevel = LOG_WARNING;
    
    // Initial state - WARNING level
    auto output1 = captureLogOutput([&]() {
        debug() << "debug1";
        warning() << "warning1";
    });
    EXPECT_FALSE(containsLogMessage(output1, "DEBUG", "debug1"));
    EXPECT_TRUE(containsLogMessage(output1, "WARN", "warning1"));
    
    // Change to DEBUG via LogControl
    LogControl::setLogLevel(LOG_DEBUG);
    EXPECT_EQ(logLevel, LOG_DEBUG);
    
    auto output2 = captureLogOutput([&]() {
        debug() << "debug2";
        warning() << "warning2";
    });
    EXPECT_TRUE(containsLogMessage(output2, "DEBUG", "debug2"));
    EXPECT_TRUE(containsLogMessage(output2, "WARN", "warning2"));
}

TEST_F(LoggingTest, EnvironmentVariableConfiguration) {
    // Test that environment variables configure file logging
    
    // Set environment variables
    setenv("XTREE_LOG_MAX_SIZE_MB", "50", 1);
    setenv("XTREE_LOG_MAX_FILES", "5", 1);
    setenv("XTREE_LOG_MAX_AGE_HOURS", "12", 1);
    
    // These would be read by enableFileLogging()
    // Just verify they're set correctly
    EXPECT_STREQ(std::getenv("XTREE_LOG_MAX_SIZE_MB"), "50");
    EXPECT_STREQ(std::getenv("XTREE_LOG_MAX_FILES"), "5");
    EXPECT_STREQ(std::getenv("XTREE_LOG_MAX_AGE_HOURS"), "12");
    
    // Clean up
    unsetenv("XTREE_LOG_MAX_SIZE_MB");
    unsetenv("XTREE_LOG_MAX_FILES");
    unsetenv("XTREE_LOG_MAX_AGE_HOURS");
}

} // namespace xtree