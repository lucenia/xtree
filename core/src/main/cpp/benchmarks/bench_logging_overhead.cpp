/*
 * Benchmark to measure logging overhead in critical paths
 */

#include <gtest/gtest.h>
#include "../src/util/log.h"
#include "../src/util/log_runtime.h"
#include <chrono>
#include <atomic>
#include <filesystem>
#include <thread>
#include <mutex>

class LoggingOverheadBenchmark : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original log level
        original_level_ = xtree::logLevel.load(std::memory_order_relaxed);
        // Ensure clean state - shutdown any existing logging
        xtree::shutdownLogging();
        xtree::Logger::setLogFile(nullptr);
    }
    
    void TearDown() override {
        // Ensure complete cleanup
        xtree::shutdownLogging();
        // Restore original level
        xtree::logLevel.store(original_level_, std::memory_order_relaxed);
        // Ensure we're back to stderr logging
        xtree::Logger::setLogFile(nullptr);
        // Force cleanup of thread-local Logger to avoid interference between tests
        xtree::Logger::get().flush();
    }
    
    int original_level_;
};

TEST_F(LoggingOverheadBenchmark, FilteredMessageOverhead) {
    std::cout << "[DEBUG] Starting FilteredMessageOverhead test\n" << std::flush;
    // Set log level to WARNING so DEBUG messages are filtered
    xtree::logLevel.store(xtree::LOG_WARNING, std::memory_order_relaxed);
    
    const int iterations = 10000000; // 10 million
    std::atomic<int> counter{0};
    
    // Baseline: just increment counter
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
        counter++;
    }
    auto baseline_time = std::chrono::high_resolution_clock::now() - start;
    
    // With filtered debug message
    counter = 0;
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
        xtree::debug() << "This message is filtered out: " << i;
        counter++;
    }
    auto with_logging_time = std::chrono::high_resolution_clock::now() - start;
    
    auto baseline_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(baseline_time).count();
    auto logging_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(with_logging_time).count();
    auto overhead_ns = logging_ns - baseline_ns;
    auto overhead_per_call = overhead_ns / iterations;
    
    std::cout << "\nFiltered Message Overhead Test (10M iterations):\n";
    std::cout << "  Baseline:        " << baseline_ns << " ns total\n";
    std::cout << "  With logging:    " << logging_ns << " ns total\n";
    std::cout << "  Total overhead:  " << overhead_ns << " ns\n";
    std::cout << "  Per-call overhead: " << overhead_per_call << " ns/call\n";
    std::cout << "  Overhead ratio:  " << (double)logging_ns / baseline_ns << "x\n";
    
    // In optimized build, overhead should be < 5ns per call
    EXPECT_LT(overhead_per_call, 5) << "Filtered logging overhead too high!";
    std::cout << "[DEBUG] Completed FilteredMessageOverhead test\n" << std::flush;
}

TEST_F(LoggingOverheadBenchmark, ActiveMessageOverhead) {
    std::cout << "[DEBUG] Starting ActiveMessageOverhead test\n" << std::flush;
    // Set log level to TRACE so messages are active
    xtree::logLevel.store(xtree::LOG_TRACE, std::memory_order_relaxed);
    
    const int iterations = 10000; // Reduced to 10k for consistency
    
    // Redirect stderr to /dev/null to avoid terminal I/O overhead
    FILE* original_stderr = stderr;
    FILE* null_file = fopen("/dev/null", "w");
    stderr = null_file;
    
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
        xtree::debug() << "Active message: " << i;
    }
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    
    // Restore stderr
    stderr = original_stderr;
    fclose(null_file);
    
    auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    auto per_call_ns = (total_us * 1000) / iterations;
    
    std::cout << "\nActive Message Overhead Test (100k iterations):\n";
    std::cout << "  Total time:      " << total_us << " µs\n";
    std::cout << "  Per-call time:   " << per_call_ns << " ns/call\n";
    std::cout << "  Throughput:      " << (iterations * 1000000.0 / total_us) << " msgs/sec\n";
    
    // Active messages will be slower due to I/O, but should still be reasonable
    EXPECT_LT(per_call_ns, 10000) << "Active logging too slow!"; // < 10µs per message
    std::cout << "[DEBUG] Completed ActiveMessageOverhead test\n" << std::flush;
}

TEST_F(LoggingOverheadBenchmark, ComparisonWithOldLogger) {
    std::cout << "[DEBUG] Starting ComparisonWithOldLogger test\n" << std::flush;
    // Compare our optimized logger with the old ILogger approach
    xtree::logLevel.store(xtree::LOG_WARNING, std::memory_order_relaxed);
    
    // Sanity check: verify that DEBUG messages are actually filtered at WARNING level
    auto& filtered_old = xtree::_log(xtree::LOG_DEBUG);
    ASSERT_EQ(&filtered_old, &xtree::iLogger) << "Old API should return no-op logger for filtered messages";
    // Also verify new API filtering works (can't compare LoggerWrapper directly as it's a value type)
    
    const int iterations = 10000000;
    
    // Old style (using log(int) which returns ILogger)
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
        xtree::_log(xtree::LOG_DEBUG) << "Filtered message";
    }
    auto old_style = std::chrono::high_resolution_clock::now() - start;
    
    // New style (using debug() which returns LoggerWrapper)
    start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; i++) {
        xtree::debug() << "Filtered message";
    }
    auto new_style = std::chrono::high_resolution_clock::now() - start;
    
    auto old_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(old_style).count();
    auto new_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(new_style).count();
    
    std::cout << "\nOld vs New Logger Comparison (10M filtered messages):\n";
    std::cout << "  Old ILogger:     " << old_ns / iterations << " ns/call\n";
    std::cout << "  New Wrapper:     " << new_ns / iterations << " ns/call\n";
    std::cout << "  Speedup:         " << (double)old_ns / new_ns << "x\n";
    
    // New should be at least as fast as old
    EXPECT_LE(new_ns, old_ns * 1.1) << "New logger slower than old!";
    std::cout << "[DEBUG] Completed ComparisonWithOldLogger test\n" << std::flush;
}

TEST_F(LoggingOverheadBenchmark, FilteredMessageOverheadWithFileLogging) {
    std::cout << "[DEBUG] Starting FilteredMessageOverheadWithFileLogging test\n" << std::flush;
    
    // TEST WITH LOGMANAGER (FILE OUTPUT) ENABLED
    // This is the critical test for production scenarios
    
    // Create temp directory for log file
    std::string test_dir = "/tmp/bench_logging_" + std::to_string(getpid());
    std::filesystem::create_directories(test_dir);
    
    {
        // Use LogRuntime for proper RAII management
        xtree::LogRuntime::Config config;
        config.enable_file_logging = true;
        config.log_dir = test_dir;
        config.rotation_config.enable_auto_rotation = false;  // No background thread for benchmark
        config.initial_level = xtree::LOG_WARNING;
        
        xtree::LogRuntimeGuard runtime(config);
        
        // Verify log level is set
        ASSERT_EQ(xtree::logLevel.load(std::memory_order_relaxed), xtree::LOG_WARNING);
        
        const int iterations = 10000000; // 10 million
        std::atomic<int> counter{0};
        
        // Baseline: just increment counter
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            counter++;
        }
        auto baseline_time = std::chrono::high_resolution_clock::now() - start;
        
        // With filtered debug message (SHOULD BE ZERO OVERHEAD WITH FILE LOGGING)
        counter = 0;
        start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            xtree::debug() << "This message is filtered out: " << i;
            counter++;
        }
        auto with_logging_time = std::chrono::high_resolution_clock::now() - start;
        
        auto baseline_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(baseline_time).count();
        auto logging_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(with_logging_time).count();
        auto overhead_ns = logging_ns - baseline_ns;
        auto overhead_per_call = overhead_ns / iterations;
        
        std::cout << "\nFiltered Message Overhead WITH FILE LOGGING (10M iterations):\n";
        std::cout << "  LogManager:      ENABLED (writing to " << test_dir << "/xtree.log)\n";
        std::cout << "  Baseline:        " << baseline_ns << " ns total\n";
        std::cout << "  With logging:    " << logging_ns << " ns total\n";
        std::cout << "  Total overhead:  " << overhead_ns << " ns\n";
        std::cout << "  Per-call overhead: " << overhead_per_call << " ns/call\n";
        std::cout << "  Overhead ratio:  " << (double)logging_ns / baseline_ns << "x\n";
        
        // CRITICAL: Even with file logging enabled, filtered messages should have ZERO overhead
        EXPECT_LT(overhead_per_call, 5) << "Filtered logging overhead too high even with file output!";
    }
    
    // Clean up
    std::filesystem::remove_all(test_dir);
    std::cout << "[DEBUG] Completed FilteredMessageOverheadWithFileLogging test\n" << std::flush;
}

TEST_F(LoggingOverheadBenchmark, ActiveMessageOverheadWithFileLogging) {
    std::cout << "[DEBUG] Starting ActiveMessageOverheadWithFileLogging test\n" << std::flush;
    
    // Test active message performance when writing to file
    
    // Create temp directory for log file with unique suffix to avoid conflicts
    static int test_counter = 0;
    std::string test_dir = "/tmp/bench_logging_active_" + std::to_string(getpid()) + "_" + std::to_string(++test_counter);
    std::filesystem::create_directories(test_dir);
    
    {
        // Use LogRuntime for proper RAII management
        xtree::LogRuntime::Config config;
        config.enable_file_logging = true;
        config.log_dir = test_dir;
        config.rotation_config.enable_auto_rotation = false;
        config.initial_level = xtree::LOG_TRACE;
        
        xtree::LogRuntimeGuard runtime(config);
        
        // Verify log level is set
        ASSERT_EQ(xtree::logLevel.load(std::memory_order_relaxed), xtree::LOG_TRACE);
        
        const int iterations = 10000; // Reduced to 10k to avoid timeout issues
        
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            xtree::debug() << "Active message to file: " << i;
        }
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        
        // Force flush before measuring
        xtree::Logger::get().flush();
        
        auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        auto per_call_ns = (total_us * 1000) / iterations;
        
        // Check file size to verify messages were written
        auto log_size = std::filesystem::file_size(test_dir + "/xtree.log");
        
        std::cout << "\nActive Message Overhead WITH FILE LOGGING (10k iterations):\n";
        std::cout << "  LogManager:      ENABLED (file output)\n";
        std::cout << "  Total time:      " << total_us << " µs\n";
        std::cout << "  Per-call time:   " << per_call_ns << " ns/call\n";
        std::cout << "  Throughput:      " << (iterations * 1000000.0 / total_us) << " msgs/sec\n";
        std::cout << "  Log file size:   " << log_size << " bytes\n";
        
        // File I/O is slower than stderr but should still be reasonable
        EXPECT_LT(per_call_ns, 20000) << "Active file logging too slow!"; // < 20µs per message
        EXPECT_GT(log_size, 1000) << "Log file should have content!";
        std::cout << "[DEBUG] About to destroy LogRuntime at " 
                  << std::chrono::steady_clock::now().time_since_epoch().count() << "\n" << std::flush;
    } // LogRuntime destroyed here - proper RAII cleanup
    
    std::cout << "[DEBUG] LogRuntime destroyed at " 
              << std::chrono::steady_clock::now().time_since_epoch().count() << "\n" << std::flush;
    
    // Clean up test directory
    std::cout << "[DEBUG] Removing test directory\n" << std::flush;
    std::filesystem::remove_all(test_dir);
    std::cout << "[DEBUG] Completed ActiveMessageOverheadWithFileLogging test\n" << std::flush;
}