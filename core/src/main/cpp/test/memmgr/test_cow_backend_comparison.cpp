/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Benchmark comparison of COW backends: IN_MEMORY vs MMAP vs FILE_IO
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <chrono>
#include <random>
#include <iomanip>
#include <fstream>
#include <cstdlib>
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/xtiter.h"
#include "../src/indexdetails.hpp"

using namespace xtree;
using namespace std::chrono;

// Simple test to verify test framework works
TEST(BasicTest, TestFrameworkWorks) {
    std::cout << "Test framework is working!\n";
    EXPECT_TRUE(true);
}

class COWBackendBenchmark : public ::testing::Test {
protected:
    static constexpr int NUM_RECORDS = 1000;  // Reduced from 10000
    static constexpr int NUM_QUERIES = 100;   // Reduced from 1000
    static constexpr int DIMENSION = 2;
    static constexpr int PRECISION = 32;
    
    std::vector<const char*>* dimLabels;
    std::vector<DataRecord*> testRecords;
    std::vector<DataRecord*> testQueries;
    
    void SetUp() override {
        // Create dimension labels
        dimLabels = new std::vector<const char*>();
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        
        // Generate test data
        std::mt19937 gen(42); // Fixed seed for reproducibility
        std::uniform_real_distribution<float> dist(0.0f, 100.0f);
        
        // Generate records - using DataRecord constructor properly
        testRecords.reserve(NUM_RECORDS);
        for (int i = 0; i < NUM_RECORDS; i++) {
            DataRecord* rec = new DataRecord(DIMENSION, PRECISION, "rec_" + std::to_string(i));
            std::vector<double> point = {(double)dist(gen), (double)dist(gen)};
            rec->putPoint(&point);
            testRecords.push_back(rec);
        }
        
        // Generate query ranges
        testQueries.reserve(NUM_QUERIES);
        for (int i = 0; i < NUM_QUERIES; i++) {
            DataRecord* query = new DataRecord(DIMENSION, PRECISION, "query_" + std::to_string(i));
            float x_min = dist(gen);
            float y_min = dist(gen);
            std::vector<double> p1 = {(double)x_min, (double)y_min};
            std::vector<double> p2 = {(double)(x_min + dist(gen) * 0.2f), (double)(y_min + dist(gen) * 0.2f)};
            query->putPoint(&p1);
            query->putPoint(&p2);
            testQueries.push_back(query);
        }
    }
    
    void TearDown() override {
        delete dimLabels;
        for (auto rec : testRecords) delete rec;
        for (auto query : testQueries) delete query;
    }
    
    struct BenchmarkResult {
        double insert_time_ms;
        double query_time_ms;
        double snapshot_time_ms;
        double total_time_ms;
        size_t memory_usage_bytes;
        std::string backend_name;
        
        void print() const {
            std::cout << "\n=== " << backend_name << " Performance ===\n";
            std::cout << "Insert time: " << std::fixed << std::setprecision(2) 
                     << insert_time_ms << " ms (" 
                     << (NUM_RECORDS * 1000.0 / insert_time_ms) << " inserts/sec)\n";
            std::cout << "Query time: " << query_time_ms << " ms (" 
                     << (NUM_QUERIES * 1000.0 / query_time_ms) << " queries/sec)\n";
            std::cout << "Snapshot time: " << snapshot_time_ms << " ms\n";
            std::cout << "Total time: " << total_time_ms << " ms\n";
            std::cout << "Memory usage: " << (memory_usage_bytes / 1024.0 / 1024.0) << " MB\n";
        }
    };
    
    BenchmarkResult benchmarkBackend(IndexDetails<DataRecord>::PersistenceMode mode, 
                                    const std::string& name,
                                    const std::string& snapshot_file = "") {
        BenchmarkResult result;
        result.backend_name = name;
        
        std::cout << "\nBenchmarking " << name << " backend...\n";
        std::cout.flush();
        
        auto start_total = high_resolution_clock::now();
        
        // Create index with specified backend
        IndexDetails<DataRecord>* index = new IndexDetails<DataRecord>(
            DIMENSION, PRECISION, dimLabels, nullptr, nullptr,
            mode, snapshot_file
        );
        
        // Create root bucket using XAlloc which handles all persistence modes
        XTreeBucket<DataRecord>* root = XAlloc<DataRecord>::allocate_bucket(index, true);
        
        // Cache the root
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress(reinterpret_cast<long>(cachedRoot));
        
        // Benchmark insertions
        std::cout << "  Inserting " << testRecords.size() << " records...\n";
        std::cout.flush();
        auto start_insert = high_resolution_clock::now();
        for (size_t i = 0; i < testRecords.size(); i++) {
            root->xt_insert(cachedRoot, testRecords[i]);
            if (i % 100 == 0) {
                std::cout << "  " << i << "/" << testRecords.size() << " records inserted\r";
                std::cout.flush();
            }
        }
        auto end_insert = high_resolution_clock::now();
        std::cout << "  Insertion complete\n";
        result.insert_time_ms = duration_cast<microseconds>(end_insert - start_insert).count() / 1000.0;
        
        // Benchmark queries
        auto start_query = high_resolution_clock::now();
        int total_results = 0;
        for (const auto& query : testQueries) {
            auto iter = root->getIterator(cachedRoot, query, 0);
            
            while (iter->hasNext()) {
                iter->next();
                total_results++;
            }
            delete iter;
        }
        auto end_query = high_resolution_clock::now();
        result.query_time_ms = duration_cast<microseconds>(end_query - start_query).count() / 1000.0;
        
        // Benchmark snapshot (if applicable)
        auto start_snapshot = high_resolution_clock::now();
        if (mode != IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY && index->getCOWManager()) {
            index->getCOWManager()->trigger_memory_snapshot();
        }
        auto end_snapshot = high_resolution_clock::now();
        result.snapshot_time_ms = duration_cast<microseconds>(end_snapshot - start_snapshot).count() / 1000.0;
        
        // Get memory usage
        if (index->getCOWManager()) {
            auto stats = index->getCOWManager()->get_stats();
            result.memory_usage_bytes = stats.tracked_memory_bytes;
        } else {
            result.memory_usage_bytes = 0; // Approximate for in-memory
        }
        
        auto end_total = high_resolution_clock::now();
        result.total_time_ms = duration_cast<microseconds>(end_total - start_total).count() / 1000.0;
        
        // Cleanup
        // Don't delete root directly - it's managed by the cache
        index->clearCache();
        delete index;
        
        // Delete snapshot files
        if (!snapshot_file.empty()) {
            std::remove(snapshot_file.c_str());
        }
        
        return result;
    }
};

TEST_F(COWBackendBenchmark, CompareAllBackends) {
    std::cout << "\n========================================\n";
    std::cout << "COW Backend Performance Comparison\n";
    std::cout << "Records: " << NUM_RECORDS << ", Queries: " << NUM_QUERIES << "\n";
    std::cout << "========================================\n";
    std::cout.flush();
    
    // Benchmark each backend
    auto inMemoryResult = benchmarkBackend(
        IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY, 
        "IN_MEMORY"
    );
    
    auto mmapResult = benchmarkBackend(
        IndexDetails<DataRecord>::PersistenceMode::MMAP,
        "MMAP",
        "test_mmap.snapshot"
    );
    
    auto fileIOResult = benchmarkBackend(
        IndexDetails<DataRecord>::PersistenceMode::FILE_IO,
        "FILE_IO", 
        "test_fileio.snapshot"
    );
    
    // Print results
    inMemoryResult.print();
    mmapResult.print();
    fileIOResult.print();
    
    // Print comparison
    std::cout << "\n=== Performance Comparison ===\n";
    std::cout << "MMAP vs IN_MEMORY:\n";
    std::cout << "  Insert overhead: " << std::fixed << std::setprecision(1)
             << ((mmapResult.insert_time_ms / inMemoryResult.insert_time_ms - 1) * 100) << "%\n";
    std::cout << "  Query overhead: " 
             << ((mmapResult.query_time_ms / inMemoryResult.query_time_ms - 1) * 100) << "%\n";
    
    std::cout << "\nFILE_IO vs IN_MEMORY:\n";
    std::cout << "  Insert overhead: " 
             << ((fileIOResult.insert_time_ms / inMemoryResult.insert_time_ms - 1) * 100) << "%\n";
    std::cout << "  Query overhead: " 
             << ((fileIOResult.query_time_ms / inMemoryResult.query_time_ms - 1) * 100) << "%\n";
    
    std::cout << "\nMMAP vs FILE_IO:\n";
    std::cout << "  Insert speedup: " 
             << ((fileIOResult.insert_time_ms / mmapResult.insert_time_ms - 1) * 100) << "%\n";
    std::cout << "  Query speedup: " 
             << ((fileIOResult.query_time_ms / mmapResult.query_time_ms - 1) * 100) << "%\n";
    std::cout << "  Snapshot speedup: " 
             << ((fileIOResult.snapshot_time_ms / mmapResult.snapshot_time_ms - 1) * 100) << "%\n";
    
    // Save results to file if environment variable is set
    const char* saveResults = std::getenv("SAVE_BENCHMARK_RESULTS");
    if (saveResults != nullptr && std::string(saveResults) == "1") {
        std::ofstream resultFile("cow_backend_benchmark_results.txt");
        if (resultFile.is_open()) {
            resultFile << "COW Backend Performance Comparison\n";
            resultFile << "Records: " << NUM_RECORDS << ", Queries: " << NUM_QUERIES << "\n";
            resultFile << "=====================================\n\n";
            
            resultFile << "IN_MEMORY:\n";
            resultFile << "  Insert time: " << inMemoryResult.insert_time_ms << " ms\n";
            resultFile << "  Query time: " << inMemoryResult.query_time_ms << " ms\n";
            resultFile << "  Memory: " << (inMemoryResult.memory_usage_bytes / 1024.0 / 1024.0) << " MB\n\n";
            
            resultFile << "MMAP:\n";
            resultFile << "  Insert time: " << mmapResult.insert_time_ms << " ms\n";
            resultFile << "  Query time: " << mmapResult.query_time_ms << " ms\n";
            resultFile << "  Snapshot time: " << mmapResult.snapshot_time_ms << " ms\n";
            resultFile << "  Memory: " << (mmapResult.memory_usage_bytes / 1024.0 / 1024.0) << " MB\n\n";
            
            resultFile << "FILE_IO:\n";
            resultFile << "  Insert time: " << fileIOResult.insert_time_ms << " ms\n";
            resultFile << "  Query time: " << fileIOResult.query_time_ms << " ms\n";
            resultFile << "  Snapshot time: " << fileIOResult.snapshot_time_ms << " ms\n";
            resultFile << "  Memory: " << (fileIOResult.memory_usage_bytes / 1024.0 / 1024.0) << " MB\n\n";
            
            resultFile << "Performance Comparison:\n";
            resultFile << "MMAP vs IN_MEMORY:\n";
            resultFile << "  Insert overhead: " << std::fixed << std::setprecision(1)
                       << ((mmapResult.insert_time_ms / inMemoryResult.insert_time_ms - 1) * 100) << "%\n";
            resultFile << "  Query overhead: " 
                       << ((mmapResult.query_time_ms / inMemoryResult.query_time_ms - 1) * 100) << "%\n\n";
            
            resultFile << "FILE_IO vs IN_MEMORY:\n";
            resultFile << "  Insert overhead: " 
                       << ((fileIOResult.insert_time_ms / inMemoryResult.insert_time_ms - 1) * 100) << "%\n";
            resultFile << "  Query overhead: " 
                       << ((fileIOResult.query_time_ms / inMemoryResult.query_time_ms - 1) * 100) << "%\n\n";
            
            resultFile << "MMAP vs FILE_IO:\n";
            resultFile << "  Insert speedup: " 
                       << ((fileIOResult.insert_time_ms / mmapResult.insert_time_ms - 1) * 100) << "%\n";
            resultFile << "  Query speedup: " 
                       << ((fileIOResult.query_time_ms / mmapResult.query_time_ms - 1) * 100) << "%\n";
            resultFile << "  Snapshot speedup: " 
                       << ((fileIOResult.snapshot_time_ms / mmapResult.snapshot_time_ms - 1) * 100) << "%\n";
            
            resultFile.close();
            std::cout << "\nBenchmark results saved to: cow_backend_benchmark_results.txt\n";
        }
    }
}

