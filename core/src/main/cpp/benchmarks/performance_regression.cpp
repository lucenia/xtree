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
#include <chrono>
#include <fstream>
#include <map>
#include <iomanip>
#include <sstream>
#include <cmath>
#include <ctime>
#include <algorithm>
#include "../src/indexdetails.hpp"
#include "../src/xtree.h"
#include "../src/xtree.hpp"
#include "../src/xtiter.h"
#include "../src/keymbr.h"
#include "../src/memmgr/cow_memmgr.hpp"
#include "../src/memmgr/page_write_tracker.hpp"

using namespace xtree;
using namespace std;

/**
 * Performance Regression Test Suite
 * 
 * This test suite tracks performance metrics over time and can detect
 * regressions by comparing against baseline measurements.
 * 
 * Baseline file format (JSON):
 * {
 *   "timestamp": "2024-01-15T10:30:00Z",
 *   "commit": "abc123",
 *   "metrics": {
 *     "spatial_queries_per_sec": 333333,
 *     "bulk_inserts_per_sec": 9603,
 *     "mbr_expand_ops_per_ms": 454339,
 *     "cow_snapshot_us": 177
 *   }
 * }
 */

class PerformanceRegressionTest : public ::testing::Test {
protected:
    struct PerformanceMetrics {
        double spatial_queries_per_sec;
        double bulk_inserts_per_sec;
        double mbr_expand_ops_per_ms;
        double mbr_intersect_ops_per_ms;
        double cow_snapshot_us;
        double page_write_tracking_ops_per_ms;
        
        // Calculate percentage difference
        double diff_percent(double baseline, double current) const {
            if (baseline == 0) return 0;
            return ((current - baseline) / baseline) * 100.0;
        }
    };
    
    static constexpr double REGRESSION_THRESHOLD = 10.0; // 10% regression is significant
    static constexpr double IMPROVEMENT_THRESHOLD = 20.0; // 20% improvement is noteworthy
    
    IndexDetails<DataRecord>* index;
    vector<const char*>* dimLabels;
    string baseline_file = "benchmarks/performance_baseline.json";
    
    void SetUp() override {
        dimLabels = new vector<const char*>;
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        
        index = new IndexDetails<DataRecord>(
            2, 32, dimLabels, 1024*1024*10, nullptr, nullptr
        );
    }
    
    void TearDown() override {
        delete index;
        delete dimLabels;
    }
    
    // Measure spatial query performance
    double measureSpatialQueries() {
        // Create and populate tree
        XTreeBucket<DataRecord>* root = new XTreeBucket<DataRecord>(index, true);
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress(reinterpret_cast<long>(cachedRoot));
        
        // Insert test data in a grid pattern
        const int GRID_SIZE = 100;
        for (int x = 0; x < GRID_SIZE; x++) {
            for (int y = 0; y < GRID_SIZE; y++) {
                DataRecord* dr = new DataRecord(2, 32, "p_" + to_string(x) + "_" + to_string(y));
                vector<double> point = {(double)x, (double)y};
                dr->putPoint(&point);
                root->xt_insert(cachedRoot, dr);
            }
        }
        
        // Perform queries
        const int NUM_QUERIES = 1000;
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < NUM_QUERIES; i++) {
            // Query different regions
            double x = (i % 10) * 10.0;
            double y = (i / 10) * 10.0;
            
            // Create search query as DataRecord
            DataRecord* searchQuery = new DataRecord(2, 32, "search_query");
            vector<double> p1 = {x, y};
            vector<double> p2 = {x + 5.0, y + 5.0};
            searchQuery->putPoint(&p1);
            searchQuery->putPoint(&p2);
            
            auto iter = root->getIterator(cachedRoot, searchQuery, 0);
            
            // Iterate through results
            int count = 0;
            while (iter->hasNext()) {
                iter->next();
                count++;
            }
            delete iter;
            delete searchQuery;
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        // Clean up
        index->clearCache();
        
        return (NUM_QUERIES * 1000.0) / duration_ms; // queries per second
    }
    
    // Measure bulk insertion performance
    double measureBulkInserts() {
        XTreeBucket<DataRecord>* root = new XTreeBucket<DataRecord>(index, true);
        auto* cachedRoot = index->getCache().add(index->getNextNodeID(), root);
        index->setRootAddress(reinterpret_cast<long>(cachedRoot));
        
        const int NUM_INSERTS = 10000;
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < NUM_INSERTS; i++) {
            DataRecord* dr = new DataRecord(2, 32, "rec_" + to_string(i));
            vector<double> point = {(double)(i % 1000), (double)(i / 1000)};
            dr->putPoint(&point);
            root->xt_insert(cachedRoot, dr);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        // Clean up
        index->clearCache();
        
        return (NUM_INSERTS * 1000.0) / duration_ms; // inserts per second
    }
    
    // Measure MBR operations performance
    pair<double, double> measureMBROperations() {
        const int NUM_OPS = 1000000;
        KeyMBR mbr1(2, 32);
        KeyMBR mbr2(2, 32);
        
        // Initialize MBRs
        vector<double> p1 = {0.0, 0.0};
        vector<double> p2 = {10.0, 10.0};
        vector<double> p3 = {5.0, 5.0};
        vector<double> p4 = {15.0, 15.0};
        mbr1.expandWithPoint(&p1);
        mbr1.expandWithPoint(&p2);
        mbr2.expandWithPoint(&p3);
        mbr2.expandWithPoint(&p4);
        
        // Measure expand operations
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < NUM_OPS; i++) {
            KeyMBR temp(mbr1);
            temp.expand(mbr2);
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto expand_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        double expand_ops_per_ms = (NUM_OPS * 1000.0) / expand_us;
        
        // Measure intersect operations
        start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < NUM_OPS; i++) {
            bool result = mbr1.intersects(mbr2);
            (void)result; // Avoid unused variable warning
        }
        end = std::chrono::high_resolution_clock::now();
        auto intersect_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        double intersect_ops_per_ms = (NUM_OPS * 1000.0) / intersect_us;
        
        return {expand_ops_per_ms, intersect_ops_per_ms};
    }
    
    // Measure COW snapshot performance
    double measureCOWSnapshot() {
        // Create COW-enabled index
        IndexDetails<DataRecord> cow_index(
            2, 32, dimLabels, 1024*1024*10, nullptr, nullptr,
            true, "test_perf.snapshot"
        );
        
        XTreeBucket<DataRecord>* root = cow_index.getCOWAllocator()->allocate_bucket(&cow_index, true);
        auto* cachedRoot = cow_index.getCache().add(cow_index.getNextNodeID(), root);
        cow_index.setRootAddress(reinterpret_cast<long>(cachedRoot));
        
        // Insert some data
        for (int i = 0; i < 1000; i++) {
            DataRecord* dr = new DataRecord(2, 32, "rec_" + to_string(i));
            vector<double> point = {(double)i, (double)i};
            dr->putPoint(&point);
            root->xt_insert(cachedRoot, dr);
        }
        
        // Measure snapshot creation time
        auto start = std::chrono::high_resolution_clock::now();
        cow_index.getCOWManager()->trigger_memory_snapshot();
        auto end = std::chrono::high_resolution_clock::now();
        
        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        
        // Clean up
        cow_index.clearCache();
        remove("test_perf.snapshot");
        
        return (double)duration_us;
    }
    
    // Measure PageWriteTracker performance
    double measurePageWriteTracker() {
        PageWriteTracker tracker;
        const int NUM_OPS = 1000000;
        
        // Allocate some pages
        vector<void*> pages;
        for (int i = 0; i < 100; i++) {
            pages.push_back(reinterpret_cast<void*>(0x1000 + i * 4096));
        }
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Perform mixed operations
        for (int i = 0; i < NUM_OPS; i++) {
            void* page = pages[i % pages.size()];
            if (i % 3 == 0) {
                tracker.record_write(page);
            } else {
                tracker.record_access(page);
            }
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        
        return (NUM_OPS * 1000.0) / duration_us; // ops per millisecond
    }
    
    // Run all performance measurements
    PerformanceMetrics measureAll() {
        PerformanceMetrics metrics;
        
        cout << "Running performance measurements...\n";
        
        // Run each test multiple times and take the median
        const int RUNS = 3;
        vector<double> spatial_results, insert_results, expand_results, intersect_results, cow_results, tracker_results;
        
        for (int run = 0; run < RUNS; run++) {
            cout << "  Run " << (run + 1) << "/" << RUNS << "...\n";
            
            spatial_results.push_back(measureSpatialQueries());
            insert_results.push_back(measureBulkInserts());
            
            auto [expand, intersect] = measureMBROperations();
            expand_results.push_back(expand);
            intersect_results.push_back(intersect);
            
            cow_results.push_back(measureCOWSnapshot());
            tracker_results.push_back(measurePageWriteTracker());
        }
        
        // Take median of results
        auto getMedian = [](vector<double>& v) {
            sort(v.begin(), v.end());
            return v[v.size() / 2];
        };
        
        metrics.spatial_queries_per_sec = getMedian(spatial_results);
        metrics.bulk_inserts_per_sec = getMedian(insert_results);
        metrics.mbr_expand_ops_per_ms = getMedian(expand_results);
        metrics.mbr_intersect_ops_per_ms = getMedian(intersect_results);
        metrics.cow_snapshot_us = getMedian(cow_results);
        metrics.page_write_tracking_ops_per_ms = getMedian(tracker_results);
        
        return metrics;
    }
    
    // Load baseline metrics from file
    bool loadBaseline(PerformanceMetrics& baseline) {
        ifstream file(baseline_file);
        if (!file.is_open()) {
            return false;
        }
        
        // Simple parsing - in production, use a JSON library
        string line;
        while (getline(file, line)) {
            if (line.find("spatial_queries_per_sec") != string::npos) {
                sscanf(line.c_str(), "    \"spatial_queries_per_sec\": %lf,", &baseline.spatial_queries_per_sec);
            } else if (line.find("bulk_inserts_per_sec") != string::npos) {
                sscanf(line.c_str(), "    \"bulk_inserts_per_sec\": %lf,", &baseline.bulk_inserts_per_sec);
            } else if (line.find("mbr_expand_ops_per_ms") != string::npos) {
                sscanf(line.c_str(), "    \"mbr_expand_ops_per_ms\": %lf,", &baseline.mbr_expand_ops_per_ms);
            } else if (line.find("mbr_intersect_ops_per_ms") != string::npos) {
                sscanf(line.c_str(), "    \"mbr_intersect_ops_per_ms\": %lf,", &baseline.mbr_intersect_ops_per_ms);
            } else if (line.find("cow_snapshot_us") != string::npos) {
                sscanf(line.c_str(), "    \"cow_snapshot_us\": %lf,", &baseline.cow_snapshot_us);
            } else if (line.find("page_write_tracking_ops_per_ms") != string::npos) {
                sscanf(line.c_str(), "    \"page_write_tracking_ops_per_ms\": %lf", &baseline.page_write_tracking_ops_per_ms);
            }
        }
        
        return true;
    }
    
    // Save metrics as new baseline
    void saveBaseline(const PerformanceMetrics& metrics) {
        ofstream file(baseline_file);
        
        // Get current timestamp
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        
        file << "{\n";
        file << "  \"timestamp\": \"" << std::put_time(gmtime(&time_t), "%Y-%m-%dT%H:%M:%SZ") << "\",\n";
        file << "  \"metrics\": {\n";
        file << "    \"spatial_queries_per_sec\": " << std::fixed << std::setprecision(1) << metrics.spatial_queries_per_sec << ",\n";
        file << "    \"bulk_inserts_per_sec\": " << metrics.bulk_inserts_per_sec << ",\n";
        file << "    \"mbr_expand_ops_per_ms\": " << metrics.mbr_expand_ops_per_ms << ",\n";
        file << "    \"mbr_intersect_ops_per_ms\": " << metrics.mbr_intersect_ops_per_ms << ",\n";
        file << "    \"cow_snapshot_us\": " << metrics.cow_snapshot_us << ",\n";
        file << "    \"page_write_tracking_ops_per_ms\": " << metrics.page_write_tracking_ops_per_ms << "\n";
        file << "  }\n";
        file << "}\n";
    }
};

TEST_F(PerformanceRegressionTest, DISABLED_CheckPerformanceRegression) {
    // Measure current performance
    PerformanceMetrics current = measureAll();
    
    // Try to load baseline
    PerformanceMetrics baseline;
    bool has_baseline = loadBaseline(baseline);
    
    // Output current metrics
    cout << "\n=== Current Performance Metrics ===\n";
    cout << "Spatial queries: " << std::fixed << std::setprecision(0) << current.spatial_queries_per_sec << " queries/sec\n";
    cout << "Bulk inserts: " << current.bulk_inserts_per_sec << " inserts/sec\n";
    cout << "MBR expand: " << current.mbr_expand_ops_per_ms << " ops/ms\n";
    cout << "MBR intersect: " << current.mbr_intersect_ops_per_ms << " ops/ms\n";
    cout << "COW snapshot: " << current.cow_snapshot_us << " microseconds\n";
    cout << "Page write tracking: " << current.page_write_tracking_ops_per_ms << " ops/ms\n";
    
    if (!has_baseline) {
        cout << "\nNo baseline found. Saving current metrics as baseline.\n";
        saveBaseline(current);
        GTEST_SKIP() << "No baseline to compare against. Current metrics saved as baseline.";
    }
    
    // Compare against baseline
    cout << "\n=== Performance Comparison ===\n";
    
    auto checkMetric = [&](const string& name, double baseline_val, double current_val) {
        double diff = current.diff_percent(baseline_val, current_val);
        cout << name << ": " << std::fixed << std::setprecision(1) << diff << "% ";
        
        if (diff < -REGRESSION_THRESHOLD) {
            cout << "(REGRESSION!)\n";
            ADD_FAILURE() << name << " regressed by " << -diff << "% (baseline: " 
                         << baseline_val << ", current: " << current_val << ")";
        } else if (diff > IMPROVEMENT_THRESHOLD) {
            cout << "(improvement)\n";
        } else {
            cout << "(stable)\n";
        }
    };
    
    checkMetric("Spatial queries", baseline.spatial_queries_per_sec, current.spatial_queries_per_sec);
    checkMetric("Bulk inserts", baseline.bulk_inserts_per_sec, current.bulk_inserts_per_sec);
    checkMetric("MBR expand", baseline.mbr_expand_ops_per_ms, current.mbr_expand_ops_per_ms);
    checkMetric("MBR intersect", baseline.mbr_intersect_ops_per_ms, current.mbr_intersect_ops_per_ms);
    checkMetric("COW snapshot", baseline.cow_snapshot_us, current.cow_snapshot_us);
    checkMetric("Page tracking", baseline.page_write_tracking_ops_per_ms, current.page_write_tracking_ops_per_ms);
    
    // Option to update baseline (controlled by environment variable)
    if (getenv("UPDATE_PERFORMANCE_BASELINE")) {
        cout << "\nUpdating baseline with current metrics.\n";
        saveBaseline(current);
    }
}

// Test to verify performance metrics are reasonable
TEST_F(PerformanceRegressionTest, DISABLED_SanityCheckMetrics) {
    PerformanceMetrics metrics = measureAll();
    
    // Sanity checks - these should always pass unless something is very wrong
    EXPECT_GT(metrics.spatial_queries_per_sec, 1000) << "Spatial queries too slow";
    EXPECT_GT(metrics.bulk_inserts_per_sec, 1000) << "Bulk inserts too slow";
    EXPECT_GT(metrics.mbr_expand_ops_per_ms, 10000) << "MBR expand too slow";
    EXPECT_GT(metrics.mbr_intersect_ops_per_ms, 10000) << "MBR intersect too slow";
    EXPECT_LT(metrics.cow_snapshot_us, 10000) << "COW snapshot too slow (>10ms)";
    EXPECT_GT(metrics.page_write_tracking_ops_per_ms, 1000) << "Page tracking too slow";
}