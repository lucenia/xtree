/*
 * SPDX-License-Identifier: SSPL-1.0
 *
 * The Lucenia project is source-available software: you can
 * redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 *
 * As per the terms of the Server Side Public License, if you
 * make the functionality of this program or a modified version
 * available over a network, you must make the source code
 * available for download.
 *
 * The full text of the Server Side Public License, version 1,
 * can be found at:
 * https://www.mongodb.com/licensing/server-side-public-license
 */

#include <gtest/gtest.h>
#include <filesystem>
#include "../src/xtree_serialization.h"
#include "../src/mmapfile.h"
#include "../src/xtree.h"
#include "../src/util/log.h"

using namespace xtree;
using namespace std;

class DataRecordSerializationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create unique test directory
        test_dir_ = std::filesystem::temp_directory_path() / 
                   ("datarecord_test_" + to_string(getpid()) + "_" + to_string(time(nullptr)));
        std::filesystem::create_directories(test_dir_);
        
        tree_file_path_ = test_dir_ / "test.xtree";
        data_file_path_ = test_dir_ / "test.xdata";
    }
    
    void TearDown() override {
        // Clean up test files
        std::filesystem::remove_all(test_dir_);
    }
    
    std::filesystem::path test_dir_;
    std::filesystem::path tree_file_path_;
    std::filesystem::path data_file_path_;
};

// Test basic DataRecord serialization
TEST_F(DataRecordSerializationTest, BasicSerializationTest) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    // Create a simple DataRecord
    DataRecord record(2, 32, "test_row_001");
    
    // Add some points
    vector<double> point1 = {10.5, 20.5};
    vector<double> point2 = {15.5, 25.5};
    vector<double> point3 = {12.5, 22.5};
    
    record.putPoint(&point1);
    record.putPoint(&point2);
    record.putPoint(&point3);
    
    // Serialize the record
    uint64_t offset = serializer.serializeDataRecord(&record);
    EXPECT_GT(offset, 0);
    
    // Force sync to ensure data is written
    data_file->sync();
    
    // Deserialize and verify
    DataRecord* deserialized = serializer.deserializeDataRecord(offset);
    ASSERT_NE(deserialized, nullptr);
    
    // Verify basic properties
    EXPECT_EQ(deserialized->getRowID(), "test_row_001");
    
    // Verify points
    auto points = deserialized->getPoints();
    EXPECT_EQ(points.size(), 3);
    
    // Check first point
    EXPECT_DOUBLE_EQ(points[0][0], 10.5);
    EXPECT_DOUBLE_EQ(points[0][1], 20.5);
    
    // Check KeyMBR
    KeyMBR* key = deserialized->getKey();
    ASSERT_NE(key, nullptr);
    EXPECT_EQ(key->getDimensionCount(), 2);
    
    // MBR should encompass all points
    EXPECT_LE(key->getMin(0), 10.5);
    EXPECT_GE(key->getMax(0), 15.5);
    EXPECT_LE(key->getMin(1), 20.5);
    EXPECT_GE(key->getMax(1), 25.5);
    
    delete deserialized;
}

// Test empty DataRecord
TEST_F(DataRecordSerializationTest, EmptyRecordTest) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    // Create empty DataRecord
    DataRecord record(3, 32, "empty_record");
    
    // Serialize without adding points
    uint64_t offset = serializer.serializeDataRecord(&record);
    EXPECT_GT(offset, 0);
    
    data_file->sync();
    
    // Deserialize and verify
    DataRecord* deserialized = serializer.deserializeDataRecord(offset);
    ASSERT_NE(deserialized, nullptr);
    
    EXPECT_EQ(deserialized->getRowID(), "empty_record");
    EXPECT_EQ(deserialized->getPoints().size(), 0);
    
    delete deserialized;
}

// Test large DataRecord with many points
TEST_F(DataRecordSerializationTest, LargeRecordTest) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 10*1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 10*1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    // Create DataRecord with many points
    DataRecord record(3, 32, "large_record_with_many_points");
    
    // Add 1000 points
    for (int i = 0; i < 1000; ++i) {
        vector<double> point = {
            static_cast<double>(i),
            static_cast<double>(i * 2),
            static_cast<double>(i * 3)
        };
        record.putPoint(&point);
    }
    
    // Serialize
    uint64_t offset = serializer.serializeDataRecord(&record);
    EXPECT_GT(offset, 0);
    
    data_file->sync();
    
    // Deserialize and verify
    DataRecord* deserialized = serializer.deserializeDataRecord(offset);
    ASSERT_NE(deserialized, nullptr);
    
    EXPECT_EQ(deserialized->getRowID(), "large_record_with_many_points");
    
    auto points = deserialized->getPoints();
    EXPECT_EQ(points.size(), 1000);
    
    // Spot check some points
    EXPECT_DOUBLE_EQ(points[0][0], 0.0);
    EXPECT_DOUBLE_EQ(points[999][2], 2997.0);
    
    // Check MBR
    KeyMBR* key = deserialized->getKey();
    ASSERT_NE(key, nullptr);
    EXPECT_DOUBLE_EQ(key->getMin(0), 0.0);
    EXPECT_DOUBLE_EQ(key->getMax(0), 999.0);
    
    delete deserialized;
}

// Test multiple records
TEST_F(DataRecordSerializationTest, MultipleRecordsTest) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    vector<uint64_t> offsets;
    
    // Create and serialize multiple records
    for (int i = 0; i < 10; ++i) {
        DataRecord record(2, 32, "record_" + to_string(i));
        
        // Add a point unique to each record
        vector<double> point = {static_cast<double>(i * 10), static_cast<double>(i * 20)};
        record.putPoint(&point);
        
        uint64_t offset = serializer.serializeDataRecord(&record);
        EXPECT_GT(offset, 0);
        offsets.push_back(offset);
    }
    
    data_file->sync();
    
    // Deserialize and verify each record
    for (int i = 0; i < 10; ++i) {
        DataRecord* deserialized = serializer.deserializeDataRecord(offsets[i]);
        ASSERT_NE(deserialized, nullptr);
        
        EXPECT_EQ(deserialized->getRowID(), "record_" + to_string(i));
        
        auto points = deserialized->getPoints();
        EXPECT_EQ(points.size(), 1);
        EXPECT_DOUBLE_EQ(points[0][0], i * 10);
        EXPECT_DOUBLE_EQ(points[0][1], i * 20);
        
        delete deserialized;
    }
}

// Test error handling
TEST_F(DataRecordSerializationTest, ErrorHandlingTest) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    // Test null record
    EXPECT_EQ(serializer.serializeDataRecord(nullptr), 0);
    
    // Test invalid offset
    EXPECT_EQ(serializer.deserializeDataRecord(0), nullptr);
    EXPECT_EQ(serializer.deserializeDataRecord(999999), nullptr);
}

// Test performance of serialization
TEST_F(DataRecordSerializationTest, PerformanceTest) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 50*1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 50*1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    const int num_records = 1000;
    vector<uint64_t> offsets;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Serialize many records
    for (int i = 0; i < num_records; ++i) {
        DataRecord record(3, 32, "perf_record_" + to_string(i));
        
        // Add 10 points per record
        for (int j = 0; j < 10; ++j) {
            vector<double> point = {
                static_cast<double>(i + j),
                static_cast<double>(i * j),
                static_cast<double>(i - j)
            };
            record.putPoint(&point);
        }
        
        uint64_t offset = serializer.serializeDataRecord(&record);
        offsets.push_back(offset);
    }
    
    auto serialize_time = std::chrono::high_resolution_clock::now();
    
    // Deserialize all records
    for (uint64_t offset : offsets) {
        DataRecord* deserialized = serializer.deserializeDataRecord(offset);
        EXPECT_NE(deserialized, nullptr);
        delete deserialized;
    }
    
    auto deserialize_time = std::chrono::high_resolution_clock::now();
    
    auto serialize_duration = std::chrono::duration_cast<std::chrono::milliseconds>(serialize_time - start_time);
    auto deserialize_duration = std::chrono::duration_cast<std::chrono::milliseconds>(deserialize_time - serialize_time);
    
#ifdef _DEBUG
    log() << "[PERF] Serialized " << num_records << " DataRecords in " << serialize_duration.count() << "ms" << endl;
    log() << "[PERF] Deserialized " << num_records << " DataRecords in " << deserialize_duration.count() << "ms" << endl;
#endif
    
    // Performance expectations
    EXPECT_LT(serialize_duration.count(), 5000);   // Under 5 seconds
    EXPECT_LT(deserialize_duration.count(), 3000); // Under 3 seconds
}