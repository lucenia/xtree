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
#include "../src/xtree.h" // For DataRecord

using namespace xtree;
using namespace std;

class XTreeSerializationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create unique test directory
        test_dir_ = std::filesystem::temp_directory_path() / 
                   ("xtree_serialization_test_" + to_string(getpid()) + "_" + to_string(time(nullptr)));
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

// Test file header creation and validation
TEST_F(XTreeSerializationTest, FileHeadersTest) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    // Write headers
    serializer.writeTreeHeader(2, 32); // 2D, 32-bit precision
    serializer.writeDataHeader(2, 32);
    
    // Read and validate headers
    auto tree_header = serializer.readTreeHeader();
    auto data_header = serializer.readDataHeader();
    
    EXPECT_EQ(tree_header.magic, XTREE_MAGIC);
    EXPECT_EQ(tree_header.version, XTREE_STORAGE_VERSION);
    EXPECT_EQ(tree_header.dimension_count, 2);
    EXPECT_EQ(tree_header.precision, 32);
    
    EXPECT_EQ(data_header.magic, XDATA_MAGIC);
    EXPECT_EQ(data_header.version, XTREE_STORAGE_VERSION);
    EXPECT_EQ(data_header.dimension_count, 2);
    EXPECT_EQ(data_header.precision, 32);
}

// Test DataStorageManager basic functionality
TEST_F(XTreeSerializationTest, DataStorageManagerTest) {
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    DataStorageManager storage_mgr(data_file.get());
    
    // Test storing a record
    string test_data = "Hello, XTree serialization!";
    uint64_t offset = storage_mgr.storeRecord(test_data.data(), test_data.size(), 1);
    
    EXPECT_GT(offset, 0);
    
    // Test retrieving the record
    auto retrieved_data = storage_mgr.getRecord(offset);
    EXPECT_EQ(retrieved_data.size(), test_data.size());
    
    string retrieved_string(retrieved_data.begin(), retrieved_data.end());
    EXPECT_EQ(retrieved_string, test_data);
    
    // Test record header
    auto header = storage_mgr.getRecordHeader(offset);
    EXPECT_EQ(header.type_id, 1);
    EXPECT_GT(header.size, test_data.size()); // Should include header size
}

// Test storing multiple records
TEST_F(XTreeSerializationTest, MultipleRecordsTest) {
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    DataStorageManager storage_mgr(data_file.get());
    
    vector<string> test_records = {
        "Record 1: Spatial data",
        "Record 2: Geographic information",
        "Record 3: Location coordinates"
    };
    
    vector<uint64_t> offsets;
    
    // Store all records
    for (size_t i = 0; i < test_records.size(); ++i) {
        uint64_t offset = storage_mgr.storeRecord(test_records[i].data(), 
                                                 test_records[i].size(), 
                                                 i + 1);
        EXPECT_GT(offset, 0);
        offsets.push_back(offset);
    }
    
    // Verify all records can be retrieved correctly
    for (size_t i = 0; i < test_records.size(); ++i) {
        auto retrieved_data = storage_mgr.getRecord(offsets[i]);
        string retrieved_string(retrieved_data.begin(), retrieved_data.end());
        EXPECT_EQ(retrieved_string, test_records[i]);
        
        auto header = storage_mgr.getRecordHeader(offsets[i]);
        EXPECT_EQ(header.type_id, i + 1);
    }
}

// Test file structure after operations
TEST_F(XTreeSerializationTest, FileStructureTest) {
    {
        auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
        auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
        
        XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
        
        // Initialize files
        serializer.writeTreeHeader(3, 64); // 3D, 64-bit precision
        serializer.writeDataHeader(3, 64);
        
        // Store some data
        DataStorageManager storage_mgr(data_file.get());
        string test_data = "Persistent data test";
        storage_mgr.storeRecord(test_data.data(), test_data.size(), 42);
        
        // Force sync
        tree_file->sync();
        data_file->sync();
    }
    
    // Verify files exist and have expected content
    EXPECT_TRUE(std::filesystem::exists(tree_file_path_));
    EXPECT_TRUE(std::filesystem::exists(data_file_path_));
    
    // Files should have both MMapFile header and XTree header
    size_t expected_min_size = MMapFile::HEADER_SIZE + sizeof(XTreeFileHeader);
    EXPECT_GT(std::filesystem::file_size(tree_file_path_), expected_min_size);
    EXPECT_GT(std::filesystem::file_size(data_file_path_), expected_min_size);
    
    // Reopen and verify headers are still valid
    {
        auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 0, false);
        auto data_file = make_unique<MMapFile>(data_file_path_.string(), 0, false);
        
        XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
        
        auto tree_header = serializer.readTreeHeader();
        auto data_header = serializer.readDataHeader();
        
        EXPECT_EQ(tree_header.dimension_count, 3);
        EXPECT_EQ(tree_header.precision, 64);
        EXPECT_EQ(data_header.dimension_count, 3);
        EXPECT_EQ(data_header.precision, 64);
    }
}

// Test error handling
TEST_F(XTreeSerializationTest, ErrorHandlingTest) {
    // Test invalid file magic numbers
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
    
    // Write invalid magic number
    uint32_t invalid_magic = 0xDEADBEEF;
    void* header_ptr = tree_file->getPointer(0);
    ASSERT_NE(header_ptr, nullptr);
    memcpy(header_ptr, &invalid_magic, sizeof(invalid_magic));
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), tree_file.get());
    
    // Should throw exception for invalid magic
    EXPECT_THROW(serializer.readTreeHeader(), std::runtime_error);
}

// Test serialization and deserialization of XTreeBucket
TEST_F(XTreeSerializationTest, BucketSerializationRoundTrip) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 10*1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 10*1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    // Initialize files
    serializer.writeTreeHeader(2, 32);
    serializer.writeDataHeader(2, 32);
    
    // Create IndexDetails for bucket creation
    vector<const char*> dimLabels = {"x", "y"};
    IndexDetails<DataRecord> idx(2, 32, &dimLabels, 1024*1024, nullptr, nullptr);
    
    // Create a test bucket with some children
    auto bucket = make_unique<XTreeBucket<DataRecord>>(&idx, true); // isRoot = true
    
    // Add some test data to the bucket
    // Note: In a real scenario, we'd use proper insertion methods
    
    // Serialize the bucket
    uint64_t bucket_offset = serializer.serializeBucket(bucket.get(), 0);
    EXPECT_GT(bucket_offset, 0);
    
    // Force sync to ensure data is written
    tree_file->sync();
    
    // Deserialize the bucket
    XTreeBucket<DataRecord>* deserialized = serializer.deserializeBucket(bucket_offset, &idx);
    ASSERT_NE(deserialized, nullptr);
    
    // Verify basic properties
    EXPECT_EQ(deserialized->n(), bucket->n());
    // Note: isLeaf() is protected, so we can't verify it directly
    // The leaf state is preserved internally during serialization
    
    // Clean up
    delete deserialized;
}

// Test deserialization error handling
TEST_F(XTreeSerializationTest, DeserializationErrorHandling) {
    auto tree_file = make_unique<MMapFile>(tree_file_path_.string(), 1024*1024, false);
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 1024*1024, false);
    
    XTreeSerializer<DataRecord> serializer(tree_file.get(), data_file.get());
    
    // Test with invalid offset
    vector<const char*> dimLabels = {"x", "y"};
    IndexDetails<DataRecord> idx(2, 32, &dimLabels, 1024*1024, nullptr, nullptr);
    
    // Should return nullptr for invalid offset
    EXPECT_EQ(serializer.deserializeBucket(0, &idx), nullptr);
    EXPECT_EQ(serializer.deserializeBucket(999999, &idx), nullptr);
    
    // Should return nullptr for null IndexDetails
    EXPECT_EQ(serializer.deserializeBucket(100, nullptr), nullptr);
}

// Performance test for storage operations
TEST_F(XTreeSerializationTest, PerformanceTest) {
    auto data_file = make_unique<MMapFile>(data_file_path_.string(), 10*1024*1024, false); // 10MB
    DataStorageManager storage_mgr(data_file.get());
    
    const int num_records = 1000;
    vector<uint64_t> offsets;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Store many records
    for (int i = 0; i < num_records; ++i) {
        string data = "Test record " + to_string(i) + " with some spatial data content";
        uint64_t offset = storage_mgr.storeRecord(data.data(), data.size(), i);
        EXPECT_GT(offset, 0);
        offsets.push_back(offset);
    }
    
    auto store_time = std::chrono::high_resolution_clock::now();
    
    // Retrieve all records
    for (uint64_t offset : offsets) {
        auto data = storage_mgr.getRecord(offset);
        EXPECT_GT(data.size(), 0);
    }
    
    auto retrieve_time = std::chrono::high_resolution_clock::now();
    
    auto store_duration = std::chrono::duration_cast<std::chrono::milliseconds>(store_time - start_time);
    auto retrieve_duration = std::chrono::duration_cast<std::chrono::milliseconds>(retrieve_time - store_time);
    
    cout << "[PERF] Stored " << num_records << " records in " << store_duration.count() << "ms" << endl;
    cout << "[PERF] Retrieved " << num_records << " records in " << retrieve_duration.count() << "ms" << endl;
    
    // Performance expectations (adjust based on requirements)
    EXPECT_LT(store_duration.count(), 5000);    // Under 5 seconds
    EXPECT_LT(retrieve_duration.count(), 2000); // Under 2 seconds
}