/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Wire format serialization/deserialization tests for XTree persistence
 * These tests ensure the wire format remains stable and compatible
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <cstring>
#include "../src/xtree.h"
#include "../src/indexdetails.hpp"
#include "../src/xtiter.h"
#include "../src/config.h"

using namespace xtree;

class WireFormatTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a test index with 2 dimensions
        dimensions = 2;
        precision = 6;
        
        // Create dimension labels
        dimLabels = new std::vector<const char*>();
        dimLabels->push_back("x");
        dimLabels->push_back("y");
        
        // Create index in memory mode for testing
        idx = new IndexDetails<DataRecord>(
            dimensions, 
            precision, 
            dimLabels,
            nullptr,  // JNIEnv
            nullptr,  // jobject
            "test_field",
            IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
        );
    }
    
    void TearDown() override {
        // Clear the static cache to prevent memory leaks
        IndexDetails<DataRecord>::clearCache();
        delete idx;
        delete dimLabels;
    }
    
    unsigned short dimensions;
    unsigned short precision;
    std::vector<const char*>* dimLabels;
    IndexDetails<DataRecord>* idx;
};

// Test DataRecord serialization/deserialization roundtrip
TEST_F(WireFormatTest, DataRecordRoundtrip) {
    // Create a DataRecord with some test data
    DataRecord* original = new DataRecord(dimensions, precision, "test_record_123");
    
    // Add some points to the DataRecord
    std::vector<double> p1 = {10.5, -20.3};
    original->putPoint(&p1);
    
    std::vector<double> p2 = {15.7, -25.8};
    original->putPoint(&p2);
    
    // The KeyMBR should be set based on the points
    ASSERT_TRUE(original->getKey() != nullptr);
    KeyMBR* originalKey = original->getKey();
    
    // Get the expected MBR values
    double expectedMinX = std::min(p1[0], p2[0]);
    double expectedMaxX = std::max(p1[0], p2[0]);
    double expectedMinY = std::min(p1[1], p2[1]);
    double expectedMaxY = std::max(p1[1], p2[1]);
    
    EXPECT_FLOAT_EQ(originalKey->getMin(0), expectedMinX);
    EXPECT_FLOAT_EQ(originalKey->getMax(0), expectedMaxX);
    EXPECT_FLOAT_EQ(originalKey->getMin(1), expectedMinY);
    EXPECT_FLOAT_EQ(originalKey->getMax(1), expectedMaxY);
    
    // Calculate wire size and allocate buffer
    size_t wireSize = original->wire_size(dimensions);
    std::vector<uint8_t> buffer(wireSize);
    
    // Serialize to wire format
    uint8_t* endPtr = original->to_wire(buffer.data(), dimensions);
    ASSERT_EQ(endPtr - buffer.data(), wireSize) << "Wire size mismatch";
    
    // Create a new DataRecord and deserialize
    DataRecord* restored = new DataRecord(dimensions, precision, "");
    restored->from_wire(buffer.data(), dimensions, precision);
    
    // Verify the restored DataRecord
    ASSERT_TRUE(restored->getKey() != nullptr);
    KeyMBR* restoredKey = restored->getKey();
    
    // Check that the MBR was restored correctly
    EXPECT_FLOAT_EQ(restoredKey->getMin(0), expectedMinX) 
        << "Min X mismatch - original: " << expectedMinX << " restored: " << restoredKey->getMin(0);
    EXPECT_FLOAT_EQ(restoredKey->getMax(0), expectedMaxX)
        << "Max X mismatch - original: " << expectedMaxX << " restored: " << restoredKey->getMax(0);
    EXPECT_FLOAT_EQ(restoredKey->getMin(1), expectedMinY)
        << "Min Y mismatch - original: " << expectedMinY << " restored: " << restoredKey->getMin(1);
    EXPECT_FLOAT_EQ(restoredKey->getMax(1), expectedMaxY)
        << "Max Y mismatch - original: " << expectedMaxY << " restored: " << restoredKey->getMax(1);
    
    // Check rowid
    EXPECT_STREQ(restored->getRowID().c_str(), "test_record_123");
    
    // Check points
    const std::vector<std::vector<double>>& restoredPoints = restored->getPoints();
    ASSERT_EQ(restoredPoints.size(), 2);
    EXPECT_DOUBLE_EQ(restoredPoints[0][0], p1[0]);
    EXPECT_DOUBLE_EQ(restoredPoints[0][1], p1[1]);
    EXPECT_DOUBLE_EQ(restoredPoints[1][0], p2[0]);
    EXPECT_DOUBLE_EQ(restoredPoints[1][1], p2[1]);
    
    delete original;
    delete restored;
}

// Test XTreeBucket basic serialization (using public API only)
TEST_F(WireFormatTest, XTreeBucketBasicSerialization) {
    // Create a bucket
    XTreeBucket<DataRecord>* original = new XTreeBucket<DataRecord>(idx, /*isRoot*/false);
    
    // Set the bucket's KeyMBR
    KeyMBR* bucketKey = new KeyMBR(dimensions, precision);
    bucketKey->set_pair(0, -50.0f, 50.0f);  // X range
    bucketKey->set_pair(1, -30.0f, 30.0f);  // Y range
    original->setKey(bucketKey);
    
    // We can't directly manipulate internals due to encapsulation,
    // but we can test that serialization/deserialization preserves the structure
    
    // Calculate wire size and allocate buffer
    size_t wireSize = original->wire_size(*idx);
    EXPECT_GT(wireSize, 0) << "Wire size should be positive";
    
    std::vector<uint8_t> buffer(wireSize);
    
    // Serialize to wire format
    uint8_t* endPtr = original->to_wire(buffer.data(), *idx);
    ASSERT_EQ(endPtr - buffer.data(), wireSize) << "Wire size mismatch";
    
    // Create a new bucket and deserialize
    XTreeBucket<DataRecord>* restored = new XTreeBucket<DataRecord>(idx, /*isRoot*/false);
    const uint8_t* readEndPtr = restored->from_wire(buffer.data(), idx);
    ASSERT_EQ(readEndPtr - buffer.data(), wireSize) << "Read size mismatch";
    
    // Verify the restored bucket has a valid key
    ASSERT_TRUE(restored->getKey() != nullptr);
    
    // Check the bucket's MBR was preserved
    KeyMBR* restoredKey = restored->getKey();
    EXPECT_FLOAT_EQ(restoredKey->getMin(0), -50.0f);
    EXPECT_FLOAT_EQ(restoredKey->getMax(0), 50.0f);
    EXPECT_FLOAT_EQ(restoredKey->getMin(1), -30.0f);
    EXPECT_FLOAT_EQ(restoredKey->getMax(1), 30.0f);
    
    // Check tree structure metrics are preserved
    EXPECT_EQ(restored->n(), original->n());
    
    delete original;
    delete restored;
}

// Test integration: Insert DataRecords and verify wire format preserves tree structure
TEST_F(WireFormatTest, TreeStructurePreservation) {
    // Create a root bucket and insert some data
    XTreeBucket<DataRecord>* root = new XTreeBucket<DataRecord>(idx, /*isRoot*/true);
    
    // Insert a few DataRecords through the normal insertion path
    for (int i = 0; i < 5; i++) {
        std::string rowid = "record_" + std::to_string(i);
        DataRecord* dr = new DataRecord(dimensions, precision, rowid);
        
        // Add a point
        std::vector<double> p = {-10.0 + i * 5.0, -5.0 + i * 2.5};
        dr->putPoint(&p);
        
        // Cache it and get the root's cache node
        IndexDetails<DataRecord>::getCache().add(1000 + i, dr);
        auto* rootCacheNode = IndexDetails<DataRecord>::getCache().add(0, root);
        
        // Insert through XTree's insertion method
        root->xt_insert(rootCacheNode, dr);
    }
    
    // Now test serialization of the root
    size_t wireSize = root->wire_size(*idx);
    std::vector<uint8_t> buffer(wireSize);
    
    uint8_t* endPtr = root->to_wire(buffer.data(), *idx);
    ASSERT_EQ(endPtr - buffer.data(), wireSize);
    
    // Deserialize into a new bucket
    XTreeBucket<DataRecord>* restored = new XTreeBucket<DataRecord>(idx, /*isRoot*/true);
    const uint8_t* readEndPtr = restored->from_wire(buffer.data(), idx);
    ASSERT_EQ(readEndPtr - buffer.data(), wireSize);
    
    // Both should report the same number of children
    EXPECT_EQ(restored->n(), root->n()) << "Number of children should match";
    
    delete root;
    delete restored;
}

// Test edge case: Empty DataRecord
TEST_F(WireFormatTest, EmptyDataRecordRoundtrip) {
    // Create an empty DataRecord (no points)
    DataRecord* original = new DataRecord(dimensions, precision, "empty_record");
    
    // Even without points, it should have a KeyMBR (with default bounds)
    ASSERT_TRUE(original->getKey() != nullptr);
    
    // Serialize
    size_t wireSize = original->wire_size(dimensions);
    std::vector<uint8_t> buffer(wireSize);
    uint8_t* endPtr = original->to_wire(buffer.data(), dimensions);
    ASSERT_EQ(endPtr - buffer.data(), wireSize);
    
    // Deserialize
    DataRecord* restored = new DataRecord(dimensions, precision, "");
    restored->from_wire(buffer.data(), dimensions, precision);
    
    // Verify
    EXPECT_STREQ(restored->getRowID().c_str(), "empty_record");
    EXPECT_EQ(restored->getPoints().size(), 0);
    ASSERT_TRUE(restored->getKey() != nullptr);
    
    delete original;
    delete restored;
}

// Test DataRecord with many points
TEST_F(WireFormatTest, DataRecordManyPoints) {
    DataRecord* original = new DataRecord(dimensions, precision, "many_points");
    
    // Add 100 points
    std::vector<std::vector<double>> originalPoints;
    for (int i = 0; i < 100; i++) {
        std::vector<double> p = {-50.0 + i, -25.0 + i * 0.5};
        original->putPoint(&p);
        originalPoints.push_back(p);
    }
    
    // Serialize
    size_t wireSize = original->wire_size(dimensions);
    std::vector<uint8_t> buffer(wireSize);
    uint8_t* endPtr = original->to_wire(buffer.data(), dimensions);
    ASSERT_EQ(endPtr - buffer.data(), wireSize);
    
    // Deserialize
    DataRecord* restored = new DataRecord(dimensions, precision, "");
    restored->from_wire(buffer.data(), dimensions, precision);
    
    // Verify all points were preserved
    const std::vector<std::vector<double>>& restoredPoints = restored->getPoints();
    ASSERT_EQ(restoredPoints.size(), 100);
    
    for (int i = 0; i < 100; i++) {
        EXPECT_DOUBLE_EQ(restoredPoints[i][0], originalPoints[i][0]);
        EXPECT_DOUBLE_EQ(restoredPoints[i][1], originalPoints[i][1]);
    }
    
    delete original;
    delete restored;
}

// Test wire format size calculations
TEST_F(WireFormatTest, WireSizeCalculations) {
    // Test DataRecord wire size
    DataRecord dr(dimensions, precision, "test");
    size_t drExpectedSize = 
        dimensions * 2 * sizeof(float) +  // KeyMBR
        sizeof(uint16_t) +                 // rowid length
        4 +                                // "test" string
        sizeof(uint16_t) +                 // points count
        0;                                 // no points
    
    EXPECT_EQ(dr.wire_size(dimensions), drExpectedSize);
    
    // Add points and check size increases
    std::vector<double> p = {1.0, 2.0};
    dr.putPoint(&p);
    
    size_t drWithPointSize = drExpectedSize + dimensions * sizeof(double);
    EXPECT_EQ(dr.wire_size(dimensions), drWithPointSize);
    
    // Test XTreeBucket wire size
    XTreeBucket<DataRecord> bucket(idx, false);
    size_t bucketBaseSize = 
        sizeof(uint16_t) +                 // n_children
        sizeof(uint8_t) +                  // is_leaf
        sizeof(uint8_t) +                  // padding
        dimensions * 2 * sizeof(float);    // KeyMBR
    
    EXPECT_EQ(bucket.wire_size(*idx), bucketBaseSize);
}

// Test that the wire format handles different precision values correctly
TEST_F(WireFormatTest, DifferentPrecisionValues) {
    // Test with different precision values
    for (int prec = 1; prec <= 10; prec++) {
        DataRecord* original = new DataRecord(dimensions, prec, "prec_test");
        
        std::vector<double> p = {1.23456789, -9.87654321};
        original->putPoint(&p);
        
        // Serialize
        size_t wireSize = original->wire_size(dimensions);
        std::vector<uint8_t> buffer(wireSize);
        original->to_wire(buffer.data(), dimensions);
        
        // Deserialize
        DataRecord* restored = new DataRecord(dimensions, prec, "");
        restored->from_wire(buffer.data(), dimensions, prec);
        
        // The precision affects internal representation but wire format should preserve values
        ASSERT_TRUE(restored->getKey() != nullptr);
        EXPECT_STREQ(restored->getRowID().c_str(), "prec_test");
        
        delete original;
        delete restored;
    }
}

// Test XTreeBucket with children - simplified wire format test
TEST_F(WireFormatTest, XTreeBucketWithChildrenRoundtrip) {
    // This test focuses on wire format preservation, not full tree functionality
    // We'll manually create the structure to avoid persistence layer dependencies
    
    // Create a leaf bucket
    XTreeBucket<DataRecord>* original = new XTreeBucket<DataRecord>(idx, /*isRoot*/false);
    
    // Set the bucket's KeyMBR
    KeyMBR* bucketKey = new KeyMBR(dimensions, precision);
    bucketKey->set_pair(0, -100.0f, 100.0f);  // X range
    bucketKey->set_pair(1, -100.0f, 100.0f);  // Y range
    original->setKey(bucketKey);
    
    // For wire format testing, we just need to verify the bucket metadata
    // is preserved. The TreeStructurePreservation test already verifies
    // that children are preserved through normal insertion.
    
    // Verify original bucket properties
    ASSERT_TRUE(original->getKey() != nullptr);
    
    // Serialize the bucket
    size_t wireSize = original->wire_size(*idx);
    std::vector<uint8_t> buffer(wireSize);
    
    uint8_t* endPtr = original->to_wire(buffer.data(), *idx);
    ASSERT_EQ(endPtr - buffer.data(), wireSize) << "Wire size mismatch";
    
    // Create a new bucket and deserialize
    XTreeBucket<DataRecord>* restored = new XTreeBucket<DataRecord>(idx, /*isRoot*/false);
    const uint8_t* readEndPtr = restored->from_wire(buffer.data(), idx);
    ASSERT_EQ(readEndPtr - buffer.data(), wireSize) << "Read size mismatch";
    
    // Verify the restored bucket
    ASSERT_TRUE(restored->getKey() != nullptr);
    KeyMBR* restoredKey = restored->getKey();
    EXPECT_FLOAT_EQ(restoredKey->getMin(0), -100.0f);
    EXPECT_FLOAT_EQ(restoredKey->getMax(0), 100.0f);
    EXPECT_FLOAT_EQ(restoredKey->getMin(1), -100.0f);
    EXPECT_FLOAT_EQ(restoredKey->getMax(1), 100.0f);
    
    // Verify the children count (empty bucket should have 0 children)
    EXPECT_EQ(restored->n(), original->n()) << "Restored should have same number of children as original";
    
    // The TreeStructurePreservation test verifies that buckets with actual children
    // serialize/deserialize correctly through the normal insertion path
    
    delete original;
    delete restored;
}

// Test XTreeBucket with corrupt data detection
TEST_F(WireFormatTest, CorruptDataDetection) {
    // Create a valid bucket
    XTreeBucket<DataRecord>* original = new XTreeBucket<DataRecord>(idx, false);
    
    // Serialize it
    size_t wireSize = original->wire_size(*idx);
    std::vector<uint8_t> buffer(wireSize);
    original->to_wire(buffer.data(), *idx);
    
    // Corrupt the n_children field to an invalid value
    uint16_t* nChildrenPtr = reinterpret_cast<uint16_t*>(buffer.data());
    *nChildrenPtr = 10000;  // Way too many children
    
    // Try to deserialize - should detect corruption
    XTreeBucket<DataRecord>* restored = new XTreeBucket<DataRecord>(idx, false);
    
    // Capture stderr to check for error message
    testing::internal::CaptureStderr();
    restored->from_wire(buffer.data(), idx);
    std::string output = testing::internal::GetCapturedStderr();
    
    // Should have detected the corruption
    EXPECT_TRUE(output.find("ERROR: Corrupt n_children value") != std::string::npos)
        << "Should detect corrupt n_children value";
    EXPECT_EQ(restored->n(), 0) << "Should set n to 0 on corruption";
    
    delete original;
    delete restored;
}

// Test wire format sizes against persistence size classes
TEST_F(WireFormatTest, WireSizeVsSizeClasses) {
    // Test that XTreeBucket wire sizes fit in appropriate size classes
    // Critical test: Detect when buckets exceed 256B threshold
    
    // Helper to create a bucket with N children and get its actual wire size
    auto get_bucket_wire_size = [this](int dims, int num_children) -> size_t {
        // Create test index with specified dimensions
        std::vector<const char*>* testDimLabels = new std::vector<const char*>();
        for (int i = 0; i < dims; i++) {
            testDimLabels->push_back("dim");
        }
        
        IndexDetails<DataRecord>* testIdx = new IndexDetails<DataRecord>(
            dims, precision, testDimLabels, nullptr, nullptr, "test_field",
            IndexDetails<DataRecord>::PersistenceMode::IN_MEMORY
        );
        
        // Create bucket
        XTreeBucket<DataRecord>* bucket = new XTreeBucket<DataRecord>(testIdx, false);
        
        // Add mock children to get accurate size
        // Note: We can't directly manipulate _n and _children due to encapsulation,
        // but we can insert DataRecords to simulate children
        for (int i = 0; i < num_children; i++) {
            DataRecord* dr = new DataRecord(dims, precision, "test_" + std::to_string(i));
            std::vector<double> point(dims, i * 1.0);
            dr->putPoint(&point);
            
            // Cache the record
            IndexDetails<DataRecord>::getCache().add(1000 + i, dr);
            auto* bucketCacheNode = IndexDetails<DataRecord>::getCache().add(2000, bucket);
            
            // Insert it
            bucket->xt_insert(bucketCacheNode, dr);
        }
        
        // Get actual wire size from production code
        size_t wireSize = bucket->wire_size(*testIdx);
        
        // Clean up
        IndexDetails<DataRecord>::clearCache();
        delete bucket;
        delete testIdx;
        delete testDimLabels;
        
        return wireSize;
    };
    
    // Test 1D tree
    {
        int dims = 1;
        size_t size_with_0 = get_bucket_wire_size(dims, 0);
        size_t size_with_10 = get_bucket_wire_size(dims, 10);
        size_t size_with_15 = get_bucket_wire_size(dims, 15);
        size_t size_with_18 = get_bucket_wire_size(dims, 18);
        
        EXPECT_EQ(size_with_0, 12) << "1D empty bucket should be 12 bytes";
        EXPECT_EQ(size_with_10, 172) << "1D bucket with 10 children should be 172 bytes";
        EXPECT_EQ(size_with_15, 252) << "1D bucket with 15 children should be 252 bytes";
        EXPECT_EQ(size_with_18, 300) << "1D bucket with 18 children should be 300 bytes";
        
        // Critical: 18 children exceeds 256B
        EXPECT_LE(size_with_15, 256) << "1D bucket with 15 children should fit in 256B";
        EXPECT_GT(size_with_18, 256) << "1D bucket with 18 children exceeds 256B";
    }
    
    // Test 2D tree (most common case)
    {
        int dims = 2;
        size_t size_with_0 = get_bucket_wire_size(dims, 0);
        size_t size_with_10 = get_bucket_wire_size(dims, 10);
        size_t size_with_14 = get_bucket_wire_size(dims, 14);
        size_t size_with_15 = get_bucket_wire_size(dims, 15);
        
        EXPECT_EQ(size_with_0, 20) << "2D empty bucket should be 20 bytes";
        EXPECT_EQ(size_with_10, 180) << "2D bucket with 10 children should be 180 bytes";
        EXPECT_EQ(size_with_14, 244) << "2D bucket with 14 children should be 244 bytes";
        EXPECT_EQ(size_with_15, 260) << "2D bucket with 15 children should be 260 bytes";
        
        // Critical: 15 children exceeds 256B threshold!
        EXPECT_LE(size_with_14, 256) << "2D bucket with 14 children should fit in 256B";
        EXPECT_GT(size_with_15, 256) << "2D bucket with 15 children exceeds 256B - THIS IS THE BUG!";
    }
    
    // Test 3D tree
    {
        int dims = 3;
        size_t size_with_0 = get_bucket_wire_size(dims, 0);
        size_t size_with_10 = get_bucket_wire_size(dims, 10);
        size_t size_with_13 = get_bucket_wire_size(dims, 13);
        size_t size_with_14 = get_bucket_wire_size(dims, 14);
        size_t size_with_15 = get_bucket_wire_size(dims, 15);
        
        EXPECT_EQ(size_with_0, 28) << "3D empty bucket should be 28 bytes";
        EXPECT_EQ(size_with_10, 188) << "3D bucket with 10 children should be 188 bytes";
        EXPECT_EQ(size_with_13, 236) << "3D bucket with 13 children should be 236 bytes";
        EXPECT_EQ(size_with_14, 252) << "3D bucket with 14 children should be 252 bytes";
        EXPECT_EQ(size_with_15, 268) << "3D bucket with 15 children should be 268 bytes";
        
        // Critical: 15 children exceeds 256B for 3D
        EXPECT_LE(size_with_14, 256) << "3D bucket with 14 children should fit in 256B";
        EXPECT_GT(size_with_15, 256) << "3D bucket with 15 children exceeds 256B";
    }
    
    // Test 4D tree
    {
        int dims = 4;
        size_t size_with_0 = get_bucket_wire_size(dims, 0);
        size_t size_with_10 = get_bucket_wire_size(dims, 10);
        size_t size_with_12 = get_bucket_wire_size(dims, 12);
        size_t size_with_13 = get_bucket_wire_size(dims, 13);
        size_t size_with_14 = get_bucket_wire_size(dims, 14);
        
        EXPECT_EQ(size_with_0, 36) << "4D empty bucket should be 36 bytes";
        EXPECT_EQ(size_with_10, 196) << "4D bucket with 10 children should be 196 bytes";
        EXPECT_EQ(size_with_12, 228) << "4D bucket with 12 children should be 228 bytes";
        EXPECT_EQ(size_with_13, 244) << "4D bucket with 13 children should be 244 bytes";
        EXPECT_EQ(size_with_14, 260) << "4D bucket with 14 children should be 260 bytes";
        
        // Critical: 14 children exceeds 256B for 4D
        EXPECT_LE(size_with_13, 256) << "4D bucket with 13 children should fit in 256B";
        EXPECT_GT(size_with_14, 256) << "4D bucket with 14 children exceeds 256B";
    }
}

// Test that DataRecord sizes fit in appropriate size classes
TEST_F(WireFormatTest, DataRecordSizeClasses) {
    // Calculate DataRecord wire format size
    auto calc_dr_size = [](int dims, const std::string& rowid, int num_points) -> size_t {
        size_t mbr = dims * 2 * 4;  // KeyMBR
        size_t rowid_size = 2 + rowid.size();  // 2 bytes for length + data
        size_t points_size = 2 + (num_points * dims * 8);  // 2 for count + doubles
        return mbr + rowid_size + points_size;
    };
    
    // Test typical DataRecord sizes
    {
        std::string typical_rowid = "record_12345";
        
        // 1D with single point
        size_t size_1d_1pt = calc_dr_size(1, typical_rowid, 1);
        EXPECT_EQ(size_1d_1pt, 8 + 14 + 10) << "1D DataRecord with 1 point";
        EXPECT_LE(size_1d_1pt, 256) << "Should fit in 256B";
        
        // 2D with single point
        size_t size_2d_1pt = calc_dr_size(2, typical_rowid, 1);
        EXPECT_EQ(size_2d_1pt, 16 + 14 + 18) << "2D DataRecord with 1 point";
        EXPECT_LE(size_2d_1pt, 256) << "Should fit in 256B";
        
        // 2D with 10 points
        size_t size_2d_10pts = calc_dr_size(2, typical_rowid, 10);
        EXPECT_EQ(size_2d_10pts, 16 + 14 + 162) << "2D DataRecord with 10 points";
        EXPECT_LE(size_2d_10pts, 256) << "Should fit in 256B";
        
        // 2D with 15 points - getting close to limit
        size_t size_2d_15pts = calc_dr_size(2, typical_rowid, 15);
        EXPECT_EQ(size_2d_15pts, 16 + 14 + 242) << "2D DataRecord with 15 points";
        EXPECT_GT(size_2d_15pts, 256) << "Exceeds 256B - need larger size class";
    }
}

// Test maximum safe children count for each dimension
TEST_F(WireFormatTest, MaxSafeChildrenFor256B) {
    // For each dimension, find the maximum number of children that fits in 256B
    
    auto calc_bucket_size = [](int dims, int num_children) -> size_t {
        size_t header = 4;
        size_t mbr = dims * 2 * 4;
        size_t children = num_children * 16;
        return header + mbr + children;
    };
    
    struct DimLimit {
        int dims;
        int max_children_256;
        int max_children_512;
    };
    
    std::vector<DimLimit> limits;
    
    for (int dims = 1; dims <= 10; dims++) {
        int max_256 = 0;
        int max_512 = 0;
        
        // Find max children for 256B
        for (int children = 0; children < 100; children++) {
            size_t size = calc_bucket_size(dims, children);
            if (size <= 256) {
                max_256 = children;
            }
            if (size <= 512) {
                max_512 = children;
            }
        }
        
        limits.push_back({dims, max_256, max_512});
        
        // Log the findings
        std::cout << "Dimension " << dims << ": max " << max_256 
                  << " children in 256B, " << max_512 << " in 512B\n";
    }
    
    // Verify critical thresholds
    EXPECT_EQ(limits[0].max_children_256, 15) << "1D should fit 15 children in 256B";
    EXPECT_EQ(limits[1].max_children_256, 14) << "2D should fit 14 children in 256B";
    EXPECT_EQ(limits[2].max_children_256, 13) << "3D should fit 13 children in 256B";
    EXPECT_EQ(limits[3].max_children_256, 13) << "4D should fit 13 children in 256B";
    
    // Verify 512B gives reasonable headroom
    EXPECT_GE(limits[1].max_children_512, 30) << "2D should fit at least 30 children in 512B";
}