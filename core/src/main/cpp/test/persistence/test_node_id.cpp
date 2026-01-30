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
#include <type_traits>
#include "persistence/node_id.hpp"

using namespace xtree::persist;

class NodeIDTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(NodeIDTest, TriviallyCopyable) {
    // NodeID must be trivially copyable for atomic operations
    EXPECT_TRUE(std::is_trivially_copyable<NodeID>::value);
    EXPECT_TRUE(std::is_trivial<NodeID>::value);
    
    // Ensure it's 8 bytes for atomicity
    EXPECT_EQ(sizeof(NodeID), 8u);
}

TEST_F(NodeIDTest, DefaultConstruction) {
    // Default constructed NodeID is uninitialized (trivial type)
    // To get an invalid NodeID, use the factory method
    NodeID id = NodeID::invalid();
    EXPECT_FALSE(id.valid());
    EXPECT_EQ(id.raw(), NodeID::INVALID_RAW);
}

TEST_F(NodeIDTest, HandleAndTagConstruction) {
    uint64_t handle_idx = 0x123456789ABC;
    uint16_t tag = 0x12DE;
    
    NodeID id = NodeID::from_parts(handle_idx, tag);
    EXPECT_TRUE(id.valid());
    EXPECT_EQ(id.handle_index(), handle_idx);
    EXPECT_EQ(id.tag(), tag);
    
    // Verify raw encoding
    uint64_t expected_raw = (handle_idx << 16) | tag;
    EXPECT_EQ(id.raw(), expected_raw);
}

TEST_F(NodeIDTest, FromRawConstruction) {
    uint64_t raw_value = 0x123456789ABCDE;
    NodeID id = NodeID::from_raw(raw_value);
    
    EXPECT_TRUE(id.valid());
    EXPECT_EQ(id.raw(), raw_value);
    EXPECT_EQ(id.handle_index(), raw_value >> 16);
    EXPECT_EQ(id.tag(), static_cast<uint16_t>(raw_value & 0xFFFF));
}

TEST_F(NodeIDTest, InvalidNodeID) {
    NodeID invalid = NodeID::from_raw(NodeID::INVALID_RAW);
    EXPECT_FALSE(invalid.valid());
}

TEST_F(NodeIDTest, EqualityOperators) {
    NodeID id1 = NodeID::from_parts(12345, 67);
    NodeID id2 = NodeID::from_parts(12345, 67);
    NodeID id3 = NodeID::from_parts(12345, 68);  // Different tag
    NodeID id4 = NodeID::from_parts(12346, 67);  // Different handle
    
    EXPECT_EQ(id1, id2);
    EXPECT_NE(id1, id3);
    EXPECT_NE(id1, id4);
}

TEST_F(NodeIDTest, TagOverflow) {
    // Test that tag is properly masked to 16 bits
    uint64_t handle_idx = 0x1234;
    uint16_t tag = 0xFFFF;
    
    NodeID id = NodeID::from_parts(handle_idx, tag);
    EXPECT_EQ(id.tag(), 0xFFFF);
    
    // Tag should increment and wrap around (but from_parts skips 0)
    NodeID next_id = NodeID::from_parts(handle_idx, static_cast<uint16_t>(tag + 1));
    EXPECT_EQ(next_id.tag(), 0x0001);  // from_parts converts 0 to 1
}

TEST_F(NodeIDTest, MaxHandleIndex) {
    // Test maximum handle index (48 bits)
    uint64_t max_handle = (1ULL << 48) - 1;
    uint16_t tag = 0x12AB;
    
    NodeID id = NodeID::from_parts(max_handle, tag);
    EXPECT_EQ(id.handle_index(), max_handle);
    EXPECT_EQ(id.tag(), tag);
}

TEST_F(NodeIDTest, ConstexprConstruction) {
    // Verify construction and accessors work
    const NodeID const_id = NodeID::from_parts(100, 5);
    uint64_t handle = const_id.handle_index();
    uint16_t tag = const_id.tag();
    bool valid = const_id.valid();
    
    EXPECT_EQ(handle, 100u);
    EXPECT_EQ(tag, 5u);
    EXPECT_TRUE(valid);
}

TEST_F(NodeIDTest, NodeKindEnum) {
    // Test NodeKind enum values
    EXPECT_EQ(static_cast<uint8_t>(NodeKind::Invalid), 0);   // Free OT slot
    EXPECT_EQ(static_cast<uint8_t>(NodeKind::Internal), 1);
    EXPECT_EQ(static_cast<uint8_t>(NodeKind::Leaf), 2);
    EXPECT_EQ(static_cast<uint8_t>(NodeKind::ChildVec), 3);
    EXPECT_EQ(static_cast<uint8_t>(NodeKind::ValueVec), 4);
    EXPECT_EQ(static_cast<uint8_t>(NodeKind::Tombstone), 255);  // For leaf-record MVCC
}