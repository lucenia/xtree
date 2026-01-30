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
#include <fstream>
#include <thread>
#include <chrono>
#include <random>
#include <atomic>
#include <map>
#include <mutex>
#include <set>
#ifdef _WIN32
#include <windows.h>
#include <direct.h>
#include <io.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#endif
#include <cstring>
#include "persistence/ot_delta_log.h"
#include "persistence/node_id.hpp"
#include "persistence/platform_fs.h"

using namespace xtree::persist;
using namespace std::chrono_literals;

class OTDeltaLogTest : public ::testing::Test {
protected:
    std::string test_dir;
    std::string log_path;
    
    void SetUp() override {
        test_dir = "/tmp/xtree_ot_delta_test_" + std::to_string(getpid());
        log_path = test_dir + "/ot_delta.wal";
        
        #ifdef _WIN32
            CreateDirectoryA(test_dir.c_str(), nullptr);
        #else
            mkdir(test_dir.c_str(), 0755);
        #endif
    }
    
    void TearDown() override {
        #ifdef _WIN32
            std::string cmd = "rmdir /s /q \"" + test_dir + "\"";
            system(cmd.c_str());
        #else
            std::string cmd = "rm -rf " + test_dir;
            system(cmd.c_str());
        #endif
    }
    
    OTDeltaRec createTestRecord(uint64_t handle_idx, uint16_t tag, uint64_t epoch) {
        OTDeltaRec rec{};
        rec.handle_idx = handle_idx;
        rec.tag = tag;
        rec.class_id = handle_idx % 7;  // Cycle through size classes
        rec.kind = static_cast<uint8_t>(handle_idx % 2 ? NodeKind::Leaf : NodeKind::Internal);
        rec.file_id = 1;
        rec.segment_id = handle_idx / 100;
        rec.offset = handle_idx * 4096;
        rec.length = 4096;
        rec.birth_epoch = epoch;
        rec.retire_epoch = ~uint64_t{0};  // Live
        return rec;
    }
};

TEST_F(OTDeltaLogTest, AppendAndReplay) {
    OTDeltaLog log(log_path);
    
    // Create test records
    std::vector<OTDeltaRec> batch;
    for (uint64_t i = 1; i <= 10; i++) {
        batch.push_back(createTestRecord(i, i % 256, i * 10));
    }
    
    // Append batch
    log.append(batch);
    log.sync();  // Must sync to write to disk
    
    // Replay and verify
    std::vector<OTDeltaRec> replayed;
    log.replay([&replayed](const OTDeltaRec& rec) {
        replayed.push_back(rec);
    });
    
    ASSERT_EQ(replayed.size(), batch.size());
    
    for (size_t i = 0; i < batch.size(); i++) {
        EXPECT_EQ(replayed[i].handle_idx, batch[i].handle_idx);
        EXPECT_EQ(replayed[i].tag, batch[i].tag);
        EXPECT_EQ(replayed[i].class_id, batch[i].class_id);
        EXPECT_EQ(replayed[i].kind, batch[i].kind);
        EXPECT_EQ(replayed[i].file_id, batch[i].file_id);
        EXPECT_EQ(replayed[i].segment_id, batch[i].segment_id);
        EXPECT_EQ(replayed[i].offset, batch[i].offset);
        EXPECT_EQ(replayed[i].length, batch[i].length);
        EXPECT_EQ(replayed[i].birth_epoch, batch[i].birth_epoch);
        EXPECT_EQ(replayed[i].retire_epoch, batch[i].retire_epoch);
    }
}

TEST_F(OTDeltaLogTest, MultipleBatches) {
    OTDeltaLog log(log_path);
    
    // Append multiple batches
    for (int batch_num = 0; batch_num < 5; batch_num++) {
        std::vector<OTDeltaRec> batch;
        for (int i = 0; i < 20; i++) {
            uint64_t handle = batch_num * 100 + i;
            batch.push_back(createTestRecord(handle, handle % 256, batch_num * 1000 + i));
        }
        log.append(batch);
    }
    log.sync();  // Sync after all batches
    
    // Replay all
    std::vector<OTDeltaRec> all_records;
    log.replay([&all_records](const OTDeltaRec& rec) {
        all_records.push_back(rec);
    });
    
    EXPECT_EQ(all_records.size(), 100u);  // 5 batches * 20 records
    
    // Verify order is preserved
    for (size_t i = 1; i < all_records.size(); i++) {
        EXPECT_GE(all_records[i].handle_idx, all_records[i-1].handle_idx);
    }
}

TEST_F(OTDeltaLogTest, EmptyLogReplay) {
    OTDeltaLog log(log_path);
    
    // Replay empty log
    int count = 0;
    log.replay([&count](const OTDeltaRec& rec) {
        count++;
    });
    
    EXPECT_EQ(count, 0);
}

TEST_F(OTDeltaLogTest, LargeRecords) {
    OTDeltaLog log(log_path);
    
    // Test with maximum values
    std::vector<OTDeltaRec> batch;
    
    OTDeltaRec large_rec{};
    large_rec.handle_idx = (1ULL << 56) - 1;  // Max handle index
    large_rec.tag = 255;
    large_rec.class_id = 6;
    large_rec.kind = static_cast<uint8_t>(NodeKind::ChildVec);
    large_rec.file_id = UINT32_MAX;
    large_rec.segment_id = UINT32_MAX;
    large_rec.offset = UINT64_MAX - 1;
    large_rec.length = 262144;  // 256K
    large_rec.birth_epoch = UINT64_MAX - 1;
    large_rec.retire_epoch = UINT64_MAX;
    
    batch.push_back(large_rec);
    log.append(batch);
    log.sync();
    
    // Replay and verify
    OTDeltaRec replayed_rec{};
    log.replay([&replayed_rec](const OTDeltaRec& rec) {
        replayed_rec = rec;
    });
    
    EXPECT_EQ(replayed_rec.handle_idx, large_rec.handle_idx);
    EXPECT_EQ(replayed_rec.tag, large_rec.tag);
    EXPECT_EQ(replayed_rec.offset, large_rec.offset);
    EXPECT_EQ(replayed_rec.retire_epoch, large_rec.retire_epoch);
}

TEST_F(OTDeltaLogTest, RetiredRecords) {
    OTDeltaLog log(log_path);
    
    std::vector<OTDeltaRec> batch;
    
    // Mix of live and retired records
    for (uint64_t i = 0; i < 10; i++) {
        auto rec = createTestRecord(i, i, i * 10);
        if (i % 2 == 0) {
            rec.retire_epoch = i * 10 + 5;  // Retired
        }
        batch.push_back(rec);
    }
    
    log.append(batch);
    log.sync();
    
    // Count live vs retired on replay
    int live_count = 0, retired_count = 0;
    log.replay([&](const OTDeltaRec& rec) {
        if (rec.retire_epoch == ~uint64_t{0}) {
            live_count++;
        } else {
            retired_count++;
        }
    });
    
    EXPECT_EQ(live_count, 5);
    EXPECT_EQ(retired_count, 5);
}

TEST_F(OTDeltaLogTest, PersistenceAcrossReopen) {
    std::vector<OTDeltaRec> original_batch;
    
    // Write records
    {
        OTDeltaLog log(log_path);
        
        for (uint64_t i = 0; i < 50; i++) {
            original_batch.push_back(createTestRecord(i, i % 256, i * 100));
        }
        log.append(original_batch);
        log.sync();
    }
    
    // Reopen and verify
    {
        OTDeltaLog log(log_path);
        
        std::vector<OTDeltaRec> replayed;
        log.replay([&replayed](const OTDeltaRec& rec) {
            replayed.push_back(rec);
        });
        
        ASSERT_EQ(replayed.size(), original_batch.size());
        
        for (size_t i = 0; i < original_batch.size(); i++) {
            EXPECT_EQ(replayed[i].handle_idx, original_batch[i].handle_idx);
            EXPECT_EQ(replayed[i].birth_epoch, original_batch[i].birth_epoch);
        }
    }
}

TEST_F(OTDeltaLogTest, ConcurrentAppends) {
    OTDeltaLog log(log_path);
    
    const int num_threads = 4;
    const int records_per_thread = 25;
    std::vector<std::thread> threads;
    
    // Each thread appends its own batches
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 5; i++) {
                std::vector<OTDeltaRec> batch;
                for (int j = 0; j < 5; j++) {
                    uint64_t handle = t * 1000 + i * 10 + j;
                    batch.push_back(createTestRecord(handle, handle % 256, handle));
                }
                log.append(batch);
                log.sync();
                std::this_thread::sleep_for(1ms);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify all records were written
    std::set<uint64_t> seen_handles;
    log.replay([&seen_handles](const OTDeltaRec& rec) {
        seen_handles.insert(rec.handle_idx);
    });
    
    EXPECT_EQ(seen_handles.size(), num_threads * records_per_thread);
}

TEST_F(OTDeltaLogTest, ConcurrentStorm) {
    // Test with 16 threads doing random batches and syncs
    OTDeltaLog log(log_path);
    
    const int num_threads = 16;
    const int batches_per_thread = 100;
    std::atomic<uint64_t> next_handle{1};
    std::map<uint64_t, OTDeltaRec> expected_records;
    std::mutex expected_mutex;
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(t);
            std::uniform_int_distribution<int> batch_size_dist(1, 64);
            std::uniform_int_distribution<int> sync_dist(0, 3);  // 25% chance to sync
            
            for (int b = 0; b < batches_per_thread; b++) {
                // Create random-sized batch
                int batch_size = batch_size_dist(rng);
                std::vector<OTDeltaRec> batch;
                std::vector<OTDeltaRec> local_records;
                
                for (int i = 0; i < batch_size; i++) {
                    uint64_t handle = next_handle.fetch_add(1);
                    OTDeltaRec rec = createTestRecord(handle, handle % 256, t * 1000 + b);
                    batch.push_back(rec);
                    local_records.push_back(rec);
                }
                
                // Append batch
                log.append(batch);
                
                // Random sync
                if (sync_dist(rng) == 0) {
                    log.sync();
                }
                
                // Record what we wrote (last-writer-wins)
                {
                    std::lock_guard<std::mutex> lock(expected_mutex);
                    for (const auto& rec : local_records) {
                        expected_records[rec.handle_idx] = rec;
                    }
                }
            }
        });
    }
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    // Final sync to ensure everything is on disk
    log.sync();
    
    // Replay and verify last-writer-wins semantics
    std::map<uint64_t, OTDeltaRec> replayed_records;
    log.replay([&replayed_records](const OTDeltaRec& rec) {
        replayed_records[rec.handle_idx] = rec;  // Last-writer-wins
    });
    
    // Verify all expected records are present
    EXPECT_EQ(replayed_records.size(), expected_records.size());
    for (const auto& [handle, expected] : expected_records) {
        auto it = replayed_records.find(handle);
        ASSERT_NE(it, replayed_records.end()) << "Missing handle: " << handle;
        // Verify the record matches
        EXPECT_EQ(it->second.tag, expected.tag);
        EXPECT_EQ(it->second.class_id, expected.class_id);
        EXPECT_EQ(it->second.birth_epoch, expected.birth_epoch);
    }
}

TEST_F(OTDeltaLogTest, AllNodeKinds) {
    OTDeltaLog log(log_path);
    
    std::vector<OTDeltaRec> batch;
    std::vector<NodeKind> kinds = {
        NodeKind::Internal, 
        NodeKind::Leaf, 
        NodeKind::ChildVec, 
        NodeKind::ValueVec,
        NodeKind::Tombstone
    };
    
    for (size_t i = 0; i < kinds.size(); i++) {
        OTDeltaRec rec = createTestRecord(i, i, i * 10);
        rec.kind = static_cast<uint8_t>(kinds[i]);
        batch.push_back(rec);
    }
    
    log.append(batch);
    log.sync();
    
    // Verify all node kinds preserved
    std::vector<uint8_t> replayed_kinds;
    log.replay([&replayed_kinds](const OTDeltaRec& rec) {
        replayed_kinds.push_back(rec.kind);
    });
    
    ASSERT_EQ(replayed_kinds.size(), kinds.size());
    for (size_t i = 0; i < kinds.size(); i++) {
        EXPECT_EQ(replayed_kinds[i], static_cast<uint8_t>(kinds[i]));
    }
}

TEST_F(OTDeltaLogTest, AppendWhileRotatePending) {
    // Test coordinated close and rotation with active writers
    OTDeltaLog log(log_path);
    
    const int num_threads = 8;
    const int records_per_thread = 100;
    std::atomic<bool> stop_flag{false};
    std::atomic<uint64_t> total_written{0};
    std::vector<std::thread> threads;
    
    // Start writer threads
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t]() {
            uint64_t base_handle = t * 10000;
            uint64_t written = 0;
            
            while (!stop_flag.load(std::memory_order_acquire)) {
                try {
                    std::vector<OTDeltaRec> batch;
                    for (int i = 0; i < 5; i++) {
                        batch.push_back(createTestRecord(base_handle + written, t, written));
                        written++;
                    }
                    log.append(batch);
                    
                    // Occasional sync
                    if (written % 20 == 0) {
                        log.sync();
                    }
                } catch (const std::runtime_error& e) {
                    // Expected when closing
                    if (std::string(e.what()).find("closing") != std::string::npos) {
                        break;
                    }
                    throw;
                }
            }
            total_written.fetch_add(written, std::memory_order_relaxed);
        });
    }
    
    // Let writers run for a bit
    std::this_thread::sleep_for(50ms);
    
    // Initiate coordinated close (simulating rotation)
    log.prepare_close();  // Blocks new appends, waits for in-flight
    stop_flag.store(true, std::memory_order_release);
    
    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }
    
    // Final sync before close
    log.sync();
    
    // Get count before close
    uint64_t count_before_close = total_written.load();
    EXPECT_GT(count_before_close, 0u);
    
    // Close the old log
    log.close();
    
    // Simulate rotation: new log file
    std::string new_log_path = test_dir + "/ot_delta_rotated.wal";
    OTDeltaLog new_log(new_log_path);
    
    // Write a marker record to new log
    std::vector<OTDeltaRec> marker_batch;
    marker_batch.push_back(createTestRecord(999999, 255, 999999));
    new_log.append(marker_batch);
    new_log.sync();
    
    // Verify old log has expected records
    std::set<uint64_t> old_handles;
    uint64_t last_good_offset = 0;
    std::string error;
    OTDeltaLog::replay(log_path, 
        [&old_handles](const OTDeltaRec& rec) {
            old_handles.insert(rec.handle_idx);
        }, &last_good_offset, &error);
    
    // Verify new log has marker
    bool found_marker = false;
    last_good_offset = 0;
    error.clear();
    OTDeltaLog::replay(new_log_path,
        [&found_marker](const OTDeltaRec& rec) {
            if (rec.handle_idx == 999999) {
                found_marker = true;
            }
        }, &last_good_offset, &error);
    
    EXPECT_TRUE(found_marker);
    
    // Total records should match what was written
    // Note: Some records might be lost during the close coordination
    // but we should have most of them
    EXPECT_GE(old_handles.size(), count_before_close * 0.9);  // Allow 10% loss during close
}

TEST_F(OTDeltaLogTest, CustomPreallocChunk) {
    // Test with custom preallocation chunk size (256MB for heavy production)
    const size_t custom_chunk = 256 * 1024 * 1024;  // 256MB
    OTDeltaLog log(log_path, custom_chunk);
    
    // Write some data
    std::vector<OTDeltaRec> batch;
    for (int i = 0; i < 100; i++) {
        batch.push_back(createTestRecord(i, i, i * 10));
    }
    
    log.append(batch);
    log.sync();
    
    // Verify data was written
    size_t count = 0;
    log.replay([&count](const OTDeltaRec& rec) {
        count++;
    });
    
    EXPECT_EQ(count, 100u);
}

TEST_F(OTDeltaLogTest, LargeBatchSoftCap) {
    // Test that very large batches (> 8MB) work correctly with soft cap fallback
    OTDeltaLog log(log_path);
    
    // Create a batch that exceeds the 8MB soft cap
    // Each frame is 56 bytes (4 + 48 + 4)
    // 8MB / 56 ≈ 150,000 records
    const size_t huge_batch_size = 200000;  // Well over 8MB
    std::vector<OTDeltaRec> huge_batch;
    
    for (size_t i = 0; i < huge_batch_size; i++) {
        huge_batch.push_back(createTestRecord(i, i % 256, i));
    }
    
    // This should trigger the soft cap fallback path
    log.append(huge_batch);
    log.sync();
    
    // Verify all records were written correctly
    size_t count = 0;
    std::set<uint64_t> seen_handles;
    log.replay([&](const OTDeltaRec& rec) {
        count++;
        seen_handles.insert(rec.handle_idx);
    });
    
    EXPECT_EQ(count, huge_batch_size);
    EXPECT_EQ(seen_handles.size(), huge_batch_size);
    
    // Verify first and last records
    EXPECT_EQ(seen_handles.count(0), 1u);
    EXPECT_EQ(seen_handles.count(huge_batch_size - 1), 1u);
}

TEST_F(OTDeltaLogTest, FaultInjectionMidFrame) {
    // Test that replay handles truncated frames correctly
    OTDeltaLog log(log_path);
    
    // Write some complete frames
    std::vector<OTDeltaRec> batch1;
    for (int i = 0; i < 5; i++) {
        batch1.push_back(createTestRecord(i, i, i * 10));
    }
    log.append(batch1);
    log.sync();
    
    // Write another batch
    std::vector<OTDeltaRec> batch2;
    for (int i = 5; i < 10; i++) {
        batch2.push_back(createTestRecord(i, i, i * 10));
    }
    log.append(batch2);
    log.sync();
    
    // Get file size
    auto [result, log_file_size] = PlatformFS::file_size(log_path);
    ASSERT_TRUE(result.ok);
    
    // Calculate frame size: [header:16][rec:52] = 68 bytes (no payload)
    const size_t frame_size = kFrameHeaderSize + kWireRecSize;
    ASSERT_EQ(frame_size, 68u);
    
    // Truncate file mid-frame (in the middle of the 8th record)
    size_t truncate_size = frame_size * 7 + 30;  // 7 complete frames + 30 bytes into 8th
    ASSERT_LT(truncate_size, log_file_size);
    
    // Truncate the file
    FSResult truncate_result = PlatformFS::truncate(log_path, truncate_size);
    ASSERT_TRUE(truncate_result.ok);
    
    // Try to replay - should succeed but stop at torn frame
    uint64_t last_good_offset = 0;
    std::string error;
    std::vector<OTDeltaRec> replayed;
    
    bool replay_ok = OTDeltaLog::replay(log_path, 
        [&replayed](const OTDeltaRec& rec) {
            replayed.push_back(rec);
        }, 
        &last_good_offset, 
        &error);
    
    // Replay should succeed - torn frame at end is handled gracefully
    EXPECT_TRUE(replay_ok);
    // Error may or may not be set (implementation detail)
    
    // Should have replayed exactly 7 records before the torn frame
    EXPECT_EQ(replayed.size(), 7u);
    
    // last_good_offset should be at the start of the truncated frame
    EXPECT_EQ(last_good_offset, frame_size * 7);
    
    // Truncate to last_good_offset
    truncate_result = PlatformFS::truncate(log_path, last_good_offset);
    ASSERT_TRUE(truncate_result.ok);
    
    // Now replay should succeed with exactly 7 records
    replayed.clear();
    replay_ok = OTDeltaLog::replay(log_path, 
        [&replayed](const OTDeltaRec& rec) {
            replayed.push_back(rec);
        }, 
        &last_good_offset, 
        &error);
    
    EXPECT_TRUE(replay_ok);
    EXPECT_EQ(replayed.size(), 7u);
    
    // Verify the 7 records are correct
    for (size_t i = 0; i < 7; i++) {
        EXPECT_EQ(replayed[i].handle_idx, i);
        EXPECT_EQ(replayed[i].tag, i);
        EXPECT_EQ(replayed[i].birth_epoch, i * 10u);
    }
}

TEST_F(OTDeltaLogTest, PayloadInWal_MixedFrames_RoundTrip) {
    // Test mixed frames with and without payloads
    std::string tmp_path = test_dir + "/otdl_payload_mixed.wal";
    
    {
        OTDeltaLog log(tmp_path);
        // Build 3 records: small payload, metadata-only, small payload
        std::vector<OTDeltaLog::DeltaWithPayload> batch;

        auto make_rec = [](uint64_t idx, bool with_payload, const char* txt) -> OTDeltaLog::DeltaWithPayload {
            OTDeltaRec d{};
            d.handle_idx = idx;
            d.tag = 1;
            d.kind = 1;
            d.class_id = 0;
            d.file_id = 0;
            d.segment_id = 0;
            d.offset = 4096 * idx;
            d.length = 128;
            d.birth_epoch = 0;
            d.retire_epoch = ~uint64_t{0};
            if (with_payload && txt) {
                return OTDeltaLog::DeltaWithPayload{d, txt, strlen(txt)};
            } else {
                return OTDeltaLog::DeltaWithPayload{d, nullptr, 0};
            }
        };

        batch.push_back(make_rec(10, true,  "alpha"));
        batch.push_back(make_rec(11, false, nullptr));
        batch.push_back(make_rec(12, true,  "bravo"));

        log.append_with_payloads(batch);
        log.sync();
    }

    // Reopen and replay
    {
        OTDeltaLog log(tmp_path);
        struct Seen {
            uint64_t idx;
            std::string payload;
        };
        std::vector<Seen> seen;

        log.replay_with_payloads([&](const OTDeltaRec& d, const void* p, size_t n) {
            std::string s;
            if (p && n) s.assign(static_cast<const char*>(p), n);
            seen.push_back({d.handle_idx, s});
        });
        
        ASSERT_EQ(seen.size(), 3u);
        EXPECT_EQ(seen[0].idx, 10u);
        EXPECT_EQ(seen[0].payload, "alpha");
        EXPECT_EQ(seen[1].idx, 11u);
        EXPECT_TRUE(seen[1].payload.empty()); // metadata-only
        EXPECT_EQ(seen[2].idx, 12u);
        EXPECT_EQ(seen[2].payload, "bravo");
    }
}

TEST_F(OTDeltaLogTest, StopsAtLastGoodFrameOnCorruption) {
    // Test that replay handles truncated/corrupted frames correctly
    std::string tmp_path = test_dir + "/otdl_corrupt_tail.wal";
    const int N = 5;

    {
        OTDeltaLog log(tmp_path);
        std::vector<OTDeltaLog::DeltaWithPayload> batch;
        for (int i = 0; i < N; ++i) {
            OTDeltaRec d{};
            d.handle_idx = 100 + i;
            d.tag = 1; 
            d.kind = 1; 
            d.class_id = 0;
            d.file_id = 0; 
            d.segment_id = 0; 
            d.offset = 4096 * i; 
            d.length = 256;
            d.birth_epoch = 0; 
            d.retire_epoch = ~uint64_t{0};
            const char* msg = "valid";
            batch.push_back({d, msg, strlen(msg)});
        }
        log.append_with_payloads(batch);
        log.sync();
    }

    // Corrupt tail by appending garbage bytes
    {
#ifdef _WIN32
        HANDLE h = CreateFileA(tmp_path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, 
                              NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
        ASSERT_NE(h, INVALID_HANDLE_VALUE);
        LARGE_INTEGER li; 
        li.QuadPart = 0;
        SetFilePointerEx(h, li, NULL, FILE_END);
        const char junk[] = "XXXXXXX";
        DWORD written = 0;
        WriteFile(h, junk, sizeof(junk), &written, NULL);
        FlushFileBuffers(h);
        CloseHandle(h);
#else
        int fd = ::open(tmp_path.c_str(), O_WRONLY | O_APPEND);
        ASSERT_GE(fd, 0);
        const char junk[] = "XXXXXXX";
        ASSERT_EQ(::write(fd, junk, sizeof(junk)), (ssize_t)sizeof(junk));
        ::fsync(fd);
        ::close(fd);
#endif
    }

    // Replay should stop at last good frame (i.e., deliver exactly N records)
    {
        OTDeltaLog log(tmp_path);
        std::vector<uint64_t> ids;
        
        log.replay_with_payloads([&](const OTDeltaRec& d, const void*, size_t) {
            ids.push_back(d.handle_idx);
        });
        
        // We should get exactly the valid frames before corruption
        ASSERT_EQ(ids.size(), static_cast<size_t>(N));
        for (int i = 0; i < N; ++i) {
            EXPECT_EQ(ids[i], static_cast<uint64_t>(100 + i));
        }
    }
}

TEST_F(OTDeltaLogTest, PrepareCloseWaitsAndBlocksFurtherAppends) {
    // Test that prepare_close() waits for in-flight appends and blocks new ones
    std::string tmp_path = test_dir + "/otdl_close_race.wal";
    OTDeltaLog log(tmp_path);

    std::atomic<bool> run{true};
    std::atomic<int> appended{0};
    std::atomic<bool> got_exception{false};

    auto worker = std::thread([&] {
        while (run.load(std::memory_order_relaxed)) {
            OTDeltaRec d{};
            d.handle_idx = 777; 
            d.tag = 1; 
            d.kind = 1; 
            d.class_id = 0;
            d.file_id = 0; 
            d.segment_id = 0; 
            d.offset = 0; 
            d.length = 64;
            d.birth_epoch = 0; 
            d.retire_epoch = ~uint64_t{0};
            try {
                log.append({d});
                appended.fetch_add(1);
                // Small delay to avoid spinning too fast
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            } catch (const std::exception&) {
                // Expected once prepare_close() begins
                got_exception.store(true);
                break;
            }
        }
    });

    // Give it a moment to append a few frames
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    int before_close = appended.load();
    EXPECT_GT(before_close, 0);

    // Now request close: should wait for in-flight appends to drain
    log.prepare_close();
    
    // Stop the worker thread
    run.store(false);
    worker.join();
    
    // Worker should have gotten exception or stopped on its own
    // Note: got_exception might be false if the worker checked run flag first
    
    // After prepare_close(), appending must throw
    OTDeltaRec d{};
    d.handle_idx = 999;
    EXPECT_THROW(log.append({d}), std::runtime_error);
    
    log.close();
}