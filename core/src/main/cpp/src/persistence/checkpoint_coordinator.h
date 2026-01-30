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

#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <condition_variable>
#include <functional>

#include "object_table_sharded.hpp"
#include "ot_checkpoint.h"
#include "ot_delta_log.h"
#include "ot_log_gc.h"
#include "manifest.h"
#include "superblock.hpp"
#include "mvcc_context.h" // if you expose advance_epoch()/min_active_epoch()
#include "reclaimer.h"

namespace xtree::persist {

struct CheckpointPolicy {
  // Replay window triggers
  size_t   max_replay_bytes   = 256ull * 1024 * 1024;  // burst default
  uint64_t max_replay_epochs  = 100'000;               // burst default
  std::chrono::seconds max_age{600};                   // 10 min burst default
  std::chrono::seconds min_interval{30};               // don't thrash
  
  // Adaptive tuning based on ingest rate
  bool     adaptive_wal_rotation = true;               // Enable auto-tuning
  size_t   min_replay_bytes      = 64ull * 1024 * 1024; // Min 64MB (high throughput)
  size_t   base_replay_bytes     = 256ull * 1024 * 1024; // Base 256MB (normal)
  double   throughput_threshold  = 100'000;            // Records/sec threshold for adaptation

  // Query-only aggressiveness
  std::chrono::seconds query_only_age{45};

  // Steady state thresholds
  size_t   steady_replay_bytes  = 96ull * 1024 * 1024;
  std::chrono::seconds steady_age{90};

  // WAL rotation policy (separate from checkpoint triggers)
  size_t rotate_bytes = 256ull * 1024 * 1024;          // Rotate logs at 256MB
  std::chrono::seconds rotate_age{3600};               // Rotate logs after 1 hour

  // GC policies
  bool   gc_on_checkpoint   = true;                    // run GC after standalone checkpoint
  bool   gc_on_rotate       = false;                   // run GC immediately after rotation
  size_t gc_min_keep_logs   = 2;                       // always keep at least N newest logs (incl. active)
  std::chrono::seconds gc_min_age{0};                  // optional age guard for GC
  uint32_t gc_lag_checkpoints = 0;                     // 0 = GC up to this ckpt; 1 = up to previous, etc.
  size_t checkpoint_keep_count = 2;                    // Number of checkpoints to retain (reduced for space)

  // EWMA smoothing (if we choose to implement write/query rate)
  double ewma_alpha = 0.2;
  
  // Group commit settings (shared with DurabilityPolicy)
  size_t group_commit_interval_ms = 0;                 // 0 = disabled, >0 = batch window in ms
}; // CheckPointPolicy

class CheckpointCoordinator {
public:
  // Optional: metrics snapshot (fill your metrics struct)
  struct Stats {
    uint64_t last_epoch = 0;
    size_t   last_replay_bytes = 0;
    uint64_t last_replay_epochs = 0;
    std::chrono::milliseconds last_ckpt_ms{0};
    std::chrono::milliseconds last_rotate_ms{0};
    uint64_t checkpoints_written = 0;
    uint64_t rotations = 0;
    uint64_t pruned_logs = 0;
    uint64_t last_checkpoint_epoch = 0;
    uint64_t last_gc_epoch = 0;
  };
  
  // Error callback for monitoring/alerting
  using ErrorCallback = std::function<void(const std::string& error)>;
  
  // Metrics callback for telemetry
  using MetricsCallback = std::function<void(const Stats& stats)>;
  
  // Coordinator takes ownership of the initial log and manages its lifecycle
  // Writers use the active_log shared_ptr (ref-counted) to append
  // Note: We use a plain shared_ptr with atomic free functions for C++17 compatibility
  CheckpointCoordinator(ObjectTableSharded& ot,
                        Superblock&         sb,
                        Manifest&           manifest,
                        std::shared_ptr<OTDeltaLog>& active_log,
                        OTLogGC&            log_gc,
                        MVCCContext&        mvcc,
                        const CheckpointPolicy& policy,
                        Reclaimer*          reclaimer = nullptr);

  ~CheckpointCoordinator();

  void start();   // spawn background thread
  void stop();    // signal & join
  
  // Get the current root from the superblock
  NodeID get_persisted_root() const;

  // Initialize after recovery (adjusts timing, policy)
  void initialize_after_recovery(uint64_t recovered_epoch, size_t replay_bytes);
  
  // Set callbacks for error handling and metrics
  void set_error_callback(ErrorCallback cb) { error_callback_ = cb; }
  void set_metrics_callback(MetricsCallback cb) { metrics_callback_ = cb; }
  
  // Optional: expose a nudge (e.g., called after big bursts)
  void request_checkpoint();
  
  // Update throughput metrics for adaptive rotation
  void update_throughput(uint64_t records_inserted);

  Stats stats() const;
  
  // Get the active log for appending deltas
  std::shared_ptr<OTDeltaLog> get_active_log() const noexcept {
    return std::atomic_load(&active_log_);
  }

  // ---- Dirty range tracking for background flushing ----
  struct DirtyRange {
    uint32_t file_id;
    uint64_t offset;
    size_t length;
  };
  
  // Submit dirty ranges for background flushing (BALANCED mode)
  void submit_dirty_ranges(const std::vector<DirtyRange>& ranges);
  
  // ---- Optional: group-commit combiner (single fsync+publish) ----
  // Writers call try_publish(new_root, new_epoch). If a leader is active,
  // they can either return false (caller keeps appending) or wait_for_publish().
  bool try_publish(NodeID new_root, uint64_t new_epoch);
  void wait_for_publish(); // optional blocking wait
  void set_group_commit_interval(std::chrono::milliseconds m); // e.g. 2–5 ms

private:
  // Action decision for the coordinator loop
  enum class Action {
    None,
    CkptOnly,
    CkptAndRotate
  };
  
  void loop();
  bool should_checkpoint(uint64_t checkpoint_epoch,
                         uint64_t current_log_end_epoch,
                         size_t   replay_bytes,
                         std::chrono::steady_clock::time_point now) const;

  uint64_t choose_snapshot_epoch(); // typically mvcc.advance_epoch()
  void do_checkpoint(uint64_t epoch);
  void do_checkpoint_and_rotate(uint64_t epoch);
  void do_checkpoint_impl(uint64_t epoch, int post_op);  // Internal helper
  void run_log_gc(uint64_t checkpoint_epoch, bool invoked_from_rotate); // Centralized GC logic

  // helpers
  size_t   estimate_replay_bytes() const; // from log gc / delta log counters
  uint64_t current_log_end_epoch() const; // from manifest/log gc
  uint64_t checkpoint_epoch() const;      // from manifest

  // publish combiner internals
  void leader_publish(NodeID root, uint64_t epoch, std::shared_ptr<OTDeltaLog> captured_log);
  
  // Dirty range flushing
  void flush_dirty_ranges_if_needed();
  void do_flush_dirty_ranges(std::vector<DirtyRange> ranges);
  void flush_dirty_ranges_until(uint64_t epoch);
  
  // Helper methods for log rotation
  OTDeltaLog* open_new_log(uint64_t sequence);
  void activate_new_log(OTDeltaLog* new_log, uint64_t start_epoch);
  void close_old_log(OTDeltaLog* old_log);
  bool should_rotate_after_checkpoint(OTDeltaLog* log) const;
  void do_rotate_after_checkpoint(uint64_t epoch);
  
  // Initialize or adopt active log on startup
  void init_or_adopt_active_log();
  static uint64_t parse_sequence_from_path(const std::string& path);

private:
  ObjectTableSharded&  ot_;
  Superblock&   sb_;
  Manifest&     manifest_;
  std::shared_ptr<OTDeltaLog>& active_log_;  // Ref-counted active log (use atomic free functions)
  OTLogGC&      log_gc_;
  MVCCContext&  mvcc_;
  Reclaimer*    reclaimer_;
  CheckpointPolicy policy_;

  // bg thread
  std::atomic<bool> running_{false};
  std::thread       th_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<bool> checkpoint_requested_{false};
  std::chrono::steady_clock::time_point last_ckpt_{};

  // stats
  static constexpr uint64_t kNoCheckpoint = UINT64_MAX;
  std::atomic<uint64_t> checkpoints_written_{0};
  std::atomic<uint64_t> rotations_{0};
  std::atomic<uint64_t> pruned_logs_{0};
  std::atomic<uint64_t> last_epoch_{kNoCheckpoint};
  std::atomic<size_t>   last_replay_bytes_{0};
  std::atomic<uint64_t> last_replay_epochs_{0};
  std::atomic<int64_t>  last_ckpt_ms_{0};
  std::atomic<int64_t>  last_rotate_ms_{0};
  
  // Track checkpoint and GC completion for observability
  std::atomic<uint64_t> last_checkpoint_epoch_{kNoCheckpoint};
  std::atomic<uint64_t> last_gc_epoch_{kNoCheckpoint};
  
  // Adaptive WAL rotation tracking
  std::atomic<double> current_throughput_{0.0};        // Records/sec
  std::atomic<size_t> adjusted_replay_bytes_{256ull * 1024 * 1024}; // Current threshold
  std::chrono::steady_clock::time_point throughput_window_start_;
  std::atomic<uint64_t> records_in_window_{0};

  // group-commit combiner
  std::chrono::milliseconds group_commit_interval_{0}; // 0 = disabled
  std::condition_variable publish_cv_;
  std::mutex publish_mu_;
  
  // Checkpoint serialization (replaces busy-wait CAS)
  std::mutex sync_mu_;
  bool sync_in_progress_{false};
  
  // Dirty range tracking for background flushing
  std::mutex dirty_ranges_mu_;
  std::vector<DirtyRange> pending_dirty_ranges_;
  size_t total_dirty_bytes_{0};
  std::chrono::steady_clock::time_point oldest_dirty_time_;
  
  // callbacks
  ErrorCallback error_callback_;
  MetricsCallback metrics_callback_;
  
  // recovery state
  bool initialized_from_recovery_{false};
  uint64_t recovered_epoch_{0};
  
  // Safety flag for destructor
  bool fully_initialized_{false};
  
  // Helper to report errors
  void report_error(const std::string& error) {
    if (error_callback_) error_callback_(error);
  }
  
  // Helper to report metrics
  void report_metrics() {
    if (metrics_callback_) metrics_callback_(stats());
  }
};


} // namespace xtree::persist