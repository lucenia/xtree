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

#include "checkpoint_coordinator.h"
#include "ot_checkpoint.h"
#include "platform_fs.h"
#include "../util/log.h"
#include <algorithm>
#include <cstdio>
#include <climits>
#include <filesystem>
#ifndef _WIN32
#include <sys/stat.h>
#endif

namespace xtree::persist {

// Enum to control post-checkpoint behavior
enum class CheckpointPostOp { 
  None,           // No post-checkpoint action (used by rotation)
  MaybeRotate,    // Check if rotation needed after checkpoint
  RunGC           // Run GC after checkpoint (if policy allows)
};

CheckpointCoordinator::CheckpointCoordinator(ObjectTableSharded& ot,
                                             Superblock& sb,
                                             Manifest& manifest,
                                             std::shared_ptr<OTDeltaLog>& active_log,
                                             OTLogGC& log_gc,
                                             MVCCContext& mvcc,
                                             const CheckpointPolicy& policy,
                                             Reclaimer* reclaimer)
  : ot_(ot), sb_(sb), manifest_(manifest),
    active_log_(active_log),
    log_gc_(log_gc), mvcc_(mvcc), reclaimer_(reclaimer), policy_(policy),
    group_commit_interval_(policy.group_commit_interval_ms),
    throughput_window_start_(std::chrono::steady_clock::now()) {
    
    // Initialize or adopt the active log
    init_or_adopt_active_log();
    
    // Constructor completed successfully
    fully_initialized_ = true;
}

CheckpointCoordinator::~CheckpointCoordinator() { 
  // Only call stop if we were fully initialized
  if (fully_initialized_) {
    stop();
  }
}

void CheckpointCoordinator::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) return;
  last_ckpt_ = std::chrono::steady_clock::now();
  th_ = std::thread([this]{ loop(); });
}

void CheckpointCoordinator::stop() {
  if (!running_.exchange(false)) return;
  
  // Wake the checkpoint thread so it sees running_ = false
  {
    std::lock_guard<std::mutex> lk(mu_);
    // Don't request checkpoint, just wake the thread
  }
  cv_.notify_all();
  
  // Wake any group-commit waiters so we don't destroy a mutex they still hold
  {
    std::lock_guard<std::mutex> lk(sync_mu_);
    sync_in_progress_ = false;
  }
  publish_cv_.notify_all();
  
  if (th_.joinable()) {
    th_.join();
  }
  
  // Close active log using atomic exchange
  auto none = std::shared_ptr<OTDeltaLog>{};
  auto old = std::atomic_exchange(&active_log_, none);
  if (old) {
    old->prepare_close();
    old->sync();
    old->close();
  }
  // shared_ptr drops here; if no other holders, object is freed
}

void CheckpointCoordinator::initialize_after_recovery(uint64_t recovered_epoch, size_t replay_bytes) {
  initialized_from_recovery_ = true;
  recovered_epoch_ = recovered_epoch;
  last_ckpt_ = std::chrono::steady_clock::now();
  
  // FIX 4: Seed the replay window fields so loop doesn't wait for next sampling
  last_replay_epochs_.store(recovered_epoch, std::memory_order_relaxed);
  last_replay_bytes_.store(replay_bytes, std::memory_order_relaxed);
  
  // If we replayed a lot, trigger checkpoint soon
  if (replay_bytes > policy_.steady_replay_bytes) {
    request_checkpoint();
  }
  
  // Adjust policy if we detected a large replay
  if (replay_bytes > policy_.max_replay_bytes) {
    // Temporarily reduce thresholds to checkpoint more aggressively
    policy_.steady_replay_bytes = policy_.steady_replay_bytes / 2;
    policy_.steady_age = std::chrono::seconds{policy_.steady_age.count() / 2};
  }
}

void CheckpointCoordinator::request_checkpoint() {
  checkpoint_requested_.store(true, std::memory_order_release);
  cv_.notify_all();
}

void CheckpointCoordinator::update_throughput(uint64_t records_inserted) {
  if (!policy_.adaptive_wal_rotation) return;
  
  using namespace std::chrono;
  auto now = steady_clock::now();
  
  // Update window tracking
  records_in_window_.fetch_add(records_inserted, std::memory_order_relaxed);
  
  // Check if we should calculate new throughput (every second)
  auto elapsed = duration_cast<seconds>(now - throughput_window_start_);
  if (elapsed.count() >= 1) {
    uint64_t records = records_in_window_.exchange(0, std::memory_order_relaxed);
    double throughput = double(records) / elapsed.count();
    
    // EWMA smoothing
    double current = current_throughput_.load(std::memory_order_relaxed);
    double smoothed = (policy_.ewma_alpha * throughput) + ((1.0 - policy_.ewma_alpha) * current);
    current_throughput_.store(smoothed, std::memory_order_relaxed);
    
    // Adjust WAL rotation threshold based on throughput
    size_t new_threshold;
    if (smoothed > policy_.throughput_threshold) {
      // High throughput: use smaller WAL to rotate more frequently
      new_threshold = policy_.min_replay_bytes;
    } else {
      // Normal throughput: use base threshold
      new_threshold = policy_.base_replay_bytes;
    }
    adjusted_replay_bytes_.store(new_threshold, std::memory_order_relaxed);
    
    // Reset window
    throughput_window_start_ = now;
  }
}

CheckpointCoordinator::Stats CheckpointCoordinator::stats() const {
  Stats s;
  s.last_epoch = last_epoch_.load(std::memory_order_relaxed);
  s.last_replay_bytes = last_replay_bytes_.load(std::memory_order_relaxed);
  s.last_replay_epochs = last_replay_epochs_.load(std::memory_order_relaxed);
  s.last_ckpt_ms = std::chrono::milliseconds(last_ckpt_ms_.load(std::memory_order_relaxed));
  s.last_rotate_ms = std::chrono::milliseconds(last_rotate_ms_.load(std::memory_order_relaxed));
  s.checkpoints_written = checkpoints_written_.load(std::memory_order_relaxed);
  s.rotations = rotations_.load(std::memory_order_relaxed);
  s.pruned_logs = pruned_logs_.load(std::memory_order_relaxed);
  s.last_checkpoint_epoch = last_checkpoint_epoch_.load(std::memory_order_relaxed);
  s.last_gc_epoch = last_gc_epoch_.load(std::memory_order_relaxed);
  return s;
}

void CheckpointCoordinator::loop() {
  using clock = std::chrono::steady_clock;
  const auto quantum = std::chrono::milliseconds(200);

  int loop_iter = 0;
  while (running_.load(std::memory_order_relaxed)) {
    const auto now = clock::now();
    loop_iter++;

    // Snapshot metrics
    const uint64_t ckpt_epoch = checkpoint_epoch();
    const uint64_t log_end_ep = current_log_end_epoch();
    const size_t   replay_b   = estimate_replay_bytes();
    const uint64_t replay_e   = (log_end_ep >= ckpt_epoch) ? (log_end_ep - ckpt_epoch) : 0;

    last_replay_bytes_.store(replay_b, std::memory_order_relaxed);
    last_replay_epochs_.store(replay_e, std::memory_order_relaxed);

    // Decide what to do this iteration
    Action action = Action::None;

    // 1) Check rotation thresholds (size/age). Rotation implies checkpoint.
    bool need_rotate = false;
    auto log = std::atomic_load(&active_log_);
    if (log) {
      const size_t bytes = log->end_offset_relaxed();          // current file size
      const auto   age   = log->age_seconds_relaxed(now);      // age since creation
      const bool size_hit = (policy_.rotate_bytes > 0) &&
                           (bytes >= policy_.rotate_bytes);
      const bool age_hit  = (policy_.rotate_age.count() > 0) &&
                           (age.count() >= policy_.rotate_age.count());
      need_rotate = size_hit || age_hit;
      
    }

    // 2) Check checkpoint policy (bounded replay / cadence)
    const bool need_ckpt = should_checkpoint(ckpt_epoch, log_end_ep, replay_b, now);

    // 3) Atomically consume any outstanding checkpoint request
    bool was_requested = checkpoint_requested_.exchange(false, std::memory_order_acq_rel);

    // 4) Choose action. Rotation dominates (always pairs with checkpoint).
    if (need_rotate) {
      action = Action::CkptAndRotate;
    } else if (need_ckpt || was_requested) {
      action = Action::CkptOnly;
      if (was_requested) {
        debug() << "Checkpoint requested, will checkpoint";
      }
    } else {
      action = Action::None;
    }

    // Flush dirty ranges opportunistically even if doing nothing
    flush_dirty_ranges_if_needed();

    // 5) Sleep if no work; otherwise perform chosen action
    if (action == Action::None) {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait_for(lk, quantum, [&]{ 
        return checkpoint_requested_.load(std::memory_order_acquire) || !running_.load(); 
      });
      if (!running_.load()) break;
      continue; // re-evaluate
    }

    if (!running_.load()) break;

    const uint64_t epoch = choose_snapshot_epoch();
    last_epoch_.store(epoch, std::memory_order_relaxed);
    
    debug() << "Performing action, epoch=" << epoch << " action=" << (int)action;
    
    if (action == Action::CkptAndRotate) {
      do_checkpoint_and_rotate(epoch);
    } else if (action == Action::CkptOnly) {
      do_checkpoint(epoch);
    }

    last_ckpt_ = now;
  }
}

bool CheckpointCoordinator::should_checkpoint(uint64_t ckpt_epoch,
                                              uint64_t log_end_epoch,
                                              size_t replay_bytes,
                                              std::chrono::steady_clock::time_point now) const {
  using namespace std::chrono;
  const auto age = duration_cast<seconds>(now - last_ckpt_);

  // Use adaptive threshold if enabled
  size_t effective_threshold = policy_.max_replay_bytes;
  if (policy_.adaptive_wal_rotation) {
    effective_threshold = adjusted_replay_bytes_.load(std::memory_order_relaxed);
  }

  // Critical thresholds override min_interval
  if (replay_bytes >= effective_threshold) return true;
  if ((log_end_epoch - ckpt_epoch) >= policy_.max_replay_epochs) return true;
  
  // Time-based triggers respect min_interval
  if (age < policy_.min_interval) return false;
  
  if (age >= policy_.max_age) return true;

  // Query-only aggressiveness: if anything pending and idle long enough
  if (replay_bytes > 0 && age >= policy_.query_only_age) return true;

  // Steady baseline (also adapt this if enabled)
  size_t steady_threshold = policy_.steady_replay_bytes;
  if (policy_.adaptive_wal_rotation && current_throughput_.load() > policy_.throughput_threshold) {
    // High throughput: be more aggressive with checkpoints
    steady_threshold = policy_.min_replay_bytes;
  }
  if (replay_bytes >= steady_threshold) return true;
  if (age >= policy_.steady_age) return true;

  return false;
}

uint64_t CheckpointCoordinator::choose_snapshot_epoch() {
  auto log = std::atomic_load(&active_log_);
  
  // First-ever checkpoint on an empty system -> epoch 0
  const size_t wal_bytes = log ? log->get_end_offset() : 0;
  const uint64_t wal_epoch = log ? log->end_epoch_relaxed() : 0;
  
  if (wal_bytes == 0 && wal_epoch == 0 &&
      last_epoch_.load(std::memory_order_relaxed) == kNoCheckpoint) {
    return 0;
  }
  
  // If the WAL has any durable epochs, prefer the max WAL epoch (it is by definition covered).
  // Clamp to MVCC to be conservative if your invariants require it.
  if (wal_epoch > 0) {
    const uint64_t mvcc_epoch = mvcc_.get_global_epoch();
    return std::min(wal_epoch, mvcc_epoch);
  }
  
  // Nothing durable yet; do not advance MVCC here.
  return 0;
}

void CheckpointCoordinator::do_checkpoint(uint64_t epoch) {
  // Regular checkpoint should check for rotation and may run GC
  do_checkpoint_impl(epoch, static_cast<int>(CheckpointPostOp::MaybeRotate));
}

void CheckpointCoordinator::do_checkpoint_impl(uint64_t epoch, int post_op_int) {
  CheckpointPostOp post_op = static_cast<CheckpointPostOp>(post_op_int);
  using clock = std::chrono::steady_clock;
  const auto t0 = clock::now();

  // Serialize checkpoints without busy-wait
  std::unique_lock<std::mutex> lk(sync_mu_);
  while (sync_in_progress_) publish_cv_.wait(lk);
  sync_in_progress_ = true;
  lk.unlock();
  
  // RAII guard to ensure we always clear sync_in_progress_
  struct SyncGuard {
    CheckpointCoordinator* self;
    ~SyncGuard() {
      std::lock_guard<std::mutex> lk(self->sync_mu_);
      self->sync_in_progress_ = false;
      self->publish_cv_.notify_all();
    }
  } sync_guard{this};
  
  // Belt-and-suspenders: clamp epoch to what the WAL actually contains
  auto log = std::atomic_load(&active_log_);
  if (log) {
    log->sync();
    const uint64_t wal_epoch = log->end_epoch_relaxed();
    const size_t wal_bytes = log->get_end_offset();
    
    if (wal_bytes == 0 && wal_epoch == 0) {
      epoch = 0;  // allow empty-system checkpoint
    } else if (epoch > wal_epoch) {
      // Clamp to what's definitely durable so we never early-return due to coverage
      epoch = wal_epoch;
    }
  }

  // Verify WAL actually covers the chosen epoch (guard against races)
  // Special case: allow checkpointing at epoch 0 for empty system
  auto wal_covers_epoch = [&]{
    if (epoch == 0) return true;  // Always allow epoch 0 checkpoint
    auto cur = std::atomic_load(&active_log_);
    return cur && (cur->end_epoch_relaxed() >= epoch);
  };
  if (!wal_covers_epoch()) {
    // Wait up to a small bound; in a healthy system this is a no-op
    const auto deadline = clock::now() + std::chrono::milliseconds(50);
    while (!wal_covers_epoch() && clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!wal_covers_epoch()) {
      report_error("WAL does not cover checkpoint epoch " + std::to_string(epoch));
      return;
    }
  }

  // Flush dirty ranges for BALANCED/EVENTUAL up to epoch (bounded replay)
  flush_dirty_ranges_until(epoch);  // no-op if none / STRICT mode

  // Write checkpoint (temp -> fsync -> rename -> fsync dir)
  // OTCheckpoint expects a directory path, not a file path
  OTCheckpoint ckpt(manifest_.get_data_dir());
  bool success = ckpt.write(&ot_, epoch);
  if (!success) {
    report_error("Failed to write checkpoint at epoch " + std::to_string(epoch));
    return;
  }
  
  // OTCheckpoint creates: data_dir/ot_checkpoint_epoch-N.bin
  std::ostringstream final_path_stream;
  final_path_stream << manifest_.get_data_dir() << "/ot_checkpoint_epoch-" << epoch << ".bin";
  std::string final_path = final_path_stream.str();
  
  // Atomically record checkpoint in manifest (fsync + dir fsync)
  Manifest::CheckpointInfo ckpt_info;
  ckpt_info.path = final_path;
  ckpt_info.epoch = epoch;
  ckpt_info.size = 0;  // Could get from file stat
  ckpt_info.entries = 0;  // Could track from OT
  ckpt_info.crc32c = 0;  // Could compute
  manifest_.set_checkpoint(ckpt_info);
  if (!manifest_.store()) {
    report_error("Failed to update manifest with new checkpoint at epoch " + 
                 std::to_string(epoch));
    return;
  }
  checkpoints_written_.fetch_add(1, std::memory_order_relaxed);
  last_ckpt_ms_.store(
      (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(clock::now() - t0).count(),
      std::memory_order_relaxed);
  
  // Track successful checkpoint
  last_checkpoint_epoch_.store(epoch, std::memory_order_release);

  // Clean up old checkpoints (keep most recent based on policy)
  OTCheckpoint::cleanup_old_checkpoints(manifest_.get_data_dir(), policy_.checkpoint_keep_count);

  // Optionally rotate here (preferred for bounded replay)
  if (post_op == CheckpointPostOp::MaybeRotate && should_rotate_after_checkpoint(log.get())) {
    do_rotate_after_checkpoint(epoch);
  }

  // GC logs fully covered by checkpoint (policy-controlled)
  // Only run GC if we're not being called from rotation (rotation handles its own GC)
  if (post_op == CheckpointPostOp::MaybeRotate && policy_.gc_on_checkpoint) {
    run_log_gc(epoch, /*invoked_from_rotate=*/false);
    // Track successful GC
    last_gc_epoch_.store(epoch, std::memory_order_release);
  }

  // Run reclaimer adaptively
  if (reclaimer_) {
    const bool heavy_replay = last_replay_bytes_.load(std::memory_order_relaxed) >
                              (policy_.max_replay_bytes / 2);
    static std::atomic<uint64_t> every{0};
    if (heavy_replay || (every.fetch_add(1, std::memory_order_relaxed) % 10 == 0)) {
      size_t reclaimed = reclaimer_->run_once();
      if (reclaimed) report_metrics();
    }
  }
  
  // Report metrics after successful checkpoint
  report_metrics();
}

size_t CheckpointCoordinator::estimate_replay_bytes() const {
  // Estimate based on logs after checkpoint
  const auto& ckpt = manifest_.get_checkpoint();
  auto logs = manifest_.get_logs_after_checkpoint(ckpt.epoch);
  
  size_t total_bytes = 0;
  for (const auto& log : logs) {
    // If manifest has cached size, use it
    if (log.size > 0) {
      total_bytes += log.size;
    } else if (!log.path.empty()) {
      // Get actual file size using platform-independent method
      auto [result, size] = PlatformFS::file_size(log.path);
      if (result.ok) {
        total_bytes += size;
      }
    }
  }
  
  // Also check the current active log
  if (auto active = std::atomic_load(&active_log_)) {
    // Use the actual end_offset which tracks bytes written
    total_bytes += active->get_end_offset();
  }
  
  return total_bytes;
}

uint64_t CheckpointCoordinator::current_log_end_epoch() const {
  // Get the actual max epoch from the active log
  if (auto active = std::atomic_load(&active_log_)) {
    return active->end_epoch_relaxed();
  }
  
  // Fallback to manifest if no active log
  const auto& logs = manifest_.get_delta_logs();
  uint64_t max_epoch = 0;
  for (const auto& log : logs) {
    if (log.end_epoch > max_epoch) {
      max_epoch = log.end_epoch;
    }
  }
  return max_epoch;
}

uint64_t CheckpointCoordinator::checkpoint_epoch() const {
  return manifest_.get_checkpoint().epoch;
}

// ----------------- Dirty range tracking -----------------

void CheckpointCoordinator::submit_dirty_ranges(const std::vector<DirtyRange>& ranges) {
  if (ranges.empty()) return;
  
  std::lock_guard<std::mutex> lk(dirty_ranges_mu_);
  
  // Track the oldest dirty time if this is the first batch
  if (pending_dirty_ranges_.empty()) {
    oldest_dirty_time_ = std::chrono::steady_clock::now();
  }
  
  // Add ranges and update total bytes
  for (const auto& range : ranges) {
    pending_dirty_ranges_.push_back(range);
    total_dirty_bytes_ += range.length;
  }
  
  // Note: Actual flushing happens in the background loop
  // based on policy thresholds (dirty_flush_bytes, dirty_flush_age)
}

void CheckpointCoordinator::flush_dirty_ranges_if_needed() {
  std::vector<DirtyRange> ranges_to_flush;
  
  {
    std::lock_guard<std::mutex> lk(dirty_ranges_mu_);
    
    // Check if we should flush based on thresholds
    // This would normally check against the durability policy settings
    const size_t flush_threshold = 128 * 1024 * 1024;  // 128MB default
    const auto max_age = std::chrono::seconds(3);       // 3 seconds default
    
    auto now = std::chrono::steady_clock::now();
    auto age = now - oldest_dirty_time_;
    
    if (total_dirty_bytes_ >= flush_threshold || 
        (!pending_dirty_ranges_.empty() && age >= max_age)) {
      // Move ranges to local vector for flushing
      ranges_to_flush = std::move(pending_dirty_ranges_);
      pending_dirty_ranges_.clear();
      total_dirty_bytes_ = 0;
      oldest_dirty_time_ = now;
    }
  }
  
  if (!ranges_to_flush.empty()) {
    do_flush_dirty_ranges(std::move(ranges_to_flush));
  }
}

void CheckpointCoordinator::do_flush_dirty_ranges(std::vector<DirtyRange> ranges) {
  // Sort and coalesce ranges by file_id
  std::sort(ranges.begin(), ranges.end(),
           [](const DirtyRange& a, const DirtyRange& b) {
             if (a.file_id != b.file_id) return a.file_id < b.file_id;
             return a.offset < b.offset;
           });
  
  // Coalesce contiguous ranges
  std::vector<DirtyRange> coalesced;
  for (const auto& range : ranges) {
    if (coalesced.empty() || 
        coalesced.back().file_id != range.file_id ||
        coalesced.back().offset + coalesced.back().length < range.offset) {
      // Start new range
      coalesced.push_back(range);
    } else {
      // Extend existing range
      auto& last = coalesced.back();
      uint64_t end = std::max(last.offset + last.length, 
                             range.offset + range.length);
      last.length = end - last.offset;
    }
  }
  
  // TODO: Actually flush the coalesced ranges
  // This would need access to the segment allocator or data files
  // to map file_id/offset back to memory addresses and call msync/FlushViewOfFile
  
  // For now, just log that we would flush
  #ifdef DEBUG
  size_t total_bytes = 0;
  for (const auto& range : coalesced) {
    total_bytes += range.length;
  }
  // In production, this would call PlatformFS::flush_view() on the mapped regions
  #endif
}

// ----------------- Optional: group-commit combiner -----------------

bool CheckpointCoordinator::try_publish(NodeID new_root, uint64_t new_epoch) {
  if (group_commit_interval_.count() == 0) {
    // FIX 6: Direct publish path must also sync for durability
    if (auto log = std::atomic_load(&active_log_)) {
      log->sync();
    }
    sb_.publish(new_root, new_epoch);
    return true;
  }

  // Try to become the leader
  {
    std::unique_lock<std::mutex> lk(sync_mu_);
    if (sync_in_progress_) {
      // Another leader is aggregating; caller can return and keep appending
      return false;
    }
    sync_in_progress_ = true;
  }

  // FIX 2: Capture the log pointer NOW, before sleep
  auto captured_log = std::atomic_load(&active_log_);
  
  // Leader path: small delay to batch, then fsync+publish once
  std::this_thread::sleep_for(group_commit_interval_);
  leader_publish(new_root, new_epoch, captured_log);
  return true;
}

void CheckpointCoordinator::leader_publish(NodeID root, uint64_t epoch, std::shared_ptr<OTDeltaLog> captured_log) {
  // FIX 2: Sync the log that was captured BEFORE the sleep
  if (captured_log) {
    captured_log->sync(); // fsync the captured log
  }

  // Publish superblock snapshot (msync/flush + fsync + fsync(dir) inside)
  sb_.publish(root, epoch);

  // Wake up any waiters
  {
    std::lock_guard<std::mutex> lk(sync_mu_);
    sync_in_progress_ = false;
  }
  publish_cv_.notify_all();
}

void CheckpointCoordinator::wait_for_publish() {
  std::unique_lock<std::mutex> lk(sync_mu_);
  publish_cv_.wait(lk, [&]{ return !sync_in_progress_; });
}

void CheckpointCoordinator::set_group_commit_interval(std::chrono::milliseconds m) {
  group_commit_interval_ = m;
}

void CheckpointCoordinator::do_checkpoint_and_rotate(uint64_t epoch) {
  const auto t0 = std::chrono::steady_clock::now();

  try {
    // Phase A: Produce checkpoint (without internal rotation since we'll do it here)
    do_checkpoint_impl(epoch, static_cast<int>(CheckpointPostOp::None));
    
    // Phase B: Prepare new log
    auto cur = std::atomic_load(&active_log_);
    const uint64_t new_seq = cur ? cur->sequence() + 1 : 1;
    
    auto new_owned = std::shared_ptr<OTDeltaLog>(open_new_log(new_seq));
    
    // Phase C: Atomic cut - swap active log pointer FIRST
    auto old = std::atomic_exchange(&active_log_, new_owned);
    
    // Phase D: Quiesce old log BEFORE computing boundaries
    uint64_t final_end = epoch;  // default if no old log
    size_t final_size = 0;
    std::string old_log_manifest_path;
    
    if (old) {
      old->prepare_close();
      old->sync();
      final_end = old->end_epoch_relaxed();  // Get stable final epoch
      final_size = old->get_end_offset();
      
      // Get the exact path that was stored in manifest during activation
      std::filesystem::path full_path(old->path());
      std::filesystem::path data_dir(manifest_.get_data_dir());
      
      // Compute relative path from data_dir
      try {
        old_log_manifest_path = std::filesystem::relative(full_path, data_dir).string();
      } catch (...) {
        // Fallback: assume logs/ prefix
        old_log_manifest_path = "logs/" + full_path.filename().string();
      }
      
      // Close in manifest with exact path match
      if (!manifest_.close_delta_log(old_log_manifest_path, final_end, final_size)) {
        report_error("Failed to close log in manifest: " + old_log_manifest_path);
      }
    }
    
    // Phase E: NOW compute new log's start epoch (after old log is quiesced)
    const uint64_t new_start = std::max(final_end + 1, epoch + 1);
    
    // Phase F: Activate new log with correct start epoch
    activate_new_log(new_owned.get(), new_start);
    
    // Phase G: Persist manifest + fsync dir (always, even if no old log)
    manifest_.store();
    std::filesystem::path manifest_path = manifest_.get_manifest_path();
    PlatformFS::fsync_directory(manifest_path.parent_path().string());
    
    // Phase H: Finalize old log closure
    if (old) {
      old->close();
      // shared_ptr drops here; if no other holders, object is freed
    }
    
    // Phase I: Policy-controlled GC
    if (policy_.gc_on_rotate) {
      run_log_gc(epoch, /*invoked_from_rotate=*/true);
      // Track successful GC
      last_gc_epoch_.store(epoch, std::memory_order_release);
    }
    
    // Phase J: Stats
    const auto t1 = std::chrono::steady_clock::now();
    rotations_.fetch_add(1, std::memory_order_relaxed);
    last_rotate_ms_.store(
      std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count(),
      std::memory_order_relaxed);
    
    report_metrics();
    
  } catch (const std::exception& e) {
    report_error("Failed during checkpoint and rotate: " + std::string(e.what()));
  }
}

void CheckpointCoordinator::do_rotate_after_checkpoint(uint64_t epoch) {
  try {
    // Phase A: Prepare new log
    auto cur = std::atomic_load(&active_log_);
    const uint64_t new_seq = cur ? cur->sequence() + 1 : 1;
    
    auto new_owned = std::shared_ptr<OTDeltaLog>(open_new_log(new_seq));
    
    // Phase B: Atomic cut - swap active log pointer FIRST
    auto old = std::atomic_exchange(&active_log_, new_owned);
    
    // Phase C: Quiesce old log BEFORE computing boundaries
    uint64_t final_end = epoch;  // default if no old log
    size_t final_size = 0;
    std::string old_log_manifest_path;
    
    if (old) {
      old->prepare_close();
      old->sync();
      final_end = old->end_epoch_relaxed();  // Get stable final epoch
      final_size = old->get_end_offset();
      
      // Get the exact path that was stored in manifest during activation
      std::filesystem::path full_path(old->path());
      std::filesystem::path data_dir(manifest_.get_data_dir());
      
      // Compute relative path from data_dir
      try {
        old_log_manifest_path = std::filesystem::relative(full_path, data_dir).string();
      } catch (...) {
        // Fallback: assume logs/ prefix
        old_log_manifest_path = "logs/" + full_path.filename().string();
      }
      
      // Close in manifest with exact path match
      if (!manifest_.close_delta_log(old_log_manifest_path, final_end, final_size)) {
        report_error("Failed to close log in manifest: " + old_log_manifest_path);
      }
    }
    
    // Phase D: NOW compute new log's start epoch (after old log is quiesced)
    const uint64_t new_start = std::max(final_end + 1, epoch + 1);
    
    // Phase E: Activate new log with correct start epoch
    activate_new_log(new_owned.get(), new_start);
    
    // Phase F: Persist manifest + fsync dir (always, even if no old log)
    manifest_.store();
    std::filesystem::path manifest_path = manifest_.get_manifest_path();
    PlatformFS::fsync_directory(manifest_path.parent_path().string());
    
    // Phase G: Finalize old log closure
    if (old) {
      old->close();
      // shared_ptr drops here; if no other holders, object is freed
    }
    
    rotations_.fetch_add(1, std::memory_order_relaxed);
    
  } catch (const std::exception& e) {
    report_error("Failed during log rotation: " + std::string(e.what()));
  }
}

OTDeltaLog* CheckpointCoordinator::open_new_log(uint64_t sequence) {
  // Generate log path - use parent of current log or default
  std::filesystem::path log_dir;
  if (auto cur = std::atomic_load(&active_log_)) {
    log_dir = std::filesystem::path(cur->path()).parent_path();
  } else {
    log_dir = std::filesystem::path(manifest_.get_data_dir()) / "logs";
  }
  
  // Ensure directory exists
  PlatformFS::ensure_directory(log_dir.string());
  
  char log_name[256];
  snprintf(log_name, sizeof(log_name), "delta_%012llu.wal", (unsigned long long)sequence);
  const std::string new_log_path = (log_dir / log_name).string();
  
  // Create new log with sequence number
  auto* log = new OTDeltaLog(new_log_path, OTDeltaLog::kDefaultPreallocChunk, sequence);
  if (!log->open_for_append()) {
    delete log;
    throw std::runtime_error("Failed to open new delta log: " + new_log_path);
  }
  
  // Fsync directory to make file creation durable
  PlatformFS::fsync_directory(log_dir.string());
  
  return log;
}

void CheckpointCoordinator::activate_new_log(OTDeltaLog* new_log, uint64_t start_epoch) {
  // Update manifest with new log info
  // Store just the relative path in manifest (logs/filename.wal)
  std::filesystem::path log_path(new_log->path());
  std::string filename = log_path.filename().string();
  
  Manifest::DeltaLogInfo log_info;
  log_info.path = "logs/" + filename;  // Simple string concatenation
  log_info.start_epoch = start_epoch;
  log_info.end_epoch = 0;  // Active log
  log_info.size = 0;
  manifest_.add_delta_log(log_info);
  
  if (!manifest_.store()) {
    throw std::runtime_error("Failed to persist manifest after log activation");
  }
  
  // Fsync manifest directory for crash consistency
  std::filesystem::path manifest_path = manifest_.get_manifest_path();
  PlatformFS::fsync_directory(manifest_path.parent_path().string());
  std::string log_dir = manifest_.get_data_dir() + "/logs";
  PlatformFS::fsync_directory(log_dir);
}

// Note: close_old_log is now unused - rotation methods handle closing inline
// Keeping for potential future use
void CheckpointCoordinator::close_old_log(OTDeltaLog* old_log) {
  if (!old_log) return;
  
  // Begin close - blocks new appends
  old_log->prepare_close();
  
  // Sync to ensure all deltas are durable
  old_log->sync();
  
  // Record end epoch and size in manifest
  const uint64_t end_epoch = old_log->end_epoch_relaxed();
  const size_t final_size = old_log->get_end_offset();
  
  if (!manifest_.close_delta_log(old_log->path(), end_epoch, final_size)) {
    report_error("close_delta_log: path not found: " + old_log->path());
  }
  manifest_.store();
  
  // Fsync manifest directory for crash consistency
  std::filesystem::path manifest_path = manifest_.get_manifest_path();
  PlatformFS::fsync_directory(manifest_path.parent_path().string());
  
  // Close the log file
  old_log->close();
  
  // Caller is responsible for deletion
}

bool CheckpointCoordinator::should_rotate_after_checkpoint(OTDeltaLog* log) const {
  if (!log) return false;
  
  // Check rotation criteria
  const size_t log_bytes = log->end_offset_relaxed();
  const auto log_age = log->age_seconds_relaxed(std::chrono::steady_clock::now());
  
  return (policy_.rotate_bytes && log_bytes >= policy_.rotate_bytes) ||
         (policy_.rotate_age.count() && log_age.count() >= policy_.rotate_age.count());
}

void CheckpointCoordinator::run_log_gc(uint64_t checkpoint_epoch, bool invoked_from_rotate) {
  // Check policy gates
  if (invoked_from_rotate && !policy_.gc_on_rotate) return;
  if (!invoked_from_rotate && !policy_.gc_on_checkpoint) return;

  // Get current logs from manifest
  auto logs = manifest_.get_delta_logs();
  if (logs.size() <= policy_.gc_min_keep_logs) return;  // Keep minimum number of logs

  // TODO: Implement gc_lag_checkpoints if needed
  if (policy_.gc_lag_checkpoints > 0) {
    // For now, skip GC if lag is configured
    // In production, you'd track previous checkpoint epochs
    return;
  }

  // Sort logs newest-first (by start_epoch) for deterministic processing
  std::sort(logs.begin(), logs.end(), [](const auto& a, const auto& b) {
    // Active logs (end_epoch=0) should sort as newest
    if (a.end_epoch == 0) return true;
    if (b.end_epoch == 0) return false;
    // Otherwise sort by start_epoch descending (newest first)
    return a.start_epoch > b.start_epoch;
  });

  // Count of CLOSED logs we decided to keep (active logs do NOT contribute)
  size_t kept_closed = 0;
  size_t pruned_count = 0;
  
  for (const auto& log : logs) {
    const bool is_active = (log.end_epoch == 0);
    if (is_active) {
      // Never prune active; do not count toward kept_closed floor
      continue;
    }
    
    const bool fully_covered = (log.end_epoch <= checkpoint_epoch);
    
    // Enforce "keep at least N closed logs" floor
    if (kept_closed < policy_.gc_min_keep_logs) {
      // Keep this closed log unconditionally to satisfy min-keep floor
      kept_closed++;
      continue;
    }
    
    // Now we can prune only if fully covered by the checkpoint
    if (fully_covered) {
      // Optional age check
      if (policy_.gc_min_age.count() > 0) {
        // TODO: Implement file age check if needed
        // For now, skip age-based filtering
      }
      
      // This log can be pruned
      pruned_count++;
    } else {
      // Not covered; we keep it (and since it's closed, it contributes to kept_closed)
      kept_closed++;
    }
  }
  
  // Actually prune the logs if any were identified
  if (pruned_count > 0) {
    log_gc_.truncate_logs_before_checkpoint(checkpoint_epoch);
    pruned_logs_.fetch_add(pruned_count, std::memory_order_relaxed);
  }
}

void CheckpointCoordinator::flush_dirty_ranges_until(uint64_t epoch) {
  // For BALANCED/EVENTUAL modes - flush any dirty ranges <= epoch
  // This is a no-op for STRICT mode where data is already flushed
  
  std::vector<DirtyRange> to_flush;
  {
    std::lock_guard<std::mutex> lk(dirty_ranges_mu_);
    // Filter ranges that need flushing
    // TODO: Implement based on durability mode and epoch tracking
    to_flush = std::move(pending_dirty_ranges_);
    pending_dirty_ranges_.clear();
    total_dirty_bytes_ = 0;
  }
  
  if (!to_flush.empty()) {
    do_flush_dirty_ranges(std::move(to_flush));
  }
}

void CheckpointCoordinator::init_or_adopt_active_log() {
  // Already initialized?
  if (std::atomic_load(&active_log_)) return;

  // Ensure logs/ exists
  const std::string log_dir = manifest_.get_data_dir() + "/logs";
  std::filesystem::create_directories(log_dir);

  auto logs = manifest_.get_delta_logs();

  // Case A: Fresh install — no logs in manifest
  if (logs.empty()) {
    const uint64_t seq = 1;
    char name[256];
    snprintf(name, sizeof(name), "delta_%012llu.wal",
             static_cast<unsigned long long>(seq));
    const std::string path = log_dir + "/" + name;

    // Create and open new active log
    auto* raw_log = new OTDeltaLog(path, OTDeltaLog::kDefaultPreallocChunk, seq);
    if (!raw_log->open_for_append()) {
      delete raw_log;
      throw std::runtime_error("Failed to open new log for append: " + path);
    }
    auto log = std::shared_ptr<OTDeltaLog>(raw_log);
    
    // Publish as active
    std::atomic_store(&active_log_, log);

    // Register in manifest as active (end_epoch == 0)
    // Store path relative to data_dir in manifest
    Manifest::DeltaLogInfo info;
    info.path        = std::string("logs/") + name;  // Store relative path
    info.start_epoch = 1;  // Start at epoch 1
    info.end_epoch   = 0;  // Active log
    info.size        = 0;
    manifest_.add_delta_log(info);
    manifest_.store();
    PlatformFS::fsync_directory(std::filesystem::path(manifest_.get_manifest_path()).parent_path().string());
    PlatformFS::fsync_directory(log_dir);
    return;
  }

  // Case B: Manifest has logs. Find the active one (end_epoch == 0).
  const Manifest::DeltaLogInfo* active = nullptr;
  const Manifest::DeltaLogInfo* last   = nullptr;
  for (const auto& l : logs) {
    if (l.end_epoch == 0) active = &l;
    if (!last || l.start_epoch > last->start_epoch) last = &l;
  }

  if (active) {
    // Adopt existing active log from manifest
    // Path in manifest is relative to data_dir, construct full path
    std::filesystem::path full_path = std::filesystem::path(manifest_.get_data_dir()) / active->path;
    const uint64_t seq = parse_sequence_from_path(active->path);
    auto* raw_log = new OTDeltaLog(full_path.string(), OTDeltaLog::kDefaultPreallocChunk, seq);
    if (!raw_log->open_for_append()) {
      delete raw_log;
      throw std::runtime_error("Failed to open active log for append: " + full_path.string());
    }
    auto log = std::shared_ptr<OTDeltaLog>(raw_log);
    std::atomic_store(&active_log_, log);
    return;
  }

  // Case C: All logs are closed; start a new active one that continues after the last
  const uint64_t new_seq = last ? (parse_sequence_from_path(last->path) + 1) : 1;
  char name[256];
  snprintf(name, sizeof(name), "delta_%012llu.wal",
           static_cast<unsigned long long>(new_seq));
  const std::string path = log_dir + "/" + name;

  auto* raw_log = new OTDeltaLog(path, OTDeltaLog::kDefaultPreallocChunk, new_seq);
  if (!raw_log->open_for_append()) {
    delete raw_log;
    throw std::runtime_error("Failed to open new log for append: " + path);
  }
  auto log = std::shared_ptr<OTDeltaLog>(raw_log);
  std::atomic_store(&active_log_, log);

  Manifest::DeltaLogInfo info;
  info.path        = std::string("logs/") + name;  // Store relative path
  info.start_epoch = last ? (last->end_epoch + 1) : 1;
  info.end_epoch   = 0;
  info.size        = 0;
  manifest_.add_delta_log(info);
  manifest_.store();
  PlatformFS::fsync_directory(std::filesystem::path(manifest_.get_manifest_path()).parent_path().string());
  PlatformFS::fsync_directory(log_dir);
}

uint64_t CheckpointCoordinator::parse_sequence_from_path(const std::string& path) {
  // Parse sequence from path like "delta_000000000001.wal"
  std::filesystem::path p(path);
  std::string filename = p.filename().string();
  
  // Expected format: delta_NNNNNNNNNNNN.wal
  if (filename.size() >= 17 && filename.substr(0, 6) == "delta_") {
    std::string seq_str = filename.substr(6, 12);
    try {
      return std::stoull(seq_str);
    } catch (...) {
      // Fall through to return 1
    }
  }
  return 1;  // Default if we can't parse
}

} // namespace xtree::persist