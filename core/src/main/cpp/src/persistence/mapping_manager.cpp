/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mapping_manager.h"
#include "config.h"  // For sys_config::get_page_size()

#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

#include <errno.h>
#include <stdexcept>
#include <algorithm>
#include <iostream>
#include <cstring>
#include "../util/log.h"

namespace xtree {
namespace persist {

MappingManager& MappingManager::global() {
    // Meyers' singleton - thread-safe lazy initialization (C++11)
    static MappingManager* instance = []() {
        // Default configuration for global manager:
        // - 128MB window size (smaller for better granularity)
        // - 4GB max memory budget (configurable via environment)
        size_t window_size = 128ULL << 20;  // 128 MB
        size_t max_memory = 4ULL << 30;     // 4 GB default budget
        size_t max_extents = 8192;          // VMA limit fallback

        // Check environment for overrides
        if (const char* env = std::getenv("XTREE_MMAP_WINDOW_SIZE")) {
            window_size = std::stoull(env);
        }
        if (const char* env = std::getenv("XTREE_MMAP_BUDGET")) {
            // Parse memory size with suffixes (e.g., "2GB", "512MB")
            std::string val(env);
            size_t multiplier = 1;
            if (val.size() > 2) {
                std::string suffix = val.substr(val.size() - 2);
                if (suffix == "GB" || suffix == "gb") {
                    multiplier = 1ULL << 30;
                    val = val.substr(0, val.size() - 2);
                } else if (suffix == "MB" || suffix == "mb") {
                    multiplier = 1ULL << 20;
                    val = val.substr(0, val.size() - 2);
                } else if (suffix == "KB" || suffix == "kb") {
                    multiplier = 1ULL << 10;
                    val = val.substr(0, val.size() - 2);
                }
            }
            max_memory = std::stoull(val) * multiplier;
        }

        auto* mm = new MappingManager(
            FileHandleRegistry::global(),
            window_size,
            max_extents
        );
        mm->set_memory_budget(max_memory);
        return mm;
    }();
    return *instance;
}

void MappingExtent::unmap() {
    if (base != nullptr) {
        // Ensure mmapped data is on disk before we drop the mapping
        // (safe for RO mappings: it's a no-op for clean pages)
        msync(base, length, MS_SYNC);
        munmap(base, length);
        base = nullptr;
        length = 0;
    }
}

MappingExtent* FileMapping::find_extent(size_t off, size_t len) {
    // Binary search since extents are sorted by file_off
    auto it = std::lower_bound(extents.begin(), extents.end(), off,
        [](const std::unique_ptr<MappingExtent>& ext, size_t offset) {
            return ext->file_off + ext->length <= offset;
        });
    
    if (it != extents.end() && (*it)->contains(off, len)) {
        return it->get();
    }
    return nullptr;
}

void FileMapping::insert_extent(std::unique_ptr<MappingExtent> ext) {
    // Find insertion point to maintain sort order
    auto it = std::lower_bound(extents.begin(), extents.end(), ext->file_off,
        [](const std::unique_ptr<MappingExtent>& a, size_t off) {
            return a->file_off < off;
        });
    extents.insert(it, std::move(ext));
}

void MappingManager::Pin::release() {
    if (mgr_ && ext_ && ext_->pins > 0) {
        // Advise OS that this memory region is no longer needed.
        // This allows the OS to drop the pages from physical memory,
        // reducing RSS without unmapping the entire extent.
        // The data can be paged back in from disk if accessed again.
        if (ptr_ && size_ > 0) {
#if defined(_WIN32)
            // Windows: VirtualUnlock allows pages to be swapped out
            // DiscardVirtualMemory is stronger but requires Windows 8.1+
            // For now, VirtualUnlock is the most portable option
            VirtualUnlock(ptr_, size_);
#elif defined(__APPLE__)
            // macOS: MADV_FREE is more aggressive than MADV_DONTNEED
            // It marks pages as "can be reused immediately" rather than just hinting
            madvise(ptr_, size_, MADV_FREE);
#else
            // Linux: MADV_DONTNEED immediately drops pages from RSS
            madvise(ptr_, size_, MADV_DONTNEED);
#endif
        }

        std::lock_guard<std::mutex> lock(mgr_->mu_);
        ext_->pins--;
        mgr_->total_pins_--;
    }
}

MappingManager::MappingManager(FileHandleRegistry& fhr,
                               size_t window_size,
                               size_t max_extents_global)
    : fhr_(fhr), 
      window_size_(sys_config::page_align(window_size)),  // Align in initializer list
      max_extents_global_(std::max<size_t>(1, max_extents_global)) {
    
    // Verify window size is at least one page
    size_t page_size = sys_config::get_page_size();
    if (window_size_ < page_size) {
        // This should not happen after page_align, but be defensive
        const_cast<size_t&>(window_size_) = page_size;
    }
}

MappingManager::~MappingManager() {
    std::lock_guard<std::mutex> lock(mu_);
    
    // Unmap all extents and properly unpin/release file handles
    for (auto& [path, fmap] : by_file_) {
        if (!fmap) continue;
        
        // Unmap and unpin each extent
        for (auto& ext : fmap->extents) {
            if (ext) {
                ext->unmap();
                // Unpin the file handle for this extent
                if (fmap->fh) {
                    fhr_.unpin(fmap->fh);
                }
            }
        }
        fmap->extents.clear();
        
        // Release the file handle
        if (fmap->fh) {
            fhr_.release(fmap->fh);
            fmap->fh.reset();
        }
    }
    by_file_.clear();
}

MappingManager::Pin MappingManager::pin(const std::string& path, 
                                        size_t off, 
                                        size_t len, 
                                        bool writable) {
    // Protect against zero-length mappings
    if (len == 0) {
        return Pin();  // Return null pin
    }
    
    // Canonicalize the path first (before taking lock)
    std::string cpath = fhr_.canonicalize_path(path);
    
    std::lock_guard<std::mutex> lock(mu_);
    
    // Get or create FileMapping using canonical path
    auto& fmap_ptr = by_file_[cpath];
    if (!fmap_ptr) {
        fmap_ptr = std::make_unique<FileMapping>();
        fmap_ptr->path = cpath;  // Store canonical path  // Store the path for later use
        // Note: We don't acquire the file handle here - that happens in ensure_extent
    }
    FileMapping* fmap = fmap_ptr.get();
    
    // Find or create extent
    MappingExtent* ext = ensure_extent(*fmap, writable, off, len);
    if (!ext) {
        throw std::runtime_error("Failed to map range [" + std::to_string(off) + 
                               ", " + std::to_string(off + len) + ") in " + path);
    }
    
    // Get pointer within extent
    uint8_t* ptr = ext->ptr_at(off);
    if (!ptr) {
        throw std::runtime_error("Invalid pointer calculation for offset " + 
                               std::to_string(off) + " in extent");
    }
    
    // Update usage tracking
    ext->pins++;
    ext->update_last_use();
    total_pins_++;

    return Pin(this, fmap, ext, ptr, len);
}

void MappingManager::unpin(Pin&& p) {
    // Pin destructor handles this
    p.reset();
}

void MappingManager::prefetch(const std::string& path,
                              const std::vector<std::pair<size_t, size_t>>& ranges) {
    // Canonicalize the path first
    std::string cpath = fhr_.canonicalize_path(path);
    
    std::lock_guard<std::mutex> lock(mu_);
    
    // Get FileMapping using canonical path
    auto it = by_file_.find(cpath);
    if (it == by_file_.end() || !it->second) {
        return;  // Nothing to prefetch
    }
    
    FileMapping* fmap = it->second.get();
    
    // For each range, issue madvise(MADV_WILLNEED) if mapped
    for (const auto& [off, len] : ranges) {
        MappingExtent* ext = fmap->find_extent(off, len);
        if (ext && ext->base) {
            uint8_t* ptr = ext->ptr_at(off);
            if (ptr) {
                madvise(ptr, len, MADV_WILLNEED);
            }
        }
    }
}

size_t MappingManager::extent_count() const {
    std::lock_guard<std::mutex> lock(mu_);
    return total_extents_;
}

size_t MappingManager::debug_total_extents() const {
    return extent_count();
}

void MappingManager::debug_evict_all_unpinned() {
    std::lock_guard<std::mutex> lock(mu_);

    for (auto& [path, fmap] : by_file_) {
        if (!fmap) continue;

        FileMapping* fmap_ptr = fmap.get();  // Get raw pointer for lambda capture
        auto& extents = fmap_ptr->extents;
        extents.erase(
            std::remove_if(extents.begin(), extents.end(),
                [this, fmap_ptr](std::unique_ptr<MappingExtent>& ext) {
                    if (ext && ext->pins == 0) {
                        size_t evicted_bytes = ext->length;
                        ext->unmap();

                        // Track memory reduction
                        total_memory_mapped_ -= evicted_bytes;
                        evictions_bytes_ += evicted_bytes;

                        // Unpin the file handle for this extent
                        fhr_.unpin(fmap_ptr->fh);

                        total_extents_--;
                        total_evictions_++;
                        return true;
                    }
                    return false;
                }),
            extents.end()
        );

        // If no more extents, release the file handle
        if (extents.empty() && fmap_ptr->fh) {
            fhr_.release(fmap_ptr->fh);
            fmap_ptr->fh.reset();
        }
    }

    // NEW: tell FHR to drop any now-unpinned FDs immediately
    fhr_.debug_evict_all_unpinned();
}

void MappingManager::set_memory_budget(size_t max_bytes, float eviction_headroom) {
    std::lock_guard<std::mutex> lock(mu_);
    max_memory_budget_ = max_bytes;
    // Clamp headroom to [0.0, 0.5]
    if (eviction_headroom < 0.0f) eviction_headroom = 0.0f;
    if (eviction_headroom > 0.5f) eviction_headroom = 0.5f;
    eviction_headroom_ = eviction_headroom;
}

size_t MappingManager::get_total_memory_mapped() const {
    std::lock_guard<std::mutex> lock(mu_);
    return total_memory_mapped_;
}

MappingManager::MappingStats MappingManager::getStats() const {
    std::lock_guard<std::mutex> lock(mu_);
    MappingStats stats;
    stats.total_extents = total_extents_;
    stats.total_memory_mapped = total_memory_mapped_;
    stats.max_memory_budget = max_memory_budget_;
    stats.total_pins_active = total_pins_;
    stats.evictions_count = total_evictions_;
    stats.evictions_bytes = evictions_bytes_;
    if (max_memory_budget_ > 0) {
        stats.memory_utilization =
            static_cast<double>(total_memory_mapped_) / static_cast<double>(max_memory_budget_);
    }
    return stats;
}

MappingExtent* MappingManager::ensure_extent(FileMapping& fm, 
                                             bool writable, 
                                             size_t off, 
                                             size_t len) {
    // Reuse if an existing one fully contains the requested range
    if (auto* e = fm.find_extent(off, len)) {
        e->update_last_use();  // keep LRU fresh on reuse
        if (fm.fh) fm.fh->update_last_use();
        return e;
    }
    
    evict_extents_if_needed();
    
    // Ensure we have an FD, upgrade to writable if needed
    if (!fm.fh) {
        fm.fh = fhr_.acquire(fm.path, writable, /*create=*/writable);
        // acquire() pins once
    } else if (writable && !fm.fh->writable) {
        fhr_.ensure_writable(fm.fh, /*create=*/true);
    }
    
    // --- Compute the window we want to map (before growing the file) ---
    const size_t ws = window_size_;
    size_t window_start = (off / ws) * ws;
    size_t needed_end = off + len;
    size_t window_end = window_start + ws;
    if (window_end < needed_end) {
        // spill into more windows until it covers [off, off+len)
        window_end = ((needed_end + ws - 1) / ws) * ws;
    }
    
    // --- Make sure the file is big enough for the whole window when writable ---
    if (writable) {
        // Grow to the full window end so first mapping is "final"
        if (window_end > fm.fh->size_bytes) {
            fhr_.ensure_size(fm.fh, window_end);
            fm.fh->update_last_use();
        }
    } else {
        // Read-only: refuse mapping that starts beyond EOF
        if (off >= fm.fh->size_bytes) {
            throw std::runtime_error("Read mapping starts beyond EOF: offset=" + 
                                   std::to_string(off) + ", file_size=" + 
                                   std::to_string(fm.fh->size_bytes));
        }
        // Clamp the window to existing file size
        if (window_end > fm.fh->size_bytes) {
            window_end = fm.fh->size_bytes;
        }
    }
    
    size_t window_len = window_end - window_start;
    if (window_len == 0) {
        // Nothing to map (e.g., RO and file smaller than a page at tail)
        throw std::runtime_error("zero-length window after clamping");
    }
    
    // Replace at most one prior window with the same window_start,
    // otherwise leave disjoint windows alone. This preserves reuse and
    // avoids nuking unrelated windows in the same file.
    {
        auto& exts = fm.extents;
        for (size_t i = 0; i < exts.size(); ++i) {
            auto& e = exts[i];
            if (e->file_off == window_start) {
                // Found the same window start. If identical size, just reuse.
                if (e->length == window_len) {
                    e->update_last_use();
                    if (fm.fh) fm.fh->update_last_use();
                    return e.get();
                }
                // If pinned, we cannot replace; fall through to map a disjoint
                // (This will create a second extent in the same window region,
                //  which is rare now that first map is full-window. But it's
                //  still safe; tests won't hit this path anymore.)
                if (e->pins == 0) {
                    e->unmap();
                    fhr_.unpin(fm.fh);
                    exts.erase(exts.begin() + i);
                    total_extents_--;
                    total_evictions_++;
                }
                break;
            }
        }
    }
    
    // Create the mapping
    auto new_ext = create_extent(*fm.fh, window_start, window_len, writable);
    MappingExtent* result = new_ext.get();
    
    // Pin the file handle for this mapped extent
    fhr_.pin(fm.fh);
    
    // Insert into FileMapping and track memory
    size_t extent_size = result->length;
    fm.insert_extent(std::move(new_ext));
    total_extents_++;
    total_memory_mapped_ += extent_size;  // Track memory usage

    return result;
}

void MappingManager::evict_extents_if_needed() {
    // Primary: Memory-based eviction (if budget is set)
    if (max_memory_budget_ > 0 && total_memory_mapped_ > max_memory_budget_) {
        // Evict to target with hysteresis to avoid thrashing
        size_t target = static_cast<size_t>(
            max_memory_budget_ * (1.0f - eviction_headroom_));
        evict_to_memory_target(target);
    }
    // Secondary: Count-based eviction (VMA limit fallback)
    else if (total_extents_ >= max_extents_global_) {
        size_t to_evict = (total_extents_ - max_extents_global_) + 1;
        auto candidates = find_eviction_candidates(to_evict);

        for (const auto& [path, idx] : candidates) {
            auto it = by_file_.find(path);
            if (it != by_file_.end() && it->second) {
                auto& fmap = it->second;
                auto& extents = fmap->extents;
                if (idx < extents.size() && extents[idx]) {
                    size_t evicted_bytes = extents[idx]->length;
                    extents[idx]->unmap();

                    // Track memory reduction
                    total_memory_mapped_ -= evicted_bytes;
                    evictions_bytes_ += evicted_bytes;

                    // Unpin the file handle for this extent
                    fhr_.unpin(fmap->fh);

                    extents.erase(extents.begin() + idx);
                    total_extents_--;
                    total_evictions_++;

                    // If no more extents, release the file handle
                    if (extents.empty() && fmap->fh) {
                        fhr_.release(fmap->fh);
                        fmap->fh.reset();
                    }
                }
            }
        }

        if (candidates.empty() && total_extents_ >= max_extents_global_) {
            trace() << "[MappingManager] Warning: Cannot evict - all extents are pinned. "
                      << "Total: " << total_extents_
                      << ", Max: " << max_extents_global_ << std::endl;
        }
    }
}

void MappingManager::evict_to_memory_target(size_t target_bytes) {
    // Find candidates sorted by LRU (oldest first)
    auto candidates = find_eviction_candidates(total_extents_);  // Get all candidates

    for (const auto& [path, idx] : candidates) {
        if (total_memory_mapped_ <= target_bytes) {
            break;  // Reached target
        }

        auto it = by_file_.find(path);
        if (it != by_file_.end() && it->second) {
            auto& fmap = it->second;
            auto& extents = fmap->extents;
            if (idx < extents.size() && extents[idx] && extents[idx]->pins == 0) {
                size_t evicted_bytes = extents[idx]->length;
                extents[idx]->unmap();

                // Track memory reduction
                total_memory_mapped_ -= evicted_bytes;
                evictions_bytes_ += evicted_bytes;

                // Unpin the file handle for this extent
                fhr_.unpin(fmap->fh);

                extents.erase(extents.begin() + idx);
                total_extents_--;
                total_evictions_++;

                // If no more extents, release the file handle
                if (extents.empty() && fmap->fh) {
                    fhr_.release(fmap->fh);
                    fmap->fh.reset();
                }
            }
        }
    }

    if (total_memory_mapped_ > target_bytes) {
        trace() << "[MappingManager] Warning: Cannot reach memory target - extents pinned. "
                  << "Current: " << total_memory_mapped_
                  << ", Target: " << target_bytes << std::endl;
    }
}

std::vector<std::pair<std::string, size_t>> 
MappingManager::find_eviction_candidates(size_t count) {
    struct Candidate {
        std::string path;
        size_t index;
        uint64_t last_use_ns;
    };
    
    std::vector<Candidate> candidates;
    
    // Find all unpinned extents
    for (const auto& [path, fmap] : by_file_) {
        if (!fmap) continue;
        
        for (size_t i = 0; i < fmap->extents.size(); i++) {
            const auto& ext = fmap->extents[i];
            if (ext && ext->pins == 0) {
                candidates.push_back({path, i, ext->last_use_ns});
            }
        }
    }
    
    // Sort by LRU (oldest first)
    std::sort(candidates.begin(), candidates.end(),
              [](const Candidate& a, const Candidate& b) {
                  return a.last_use_ns < b.last_use_ns;
              });
    
    // Take up to 'count' candidates
    std::vector<std::pair<std::string, size_t>> result;
    for (size_t i = 0; i < std::min(count, candidates.size()); i++) {
        result.push_back({candidates[i].path, candidates[i].index});
    }
    
    return result;
}

std::unique_ptr<MappingExtent> MappingManager::create_extent(
    const FileHandle& fh,
    size_t file_off,
    size_t len,
    bool writable) {
    
    // Ensure offset is page-aligned (mmap requirement)
    size_t page_size = sys_config::get_page_size();
    if (file_off % page_size != 0) {
        throw std::runtime_error("mmap offset must be page-aligned. Got " + 
                               std::to_string(file_off) + " (page size: " + 
                               std::to_string(page_size) + ")");
    }
    
    // Ensure length is positive
    if (len == 0) {
        throw std::runtime_error("Cannot mmap zero-length region");
    }
    
    // Map the file
    int prot = PROT_READ;
    if (writable) {
        prot |= PROT_WRITE;
    }
    
    void* addr = mmap(nullptr, len, prot, MAP_SHARED, fh.fd, file_off);
    if (addr == MAP_FAILED) {
        throw std::runtime_error("mmap failed for " + fh.path + 
                               " at offset " + std::to_string(file_off) +
                               " length " + std::to_string(len) +
                               ": " + strerror(errno));
    }
    
    // Advise the kernel about our access pattern
    // For segment allocator, random access is typical
    madvise(addr, len, MADV_RANDOM);
    
    return std::make_unique<MappingExtent>(static_cast<uint8_t*>(addr), len, file_off);
}

} // namespace persist
} // namespace xtree