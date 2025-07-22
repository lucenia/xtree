#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <array>
#include <thread>

#ifdef __linux__
#include <sys/mman.h>
#endif

#ifdef _WIN32
#include <windows.h>
#endif

namespace xtree {

/**
 * Pool allocator for PageStats to avoid dynamic allocations in hot path
 */
template<typename T, size_t PoolSizeValue = 4096>
class ObjectPool {
public:
    static constexpr size_t PoolSize = PoolSizeValue;
    
    struct Node {
        alignas(T) char data[sizeof(T)];
        std::atomic<Node*> next;
        std::atomic<bool> in_use{false};
    };
    
    std::array<Node, PoolSize> pool_;
    
private:
    std::atomic<Node*> free_list_{nullptr};
    std::atomic<size_t> allocated_count_{0};
    
public:
    ObjectPool() {
        // Initialize free list
        for (size_t i = 0; i < PoolSize - 1; ++i) {
            pool_[i].next.store(&pool_[i + 1], std::memory_order_relaxed);
        }
        pool_[PoolSize - 1].next.store(nullptr, std::memory_order_relaxed);
        free_list_.store(&pool_[0], std::memory_order_relaxed);
    }
    
    T* allocate() {
        Node* node = nullptr;
        
        // Try to get from free list
        while (true) {
            node = free_list_.load(std::memory_order_acquire);
            if (!node) {
                return nullptr; // Pool exhausted
            }
            
            Node* next = node->next.load(std::memory_order_relaxed);
            if (free_list_.compare_exchange_weak(node, next, 
                                                std::memory_order_release,
                                                std::memory_order_relaxed)) {
                break;
            }
        }
        
        // Mark as in use and construct
        node->in_use.store(true, std::memory_order_release);
        T* obj = reinterpret_cast<T*>(&node->data);
        new (obj) T();
        allocated_count_.fetch_add(1, std::memory_order_relaxed);
        return obj;
    }
    
    void deallocate(T* ptr) {
        if (!ptr) return;
        
        // Find the node
        Node* node = nullptr;
        for (size_t i = 0; i < PoolSize; ++i) {
            if (reinterpret_cast<T*>(&pool_[i].data) == ptr) {
                node = &pool_[i];
                break;
            }
        }
        
        if (!node || !node->in_use.load(std::memory_order_acquire)) {
            return; // Invalid pointer or not in use
        }
        
        // Destruct and return to free list
        ptr->~T();
        node->in_use.store(false, std::memory_order_release);
        
        Node* old_head;
        do {
            old_head = free_list_.load(std::memory_order_relaxed);
            node->next.store(old_head, std::memory_order_relaxed);
        } while (!free_list_.compare_exchange_weak(old_head, node,
                                                  std::memory_order_release,
                                                  std::memory_order_relaxed));
        
        allocated_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    
    size_t allocated_count() const {
        return allocated_count_.load(std::memory_order_relaxed);
    }
};

/**
 * Lock-free page statistics using per-page spinlocks
 */
class PageWriteTracker {
public:
    struct PageStats {
        std::atomic<uint32_t> write_count{0};
        std::atomic<uint32_t> access_count{0};
        std::atomic<uint64_t> last_write_epoch{0}; // Epoch time to avoid frequent clock calls
        std::atomic<bool> is_hot{false};
        std::atomic_flag spinlock = ATOMIC_FLAG_INIT;
        
        // Default constructor
        PageStats() = default;
        
        // Copy constructor for returning by value
        PageStats(const PageStats& other) 
            : write_count(other.write_count.load(std::memory_order_relaxed)),
              access_count(other.access_count.load(std::memory_order_relaxed)),
              last_write_epoch(other.last_write_epoch.load(std::memory_order_relaxed)),
              is_hot(other.is_hot.load(std::memory_order_relaxed)) {}
        
        // Assignment operator
        PageStats& operator=(const PageStats& other) {
            if (this != &other) {
                write_count.store(other.write_count.load(std::memory_order_relaxed), std::memory_order_relaxed);
                access_count.store(other.access_count.load(std::memory_order_relaxed), std::memory_order_relaxed);
                last_write_epoch.store(other.last_write_epoch.load(std::memory_order_relaxed), std::memory_order_relaxed);
                is_hot.store(other.is_hot.load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
            return *this;
        }
        
        // Lock-free operations
        void increment_writes(uint32_t hot_threshold) {
            uint32_t writes = write_count.fetch_add(1, std::memory_order_relaxed) + 1;
            if (writes >= hot_threshold && !is_hot.load(std::memory_order_relaxed)) {
                is_hot.store(true, std::memory_order_relaxed);
            }
        }
        
        void increment_access() {
            access_count.fetch_add(1, std::memory_order_relaxed);
        }
        
        void update_timestamp(uint64_t epoch) {
            last_write_epoch.store(epoch, std::memory_order_relaxed);
        }
    };
    
private:
    // Thread-local cache for page stats to reduce contention
    struct ThreadLocalCache {
        static constexpr size_t CACHE_SIZE = 16;
        struct CacheEntry {
            void* page = nullptr;
            PageStats* stats = nullptr;
            uint32_t access_count = 0;
        };
        std::array<CacheEntry, CACHE_SIZE> entries;
        size_t next_slot = 0;
        
        PageStats* find(void* page) {
            for (auto& entry : entries) {
                if (entry.page == page) {
                    entry.access_count++;
                    return entry.stats;
                }
            }
            return nullptr;
        }
        
        void insert(void* page, PageStats* stats) {
            // Simple round-robin replacement
            entries[next_slot] = {page, stats, 1};
            next_slot = (next_slot + 1) % CACHE_SIZE;
        }
    };
    
    // Primary storage - use fixed-size hash table to avoid allocations
    static constexpr size_t HASH_TABLE_SIZE = 65536; // Power of 2 for fast modulo
    struct HashEntry {
        std::atomic<void*> page{nullptr};
        PageStats stats;
        std::atomic<HashEntry*> next{nullptr};
    };
    
    std::array<std::atomic<HashEntry*>, HASH_TABLE_SIZE> hash_table_;
    ObjectPool<HashEntry, 8192> entry_pool_; // Pool for overflow entries
    
    const size_t page_size_;
    const uint32_t hot_write_threshold_;
    
    // Batched timestamp updates
    std::atomic<uint64_t> current_epoch_{0};
    std::atomic<bool> timer_running_{false};
    std::thread epoch_timer_;
    
    // Thread-local cache getter - defined per translation unit
    static ThreadLocalCache& get_tl_cache() {
        static thread_local ThreadLocalCache tl_cache;
        return tl_cache;
    }
    
    void* get_page_base(void* ptr) const {
        return reinterpret_cast<void*>(
            reinterpret_cast<uintptr_t>(ptr) & ~(page_size_ - 1)
        );
    }
    
    size_t hash_page(void* page) const {
        // Fast hash function for aligned addresses
        uintptr_t addr = reinterpret_cast<uintptr_t>(page);
        addr >>= 12; // Remove page offset bits
        return (addr * 2654435761ULL) & (HASH_TABLE_SIZE - 1);
    }
    
    PageStats* find_or_create_stats(void* page) {
        size_t bucket = hash_page(page);
        
        // Debug logging
        // if (reinterpret_cast<uintptr_t>(page) == 0x1000) {
        //     std::cout << "[DEBUG] find_or_create_stats: page=0x1000, bucket=" << bucket 
        //               << ", hash_table_[bucket]=" << hash_table_[bucket].load() << std::endl;
        // }
        
        // Try to find existing entry
        HashEntry* current = hash_table_[bucket].load(std::memory_order_acquire);
        HashEntry* prev = nullptr;
        
        while (current) {
            void* entry_page = current->page.load(std::memory_order_relaxed);
            if (entry_page == page) {
                return &current->stats;
            }
            if (entry_page == nullptr) {
                // Try to claim this entry
                void* expected = nullptr;
                if (current->page.compare_exchange_strong(expected, page,
                                                         std::memory_order_release,
                                                         std::memory_order_relaxed)) {
                    // Reset stats for reused entry
                    current->stats = PageStats{};
                    return &current->stats;
                }
            }
            prev = current;
            current = current->next.load(std::memory_order_acquire);
        }
        
        // Need to allocate new entry
        HashEntry* new_entry = entry_pool_.allocate();
        bool from_pool = (new_entry != nullptr);
        if (!new_entry) {
            // Pool exhausted - fallback to heap
            new_entry = new HashEntry();
        }
        
        new_entry->page.store(page, std::memory_order_relaxed);
        new_entry->next.store(nullptr, std::memory_order_relaxed);
        
        // Add to chain
        if (prev) {
            HashEntry* expected = nullptr;
            if (!prev->next.compare_exchange_strong(expected, new_entry,
                                                   std::memory_order_release,
                                                   std::memory_order_acquire)) {
                // Someone else added, retry
                if (from_pool) {
                    entry_pool_.deallocate(new_entry);
                } else {
                    delete new_entry;
                }
                return find_or_create_stats(page);
            }
        } else {
            // Add as first entry
            HashEntry* expected = nullptr;
            if (!hash_table_[bucket].compare_exchange_strong(expected, new_entry,
                                                           std::memory_order_release,
                                                           std::memory_order_acquire)) {
                // Someone else added, retry
                if (from_pool) {
                    entry_pool_.deallocate(new_entry);
                } else {
                    delete new_entry;
                }
                return find_or_create_stats(page);
            }
        }
        
        return &new_entry->stats;
    }
    
    void start_epoch_timer() {
        bool expected = false;
        if (!timer_running_.compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel)) {
            return; // Already running
        }
        
        epoch_timer_ = std::thread([this]() {
            while (timer_running_.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                current_epoch_.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    
public:
    explicit PageWriteTracker(size_t page_size = 4096, uint32_t hot_threshold = 10) 
        : page_size_(page_size), hot_write_threshold_(hot_threshold) {
        // Initialize hash table
        for (auto& bucket : hash_table_) {
            bucket.store(nullptr, std::memory_order_relaxed);
        }
        
        // Start epoch timer
        start_epoch_timer();
    }
    
    ~PageWriteTracker() {
        // Clear thread-local cache to prevent dangling pointers
        auto& tl_cache = get_tl_cache();
        for (auto& entry : tl_cache.entries) {
            entry.page = nullptr;
            entry.stats = nullptr;
            entry.access_count = 0;
        }
        tl_cache.next_slot = 0;
        
        // Stop epoch timer
        timer_running_.store(false, std::memory_order_release);
        if (epoch_timer_.joinable()) {
            epoch_timer_.join();
        }
        
        // Clean up allocated entries
        for (auto& bucket : hash_table_) {
            HashEntry* current = bucket.load(std::memory_order_relaxed);
            while (current) {
                HashEntry* next = current->next.load(std::memory_order_relaxed);
                // Check if it's from the pool by checking address range
                void* pool_start = &entry_pool_.pool_[0];
                void* pool_end = &entry_pool_.pool_[0] + ObjectPool<HashEntry, 8192>::PoolSize;
                void* current_ptr = static_cast<void*>(current);
                
                if (current_ptr >= pool_start && current_ptr < pool_end) {
                    entry_pool_.deallocate(current);
                } else {
                    delete current;
                }
                current = next;
            }
        }
    }
    
    void record_write(void* ptr) {
        void* page = get_page_base(ptr);
        
        // if (reinterpret_cast<uintptr_t>(page) == 0x1000) {
        //     std::cout << "[DEBUG] record_write: ptr=" << ptr << ", page=" << page << std::endl;
        // }
        
        // Check thread-local cache first
        auto& tl_cache = get_tl_cache();
        PageStats* stats = tl_cache.find(page);
        if (!stats) {
            stats = find_or_create_stats(page);
            tl_cache.insert(page, stats);
        }
        
        // Lock-free updates
        stats->increment_writes(hot_write_threshold_);
        stats->update_timestamp(current_epoch_.load(std::memory_order_relaxed));
    }
    
    void record_access(void* ptr) {
        void* page = get_page_base(ptr);
        
        // if (reinterpret_cast<uintptr_t>(page) == 0x1000) {
        //     std::cout << "[DEBUG] record_access: ptr=" << ptr << ", page=" << page << std::endl;
        // }
        
        // Check thread-local cache first
        auto& tl_cache = get_tl_cache();
        PageStats* stats = tl_cache.find(page);
        if (!stats) {
            stats = find_or_create_stats(page);
            tl_cache.insert(page, stats);
        }
        
        stats->increment_access();
    }
    
    std::vector<void*> get_hot_pages() const {
        std::vector<void*> hot_pages;
        hot_pages.reserve(1024); // Pre-allocate reasonable size
        
        // Scan hash table
        for (size_t i = 0; i < HASH_TABLE_SIZE; ++i) {
            HashEntry* current = hash_table_[i].load(std::memory_order_acquire);
            while (current) {
                void* page = current->page.load(std::memory_order_relaxed);
                if (page && current->stats.is_hot.load(std::memory_order_relaxed)) {
                    hot_pages.push_back(page);
                }
                current = current->next.load(std::memory_order_acquire);
            }
        }
        
        return hot_pages;
    }
    
    PageStats get_page_stats(void* ptr) const {
        void* page = get_page_base(ptr);
        size_t bucket = hash_page(page);
        
        HashEntry* current = hash_table_[bucket].load(std::memory_order_acquire);
        while (current) {
            if (current->page.load(std::memory_order_relaxed) == page) {
                return current->stats; // Copy constructor handles atomics
            }
            current = current->next.load(std::memory_order_acquire);
        }
        
        return PageStats{};
    }
    
    void reset_stats() {
        // Clear all entries
        for (size_t i = 0; i < HASH_TABLE_SIZE; ++i) {
            HashEntry* current = hash_table_[i].load(std::memory_order_acquire);
            while (current) {
                current->page.store(nullptr, std::memory_order_relaxed);
                current->stats = PageStats{};
                current = current->next.load(std::memory_order_acquire);
            }
        }
    }
    
    size_t get_tracked_page_count() const {
        size_t count = 0;
        for (size_t i = 0; i < HASH_TABLE_SIZE; ++i) {
            HashEntry* current = hash_table_[i].load(std::memory_order_acquire);
            while (current) {
                if (current->page.load(std::memory_order_relaxed) != nullptr) {
                    count++;
                }
                current = current->next.load(std::memory_order_acquire);
            }
        }
        return count;
    }
    
    void prefault_hot_pages() {
        auto hot_pages = get_hot_pages();
        
        for (void* page : hot_pages) {
            // Touch the page to ensure it's resident and writable
            volatile char* p = static_cast<volatile char*>(page);
            char dummy = *p;  // Read
            *p = dummy;       // Write back - ensures page is writable
        }
    }
};


/**
 * Batch update coordinator for minimizing COW faults
 * Groups updates by page to trigger only one COW fault per page
 */
template<typename T>
class BatchUpdateCoordinator {
public:
    struct PendingUpdate {
        T* target;
        std::function<void()> update;
    };
    
private:
    // Group updates by page
    std::unordered_map<void*, std::vector<PendingUpdate>> updates_by_page_;
    mutable std::mutex updates_mutex_;
    const size_t page_size_;
    
    void* get_page_base(void* ptr) const {
        return reinterpret_cast<void*>(
            reinterpret_cast<uintptr_t>(ptr) & ~(page_size_ - 1)
        );
    }
    
public:
    explicit BatchUpdateCoordinator(size_t page_size = 4096) 
        : page_size_(page_size) {}
    
    ~BatchUpdateCoordinator() {
        // Clear any pending updates without executing them
        std::lock_guard<std::mutex> lock(updates_mutex_);
        updates_by_page_.clear();
    }
    
    void add_update(T* target, std::function<void()> update) {
        void* page = get_page_base(target);
        
        std::lock_guard<std::mutex> lock(updates_mutex_);
        updates_by_page_[page].emplace_back(PendingUpdate{target, std::move(update)});
    }
    
    // Execute all updates, returns number of pages modified
    size_t execute_updates() {
        std::lock_guard<std::mutex> lock(updates_mutex_);
        size_t pages_modified = updates_by_page_.size();
        
        // Process one page at a time - single COW fault per page
        for (auto& [page, updates] : updates_by_page_) {
            for (auto& pending : updates) {
                pending.update();
            }
        }
        
        updates_by_page_.clear();
        return pages_modified;
    }
    
    size_t pending_update_count() const {
        std::lock_guard<std::mutex> lock(updates_mutex_);
        size_t count = 0;
        for (const auto& [page, updates] : updates_by_page_) {
            count += updates.size();
        }
        return count;
    }
    
    size_t pending_page_count() const {
        std::lock_guard<std::mutex> lock(updates_mutex_);
        return updates_by_page_.size();
    }
};

/**
 * Huge page support for reducing COW overhead
 * Detects huge page size once at runtime for optimal performance
 */
class HugePageAllocator {
private:
    static size_t detect_huge_page_size() {
#ifdef __linux__
        // Try to read the actual huge page size from sysfs
        FILE* fp = fopen("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size", "r");
        if (fp) {
            size_t size = 0;
            if (fscanf(fp, "%zu", &size) == 1) {
                fclose(fp);
                return size;
            }
            fclose(fp);
        }
        // Fallback to 2MB (standard for x86_64)
        return 2 * 1024 * 1024;
#elif defined(__APPLE__)
        // macOS doesn't expose huge page size directly
        // Use 2MB for Intel, regular page size for Apple Silicon
        #ifdef __arm64__
            return 4096; // No separate huge pages on M1/M2
        #else
            return 2 * 1024 * 1024; // Intel Macs use 2MB superpages
        #endif
#elif defined(_WIN32)
        // Windows: GetLargePageMinimum() returns the size
        typedef SIZE_T (WINAPI *GetLargePageMinimumPtr)(void);
        HMODULE kernel32 = GetModuleHandle("kernel32.dll");
        if (kernel32) {
            GetLargePageMinimumPtr getLargePageMin = 
                (GetLargePageMinimumPtr)GetProcAddress(kernel32, "GetLargePageMinimum");
            if (getLargePageMin) {
                SIZE_T size = getLargePageMin();
                if (size > 0) return size;
            }
        }
        return 2 * 1024 * 1024; // Fallback to 2MB
#else
        // Other platforms: use a reasonable multiplier
        return 4096 * 512;
#endif
    }
    
    static size_t get_cached_huge_page_size() {
        static size_t cached_size = detect_huge_page_size();
        return cached_size;
    }
    
public:
    // This is called once and cached - no runtime penalty after first access
    static size_t HUGE_PAGE_SIZE() {
        return get_cached_huge_page_size();
    }
    
    static void* allocate_huge_aligned(size_t size) {
        size_t huge_page_size = HUGE_PAGE_SIZE();
        size_t aligned_size = (size + huge_page_size - 1) & ~(huge_page_size - 1);
        
#ifdef __linux__
        // Try to allocate with huge pages
        void* ptr = aligned_alloc(huge_page_size, aligned_size);
        if (ptr) {
            // Advise kernel to use huge pages
            if (madvise(ptr, aligned_size, MADV_HUGEPAGE) == 0) {
                return ptr;
            }
            // If madvise fails, still use the allocation
        }
        return ptr;
#elif defined(_WIN32)
        // Windows: Try large pages if available
        static bool large_pages_checked = false;
        static bool large_pages_available = false;
        
        if (!large_pages_checked) {
            large_pages_available = enable_large_page_support();
            large_pages_checked = true;
        }
        
        if (large_pages_available) {
            void* ptr = VirtualAlloc(nullptr, aligned_size, 
                                   MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES, 
                                   PAGE_READWRITE);
            if (ptr) return ptr;
        }
        
        // Fallback to regular aligned allocation
        return _aligned_malloc(aligned_size, huge_page_size);
#else
        // Other platforms: regular aligned allocation
        return aligned_alloc(huge_page_size, aligned_size);
#endif
    }
    
    static void deallocate_huge_aligned(void* ptr) {
#ifdef _WIN32
        // Try VirtualFree first (for large pages), then _aligned_free
        if (!VirtualFree(ptr, 0, MEM_RELEASE)) {
            _aligned_free(ptr);
        }
#else
        std::free(ptr);
#endif
    }
    
    static bool is_huge_page_available() {
#ifdef __linux__
        // Check if transparent huge pages are enabled
        FILE* fp = fopen("/sys/kernel/mm/transparent_hugepage/enabled", "r");
        if (fp) {
            char buffer[256];
            if (fgets(buffer, sizeof(buffer), fp)) {
                fclose(fp);
                // Look for [always] or [madvise]
                return strstr(buffer, "[always]") != nullptr || 
                       strstr(buffer, "[madvise]") != nullptr;
            }
            fclose(fp);
        }
        return false;
#elif defined(_WIN32)
        // Check if we can get large page size
        return HUGE_PAGE_SIZE() > 4096;
#else
        return false;
#endif
    }
    
private:
#ifdef _WIN32
    static bool enable_large_page_support() {
        // Try to enable SeLockMemoryPrivilege
        HANDLE token;
        if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &token)) {
            return false;
        }
        
        TOKEN_PRIVILEGES tp;
        tp.PrivilegeCount = 1;
        tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
        
        if (!LookupPrivilegeValue(nullptr, SE_LOCK_MEMORY_NAME, &tp.Privileges[0].Luid)) {
            CloseHandle(token);
            return false;
        }
        
        BOOL result = AdjustTokenPrivileges(token, FALSE, &tp, 0, nullptr, nullptr);
        DWORD error = GetLastError();
        CloseHandle(token);
        
        return result && error == ERROR_SUCCESS;
    }
#endif
};

} // namespace xtree