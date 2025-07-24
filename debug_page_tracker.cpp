#include <iostream>
#include "../core/src/main/cpp/src/memmgr/cow_memmgr.hpp"

using namespace xtree;

int main() {
    const size_t TEST_PAGE_SIZE = PageAlignedMemoryTracker::get_cached_page_size();
    
    std::cout << "TEST_PAGE_SIZE: " << TEST_PAGE_SIZE << std::endl;
    
    void* page1 = reinterpret_cast<void*>(TEST_PAGE_SIZE);
    void* page2 = reinterpret_cast<void*>(TEST_PAGE_SIZE * 2);
    
    std::cout << "page1 address: " << page1 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(page1) << ")" << std::endl;
    std::cout << "page2 address: " << page2 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(page2) << ")" << std::endl;
    
    // Simulate get_page_base calculation
    auto get_page_base = [TEST_PAGE_SIZE](void* ptr) -> void* {
        return reinterpret_cast<void*>(
            reinterpret_cast<uintptr_t>(ptr) & ~(TEST_PAGE_SIZE - 1)
        );
    };
    
    void* base1 = get_page_base(page1);
    void* base2 = get_page_base(page2);
    
    std::cout << "base1: " << base1 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(base1) << ")" << std::endl;
    std::cout << "base2: " << base2 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(base2) << ")" << std::endl;
    std::cout << "Are bases equal? " << (base1 == base2 ? "YES" : "NO") << std::endl;
    
    return 0;
}