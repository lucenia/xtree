/*
 * Debug program to understand Windows page address calculation
 */

#include <iostream>
#include <iomanip>
#include "../src/memmgr/cow_memmgr.hpp"

using namespace xtree;

int main() {
    const size_t TEST_PAGE_SIZE = PageAlignedMemoryTracker::RUNTIME_PAGE_SIZE;
    
    std::cout << "TEST_PAGE_SIZE: " << TEST_PAGE_SIZE << " (0x" << std::hex << TEST_PAGE_SIZE << ")" << std::endl;
    
    void* page1 = reinterpret_cast<void*>(TEST_PAGE_SIZE);
    void* page2 = reinterpret_cast<void*>(TEST_PAGE_SIZE * 2);
    
    std::cout << "\nOriginal addresses:" << std::endl;
    std::cout << "page1: " << page1 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(page1) << ")" << std::endl;
    std::cout << "page2: " << page2 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(page2) << ")" << std::endl;
    
    // Simulate get_page_base calculation
    auto get_page_base = [TEST_PAGE_SIZE](void* ptr) -> void* {
        return reinterpret_cast<void*>(
            reinterpret_cast<uintptr_t>(ptr) & ~(TEST_PAGE_SIZE - 1)
        );
    };
    
    void* base1 = get_page_base(page1);
    void* base2 = get_page_base(page2);
    
    std::cout << "\nPage bases:" << std::endl;
    std::cout << "base1: " << base1 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(base1) << ")" << std::endl;
    std::cout << "base2: " << base2 << " (0x" << std::hex << reinterpret_cast<uintptr_t>(base2) << ")" << std::endl;
    std::cout << "Are bases equal? " << (base1 == base2 ? "YES - PROBLEM!" : "NO - OK") << std::endl;
    
    // Test the mask calculation
    size_t mask = ~(TEST_PAGE_SIZE - 1);
    std::cout << "\nMask calculation:" << std::endl;
    std::cout << "TEST_PAGE_SIZE - 1: 0x" << std::hex << (TEST_PAGE_SIZE - 1) << std::endl;
    std::cout << "~(TEST_PAGE_SIZE - 1): 0x" << std::hex << mask << std::endl;
    
    uintptr_t addr1 = reinterpret_cast<uintptr_t>(page1);
    uintptr_t addr2 = reinterpret_cast<uintptr_t>(page2);
    
    std::cout << "\nMask application:" << std::endl;
    std::cout << "addr1 & mask: 0x" << std::hex << (addr1 & mask) << std::endl;
    std::cout << "addr2 & mask: 0x" << std::hex << (addr2 & mask) << std::endl;
    
    return 0;
}