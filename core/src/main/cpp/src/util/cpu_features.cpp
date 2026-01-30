/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 * 
 * Runtime CPU feature detection implementation
 */

#include "cpu_features.h"

#ifdef _MSC_VER
#include <intrin.h>
#elif defined(__x86_64__) || defined(__i386__)
#include <cpuid.h>
#endif

#ifdef __APPLE__
#include <sys/types.h>
#include <sys/sysctl.h>
#endif

namespace xtree {

CPUFeatures::CPUFeatures() {
    detect_features();
}

void CPUFeatures::detect_features() {
#if defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86)
    // x86/x64 CPU feature detection
    
#ifdef __APPLE__
    // Check if we're running under Rosetta 2
    int ret = 0;
    size_t size = sizeof(ret);
    if (sysctlbyname("sysctl.proc_translated", &ret, &size, nullptr, 0) == 0 && ret == 1) {
        // Running under Rosetta 2
        // IMPORTANT: Rosetta supports up to SSE4.2 but NOT AVX/AVX2
        // Enable SSE2 and SSE4.2, but disable AVX/AVX2 to prevent SIGILL crashes
        has_sse2 = true;
        has_sse42 = true;  // Rosetta supports up to SSE4.2
        has_avx = false;   // Rosetta does NOT support AVX
        has_avx2 = false;  // Rosetta does NOT support AVX2
        return;
    }
#endif
    
#ifdef _MSC_VER
    int info[4];
    __cpuid(info, 0);
    int max_id = info[0];
    
    if (max_id >= 1) {
        __cpuid(info, 1);
        has_sse2 = (info[3] & (1 << 26)) != 0;
        has_sse42 = (info[2] & (1 << 20)) != 0;
        has_avx = (info[2] & (1 << 28)) != 0;
    }
    
    if (max_id >= 7) {
        __cpuidex(info, 7, 0);
        has_avx2 = (info[1] & (1 << 5)) != 0;
    }
#else
    unsigned int eax, ebx, ecx, edx;
    unsigned int max_id = __get_cpuid_max(0, nullptr);
    
    if (max_id >= 1) {
        __get_cpuid(1, &eax, &ebx, &ecx, &edx);
        has_sse2 = (edx & (1 << 26)) != 0;
        has_sse42 = (ecx & (1 << 20)) != 0;
        has_avx = (ecx & (1 << 28)) != 0;
    }
    
    if (max_id >= 7) {
        __get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx);
        has_avx2 = (ebx & (1 << 5)) != 0;
    }
#endif

#elif defined(__aarch64__) || defined(__arm64__)
    // ARM64 - NEON is always available
    has_neon = true;
    
#ifdef __APPLE__
    // On Apple Silicon, check for specific features
    size_t size = sizeof(int);
    int result = 0;
    
    // Check if running on Apple Silicon
    if (sysctlbyname("hw.optional.arm64", &result, &size, nullptr, 0) == 0) {
        has_neon = result != 0;
    }
#endif

#endif
}

const CPUFeatures& CPUFeatures::get() {
    static CPUFeatures instance;
    return instance;
}

} // namespace xtree