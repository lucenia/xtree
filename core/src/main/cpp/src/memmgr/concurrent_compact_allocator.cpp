/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Implementation of thread-local storage for ConcurrentCompactAllocator
 */

#include "concurrent_compact_allocator.hpp"

namespace xtree {

// Define the thread-local variable here to avoid duplicate symbols
thread_local uint64_t ConcurrentCompactAllocator::Epoch::local_epoch = 0;

} // namespace xtree