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
#include <cstdint>
#include "config.h"

namespace xtree { 
    namespace persist {
        
        // Use size classes from config.h
        static constexpr uint8_t kNumClasses = size_class::kNumClasses;
        static constexpr auto kSizes = size_class::kSizes;
        
        constexpr uint8_t size_to_class(size_t sz) {
            // Find the smallest class that fits
            if (sz <= kSizes[0]) return 0;
            
            for (uint8_t i = 1; i < kNumClasses; ++i) {
                if (sz <= kSizes[i]) {
                    return i;
                }
            }
            
            // Size too large, use largest class
            return kNumClasses - 1;
        }

        constexpr size_t class_to_size(uint8_t c) { 
            return (c < kNumClasses) ? kSizes[c] : kSizes[kNumClasses - 1];
        }
    }
}