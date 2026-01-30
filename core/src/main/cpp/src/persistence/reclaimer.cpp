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

#include "reclaimer.h"

namespace xtree { 
    namespace persist {

        size_t Reclaimer::run_once() {
            // Get the minimum epoch that any reader is currently using
            uint64_t min_active = mvcc_.min_active_epoch();
            
            // Only reclaim objects retired before the minimum active epoch
            // This ensures no reader can be accessing these objects
            if (min_active == 0) {
                // No epochs have been advanced yet, nothing to reclaim
                return 0;
            }
            
            // Reclaim all objects retired before min_active
            // (objects with retire_epoch < min_active are safe to reclaim)
            size_t reclaimed = ot_.reclaim_before_epoch(min_active);
            
            return reclaimed;
        }

    } // namespace persist
} // namespace xtree