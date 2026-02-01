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
#include <string>
#include "superblock.hpp"
#include "object_table_sharded.hpp"
#include "ot_delta_log.h"
#include "ot_checkpoint.h"
#include "manifest.h"
#include "segment_allocator.h"

namespace xtree { 
    namespace persist {

        class Recovery {
        public:
            Recovery(Superblock& sb, ObjectTableSharded& ot, OTDeltaLog& log, OTCheckpoint& chk,
                    Manifest& mf, SegmentAllocator* alloc = nullptr)
                : sb_(sb), ot_(ot), log_(log), chk_(chk), mf_(mf), alloc_(alloc) {}

            // Full recovery: map checkpoint + replay WAL
            void cold_start();

            // Enhanced recovery with payload rehydration for EVENTUAL mode
            void cold_start_with_payloads();

            // Read-only recovery: checkpoint only, skip WAL replay
            // Fast startup for serverless readers
            void cold_start_readonly();

        private:
            Superblock&  sb_;
            ObjectTableSharded& ot_;
            OTDeltaLog&  log_;
            OTCheckpoint& chk_;
            Manifest&    mf_;
            SegmentAllocator* alloc_;  // Optional: for rehydrating payloads from WAL
        };

    } // namespace persist
} // namespace xtree