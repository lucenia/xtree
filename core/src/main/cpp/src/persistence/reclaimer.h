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
#include <cstdint>
#include "object_table_sharded.hpp"
#include "mvcc_context.h"

namespace xtree { 
    namespace persist {

        class Reclaimer {
        public:
            explicit Reclaimer(ObjectTableSharded& ot, MVCCContext& mvcc)
                : ot_(ot), mvcc_(mvcc) {}
            size_t run_once(); // reclaim rows with retire_epoch < min_active

        private:
            ObjectTableSharded& ot_;
            MVCCContext& mvcc_;
        };

    }
} // namespace xtree::persist