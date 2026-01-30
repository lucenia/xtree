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

#include "hotset.h"

namespace xtree { 
    namespace persist {

        void Hotset::prefetch_root_l1_l2(intptr_t file_handle, size_t offset, size_t len) {
            // TODO: call PlatformFS::advise_willneed on file; optionally touch mapped pages
            (void)file_handle; (void)offset; (void)len;
        }

    } // namespace persist
} // namespace xtree