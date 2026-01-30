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

#include "indexdetails.hpp"
#include "xtree.h"
#include "xtree.hpp"

namespace xtree {

// Note: Static member definitions for IndexDetails<DataRecord> (jvm, indexes) are in indexdetails.cpp
// via explicit template instantiation (template class IndexDetails<DataRecord>).
// Do NOT add explicit specializations here - it causes ODR violations and double-free on exit.

// Explicit template instantiations for XTree with DataRecord
// This ensures the template methods are compiled and available for linking
template class XTreeBucket<DataRecord>;
template class __MBRKeyNode<DataRecord>;
template class Iterator<DataRecord>;

} // namespace xtree