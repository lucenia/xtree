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

#include <vector>
#include "../src/xtree.h"
#include "../src/indexdetails.hpp"

using namespace xtree;
using namespace std;

// Global static member definitions for test use
// These are shared across all test files to avoid multiple definition errors

// DataRecord static members (shared across all test files)
template<> JNIEnv* IndexDetails<DataRecord>::jvm = nullptr;
template<> std::vector<IndexDetails<DataRecord>*> IndexDetails<DataRecord>::indexes = std::vector<IndexDetails<DataRecord>*>();
template<> LRUCache<IRecord, UniqueId, LRUDeleteNone> IndexDetails<DataRecord>::cache(1024*1024*10); // 10MB cache