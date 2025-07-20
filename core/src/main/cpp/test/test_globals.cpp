/*
 * SPDX-License-Identifier: SSPL-1.0
 *
 * The Lucenia project is source-available software: you can
 * redistribute it and/or modify it under the terms of the
 * Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 *
 * As per the terms of the Server Side Public License, if you
 * make the functionality of this program or a modified version
 * available over a network, you must make the source code
 * available for download.
 *
 * The full text of the Server Side Public License, version 1,
 * can be found at:
 * https://www.mongodb.com/licensing/server-side-public-license
 */

#include <vector>
#include "../src/xtree.h"
#include "../src/indexdetails.h"

using namespace xtree;
using namespace std;

// Global static member definitions for test use
// These are shared across all test files to avoid multiple definition errors

// DataRecord static members (shared across all test files)
template<> JNIEnv* IndexDetails<DataRecord>::jvm = nullptr;
template<> std::vector<IndexDetails<DataRecord>*> IndexDetails<DataRecord>::indexes = std::vector<IndexDetails<DataRecord>*>();
template<> LRUCache<IRecord, UniqueId, LRUDeleteObject> IndexDetails<DataRecord>::cache(1024*1024*10); // 10MB cache