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

#include "keymbr.h"
#include "util/log.h"

namespace xtree {

/**
 * IRecord is the base abstract class for all objects stored in the XTree.
 * This includes both XTreeBuckets (internal/leaf nodes) and DataRecords.
 */
class IRecord {
public:
    // Node type flags for __MBRKeyNode
    enum NodeType : uint8_t {
        INTERNAL_BUCKET = 0x00,  // Internal bucket (not leaf, not data)
        LEAF_BUCKET     = 0x01,  // Leaf bucket (leaf, not data)
        DATA_NODE       = 0x02,  // DataRecord (not leaf, data)  
        LEAF_DATA_NODE  = 0x03   // Unused (would be both leaf AND data)
    };

    IRecord() : _key(NULL) {}
    IRecord(KeyMBR* key) : _key(key) {
#ifdef _DEBUG
        if(key == NULL) {
            log() << "IRecord: KEY IS NULL!!!" << std::endl;
            log() << "Memory usage: " << getAvailableSystemMemory() << std::endl;
        }
#endif
    }

    virtual ~IRecord() {
        if (_key) {
            delete _key;
            _key = nullptr;
        }
    }
    
    virtual KeyMBR* getKey() const = 0;
    virtual const bool isLeaf() const = 0;
    virtual const bool isDataNode() const = 0;
    virtual long memoryUsage() const = 0;
    virtual void purge() {}
    
    // RTTI-free conversion to IDataRecord (only for data nodes)
    // Returns nullptr for non-data nodes (e.g., XTreeBucket)
    virtual class IDataRecord* asDataRecord() noexcept { return nullptr; }
    virtual const class IDataRecord* asDataRecord() const noexcept { return nullptr; }

protected:
    KeyMBR* _key;
};

} // namespace xtree