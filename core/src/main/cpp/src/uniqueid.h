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

#include <boost/uuid/uuid.hpp>

namespace xtree {

// defaults UID to 8 bytes or a 64 bit GUID (maps to 64 bit architecture)
#ifndef UID_SIZE
#define UID_SIZE 8
#endif

#if UID_SIZE==4
typedef uint32_t UniqueId
#else

	/**
	 * This structure makes it easy to create large GUIDs for identifying XTree nodes
	 * in Cache or on disk.  If we find ourselves with a need to reference more than 2^128
	 * we can extend this struct as necessary
	 */
    template< typename highType, typename lowType >
    struct UniqueIdType {
        typedef UniqueIdType<highType, lowType> _SelfType;

        lowType/*uint32_t*/    lo;
        highType    hi;

        bool isNull() const { return !lo && !hi; }

        // operators
        bool operator > (const _SelfType &x) const { return hi == x.hi ? lo > x.lo : hi > x.hi; }
        bool operator < (const _SelfType &x) const { return hi == x.hi ? lo < x.lo : hi < x.hi; }
        bool operator ==(const _SelfType &x) const { return lo == x.lo && hi == x.hi; }
        bool operator !=(const _SelfType &x) const { return lo != x.lo || hi != x.hi; }

        _SelfType& operator++() {
            if(!++lo) hi++;
            return *this;
        } // increment

        operator uint64_t() const {
            uint64_t retVal = ( (uint64_t)hi ) << 32;
            retVal |= (uint64_t)lo;
            return retVal;
        } // conversion operator

        UniqueIdType() {}
        UniqueIdType(uint64_t val) : lo((lowType)val), hi((highType)(val>>sizeof(lowType))) {}
        UniqueIdType(uint32_t l, highType h) : lo(l), hi(h) {}
        UniqueIdType(lowType l, highType h) : lo(l), hi(h) {}

        // serialization
        //UniqueIdType(const unsigned char* const buffer) :
        enum { recordSize = sizeof(lowType/*uint32_t*/) + sizeof(highType) };

        string toString() {
        	ostringstream oss;
            oss << hex << "0x" << hi << lo;
            return oss.str();
        }
    };

#if UID_SIZE==5
typedef UniqueIdType<uint8_t, uint32_t> UniqueId;
#elif UID_SIZE==6
typedef UniqueIdType<uint16_t, uint32_t> UniqueId;
#elif UID_SIZE==8
typedef uint64_t UniqueId;
#elif UID_SIZE==16
typedef UniqueIdType<uint64_t, uint64_t> UniqueId;
#else
#error "undefined unique id size"
#endif

#endif
} // namespace xtree
