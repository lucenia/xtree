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
