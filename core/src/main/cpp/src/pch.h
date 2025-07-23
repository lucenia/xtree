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

// pch.h : primary include file for standard system includes

#pragma once

// Fix for Boost bind deprecation warning
#define BOOST_BIND_GLOBAL_PLACEHOLDERS

#include <sys/time.h>
#include <ctime>
#include <cassert>
#include <cstring>
#include <sstream>
#include <string>
#include <memory>
#include <mutex>
#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <queue>
#include <stack>
#include <set>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <fstream>
#include <cmath>
#include <algorithm>
#include <functional>
#include <cstdarg>
#include <iterator>
#include <inttypes.h>
#include <limits>
#include <exception>
#include <jni.h>

#include "./third-party/json_spirit/json_spirit_reader_template.h"
#include "./third-party/json_spirit/json_spirit_writer_template.h"

//#include <cereal/types/map.hpp>
//#include <cereal/types/memory.hpp>
//#include <cereal/archives/binary.hpp>

#define BOOST_SPIRIT_THREADSAFE
#include <boost/thread/tss.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/filesystem/operations.hpp>
//#include <boost/thread/recursive_mutex.hpp>

namespace xtree {

    using namespace std;
    using namespace boost;
    using namespace json_spirit;

#ifndef JSON_SPIRIT_MVALUE_ENABLED
#define JSON_SPIRIT_MVALUE_ENABLED 1
#endif

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#ifndef TO_DEGREES 
#define TO_DEGREES 57.295779513082321
#endif

#ifndef TO_RADIANS
#define TO_RADIANS 0.0174532925199432958
#endif

#ifndef MIN
#define MIN(a,b) ( (a) < (b) ? (a) : (b) )
#endif

#ifndef MAX
#define MAX(a,b) ( (a) > (b) ? (a) : (b) )
#endif

//#ifdef __GNUC__
//#define NO_RETURN __attribute__((__noreturn__))
//#else
//#define NO_RETURN
//#endif

//void uasserted(int msgid, const char *msg) NO_RETURN;

//#ifndef uassert
//#define uassert(msgid, msg, expr) (void)( (bool)(!!(expr)) || (uasserted(msgid, msg), 0) )
//#endif

//    typedef cs::geographic<degree> GeoDeg;
//    typedef cs::geographic<radian> GeoRad;
//    typedef cs::cartesian Cartesian;



}
