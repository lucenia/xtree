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

#include "pch.h"

namespace xtree {

#ifdef __GNUC__
#include <unistd.h>
    size_t getTotalSystemMemory();
    size_t getAvailableSystemMemory();
#endif

#ifdef WIN32
#include <windows.h>
    size_t getTotalSystemMemory();
#endif

    /* Returns the amount of milliseconds elapsed since the UNIX epoch. Works on both
     * windows and linux. */
    unsigned long GetTimeMicro64();
}
