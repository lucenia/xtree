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

#include "pch.h"

namespace xtree {

#ifdef __GNUC__
#include <unistd.h>
#endif

#if defined(__GNUC__) || defined(_WIN32) || defined(WIN32)
    size_t getTotalSystemMemory();
    size_t getAvailableSystemMemory();
#endif

    /* Returns the amount of milliseconds elapsed since the UNIX epoch. Works on both
     * windows and linux. */
    unsigned long GetTimeMicro64();
}
