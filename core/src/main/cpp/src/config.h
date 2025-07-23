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

namespace xtree {
#ifndef XTREE_CHOOSE_SUBTREE_P
#define XTREE_CHOOSE_SUBTREE_P 132
#endif

#ifndef XTREE_M
#define XTREE_M 231
#endif

#ifndef XTREE_CHILDVEC_INIT_SIZE
#define XTREE_CHILDVEC_INIT_SIZE 32
#endif

#ifndef XTREE_MAX_OVERLAP
#define XTREE_MAX_OVERLAP 0.20   // 20% maximum overlap
#endif

#ifndef XTREE_MAX_FANOUT
#define XTREE_MAX_FANOUT XTREE_M*3  /**@todo need better heuristic*/
#endif

#ifndef XTREE_CACHE_PERCENTAGE
#define XTREE_CACHE_PERCENTAGE 0.5
#endif

#ifndef XTREE_ITER_PAGE_SIZE
#define XTREE_ITER_PAGE_SIZE 400
#endif

}
