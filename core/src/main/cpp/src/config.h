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
