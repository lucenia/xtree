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

#ifdef __cplusplus
extern "C" {
#endif

namespace xtree {
/* CUSTOMIZATION SECTION
 *
 * You can change this typedef, but must then also change the inline
 * functions in strnatcmp.c */
typedef char nat_char;

int strnatcmp(nat_char const *a, nat_char const *b);
int strnatcasecmp(nat_char const *a, nat_char const *b);

}

#ifdef __cplusplus
}
#endif
