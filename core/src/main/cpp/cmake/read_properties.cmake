// -----------------------------------------------------------------------------
// SPDX-License-Identifier: SSPL-1.0
//
// The Lucenia project is source-available software: you can redistribute it
// and/or modify it under the terms of the Server Side Public License, version 1,
// as published by MongoDB, Inc.
//
// As per the terms of the SSPL, if you make the functionality of this program
// or a modified version available to third parties as a service, you must make
// the source code available under this license.
//
// The full text of the Server Side Public License can be found at:
// https://www.mongodb.com/licensing/server-side-public-license
// -----------------------------------------------------------------------------

function(load_properties_file FILEPATH PREFIX)
    file(READ "${FILEPATH}" FILE_CONTENTS)
    string(REPLACE "\n" ";" FILE_LINES "${FILE_CONTENTS}")
    foreach(line IN LISTS FILE_LINES)
        string(REGEX MATCH "^([a-zA-Z0-9_.-]+)[ \t]*=[ \t]*(.*)$" _ "${line}")
        if(CMAKE_MATCH_1 AND CMAKE_MATCH_2)
            string(TOUPPER "${CMAKE_MATCH_1}" KEY)
            string(STRIP "${CMAKE_MATCH_2}" VALUE)
            set("${PREFIX}_${KEY}" "${VALUE}" PARENT_SCOPE)
        endif()
    endforeach()
endfunction()

