# -----------------------------------------------------------------------------
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# The Lucenia project is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see: https://www.gnu.org/licenses/agpl-3.0.html
# -----------------------------------------------------------------------------

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

