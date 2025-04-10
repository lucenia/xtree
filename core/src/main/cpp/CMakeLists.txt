# -----------------------------------------------------------------------------
# SPDX-License-Identifier: SSPL-1.0
#
# The Lucenia project is source-available software: you can redistribute it
# and/or modify it under the terms of the Server Side Public License, version 1,
# as published by MongoDB, Inc.
#
# As per the terms of the SSPL, if you make the functionality of this program
# or a modified version available to third parties as a service, you must make
# the source code available under this license.
#
# The full text of the Server Side Public License can be found at:
# https://www.mongodb.com/licensing/server-side-public-license
# -----------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.14)

# Version info

# Load version properties
set(PROJECT_ROOT ${CMAKE_SOURCE_DIR}/../../../..)

# Confirm expected cmake helper exists before include
set(READ_PROPS_PATH "${CMAKE_SOURCE_DIR}/cmake/read_properties.cmake")

if(EXISTS "${READ_PROPS_PATH}")
    message(STATUS "Including: ${READ_PROPS_PATH}")
    include(${READ_PROPS_PATH})
else()
    message(FATAL_ERROR "Missing file: ${READ_PROPS_PATH}")
endif()

include(${CMAKE_SOURCE_DIR}/cmake/read_properties.cmake)
load_properties_file(${PROJECT_ROOT}/buildSrc/version.properties VERSION)

message(STATUS "Loaded VERSION_LUCENIA = ${VERSION_LUCENIA}")


# Base version comes from lucenia version property
if(NOT DEFINED VERSION_LUCENIA OR VERSION_LUCENIA STREQUAL "")
    message(FATAL_ERROR "VERSION_LUCENIA is not defined in version.properties. Please define lucenia = <version>.")
endif()

set(BASE_VERSION "${VERSION_LUCENIA}")

# Determine version from override or Git
if(DEFINED VERSION_SUFFIX_OVERRIDE)
    set(VERSION_SUFFIX "${VERSION_SUFFIX_OVERRIDE}")
    set(GIT_DESCRIBE "${BASE_VERSION}${VERSION_SUFFIX}")
    string(REGEX MATCH "^([0-9]+)\.([0-9]+)\.([0-9]+)" _ ${BASE_VERSION})
    set(VERSION_MAJOR ${CMAKE_MATCH_1})
    set(VERSION_MINOR ${CMAKE_MATCH_2})
    set(VERSION_PATCH ${CMAKE_MATCH_3})
else()
    execute_process(
        COMMAND git describe --tags --long --dirty
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_DESCRIBE_RAW
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        RESULT_VARIABLE GIT_RESULT
    )

    if(NOT GIT_RESULT EQUAL 0)
        execute_process(
            COMMAND git rev-parse --short HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_SHA
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        if(GIT_SHA)
            set(GIT_DESCRIBE "${BASE_VERSION}-g${GIT_SHA}")
            set(VERSION_SUFFIX "-g${GIT_SHA}")
        else()
            set(GIT_DESCRIBE "${BASE_VERSION}")
            set(VERSION_SUFFIX "")
        endif()
        string(REGEX MATCH "^([0-9]+)\.([0-9]+)\.([0-9]+)" _ ${BASE_VERSION})
        set(VERSION_MAJOR ${CMAKE_MATCH_1})
        set(VERSION_MINOR ${CMAKE_MATCH_2})
        set(VERSION_PATCH ${CMAKE_MATCH_3})
    else()
        string(REGEX REPLACE "^v" "" GIT_DESCRIBE ${GIT_DESCRIBE_RAW})
        string(REGEX MATCH "^([0-9]+)\.([0-9]+)\.([0-9]+)" _ ${GIT_DESCRIBE})
        set(VERSION_MAJOR ${CMAKE_MATCH_1})
        set(VERSION_MINOR ${CMAKE_MATCH_2})
        set(VERSION_PATCH ${CMAKE_MATCH_3})
        set(PROJECT_VERSION "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")
        string(REPLACE "${PROJECT_VERSION}" "" VERSION_SUFFIX ${GIT_DESCRIBE})
        string(STRIP ${VERSION_SUFFIX} VERSION_SUFFIX)
    endif()
endif()

# Construct full version
set(FULL_VERSION "${BASE_VERSION}${VERSION_SUFFIX}")
project(XTree VERSION ${BASE_VERSION} LANGUAGES CXX)

# ---- C++ and platform setup ----
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Determine platform
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(PLATFORM_NAME "Mac_OS_X-x86_64-64")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    set(PLATFORM_NAME "Linux-amd64-64")
else()
    set(PLATFORM_NAME "Linux-i386")
endif()

set(LIBRARY_OUTPUT_NAME "XTree-${PLATFORM_NAME}-${FULL_VERSION}")

# Output directories
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# Source files
file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS
    # RELATIVE ${CMAKE_SOURCE_DIR}
    *.cpp
)

# Exclude test files and test1.cpp from library
list(FILTER SRC_FILES EXCLUDE REGEX "test/.*")
list(FILTER SRC_FILES EXCLUDE REGEX "test1.cpp")

# --- Optional for IDEs like Qt Creator ---
if(SRC_FILES)
    source_group(TREE "${CMAKE_SOURCE_DIR}" FILES ${SRC_FILES})
endif()


# Add source group for IDEs like Qt Creator
source_group("Source Files" FILES ${SRC_FILES})

# Include paths
include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/third-party
)

# Test target
file(GLOB TEST_SOURCES test/*.cpp)
source_group("Tests" FILES ${TEST_SOURCES})

if(TEST_SOURCES)
    source_group("Tests" FILES ${TEST_SOURCES})
endif()

# JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Boost
find_package(Boost REQUIRED COMPONENTS system filesystem thread iostreams)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})

# Shared library target
if(SRC_FILES)
add_library(XTree SHARED ${SRC_FILES})

if(DEFINED LIBRARY_OUTPUT_NAME AND DEFINED BASE_VERSION AND DEFINED VERSION_MAJOR)
    set_target_properties(XTree PROPERTIES
        OUTPUT_NAME ${LIBRARY_OUTPUT_NAME}
        VERSION ${BASE_VERSION}
        SOVERSION ${VERSION_MAJOR}
    )
endif()

target_link_libraries(XTree
    ${Boost_LIBRARIES}
    ${CMAKE_DL_LIBS}
)
endif()


# Test target
file(GLOB TEST_SOURCES test/*.cpp)
if(TEST_SOURCES)
add_executable(dbtest ${TEST_SOURCES})
target_link_libraries(dbtest XTree ${Boost_LIBRARIES})
endif()

# Install
if(TARGET XTree)
install(TARGETS XTree
    LIBRARY DESTINATION lib/native/xtree
)
endif()

if(TARGET dbtest)
install(TARGETS dbtest
    RUNTIME DESTINATION bin
)
endif()

# Optionally generate version.h
configure_file(
    ${CMAKE_SOURCE_DIR}/version.h.in
    ${CMAKE_BINARY_DIR}/generated/version.h
    @ONLY
)
include_directories(${CMAKE_BINARY_DIR}/generated)

message(STATUS "Building Lucenia XTree version: ${FULL_VERSION}")

