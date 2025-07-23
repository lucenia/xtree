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

# Log compiler info
execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} --version
    OUTPUT_VARIABLE COMPILER_VERSION_INFO
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

message(STATUS "Using C++ compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "Compiler version: ${COMPILER_VERSION_INFO}")

# ---- C++ and platform setup ----
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)


# Determine platform and architecture
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64" OR CMAKE_OSX_ARCHITECTURES MATCHES "arm64")
        set(PLATFORM_NAME "Mac_OS_X-arm64-64")
    else()
        set(PLATFORM_NAME "Mac_OS_X-x86_64-64")
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
        set(PLATFORM_NAME "Linux-arm64-64")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "armv7|arm")
        set(PLATFORM_NAME "Linux-arm-32")
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
        set(PLATFORM_NAME "Linux-amd64-64")
    else()
        set(PLATFORM_NAME "Linux-${CMAKE_SYSTEM_PROCESSOR}")
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "ARM64")
        set(PLATFORM_NAME "Windows-arm64-64")
    else()
        set(PLATFORM_NAME "Windows-x86_64-64")
    endif()
else()
    set(PLATFORM_NAME "${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}")
endif()

message(STATUS "Platform detected: ${PLATFORM_NAME}")

set(LIBRARY_OUTPUT_NAME "XTree-${PLATFORM_NAME}-${FULL_VERSION}")

# Output directories
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# ==========================================
# Find dependencies BEFORE processing sources
# ==========================================

# Platform-specific setup for finding dependencies
if(APPLE)
    # macOS specific paths
    # Check for Homebrew installations
    if(EXISTS "/usr/local/opt/boost")
        set(BOOST_ROOT "/usr/local/opt/boost")
    elseif(EXISTS "/opt/homebrew/opt/boost")
        set(BOOST_ROOT "/opt/homebrew/opt/boost")
    endif()
    
    # Set JAVA_HOME if not already set
    if(NOT DEFINED ENV{JAVA_HOME})
        execute_process(
            COMMAND /usr/libexec/java_home
            OUTPUT_VARIABLE JAVA_HOME_PATH
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        if(JAVA_HOME_PATH)
            set(ENV{JAVA_HOME} "${JAVA_HOME_PATH}")
        endif()
    endif()
elseif(WIN32)
    # Windows specific paths
    # When using vcpkg, let the toolchain file handle everything
    if(NOT CMAKE_TOOLCHAIN_FILE)
        # Only set manual paths if not using vcpkg
        if(DEFINED ENV{BOOST_ROOT})
            set(BOOST_ROOT $ENV{BOOST_ROOT})
            set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${BOOST_ROOT})
        endif()
        
        # Add common Boost installation paths as fallback
        set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "C:/local/boost_1_88_0" "C:/boost")
    endif()
endif()

# JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Boost
if(WIN32 AND MINGW)
    # MinGW on Windows typically uses static libs from vcpkg
    set(Boost_USE_STATIC_LIBS ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
    
    # When using vcpkg, ensure we use the correct triplet
    if(CMAKE_TOOLCHAIN_FILE AND CMAKE_TOOLCHAIN_FILE MATCHES "vcpkg")
        set(VCPKG_TARGET_TRIPLET "x64-mingw-static" CACHE STRING "")
    endif()
else()
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_STATIC_RUNTIME OFF)
endif()
set(Boost_USE_MULTITHREADED ON)

# For newer CMake versions, use the new FindBoost approach
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.30")
    cmake_policy(SET CMP0167 NEW)
endif()

# Handle BOOST_ROOT variable policy
if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()

# Debug Boost search
if(WIN32)
    message(STATUS "Windows Boost search:")
    message(STATUS "  BOOST_ROOT: ${BOOST_ROOT}")
    message(STATUS "  BOOST_INCLUDEDIR: ${BOOST_INCLUDEDIR}")
    message(STATUS "  CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")
    message(STATUS "  CMAKE_TOOLCHAIN_FILE: ${CMAKE_TOOLCHAIN_FILE}")
    message(STATUS "  VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
endif()

# For Windows with vcpkg, configure Boost discovery settings
if(WIN32 AND CMAKE_TOOLCHAIN_FILE)
    message(STATUS "Using vcpkg toolchain for Boost discovery")
endif()

# Fallback: Try to find vcpkg installation on Windows if toolchain not set
if(WIN32 AND NOT CMAKE_TOOLCHAIN_FILE)
    # Check common vcpkg installation paths
    if(EXISTS "C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
        set(CMAKE_TOOLCHAIN_FILE "C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
        message(STATUS "Found vcpkg toolchain at C:/vcpkg, using for Boost")
        # Include the toolchain file to set up vcpkg paths
        include("${CMAKE_TOOLCHAIN_FILE}")
    elseif(EXISTS "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
        set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
        message(STATUS "Found vcpkg toolchain via VCPKG_ROOT, using for Boost")
        # Include the toolchain file to set up vcpkg paths
        include("${CMAKE_TOOLCHAIN_FILE}")
    endif()
endif()

# Additional Windows-specific Boost hints
if(WIN32)
    # Check for vcpkg installation
    if(EXISTS "C:/vcpkg/installed")
        # Try to detect the vcpkg triplet
        if(NOT VCPKG_TARGET_TRIPLET)
            # Check for MinGW compiler
            if(MINGW)
                if(EXISTS "C:/vcpkg/installed/x64-mingw-dynamic")
                    set(VCPKG_TARGET_TRIPLET "x64-mingw-dynamic")
                elseif(EXISTS "C:/vcpkg/installed/x64-mingw-static")
                    set(VCPKG_TARGET_TRIPLET "x64-mingw-static")
                endif()
            else()
                # MSVC compiler
                if(EXISTS "C:/vcpkg/installed/x64-windows")
                    set(VCPKG_TARGET_TRIPLET "x64-windows")
                endif()
            endif()
        endif()
        
        # Add vcpkg paths to CMAKE_PREFIX_PATH if triplet found
        if(VCPKG_TARGET_TRIPLET)
            list(APPEND CMAKE_PREFIX_PATH "C:/vcpkg/installed/${VCPKG_TARGET_TRIPLET}")
            set(BOOST_ROOT "C:/vcpkg/installed/${VCPKG_TARGET_TRIPLET}")
            message(STATUS "Added vcpkg paths for triplet: ${VCPKG_TARGET_TRIPLET}")
        endif()
    endif()
    
    # Check environment variable first
    if(NOT BOOST_ROOT AND DEFINED ENV{BOOST_ROOT})
        set(BOOST_ROOT "$ENV{BOOST_ROOT}")
        message(STATUS "Using BOOST_ROOT from environment: ${BOOST_ROOT}")
    endif()
    
    # For MSVC, set up Boost library naming convention
    if(MSVC)
        set(Boost_USE_STATIC_LIBS ON)
        set(Boost_USE_MULTITHREADED ON)
        set(Boost_USE_STATIC_RUNTIME OFF)
    endif()
endif()

# Try to find Boost with vcpkg first
find_package(Boost 1.58 COMPONENTS system filesystem thread iostreams)

# If not found and on Windows, try alternative approaches
if(NOT Boost_FOUND AND WIN32)
    message(STATUS "Boost not found via vcpkg, trying alternative paths...")
    
    # Try common Windows Boost locations
    set(BOOST_SEARCH_PATHS
        "C:/local/boost_1_88_0"
        "C:/boost"
        "C:/boost_1_88_0"
        "$ENV{BOOST_ROOT}"
        "$ENV{ProgramFiles}/boost"
    )
    
    foreach(search_path ${BOOST_SEARCH_PATHS})
        if(EXISTS "${search_path}" AND NOT Boost_FOUND)
            message(STATUS "Trying Boost at: ${search_path}")
            set(BOOST_ROOT "${search_path}")
            set(Boost_NO_SYSTEM_PATHS ON)
            find_package(Boost 1.58 COMPONENTS system filesystem thread iostreams)
        endif()
    endforeach()
endif()

# Final check - if still not found, it's required
if(NOT Boost_FOUND)
    message(FATAL_ERROR "Boost not found. Please install Boost or set BOOST_ROOT.")
endif()

if(Boost_FOUND)
    message(STATUS "Boost version: ${Boost_VERSION}")
    message(STATUS "Boost include dirs: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
    
    # Add Boost include directories BEFORE other includes to ensure they're found first
    include_directories(BEFORE ${Boost_INCLUDE_DIRS})
    
    # Also add to compile definitions for better Windows support
    if(WIN32)
        add_compile_definitions(BOOST_ALL_NO_LIB)
    endif()
    
    link_directories(${Boost_LIBRARY_DIRS})
else()
    message(FATAL_ERROR "Boost not found. Please install Boost libraries.")
endif()

# ==========================================
# Page size configuration
# ==========================================
if(DEFINED XTREE_PAGE_SIZE)
    message(STATUS "Using page size: ${XTREE_PAGE_SIZE} bytes")
    add_compile_definitions(XTREE_PAGE_SIZE=${XTREE_PAGE_SIZE})
endif()

# ==========================================
# Include paths (after Boost)
# ==========================================
include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third-party
)

# ==========================================
# Source files
# ==========================================
file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS
    *.cpp
)

# Removed serialization and old mmap files - HybridMemoryManager handles persistence
# list(APPEND SRC_FILES ...)
file(GLOB_RECURSE HEADER_FILES CONFIGURE_DEPENDS
    *.h
    *.hpp
)

# Exclude test files and test1.cpp from library
list(FILTER SRC_FILES EXCLUDE REGEX "test/.*")
list(FILTER SRC_FILES EXCLUDE REGEX "test1.cpp")
list(FILTER SRC_FILES EXCLUDE REGEX "examples/.*")

# Add source group for IDEs
source_group("Source Files" FILES ${SRC_FILES})
source_group("Header Files" FILES ${HEADER_FILES})

# Test target
file(GLOB TEST_SOURCES test/*.cpp)
source_group("Tests" FILES ${TEST_SOURCES})

# Shared library target
if(SRC_FILES)
add_library(XTree SHARED ${SRC_FILES} ${HEADER_FILES})

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


# Google Test setup
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Enable testing
enable_testing()

# Test target with Google Test
set(GTEST_SOURCES
    test/test_globals.cpp
    test/test_xtree.cpp
    test/test_keymbr.cpp
    test/test_components.cpp
    test/test_search.cpp
    test/test_integration.cpp
    test/test_float_utils.cpp
    test/test_performance.cpp
    test/test_cow_memory.cpp
    test/test_page_write_tracker.cpp
    test/test_cow_integration.cpp
)

add_executable(xtree_tests ${GTEST_SOURCES})
target_link_libraries(xtree_tests 
    XTree 
    ${Boost_LIBRARIES}
    gtest
    gtest_main
    gmock
)

# Add test discovery
include(GoogleTest)
# Disable test discovery during build to avoid segfaults
# gtest_discover_tests(xtree_tests)

# Install
if(TARGET XTree)
install(TARGETS XTree
    LIBRARY DESTINATION lib/native/xtree
)
endif()

if(TARGET xtree_tests)
install(TARGETS xtree_tests
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

