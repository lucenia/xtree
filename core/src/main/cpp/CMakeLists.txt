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

# Check if running under Rosetta BEFORE setting SIMD flags
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # Check if we're running under Rosetta
    execute_process(
        COMMAND sysctl -n sysctl.proc_translated
        OUTPUT_VARIABLE PROC_TRANSLATED
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(PROC_TRANSLATED STREQUAL "1")
        # Running under Rosetta - force x86_64 only
        set(CMAKE_OSX_ARCHITECTURES "x86_64" CACHE STRING "" FORCE)
        message(STATUS "Running under Rosetta - forcing x86_64 architecture")
    endif()
endif()

# Enable platform-specific optimizations
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    include(CheckCXXCompilerFlag)
    
    # For SIMD implementation file only, we need to ensure the intrinsics are available
    # This doesn't affect the rest of the code - only simd_implementations.cpp
    set(SIMD_COMPILE_FLAGS "")
    
    # Check for x86 SIMD support
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64|i386|i686")
        CHECK_CXX_COMPILER_FLAG("-msse2" COMPILER_SUPPORTS_SSE2)
        CHECK_CXX_COMPILER_FLAG("-mavx2" COMPILER_SUPPORTS_AVX2)
        
        # Under Rosetta, only use SSE2 even if compiler supports AVX2
        if(PROC_TRANSLATED STREQUAL "1")
            # Running under Rosetta - only use SSE2
            if(COMPILER_SUPPORTS_SSE2)
                set(SIMD_COMPILE_FLAGS "-msse2")
                message(STATUS "Under Rosetta - using SSE2 only (AVX2 not supported by Rosetta)")
            endif()
        else()
            # Not under Rosetta - use best available
            if(COMPILER_SUPPORTS_AVX2)
                set(SIMD_COMPILE_FLAGS "-mavx2")
                message(STATUS "Compiler supports AVX2 - will use runtime detection")
            elseif(COMPILER_SUPPORTS_SSE2)
                set(SIMD_COMPILE_FLAGS "-msse2")
                message(STATUS "Compiler supports SSE2 - will use runtime detection")
            endif()
        endif()
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
        message(STATUS "ARM architecture detected - NEON will be used via runtime detection")
        # Check for CRC32 support (standard on Apple Silicon, optional on other ARM)
        CHECK_CXX_COMPILER_FLAG("-march=armv8-a+crc" COMPILER_SUPPORTS_ARM_CRC)
        if(COMPILER_SUPPORTS_ARM_CRC AND NOT APPLE)
            # On non-Apple ARM, explicitly enable CRC32
            # Apple clang enables it by default for arm64
            set(SIMD_COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -march=armv8-a+crc")
        endif()
    endif()
    
    # Don't set -O3 globally to avoid SIMD flag leakage
    # We'll set it per-file for SIMD implementations
elseif(MSVC)
    set(SIMD_COMPILE_FLAGS "")
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|amd64")
        set(SIMD_COMPILE_FLAGS "/arch:AVX2")
    endif()
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2")
endif()


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
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# For multi-configuration generators (like Visual Studio on Windows)
foreach(OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/bin)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/lib)
endforeach()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# ==========================================
# Sanitizer Support
# ==========================================
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(ENABLE_ASAN OR ENABLE_UBSAN OR ENABLE_TSAN)
    if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        message(WARNING "Sanitizers work best with Debug builds. Consider setting -DCMAKE_BUILD_TYPE=Debug")
    endif()
endif()

if(ENABLE_ASAN AND ENABLE_TSAN)
    message(FATAL_ERROR "ASan and TSan cannot be enabled at the same time")
endif()

if(ENABLE_ASAN)
    message(STATUS "Enabling AddressSanitizer")
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address)
endif()

if(ENABLE_UBSAN)
    message(STATUS "Enabling UndefinedBehaviorSanitizer")
    add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=undefined)
endif()

if(ENABLE_TSAN)
    message(STATUS "Enabling ThreadSanitizer")
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()

# ==========================================
# Find dependencies BEFORE processing sources
# ==========================================

# ==========================================
# Platform-specific dependency configuration
# ==========================================
if(APPLE)
    # macOS: Check for Homebrew Boost installations
    if(EXISTS "/usr/local/opt/boost")
        set(BOOST_ROOT "/usr/local/opt/boost")
    elseif(EXISTS "/opt/homebrew/opt/boost")
        set(BOOST_ROOT "/opt/homebrew/opt/boost")
    endif()
    
    # macOS: Set JAVA_HOME if not already set
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
    # ==========================================
    # Windows-specific configuration (consolidated)
    # ==========================================
    
    # 1. Detect vcpkg if not already using toolchain
    if(NOT CMAKE_TOOLCHAIN_FILE)
        # Check common vcpkg installation paths
        if(EXISTS "C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
            set(CMAKE_TOOLCHAIN_FILE "C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
            message(STATUS "Found vcpkg toolchain at C:/vcpkg")
        elseif(EXISTS "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
            set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
            message(STATUS "Found vcpkg toolchain via VCPKG_ROOT")
        endif()
    endif()
    
    # 2. Configure vcpkg triplet if using vcpkg
    if(CMAKE_TOOLCHAIN_FILE AND CMAKE_TOOLCHAIN_FILE MATCHES "vcpkg")
        if(NOT VCPKG_TARGET_TRIPLET)
            if(MINGW)
                set(VCPKG_TARGET_TRIPLET "x64-mingw-static" CACHE STRING "")
            else()
                set(VCPKG_TARGET_TRIPLET "x64-windows" CACHE STRING "")
            endif()
        endif()
        message(STATUS "Using vcpkg triplet: ${VCPKG_TARGET_TRIPLET}")
    endif()
    
    # 3. Set up Boost paths
    if(NOT CMAKE_TOOLCHAIN_FILE)
        # Manual Boost configuration when not using vcpkg
        if(DEFINED ENV{BOOST_ROOT})
            set(BOOST_ROOT $ENV{BOOST_ROOT})
            set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${BOOST_ROOT})
        endif()
        # Add common installation paths as fallback
        set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} "C:/local/boost_1_88_0" "C:/boost")
    elseif(EXISTS "C:/vcpkg/installed/${VCPKG_TARGET_TRIPLET}")
        # Using vcpkg - add its paths
        list(APPEND CMAKE_PREFIX_PATH "C:/vcpkg/installed/${VCPKG_TARGET_TRIPLET}")
        set(BOOST_ROOT "C:/vcpkg/installed/${VCPKG_TARGET_TRIPLET}")
    endif()
    
    # 4. Configure Boost library settings
    if(MINGW)
        set(Boost_USE_STATIC_LIBS ON)
        set(Boost_USE_STATIC_RUNTIME OFF)
    elseif(MSVC)
        set(Boost_USE_STATIC_LIBS ON)
        set(Boost_USE_STATIC_RUNTIME OFF)
    else()
        set(Boost_USE_STATIC_LIBS OFF)
        set(Boost_USE_STATIC_RUNTIME OFF)
    endif()
    set(Boost_USE_MULTITHREADED ON)
    
    # 5. Debug output for Windows
    message(STATUS "Windows configuration:")
    message(STATUS "  CMAKE_TOOLCHAIN_FILE: ${CMAKE_TOOLCHAIN_FILE}")
    message(STATUS "  VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
    message(STATUS "  BOOST_ROOT: ${BOOST_ROOT}")
    message(STATUS "  Boost_USE_STATIC_LIBS: ${Boost_USE_STATIC_LIBS}")
endif()

# JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Boost configuration (platform-specific settings already handled above)
# Default settings for non-Windows platforms
if(NOT WIN32)
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_STATIC_RUNTIME OFF)
    set(Boost_USE_MULTITHREADED ON)
endif()

# For newer CMake versions, use the new FindBoost approach
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.30")
    cmake_policy(SET CMP0167 NEW)
endif()

# Handle BOOST_ROOT variable policy
if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()


# Try to find Boost with vcpkg first
# First try modern CMake config approach
find_package(Boost 1.58 COMPONENTS system filesystem thread iostreams)

# Windows-specific: Try alternative Boost search methods if needed
if(NOT Boost_FOUND AND WIN32)
    message(STATUS "Boost not found, trying fallback methods...")
    
    # Try legacy FindBoost module
    cmake_policy(SET CMP0167 OLD)
    set(Boost_USE_STATIC_LIBS ${Boost_USE_STATIC_LIBS})
    set(Boost_USE_MULTITHREADED ON)
    set(Boost_USE_STATIC_RUNTIME OFF)
    find_package(Boost 1.58 COMPONENTS system filesystem thread iostreams)
    
    # If still not found, try common Windows locations
    if(NOT Boost_FOUND)
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
    
    # Windows-specific compile definitions
    if(WIN32)
        add_compile_definitions(
            # Boost configuration
            BOOST_ALL_NO_LIB                    # Disable auto-linking
            BOOST_DATE_TIME_NO_LIB              # Prevent date_time auto-link
            BOOST_REGEX_NO_LIB                  # Prevent regex auto-link
            BOOST_THREAD_DONT_USE_CHRONO        # Avoid xtime conflicts
            BOOST_THREAD_VERSION=5              # Thread compatibility
            BOOST_MPL_CFG_NO_PREPROCESSED_HEADERS  # MPL workaround
            BOOST_MPL_LIMIT_STRING_SIZE=128     # MPL string size limit
            
            # Windows API configuration
            WIN32_LEAN_AND_MEAN                 # Minimal Windows headers
            NOMINMAX                            # Prevent min/max macros
            _WIN32_WINNT=0x0601                 # Windows 7 minimum
        )
    endif()
    
    link_directories(${Boost_LIBRARY_DIRS})
else()
    message(FATAL_ERROR "Boost not found. Please install Boost libraries.")
endif()

# ==========================================
# RapidJSON configuration
# ==========================================
include(FetchContent)
set(FETCHCONTENT_BASE_DIR ${CMAKE_SOURCE_DIR}/third-party)
FetchContent_Declare(
    rapidjson
    GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
    GIT_TAG v1.1.0
    GIT_SHALLOW TRUE
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
)
FetchContent_GetProperties(rapidjson)
if(NOT rapidjson_POPULATED)
    FetchContent_Populate(rapidjson)
endif()
message(STATUS "RapidJSON included: ${rapidjson_SOURCE_DIR}/include")

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
    ${rapidjson_SOURCE_DIR}/include
)

# ==========================================
# Source files
# ==========================================
file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS
    src/*.cpp
    src/*/*.cpp
)

# Add CPU features and SIMD implementations
list(APPEND SRC_FILES
    src/util/cpu_features.cpp
    src/util/simd_implementations.cpp
)

# Add persistence layer source files
file(GLOB PERSISTENCE_SRC_FILES CONFIGURE_DEPENDS
    src/persistence/*.cpp
)
list(APPEND SRC_FILES ${PERSISTENCE_SRC_FILES})

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
list(FILTER SRC_FILES EXCLUDE REGEX "benchmarks/.*")

# Add source group for IDEs
source_group("Source Files" FILES ${SRC_FILES})
source_group("Header Files" FILES ${HEADER_FILES})

# Test target
file(GLOB TEST_SOURCES test/*.cpp)
source_group("Tests" FILES ${TEST_SOURCES})

# Library target configuration
if(SRC_FILES)
    # Windows: Static library to avoid DLL export complexity
    # Other platforms: Shared library
    if(WIN32)
        add_library(XTree STATIC ${SRC_FILES} ${HEADER_FILES})
    else()
        add_library(XTree SHARED ${SRC_FILES} ${HEADER_FILES})
    endif()
    
    # Set library properties
    if(DEFINED LIBRARY_OUTPUT_NAME AND DEFINED BASE_VERSION AND DEFINED VERSION_MAJOR)
        if(WIN32)
            set_target_properties(XTree PROPERTIES
                RUNTIME_OUTPUT_NAME ${LIBRARY_OUTPUT_NAME}
                ARCHIVE_OUTPUT_NAME XTree  # Simple import library name
            )
        else()
            set_target_properties(XTree PROPERTIES
                OUTPUT_NAME ${LIBRARY_OUTPUT_NAME}
                VERSION ${BASE_VERSION}
                SOVERSION ${VERSION_MAJOR}
            )
        endif()
    endif()
    
    # Apply SIMD compile flags only to simd_implementations.cpp
    # Also add -O3 here to optimize SIMD code without affecting test builds
    if(SIMD_COMPILE_FLAGS)
        set_source_files_properties(src/util/simd_implementations.cpp 
            PROPERTIES COMPILE_FLAGS "${SIMD_COMPILE_FLAGS} -O3")
    else()
        # Even without specific SIMD flags, optimize this file
        set_source_files_properties(src/util/simd_implementations.cpp 
            PROPERTIES COMPILE_FLAGS "-O3")
    endif()
    
    # Debug: Check what flags are applied
    get_source_file_property(SIMD_FLAGS src/util/simd_implementations.cpp COMPILE_FLAGS)
    message(STATUS "SIMD Flags on simd_implementations.cpp: ${SIMD_FLAGS}")

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
# NOTE: Tests use TRADITIONAL backend for COW managers to avoid MMAP hanging issues on Windows
# If you need to test MMAP backend specifically, set XTREE_COW_BACKEND=mmap environment variable
set(GTEST_SOURCES
    # Core XTree Tests
    test/test_xtree.cpp  # Re-enabled - fixed allocator references
    test/test_keymbr.cpp
    test/test_components.cpp
    test/test_search.cpp  # Re-enabled - fixed IndexDetails constructor
    test/test_performance.cpp  # Re-enabled - fixed IndexDetails constructor
    test/test_lru_unit.cpp  # LRU cache unit tests
    test/test_lru_sharded.cpp  # Sharded LRU cache tests
    test/test_lru_sharded_stress.cpp  # Sharded LRU cache stress tests
    test/test_cache_policy.cpp  # Cache memory policy tests
    test/test_cache_policy_stress.cpp  # Cache policy stress tests
    # Concurrent tests saved for adaptation to new persistence layer
    # test/test_xtree_concurrent_search.cpp
    # test/test_xtree_simple_concurrent.cpp
    test/test_xtree_point_search.cpp  # Re-enabled - fixed persistence mode
    # test/test_concurrent_point_search.cpp
    
    # Memory Management Tests
    # test/memmgr/test_page_write_tracker.cpp
    # test/memmgr/test_compact_allocator.cpp
    # test/memmgr/test_segmented_allocator.cpp
    # test/memmgr/test_concurrent_operations.cpp
    # test/memmgr/test_concurrent_multi_segment.cpp
    # test/memmgr/test_multi_segment_snapshot.cpp
    # test/memmgr/test_auto_snapshot.cpp
    # test/memmgr/test_production_ready_snapshot.cpp
    # test/memmgr/test_multi_segment_load.cpp
    # test/memmgr/test_multi_segment_load_verify.cpp
    
    # Utility Tests
    test/util/test_float_utils.cpp
    test/util/test_simd_implementations.cpp
    test/util/test_logging.cpp
    test/util/test_endian.cpp
    
    # Integration Tests
    # test/integration/test_integration.cpp  # Uses old getCompactAllocator
    # test/integration/test_compact_cow_integration.cpp
    # test/integration/test_mmap_comprehensive.cpp
    # test/integration/test_mmap_full_reload.cpp
    # test/integration/test_compact_root_persistence.cpp
    # test/integration/test_xtree_allocator_integration.cpp
    # test/integration/test_10k_issue.cpp
    # test/integration/test_root_tracking.cpp
    # test/integration/test_large_scale_segmented.cpp
    
    # Persistence Layer Tests
    test/persistence/test_node_id.cpp
    test/persistence/test_object_table.cpp
    test/persistence/test_object_table_handle_lifecycle.cpp
    test/persistence/test_object_table_sharded.cpp
    test/persistence/test_ot_checkpoint.cpp
    test/persistence/test_platform_fs.cpp
    test/persistence/test_segment_allocator.cpp
    test/persistence/test_mapping_manager.cpp
    test/persistence/test_file_handle_registry.cpp
    test/persistence/test_bucket_reallocation.cpp
    test/persistence/test_segment_reuse.cpp
    test/persistence/test_forced_reallocation.cpp
    test/persistence/test_checkpoint_coordinator.cpp
    test/persistence/test_crash_resilience.cpp
    test/persistence/test_rotation_stress.cpp
    test/persistence/test_bitmap_allocator.cpp
    test/persistence/test_superblock.cpp
    test/persistence/test_ot_delta_log.cpp
    test/persistence/test_mvcc_context.cpp
    test/persistence/test_config.cpp
    test/persistence/test_metrics.cpp
    test/persistence/test_checksums.cpp
    test/persistence/test_crc32c_hardware.cpp
    test/persistence/test_manifest.cpp
    test/persistence/test_recovery.cpp
    test/persistence/test_durable_store.cpp
    test/persistence/test_durable_store_deltas.cpp
    test/persistence/test_durable_store_regressions.cpp
    test/persistence/test_durability_policies.cpp
    test/persistence/test_crash_consistency_simple.cpp
    test/persistence/test_wal_ot_ordering.cpp
    test/persistence/test_crash_resilience.cpp
    test/persistence/test_rotation_stress.cpp
    test/persistence/test_windows_specific.cpp
    test/persistence/test_cascade_realloc.cpp
    test/test_store_integration.cpp
    test/test_persistence_integration.cpp
    test/test_xtree_durability.cpp
    test/test_xtree_durability_stress.cpp
    test/test_xtree_durability_unit.cpp
    test/test_xtree_commit_first.cpp
    test/test_xtree_split_nodeid.cpp
    test/test_xtree_root_split_invariant.cpp
    test/test_wire_format.cpp
)


# Create main test executable WITHOUT benchmarks
add_executable(xtree_tests ${GTEST_SOURCES})

# Create separate benchmark executable
set(BENCHMARK_SOURCES
    # Temporarily comment out broken benchmarks that prevent compilation
    # benchmarks/performance_regression.cpp  # Has getCompactAllocator errors
    # benchmarks/multi_segment_benchmark.cpp  # Has getCompactAllocator errors
    # benchmarks/optimized_query_benchmark.cpp
    # benchmarks/optimized_multi_segment_benchmark.cpp
    # benchmarks/parallel_simd_benchmark.cpp
    # benchmarks/simd_perf_highdim.cpp
    # benchmarks/qps_debug_benchmark.cpp
    # benchmarks/tree_structure_debug.cpp
    # benchmarks/overlap_analysis.cpp
    # benchmarks/tree_performance_analysis.cpp
    # benchmarks/tree_depth_analysis.cpp
    # benchmarks/iterator_optimization_analysis.cpp
    # benchmarks/concurrent_qps_benchmark.cpp
    # benchmarks/test_simple.cpp
    # benchmarks/concurrent_simple.cpp
    # benchmarks/realistic_data_benchmark.cpp
    benchmarks/profile_insertion_path.cpp
    
    # Persistence benchmarks
    benchmarks/persistence/bench_wal_comprehensive.cpp
    benchmarks/persistence/bench_checkpoint_performance.cpp
    benchmarks/persistence/bench_checkpoint_coordinator_performance.cpp
    benchmarks/persistence/bench_recovery_performance.cpp
    benchmarks/persistence/bench_checksum_performance.cpp
    benchmarks/bench_logging_overhead.cpp
    benchmarks/persistence/bench_segment_allocator_performance.cpp
    benchmarks/persistence/bench_object_table_performance.cpp
    benchmarks/persistence/bench_durable_store_performance.cpp
    benchmarks/persistence/bench_durability_policies.cpp
    benchmarks/persistence/bench_sharded_object_table_overhead.cpp
    benchmarks/persistence/bench_object_table_fragmentation.cpp
    benchmarks/persistence/bench_segment_allocator_fragmentation.cpp
)

add_executable(xtree_benchmarks ${BENCHMARK_SOURCES})

target_link_libraries(xtree_tests 
    XTree  # Add back for PageWriteTracker test
    ${Boost_LIBRARIES}  # Re-enable Boost now that static init is fixed
    gtest
    gtest_main
    gmock
)

target_link_libraries(xtree_benchmarks 
    XTree
    ${Boost_LIBRARIES}
    gtest
    gtest_main
    gmock
)

# Examples removed - POD/Arena no longer needed

# Add test discovery
include(GoogleTest)
# Disable test discovery during build to avoid segfaults
# gtest_discover_tests(xtree_tests)

# ==========================================
# Platform-specific build notes
# ==========================================
if(WIN32)
    if(NOT Boost_USE_STATIC_LIBS)
        message(STATUS "")
        message(STATUS "============================================================")
        message(STATUS "Windows Dynamic Library Configuration:")
        message(STATUS "To run tests, ensure Boost DLLs are in PATH.")
        message(STATUS "Add your Boost library directory to system PATH:")
        message(STATUS "  Example: C:/local/boost_1_88_0/stage/lib")
        message(STATUS "Or set BOOST_ROOT environment variable.")
        message(STATUS "============================================================")
        message(STATUS "")
    else()
        message(STATUS "Using static Boost libraries - no DLL PATH configuration needed.")
    endif()
endif()

# Install
if(TARGET XTree)
install(TARGETS XTree
    RUNTIME DESTINATION bin  # for DLLs on Windows
    LIBRARY DESTINATION lib/native/xtree  # for shared libraries on Unix
    ARCHIVE DESTINATION lib/native/xtree  # for import libraries on Windows
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

