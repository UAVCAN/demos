# This software is distributed under the terms of the MIT License.
# Copyright (C) OpenCyphal Development Team  <opencyphal.org>
# Copyright Amazon.com Inc. or its affiliates.
# SPDX-License-Identifier: MIT
# Author: Sergei Shirokov <sergei.shirokov@zubax.com>

cmake_minimum_required(VERSION 3.20)

project(libcyphal_demo
        LANGUAGES CXX C
        HOMEPAGE_URL https://github.com/OpenCyphal-Garage/libcyphal)

set(CMAKE_CXX_STANDARD "14" CACHE STRING "C++ standard to use when compiling.")
set(DISABLE_CPP_EXCEPTIONS ON CACHE STRING "Disable C++ exceptions.")

option(CETL_ENABLE_DEBUG_ASSERT "Enable or disable runtime CETL asserts." ON)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if (DISABLE_CPP_EXCEPTIONS)
        message(STATUS "DISABLE_CPP_EXCEPTIONS is true. Adding -fno-exceptions to compiler flags.")
        list(APPEND CMAKE_CXX_FLAGS "-fno-exceptions")
    endif()
endif()

add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:${CMAKE_CXX_FLAGS}>")

if (CETL_ENABLE_DEBUG_ASSERT)
    add_compile_definitions("CETL_ENABLE_DEBUG_ASSERT=1")
endif()

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules")
set(submodules "${CMAKE_SOURCE_DIR}/../submodules")

# Set up static analysis.
set(STATIC_ANALYSIS ON CACHE BOOL "enable static analysis")
if (STATIC_ANALYSIS)
    # clang-tidy (separate config files per directory)
    find_program(clang_tidy NAMES clang-tidy)
    if (NOT clang_tidy)
        message(WARNING "Could not locate clang-tidy")
    endif ()
    message(STATUS "Using clang-tidy: ${clang_tidy}")
endif ()

# Forward the revision information to the compiler so that we could expose it at runtime. This is entirely optional.
execute_process(
        COMMAND git rev-parse --short=16 HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE vcs_revision_id
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "vcs_revision_id: ${vcs_revision_id}")
add_definitions(
        -DVERSION_MAJOR=1
        -DVERSION_MINOR=0
        -DVCS_REVISION_ID=0x${vcs_revision_id}ULL
        -DNODE_NAME="org.opencyphal.demos.libcyphal"
)

# Pull in Nunavut's cmake integration
find_package("Nunavut" 3.0 REQUIRED)

set(dsdl_types_in_demo # List all the DSDL types used in the demo here
    uavcan/node/7509.Heartbeat.1.0.dsdl
    uavcan/node/430.GetInfo.1.0.dsdl
    uavcan/node/435.ExecuteCommand.1.3.dsdl
    uavcan/register/384.Access.1.0.dsdl
    uavcan/register/385.List.1.0.dsdl
)

# Define type generation and header library all in one go.
add_cyphal_library(
    NAME demo
    DSDL_FILES ${dsdl_types_in_demo}
    ALLOW_EXPERIMENTAL_LANGUAGES
    LANGUAGE cpp
    LANGUAGE_STANDARD cetl++14-17
    OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/transpiled
    OUT_LIBRARY_TARGET transpiled_headers
)

add_definitions(-DNUNAVUT_ASSERT=assert)
if (DEFINED PLATFORM_LINUX_TYPE)
    if(${PLATFORM_LINUX_TYPE} STREQUAL "bsd")
        add_definitions(-DPLATFORM_LINUX_TYPE_BSD)
    elseif(${PLATFORM_LINUX_TYPE} STREQUAL "debian")
        add_definitions(-DPLATFORM_LINUX_TYPE_DEBIAN)
    endif()
endif()

# Define the LibUDPard static library build target.
add_library(udpard STATIC ${submodules}/libudpard/libudpard/udpard.c)
target_include_directories(udpard INTERFACE SYSTEM ${submodules}/libudpard/libudpard)

# Define the LibCANard static library build target.
add_library(canard STATIC ${submodules}/libcanard/libcanard/canard.c)
target_include_directories(canard INTERFACE SYSTEM ${submodules}/libcanard/libcanard)

# Build o1heap -- a hard real-time deterministic memory allocator for embedded systems.
add_library(o1heap STATIC ${submodules}/o1heap/o1heap/o1heap.c)
target_include_directories(o1heap INTERFACE SYSTEM ${submodules}/o1heap/o1heap)

if (DEFINED PLATFORM_OS_TYPE AND NOT (${PLATFORM_OS_TYPE} STREQUAL "Darwin"))
    # OSX doesn't support socketcan 🥺
    include(${CMAKE_SOURCE_DIR}/../shared/socketcan/socketcan.cmake)
endif()
include(${CMAKE_SOURCE_DIR}/../shared/udp/udp.cmake)

add_subdirectory(src)
