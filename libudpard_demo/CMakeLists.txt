# This software is distributed under the terms of the MIT License.
# Copyright (C) OpenCyphal Development Team  <opencyphal.org>
# Copyright Amazon.com Inc. or its affiliates.
# SPDX-License-Identifier: MIT
# Author: Pavel Kirienko <pavel@opencyphal.org>

cmake_minimum_required(VERSION 3.27)

project(libudpard_demo C)
set(CMAKE_C_STANDARD 11)
set(submodules "${CMAKE_CURRENT_SOURCE_DIR}/../submodules")
set(CMAKE_PREFIX_PATH "${submodules}/nunavut")

# Set up static analysis.
set(STATIC_ANALYSIS ON CACHE BOOL "enable static analysis")
if (STATIC_ANALYSIS)
    # clang-tidy (separate config files per directory)
    find_program(clang_tidy NAMES clang-tidy)
    if (NOT clang_tidy)
        message(WARNING "Could not locate clang-tidy")
        set(STATIC_ANALYSIS OFF)
    else()
        message(STATUS "Using clang-tidy: ${clang_tidy}")
    endif ()
endif ()

# Forward the revision information to the compiler so that we could expose it at runtime. This is entirely optional.
execute_process(
        COMMAND git rev-parse --short=16 HEAD
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE vcs_revision_id
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "vcs_revision_id: ${vcs_revision_id}")
add_definitions(
        -DVERSION_MAJOR=1
        -DVERSION_MINOR=0
        -DVCS_REVISION_ID=0x${vcs_revision_id}ULL
        -DNODE_NAME="org.opencyphal.demos.libudpard"
)

# Transpile DSDL into C using Nunavut. This uses this repo's built-in submodules to setup Nunavut. See
# CMAKE_PREFIX_PATH above for how this is resolved to the local submodules.
find_package(Nunavut 3.0 REQUIRED)

set(LOCAL_PUBLIC_TYPES
    uavcan/node/7509.Heartbeat.1.0.dsdl
    uavcan/node/port/7510.List.1.0.dsdl
    uavcan/node/430.GetInfo.1.0.dsdl
    uavcan/node/435.ExecuteCommand.1.1.dsdl
    uavcan/register/384.Access.1.0.dsdl
    uavcan/register/385.List.1.0.dsdl
    uavcan/pnp/8165.NodeIDAllocationData.2.0.dsdl
    uavcan/primitive/array/Real32.1.0.dsdl
)

add_cyphal_library(
     NAME dsdl_uavcan
     EXACT_NAME
     LANGUAGE c
     LANGUAGE_STANDARD c${CMAKE_C_STANDARD}
     DSDL_FILES ${LOCAL_PUBLIC_TYPES}
     SERIALIZATION_ASSERT assert
     EXPORT_MANIFEST
     OUT_LIBRARY_TARGET LOCAL_TYPES_C_LIBRARY
)

# Define the LibUDPard static library build target. No special options are needed to use the library, it's very simple.
add_library(udpard_demo STATIC ${submodules}/libudpard/libudpard/udpard.c)
target_include_directories(udpard_demo INTERFACE SYSTEM ${submodules}/libudpard/libudpard)

include(${CMAKE_CURRENT_SOURCE_DIR}/../shared/udp/udp.cmake)

# Define the demo application build target and link it with the library.
add_executable(
        demo
        ${CMAKE_CURRENT_SOURCE_DIR}/src/main.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/storage.c
        ${CMAKE_CURRENT_SOURCE_DIR}/src/register.c
)
target_include_directories(demo PRIVATE ${submodules}/cavl)
target_link_libraries(demo PRIVATE ${LOCAL_TYPES_C_LIBRARY} udpard_demo shared_udp)
set_target_properties(
        demo
        PROPERTIES
        COMPILE_FLAGS "-Wall -Wextra -Werror -pedantic -Wdouble-promotion -Wswitch-enum -Wfloat-equal \
            -Wundef -Wconversion -Wtype-limits -Wsign-conversion -Wcast-align -Wmissing-declarations"
        C_STANDARD 11
        C_EXTENSIONS OFF
)
if (STATIC_ANALYSIS)
    set_target_properties(demo PROPERTIES C_CLANG_TIDY "${clang_tidy}")
endif ()
