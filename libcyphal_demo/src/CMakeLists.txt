# This software is distributed under the terms of the MIT License.
# Copyright (C) OpenCyphal Development Team  <opencyphal.org>
# Copyright Amazon.com Inc. or its affiliates.
# SPDX-License-Identifier: MIT
# Author: Sergei Shirokov <sergei.shirokov@zubax.com>

cmake_minimum_required(VERSION 3.20)

# Define the demo application build target and link it with the library.
add_executable(
        demo
        ${CMAKE_SOURCE_DIR}/src/application.cpp
        ${CMAKE_SOURCE_DIR}/src/main.cpp
        ${CMAKE_SOURCE_DIR}/src/no_cpp_heap.cpp
)
target_link_libraries(demo PRIVATE canard o1heap udpard shared_udp ${transpiled_headers})
target_include_directories(demo PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_include_directories(demo PRIVATE ${submodules}/cetl/include)
target_include_directories(demo PRIVATE ${submodules}/libcyphal/include)
add_dependencies(demo ${transpiled_headers})

if (STATIC_ANALYSIS)
    set_target_properties(demo PROPERTIES C_CLANG_TIDY "${clang_tidy}")
endif ()
