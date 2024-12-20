# This software is distributed under the terms of the MIT License.
# Copyright (C) 2021 OpenCyphal <consortium@opencyphal.org>
# Author: Pavel Kirienko <pavel@opencyphal.org>

cmake_minimum_required(VERSION 3.27)
project(udral_servo_demo C)

set(submodules "${CMAKE_CURRENT_SOURCE_DIR}/../submodules")
list(APPEND CMAKE_PREFIX_PATH "${submodules}/nunavut")

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic -fstrict-aliasing")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wdouble-promotion -Wswitch-enum -Wfloat-equal -Wundef")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wconversion -Wtype-limits")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wsign-conversion -Wcast-align -Wmissing-declarations")

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
        -DNODE_NAME="org.opencyphal.demos.udral.servo"
)

# Transpile DSDL into C using Nunavut. This uses this repo's built-in submodules to setup Nunavut. See
# CMAKE_PREFIX_PATH above for how this is resolved to the local submodules.
find_package(Nunavut 3.0 REQUIRED)

set(LOCAL_PUBLIC_REG_TYPES
        reg/udral/service/common/Readiness.0.1.dsdl
        reg/udral/service/actuator/common/_.0.1.dsdl
        reg/udral/service/actuator/common/Feedback.0.1.dsdl
        reg/udral/service/actuator/common/Status.0.1.dsdl
        reg/udral/physics/dynamics/translation/LinearTs.0.1.dsdl
        reg/udral/physics/electricity/PowerTs.0.1.dsdl
        uavcan/register/Name.1.0.dsdl
)

add_cyphal_library(
     NAME dsdl_uavcan
     EXACT_NAME
     LANGUAGE c
     LANGUAGE_STANDARD c${CMAKE_C_STANDARD}
     DSDL_FILES ${LOCAL_PUBLIC_REG_TYPES}
     DSDL_NAMESPACES
        ${NUNAVUT_SUBMODULES_DIR}/public_regulated_data_types/reg
     SERIALIZATION_ASSERT assert
     EXPORT_MANIFEST
     OUT_LIBRARY_TARGET LOCAL_TYPES_C_LIBRARY
)

# Build libcanard.
add_library(canard STATIC ${submodules}/libcanard/libcanard/canard.c)
include_directories(SYSTEM ${submodules}/libcanard/libcanard)

# Build o1heap -- a hard real-time deterministic memory allocator for embedded systems.
add_library(o1heap STATIC ${submodules}/o1heap/o1heap/o1heap.c)
include_directories(SYSTEM ${submodules}/o1heap/o1heap/)

include(${CMAKE_CURRENT_SOURCE_DIR}/../shared/register/register.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../shared/socketcan/socketcan.cmake)

# Build the application.
add_executable(udral_servo_demo
        src/main.c
)
target_link_libraries(udral_servo_demo ${LOCAL_TYPES_C_LIBRARY} canard o1heap shared_register shared_socketcan)
