// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#include <cetl/cetl.hpp>

#include <cstddef>
#include <cstdlib>
#include <iostream>

#if (__cplusplus >= CETL_CPP_STANDARD_17)
#    include <new>
#endif

// Disable std c++ heap allocations.
// In this demo we gonna use only stack and PMR allocations.
//
extern void* operator new(std::size_t)
{
    std::cerr << "operator `new(size_t)` has been called";
    std::exit(1);
}
extern void operator delete(void*) noexcept
{
    std::cerr << "operator `delete(void*)` has been called";
    std::exit(1);
}

#if (__cplusplus >= CETL_CPP_STANDARD_17)

extern void* operator new(std::size_t, std::align_val_t)
{
    std::cerr << "operator `new(size_t, align_val_t)` has been called";
    std::exit(1);
}
extern void operator delete(void*, std::align_val_t) noexcept
{
    std::cerr << "operator `delete(void*, align_val_t)` has been called";
    std::exit(1);
}

#endif  // (__cplusplus >= CETL_CPP_STANDARD_17)
