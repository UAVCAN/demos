// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#ifndef PLATFORM_O1_HEAP_MEMORY_RESOURCE_HPP
#define PLATFORM_O1_HEAP_MEMORY_RESOURCE_HPP

#include <cetl/cetl.hpp>
#include <cetl/pf17/cetlpf.hpp>
#include <o1heap.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace platform
{

/// Implements a C++17 PMR memory resource that uses the O(1) heap.
///
class O1HeapMemoryResource final : public cetl::pmr::memory_resource
{
public:
    template <std::size_t HeapSize>
    explicit O1HeapMemoryResource(std::array<cetl::byte, HeapSize>& heap_arena)
        : o1_heap_{o1heapInit(heap_arena.data(), heap_arena.size())}
    {
        CETL_DEBUG_ASSERT(o1_heap_ != nullptr, "");
    }

    O1HeapDiagnostics queryDiagnostics() const noexcept
    {
        return o1heapGetDiagnostics(o1_heap_);
    }

private:
    // MARK: cetl::pmr::memory_resource

    void* do_allocate(std::size_t size_bytes, std::size_t alignment) override  // NOLINT
    {
        if (alignment > alignof(std::max_align_t))
        {
#if defined(__cpp_exceptions)
            throw std::bad_alloc();
#else
            return nullptr;
#endif
        }

        // О1Heap is a C-library, and it follows `malloc` convention of returning nullptr on requesting zero bytes.
        // In contrast, C++ standard (basic.stc.dynamic.allocation) requires that a memory allocation never returns
        // nullptr (even for the zero).
        // So, we have to handle this case explicitly by returning a non-null pointer to an empty storage.
        if (size_bytes == 0)
        {
            return empty_storage_.data();
        }

        return o1heapAllocate(o1_heap_, size_bytes);
    }

    void do_deallocate(void* ptr, std::size_t size_bytes, std::size_t alignment) override  // NOLINT
    {
        CETL_DEBUG_ASSERT((nullptr != ptr) || (0 == size_bytes), "");
        (void) size_bytes;
        (void) alignment;

        // See `do_allocate` special case for zero bytes.
        if (ptr == empty_storage_.data())
        {
            CETL_DEBUG_ASSERT(0 == size_bytes, "");
            return;
        }

        o1heapFree(o1_heap_, ptr);
    }

#if (__cplusplus < CETL_CPP_STANDARD_17)

    void* do_reallocate(void*       ptr,
                        std::size_t old_size_bytes,
                        std::size_t new_size_bytes,  // NOLINT
                        std::size_t alignment) override
    {
        CETL_DEBUG_ASSERT((nullptr != ptr) || (0 == old_size_bytes), "");
        (void) alignment;

        // See `do_allocate` special case for zero bytes.
        if (new_size_bytes == 0)
        {
            if (ptr != empty_storage_.data())
            {
                o1heapFree(o1_heap_, ptr);
            }
            return empty_storage_.data();
        }

        if (auto* const new_ptr = o1heapAllocate(o1_heap_, new_size_bytes))
        {
            std::memmove(new_ptr, ptr, std::min(old_size_bytes, new_size_bytes));

            // See `do_allocate` special case for zero bytes.
            if (ptr != empty_storage_.data())
            {
                o1heapFree(o1_heap_, ptr);
            }

            return new_ptr;
        }
        return nullptr;
    }

#endif

    bool do_is_equal(const cetl::pmr::memory_resource& rhs) const noexcept override
    {
        return (&rhs == this);
    }

    // MARK: Data members:

    O1HeapInstance* o1_heap_;

    // See `do_allocate` special case for zero bytes.
    // Note that we still need at least one byte - b/c `std::array<..., 0>::data()` returns `nullptr`.
    std::array<std::uint8_t, 1> empty_storage_{};

};  // O1HeapMemoryResource

}  // namespace platform

#endif  // PLATFORM_O1_HEAP_MEMORY_RESOURCE_HPP
