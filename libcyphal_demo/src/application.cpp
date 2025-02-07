// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#include "application.hpp"

#include <libcyphal/application/registry/registry_impl.hpp>
#include <libcyphal/platform/storage.hpp>

#include <cetl/pf17/cetlpf.hpp>
#include <o1heap.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>  // for std::stoul

namespace
{

constexpr std::size_t HeapSize = 128ULL * 1024ULL;
alignas(O1HEAP_ALIGNMENT) std::array<cetl::byte, HeapSize> s_heap_arena{};  // NOLINT

constexpr std::size_t BlockHeapSize = 128ULL * 1024ULL;
alignas(O1HEAP_ALIGNMENT) std::array<cetl::byte, BlockHeapSize> s_block_heap_arena{};  // NOLINT

}  // namespace

Application::Application(const char* const root_path)
    : o1_heap_mr_{s_heap_arena}
    , o1_block_heap_mr_{s_block_heap_arena}
    , media_block_mr_{o1_block_heap_mr_}
    , storage_{root_path}
    , registry_{o1_heap_mr_}
    , regs_{o1_heap_mr_, registry_, media_block_mr_}
{
    cetl::pmr::set_default_resource(&o1_heap_mr_);

    load(storage_, registry_);

    // Maybe override some of the registry values with environment variables.
    //
    auto iface_params = getIfaceParams();
    if (const auto* const iface_addresses_str = std::getenv("CYPHAL__UDP__IFACE"))
    {
        iface_params.udp_iface.value() = iface_addresses_str;
    }
    if (const auto* const iface_addresses_str = std::getenv("CYPHAL__CAN__IFACE"))
    {
        iface_params.can_iface.value() = iface_addresses_str;
    }
    auto node_params = getNodeParams();
    if (const auto* const node_id_str = std::getenv("CYPHAL__NODE__ID"))
    {
        node_params.id.value()[0] = static_cast<std::uint16_t>(std::stoul(node_id_str));
    }
}

Application::~Application()
{
    save(storage_, registry_);

    const auto o1_diag = o1_heap_mr_.queryDiagnostics();
    std::cout << "O(1) Heap diagnostics:" << "\n"
              << "  capacity=" << o1_diag.capacity << "\n"
              << "  allocated=" << o1_diag.allocated << "\n"
              << "  peak_allocated=" << o1_diag.peak_allocated << "\n"
              << "  peak_request_size=" << o1_diag.peak_request_size << "\n"
              << "  oom_count=" << o1_diag.oom_count << "\n";

    const auto blk_diag = media_block_mr_.queryDiagnostics();
    std::cout << "Media block memory diagnostics:" << "\n"
              << "  capacity=" << blk_diag.capacity << "\n"
              << "  allocated=" << blk_diag.allocated << "\n"
              << "  peak_allocated=" << blk_diag.peak_allocated << "\n"
              << "  block_size=" << blk_diag.block_size << "\n"
              << "  oom_count=" << blk_diag.oom_count << "\n";

    cetl::pmr::set_default_resource(cetl::pmr::new_delete_resource());
}

/// Returns the 128-bit unique-ID of the local node. This value is used in `uavcan.node.GetInfo.Response`.
///
Application::UniqueId Application::getUniqueId()
{
    UniqueId out_unique_id = {};

    const auto result = storage_.get(".unique_id", out_unique_id);
    if (cetl::get_if<libcyphal::platform::storage::Error>(&result) != nullptr)
    {
        std::random_device                          rd;           // Seed for the random number engine
        std::mt19937                                gen{rd()};    // Mersenne Twister engine
        std::uniform_int_distribution<std::uint8_t> dis{0, 255};  // Distribution range for bytes

        // Populate the default; it is only used at the first run.
        for (auto& b : out_unique_id)
        {
            b = dis(gen);
        }

        (void) storage_.put(".unique_id", out_unique_id);
    }

    return out_unique_id;
}

Application::Regs::Value Application::Regs::getSysInfoMemBlock() const
{
    Value value{{&o1_heap_mr_}};
    auto& uint64s = value.set_natural64();

    const auto diagnostics = media_block_mr_.queryDiagnostics();
    uint64s.value.reserve(5);  // NOLINT five fields gonna push
    uint64s.value.push_back(diagnostics.capacity);
    uint64s.value.push_back(diagnostics.allocated);
    uint64s.value.push_back(diagnostics.peak_allocated);
    uint64s.value.push_back(diagnostics.block_size);
    uint64s.value.push_back(diagnostics.oom_count);

    return value;
}

Application::Regs::Value Application::Regs::getSysInfoMemGeneral() const
{
    Value value{{&o1_heap_mr_}};
    auto& uint64s = value.set_natural64();

    const auto diagnostics = o1_heap_mr_.queryDiagnostics();
    uint64s.value.reserve(5);  // NOLINT five fields gonna push
    uint64s.value.push_back(diagnostics.capacity);
    uint64s.value.push_back(diagnostics.allocated);
    uint64s.value.push_back(diagnostics.peak_allocated);
    uint64s.value.push_back(diagnostics.peak_request_size);
    uint64s.value.push_back(diagnostics.oom_count);

    return value;
}
