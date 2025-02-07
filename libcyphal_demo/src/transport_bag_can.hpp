// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#ifndef TRANSPORT_BAG_CAN_HPP_INCLUDED
#define TRANSPORT_BAG_CAN_HPP_INCLUDED

#include "application.hpp"
#include "platform/block_memory_resource.hpp"
#include "platform/common_helpers.hpp"
#include "platform/linux/can/can_media.hpp"

#include <cetl/pf17/cetlpf.hpp>
#include <libcyphal/executor.hpp>
#include <libcyphal/transport/can/can_transport.hpp>
#include <libcyphal/transport/can/can_transport_impl.hpp>
#include <libcyphal/transport/errors.hpp>
#include <libcyphal/types.hpp>

#include <cstddef>
#include <iostream>
#include <memory>
#include <utility>

/// Holds (internally) instance of the CAN transport and its media (if any).
///
struct TransportBagCan final
{
    TransportBagCan(cetl::pmr::memory_resource&    general_mr,
                    libcyphal::IExecutor&          executor,
                    platform::BlockMemoryResource& media_block_mr)
        : general_mr_{general_mr}
        , executor_{executor}
        , media_block_mr_{media_block_mr}
        , media_collection_{general_mr, executor, media_block_mr}
    {
    }

    libcyphal::transport::can::ICanTransport* create(const Application::IfaceParams& params)
    {
        if (params.can_iface.value().empty())
        {
            return nullptr;
        }

        media_collection_.parse(params.can_iface.value());
        auto maybe_can_transport = makeTransport({general_mr_}, executor_, media_collection_.span(), TxQueueCapacity);
        if (const auto* failure = cetl::get_if<libcyphal::transport::FactoryFailure>(&maybe_can_transport))
        {
            std::cerr << "❌ Failed to create CAN transport (iface='"
                      << static_cast<cetl::string_view>(params.can_iface.value()) << "').\n";
            return nullptr;
        }
        transport_ = cetl::get<libcyphal::UniquePtr<libcyphal::transport::can::ICanTransport>>(  //
            std::move(maybe_can_transport));

        std::cout << "CAN Iface : '" << params.can_iface.value().c_str() << "'\n";
        const std::size_t mtu = transport_->getProtocolParams().mtu_bytes;
        std::cout << "Iface MTU : " << mtu << "\n";

        // Canard allocates memory for raw bytes block only, so there is no alignment requirement.
        constexpr std::size_t block_alignment = 1;
        const std::size_t     block_size      = mtu;
        const std::size_t     pool_size       = media_collection_.count() * TxQueueCapacity * block_size;
        media_block_mr_.setup(pool_size, block_size, block_alignment);

        // To support redundancy (multiple homogeneous interfaces), it's important to have a non-default
        // handler which "swallows" expected transient failures (by returning `nullopt` result).
        // Otherwise, the default Cyphal behavior will fail/interrupt current and future transfers
        // if some of its media encounter transient failures - thus breaking the whole redundancy goal,
        // namely, maintain communication if at least one of the interfaces is still up and running.
        //
        transport_->setTransientErrorHandler([](auto&) { return cetl::nullopt; });
        // transport_->setTransientErrorHandler(platform::CommonHelpers::Can::transientErrorReporter);

        return transport_.get();
    }

private:
    // Our current max `SerializationBufferSizeBytes` is 515 bytes (for `uavcan.register.Access.Request.1.0`)
    // Assuming CAN classic presentation MTU of 7 bytes (plus a bit of overhead like CRC and stuff),
    // let's calculate the required TX queue capacity, and make it twice to accommodate 2 such messages.
    static constexpr std::size_t TxQueueCapacity = 2 * (515U + 8U) / 7U;

    cetl::pmr::memory_resource&                                    general_mr_;
    libcyphal::IExecutor&                                          executor_;
    platform::BlockMemoryResource&                                 media_block_mr_;
    platform::Linux::CanMediaCollection                            media_collection_;
    libcyphal::UniquePtr<libcyphal::transport::can::ICanTransport> transport_;

};  // TransportBagCan

#endif  // TRANSPORT_BAG_CAN_HPP_INCLUDED
