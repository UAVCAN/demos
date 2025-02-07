// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#ifndef PLATFORM_COMMON_HELPERS_HPP_INCLUDED
#define PLATFORM_COMMON_HELPERS_HPP_INCLUDED

#include <cetl/pf17/cetlpf.hpp>
#include <cetl/visit_helpers.hpp>
#include <libcyphal/errors.hpp>
#include <libcyphal/transport/can/can_transport.hpp>
#include <libcyphal/transport/errors.hpp>
#include <libcyphal/transport/udp/udp_transport.hpp>

#include <iostream>

namespace platform
{

struct CommonHelpers
{
    struct Printers
    {
        static cetl::string_view describeError(const libcyphal::ArgumentError&)
        {
            return "ArgumentError";
        }
        static cetl::string_view describeError(const libcyphal::MemoryError&)
        {
            return "MemoryError";
        }
        static cetl::string_view describeError(const libcyphal::transport::AnonymousError&)
        {
            return "AnonymousError";
        }
        static cetl::string_view describeError(const libcyphal::transport::CapacityError&)
        {
            return "CapacityError";
        }
        static cetl::string_view describeError(const libcyphal::transport::AlreadyExistsError&)
        {
            return "AlreadyExistsError";
        }
        static cetl::string_view describeError(const libcyphal::transport::PlatformError& error)
        {
            return "PlatformError";
        }

        static cetl::string_view describeAnyFailure(const libcyphal::transport::AnyFailure& failure)
        {
            return cetl::visit([](const auto& error) { return describeError(error); }, failure);
        }
    };

    struct Can
    {
        static cetl::optional<libcyphal::transport::AnyFailure> transientErrorReporter(
            libcyphal::transport::can::ICanTransport::TransientErrorReport::Variant& report_var)
        {
            using Report = libcyphal::transport::can::ICanTransport::TransientErrorReport;

            cetl::visit(  //
                cetl::make_overloaded(
                    [](const Report::CanardTxPush& report) {
                        std::cerr << "Failed to push TX frame to canard "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::CanardRxAccept& report) {
                        std::cerr << "Failed to accept RX frame at canard "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::MediaPop& report) {
                        std::cerr << "Failed to pop frame from media "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::ConfigureMedia& report) {
                        std::cerr << "Failed to configure CAN.\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::MediaConfig& report) {
                        std::cerr << "Failed to configure media "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::MediaPush& report) {
                        std::cerr << "Failed to push frame to media "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    }),
                report_var);

            // "swallows" all transient failures, thus giving a chance
            // to other redundant media interfaces to continue.
            return cetl::nullopt;
        }

    };  // Can

    struct Udp
    {
        static cetl::optional<libcyphal::transport::AnyFailure> transientErrorReporter(
            libcyphal::transport::udp::IUdpTransport::TransientErrorReport::Variant& report_var)
        {
            using Report = libcyphal::transport::udp::IUdpTransport::TransientErrorReport;

            cetl::visit(  //
                cetl::make_overloaded(
                    [](const Report::UdpardTxPublish& report) {
                        std::cerr << "Failed to TX message frame to udpard "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::UdpardTxRequest& report) {
                        std::cerr << "Failed to TX request frame to udpard "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::UdpardTxRespond& report) {
                        std::cerr << "Failed to TX response frame to udpard "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::UdpardRxMsgReceive& report) {
                        std::cerr << "Failed to accept RX message frame at udpard "
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::UdpardRxSvcReceive& report) {
                        std::cerr << "Failed to accept RX service frame at udpard "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::MediaMakeRxSocket& report) {
                        std::cerr << "Failed to make RX socket " << "(mediaIdx=" << static_cast<int>(report.media_index)
                                  << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::MediaMakeTxSocket& report) {
                        std::cerr << "Failed to make TX socket " << "(mediaIdx=" << static_cast<int>(report.media_index)
                                  << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::MediaTxSocketSend& report) {
                        std::cerr << "Failed to TX frame to socket "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    },
                    [](const Report::MediaRxSocketReceive& report) {
                        std::cerr << "Failed to RX frame from socket "
                                  << "(mediaIdx=" << static_cast<int>(report.media_index) << ").\n"
                                  << Printers::describeAnyFailure(report.failure) << "\n";
                    }),
                report_var);

            // "swallows" all transient failures, thus giving a chance
            // to other redundant media interfaces to continue.
            return cetl::nullopt;
        }

    };  // Udp

};  // CommonHelpers

}  // namespace platform

#endif  // PLATFORM_COMMON_HELPERS_HPP_INCLUDED
