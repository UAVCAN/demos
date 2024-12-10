// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#include "application.hpp"
#include "exec_cmd_provider.hpp"
#include "transport_bag_udp.hpp"
#include "platform/posix/posix_executor_extension.hpp"

#include <cetl/pf17/cetlpf.hpp>
#include <libcyphal/application/node.hpp>
#include <libcyphal/presentation/presentation.hpp>
#include <libcyphal/transport/transport.hpp>
#include <libcyphal/types.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <unistd.h>  // execve
#include <utility>

using namespace std::chrono_literals;

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)

namespace
{

class AppExecCmdProvider final : public ExecCmdProvider<AppExecCmdProvider>
{
public:
    using ExecCmdProvider::ExecCmdProvider;

    bool should_break() const noexcept
    {
        return should_power_off_ || restart_required_;
    }

    bool should_power_off() const noexcept
    {
        return should_power_off_;
    }

private:
    bool onCommand(const Request::_traits_::TypeOf::command command,
                   const cetl::string_view                  parameter,
                   Response&                                response) noexcept override
    {
        response.status = Response::STATUS_SUCCESS;

        switch (command)
        {
        case Request::COMMAND_POWER_OFF:
            //
            std::cout << "🛑 COMMAND_POWER_OFF\n";
            should_power_off_ = true;
            break;

        case Request::COMMAND_RESTART:
            //
            std::cout << "♻️ COMMAND_RESTART\n";
            restart_required_ = true;
            break;

        case Request::COMMAND_IDENTIFY:
            //
            std::cout << "🔔 COMMAND_IDENTIFY\n";
            break;

        case Request::COMMAND_STORE_PERSISTENT_STATES:
            //
            std::cout << "💾 COMMAND_STORE_PERSISTENT_STATES\n";
            restart_required_ = true;
            break;

        default:
            return ExecCmdProvider::onCommand(command, parameter, response);
        }
        return true;
    }

    bool should_power_off_{false};
    bool restart_required_{false};

};  // AppExecCmdProvider

/// Defines various exit codes for the demo application.
///
enum class ExitCode : std::uint8_t
{
    Success                        = 0,
    TransportCreationFailure       = 1,
    NodeCreationFailure            = 2,
    RegistryCreationFailure        = 3,
    ExecCmdProviderCreationFailure = 4,
    RestartFailure                 = 5,
    ConfigurationFailure           = 6,

};  // ExitCode

libcyphal::Expected<bool, ExitCode> run_application()
{
    std::cout << "\n🟢 ***************** LibCyphal demo *******************\n";

    Application application;
    auto&       executor       = application.executor();
    auto&       general_mr     = application.general_memory();
    auto&       media_block_mr = application.media_block_memory();

    auto node_params  = application.getNodeParams();
    auto iface_params = application.getIfaceParams();

    auto* posix_executor = cetl::rtti_cast<platform::posix::IPosixExecutorExtension*>(&executor);
    if (nullptr == posix_executor) {
        return ExitCode::ConfigurationFailure;
    }

    // 1. Create the transport layer object. First try CAN, then UDP.
    //
    //TransportBagCan transport_bag_can{general_mr, executor, media_block_mr};
    TransportBagUdp transport_bag_udp{general_mr, executor, media_block_mr};
    //
    libcyphal::transport::ITransport* transport_iface = transport_bag_udp.create(iface_params);
    if (transport_iface == nullptr)
    {
        std::cerr << "❌ Failed to create any transport.\n";
        return ExitCode::TransportCreationFailure;
    }

    // 2. Create the presentation layer object.
    //
    (void) transport_iface->setLocalNodeId(node_params.id.value()[0]);
    std::cout << "Node ID   : " << transport_iface->getLocalNodeId().value_or(65535) << "\n";
    std::cout << "Node Name : '" << node_params.description.value().c_str() << "'\n";
    libcyphal::presentation::Presentation presentation{general_mr, executor, *transport_iface};

    // 3. Create the node object with name.
    //
    auto maybe_node = libcyphal::application::Node::make(presentation);
    if (const auto* failure = cetl::get_if<libcyphal::application::Node::MakeFailure>(&maybe_node))
    {
        std::cerr << "❌ Failed to create node (iface='"
                  << static_cast<cetl::string_view>(iface_params.udp_iface.value()) << "').\n";
        return ExitCode::NodeCreationFailure;
        ;
    }
    auto node = cetl::get<libcyphal::application::Node>(std::move(maybe_node));

    // 4. Populate the node info.
    //
    // The hardware version is not populated in this demo because it runs on no specific hardware.
    // An embedded node would usually determine the version by querying the hardware.
    auto& get_info_prov = node.getInfoProvider();
    get_info_prov  //
        .setName(node_params.description.value())
        .setSoftwareVersion(VERSION_MAJOR, VERSION_MINOR)
        .setSoftwareVcsRevisionId(VCS_REVISION_ID)
        .setUniqueId(application.getUniqueId());
    //
    // Update node's health according to states of memory resources.
    node.heartbeatProducer().setUpdateCallback([&](const auto& arg) {
        //
        const auto gen_diag = general_mr.queryDiagnostics();
        const auto blk_diag = media_block_mr.queryDiagnostics();
        if ((gen_diag.oom_count > 0) || (blk_diag.oom_count > 0))
        {
            arg.message.health.value = uavcan::node::Health_1_0::CAUTION;
        }
    });

    // 5. Bring up registry provider.
    //
    if (const auto failure = node.makeRegistryProvider(application.registry()))
    {
        std::cerr << "❌ Failed to create registry provider.\n";
        return ExitCode::RegistryCreationFailure;
    }

    // 6. Bring up the command execution provider.
    //
    auto maybe_exec_cmd_provider = AppExecCmdProvider::make(presentation);
    if (const auto* failure = cetl::get_if<libcyphal::application::Node::MakeFailure>(&maybe_node))
    {
        std::cerr << "❌ Failed to create exec cmd provider.\n";
        return ExitCode::ExecCmdProviderCreationFailure;
    }
    auto exec_cmd_provider = cetl::get<AppExecCmdProvider>(std::move(maybe_exec_cmd_provider));

    // Main loop.
    //
    libcyphal::Duration worst_lateness{0};
    std::cout << "-----------\nRunning..." << std::endl;  // NOLINT
    //
    while (!exec_cmd_provider.should_break())
    {
        const auto spin_result = executor.spinOnce();
        worst_lateness         = std::max(worst_lateness, spin_result.worst_lateness);

        libcyphal::Duration timeout{1s};  // awake at least once per second
        if (spin_result.next_exec_time.has_value())
        {
            timeout = std::min(timeout, spin_result.next_exec_time.value() - executor.now());
        }
        (void) posix_executor->pollAwaitableResourcesFor(cetl::make_optional(timeout));
    }
    //
    std::cout << "🏁 Done.\n-----------\nRun Stats:\n";
    std::cout << "  worst_callback_lateness=" << worst_lateness.count() << "us\n";

    return !exec_cmd_provider.should_power_off();
}

}  // namespace

int main(const int, char* const argv[])
{
    const auto result = run_application();
    if (const auto* const err = cetl::get_if<ExitCode>(&result))
    {
        return static_cast<int>(*err);
    }

    // Should we restart?
    if (cetl::get<bool>(result))
    {
        //(void) ::execve(argv[0], argv, ::environ);  // NOLINT
        return static_cast<int>(ExitCode::RestartFailure);
    }

    return static_cast<int>(ExitCode::Success);
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
