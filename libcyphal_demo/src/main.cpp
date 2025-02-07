// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#include "application.hpp"
#include "exec_cmd_provider.hpp"
#include "file_downloader.hpp"
#include "transport_bag_can.hpp"
#include "transport_bag_udp.hpp"

#include <cetl/pf17/cetlpf.hpp>
#include <libcyphal/application/node.hpp>
#include <libcyphal/presentation/presentation.hpp>
#include <libcyphal/time_provider.hpp>
#include <libcyphal/transport/transfer_id_map.hpp>
#include <libcyphal/transport/transport.hpp>
#include <libcyphal/transport/types.hpp>
#include <libcyphal/types.hpp>

#include <uavcan/node/Health_1_0.hpp>
#include <uavcan/node/Mode_1_0.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <ios>
#include <iostream>
#include <unistd.h>
#include <unordered_map>
#include <utility>

using namespace std::chrono_literals;

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)

namespace
{

class AppExecCmdProvider final : public ExecCmdProvider<AppExecCmdProvider>
{
public:
    AppExecCmdProvider(libcyphal::application::Node&          node,
                       libcyphal::presentation::Presentation& presentation,
                       libcyphal::ITimeProvider&              time_provider,
                       Server&&                               server)
        : ExecCmdProvider{presentation, std::move(server)}
        , node_{node}
        , presentation_{presentation}
        , time_provider_{time_provider}
    {
    }

    bool should_break() const noexcept
    {
        return should_power_off_ || restart_required_;
    }

    bool should_power_off() const noexcept
    {
        return should_power_off_;
    }

private:
    bool onCommand(const Request::_traits_::TypeOf::command       command,
                   const cetl::string_view                        parameter,
                   const libcyphal::transport::ServiceRxMetadata& metadata,
                   Response&                                      response) noexcept override
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

        case Request::COMMAND_BEGIN_SOFTWARE_UPDATE:
            //
            std::cout << "🚧 COMMAND_BEGIN_SOFTWARE_UPDATE (file='" << parameter << "')\n";
            node_.heartbeatProducer().message().mode.value = uavcan::node::Mode_1_0::SOFTWARE_UPDATE;

            file_downloader_.emplace(FileDownloader::make(presentation_, time_provider_));
            file_downloader_->start(metadata.remote_node_id, parameter);
            break;

        default:
            return ExecCmdProvider::onCommand(command, parameter, metadata, response);
        }
        return true;
    }

    libcyphal::application::Node&          node_;
    libcyphal::presentation::Presentation& presentation_;
    libcyphal::ITimeProvider&              time_provider_;
    cetl::optional<FileDownloader>         file_downloader_;
    bool                                   should_power_off_{false};
    bool                                   restart_required_{false};

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

};  // ExitCode

class TransferIdMap final : public libcyphal::transport::ITransferIdMap
{
public:
    explicit TransferIdMap(cetl::pmr::memory_resource& memory) noexcept
        : session_spec_to_transfer_id_{&memory}
    {
    }

private:
    using TransferId = libcyphal::transport::TransferId;
    using PmvAlloc   = cetl::pmr::polymorphic_allocator<std::pair<const SessionSpec, TransferId>>;
    using PmrMap     = std::unordered_map<SessionSpec, TransferId, std::hash<SessionSpec>, std::equal_to<>, PmvAlloc>;

    // ITransferIdMap

    TransferId getIdFor(const SessionSpec& session_spec) const noexcept override
    {
        const auto it = session_spec_to_transfer_id_.find(session_spec);
        return it != session_spec_to_transfer_id_.end() ? it->second : 0;
    }

    void setIdFor(const SessionSpec& session_spec, const TransferId transfer_id) noexcept override
    {
        session_spec_to_transfer_id_[session_spec] = transfer_id;
    }

    PmrMap session_spec_to_transfer_id_;

};  // TransferIdMap

void PrintUniqueIdTo(const std::array<std::uint8_t, 16>& unique_id, std::ostream& os)
{
    const auto original_flags = os.flags();
    for (const auto byte : unique_id)
    {
        os << std::hex << std::setw(2) << std::setfill('0') << static_cast<std::uint32_t>(byte);
    }
    os.flags(original_flags);
}

libcyphal::Expected<bool, ExitCode> run_application(const char* const root_path)
{
    std::cout << "\n🟢 ***************** LibCyphal demo *******************\n";
    std::cout << "Root path : '" << root_path << "'\n";

    Application application{root_path};
    auto&       executor       = application.executor();
    auto&       general_mr     = application.general_memory();
    auto&       media_block_mr = application.media_block_memory();

    auto node_params  = application.getNodeParams();
    auto iface_params = application.getIfaceParams();

    // 1. Create the transport layer object. First try CAN, then UDP.
    //
    TransportBagCan transport_bag_can{general_mr, executor, media_block_mr};
    TransportBagUdp transport_bag_udp{general_mr, executor, media_block_mr};
    //
    libcyphal::transport::ITransport* transport_iface = transport_bag_can.create(iface_params);
    if (transport_iface == nullptr)
    {
        transport_iface = transport_bag_udp.create(iface_params);
    }
    if (transport_iface == nullptr)
    {
        std::cerr << "❌ Failed to create any transport.\n";
        return ExitCode::TransportCreationFailure;
    }
    TransferIdMap transfer_id_map{general_mr};

    // 2. Create the presentation layer object.
    //
    const auto unique_id = application.getUniqueId();
    (void) transport_iface->setLocalNodeId(node_params.id.value()[0]);
    std::cout << "Node ID   : " << transport_iface->getLocalNodeId().value_or(65535) << "\n";
    std::cout << "Node Name : '" << node_params.description.value().c_str() << "'\n";
    std::cout << "Unique-ID : ";
    PrintUniqueIdTo(unique_id, std::cout);
    std::cout << "\n";
    //
    libcyphal::presentation::Presentation presentation{general_mr, executor, *transport_iface};
    presentation.setTransferIdMap(&transfer_id_map);

    // 3. Create the node object with name.
    //
    auto maybe_node = libcyphal::application::Node::make(presentation);
    if (const auto* failure = cetl::get_if<libcyphal::application::Node::MakeFailure>(&maybe_node))
    {
        (void) failure;
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
        .setUniqueId(unique_id);
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
    auto maybe_exec_cmd_provider = AppExecCmdProvider::make(node, presentation, executor);
    if (const auto* failure = cetl::get_if<libcyphal::application::Node::MakeFailure>(&maybe_exec_cmd_provider))
    {
        (void) failure;
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
        (void) executor.pollAwaitableResourcesFor(cetl::make_optional(timeout));
    }
    //
    std::cout << "🏁 Done.\n-----------\nRun Stats:\n";
    std::cout << "  worst_callback_lateness=" << worst_lateness.count() << "us\n";

    return !exec_cmd_provider.should_power_off();
}

}  // namespace

int main(const int argc, char* const argv[])
{
    const char* root_path = "/tmp/" NODE_NAME;  // NOLINT
    if (argc > 1)
    {
        root_path = argv[1];  // NOLINT
    }

    const auto result = run_application(root_path);
    if (const auto* const err = cetl::get_if<ExitCode>(&result))
    {
        return static_cast<int>(*err);
    }

    // Should we restart?
    if (cetl::get<bool>(result))
    {
        (void) ::execve(argv[0], argv, ::environ);  // NOLINT
        return static_cast<int>(ExitCode::RestartFailure);
    }

    return static_cast<int>(ExitCode::Success);
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
