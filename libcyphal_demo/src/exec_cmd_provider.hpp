// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#ifndef COMMAND_PROVIDER_HPP_INCLUDED
#define COMMAND_PROVIDER_HPP_INCLUDED

#include "libcyphal/application/node.hpp"
#include "libcyphal/presentation/presentation.hpp"
#include "libcyphal/presentation/server.hpp"
#include "libcyphal/time_provider.hpp"
#include "libcyphal/transport/types.hpp"
#include "libcyphal/types.hpp"

#include <cetl/pf17/cetlpf.hpp>

#include <uavcan/node/ExecuteCommand_1_3.hpp>

#include <chrono>
#include <utility>

/// Defines 'ExecuteCommand' provider component for the application node.
///
/// Internally, it uses the 'ExecuteCommand' service server to handle incoming requests.
///
/// No Sonar cpp:S4963 'The "Rule-of-Zero" should be followed'
/// b/c we do directly handle resources here (namely capturing of `this` in the request callback).
///
template <typename Derived>
class ExecCmdProvider  // NOSONAR cpp:S4963
{
public:
    using Service = uavcan::node::ExecuteCommand_1_3;
    using Server  = libcyphal::presentation::ServiceServer<Service>;

    /// Defines the response type for the ExecuteCommand provider.
    ///
    using Response = Service::Response;

    /// Defines the request type for the ExecuteCommand provider.
    ///
    using Request = Service::Request;

    /// Typealias to the request command type of the ExecuteCommand provider.
    ///
    /// `std::uint16_t` is used as the command type.
    // Use `Request::COMMAND_XXX` constants to access standard command values.
    ///
    using Command = Request::_traits_::TypeOf::command;

    /// Factory method to create a ExecuteCommand instance.
    ///
    /// @param node The application layer node instance. In use to access heartbeat producer.
    /// @param presentation The presentation layer instance. In use to create 'ExecuteCommand' service server.
    /// @param time_provider The time provider - in use to calculate RPC call deadlines.
    /// @return The ExecuteCommand provider instance or a failure.
    ///
    static auto make(libcyphal::application::Node&          node,
                     libcyphal::presentation::Presentation& presentation,
                     libcyphal::ITimeProvider&              time_provider)
        -> libcyphal::Expected<Derived, libcyphal::presentation::Presentation::MakeFailure>
    {
        auto maybe_srv = presentation.makeServer<Service>();
        if (auto* const failure = cetl::get_if<libcyphal::presentation::Presentation::MakeFailure>(&maybe_srv))
        {
            return std::move(*failure);
        }

        return Derived{node, presentation, time_provider, cetl::get<Server>(std::move(maybe_srv))};
    }

    ExecCmdProvider(ExecCmdProvider&& other) noexcept
        : alloc_{other.alloc_}
        , server_{std::move(other.server_)}
        , response_timeout_{other.response_timeout_}
    {
        // We have to set up request callback again (b/c it captures its own `this` pointer),
        setupOnRequestCallback();
    }

    virtual ~ExecCmdProvider() = default;

    ExecCmdProvider(const ExecCmdProvider&)                = delete;
    ExecCmdProvider& operator=(const ExecCmdProvider&)     = delete;
    ExecCmdProvider& operator=(ExecCmdProvider&&) noexcept = delete;

    /// Sets the response transmission timeout (default is 1s).
    ///
    /// @param timeout Duration of the response transmission timeout. Applied for the next response transmission.
    ///
    void setResponseTimeout(const libcyphal::Duration& timeout) noexcept
    {
        response_timeout_ = timeout;
    }

    /// Handles incoming command requests.
    ///
    /// This method is called by the service server when a new request is received.
    /// The user should override the method to handle custom commands.
    /// If the method returns `false`, the server will respond with a `STATUS_BAD_COMMAND` status.
    ///
    /// @param command   The command to be executed.
    /// @param parameter The command parameter.
    /// @param metadata  The transport RX metadata.
    /// @param response  The response to be sent back to the requester.
    ///
    virtual bool onCommand(const Request::_traits_::TypeOf::command       command,
                           const cetl::string_view                        parameter,
                           const libcyphal::transport::ServiceRxMetadata& metadata,
                           Response&                                      response) noexcept
    {
        (void) command;
        (void) parameter;
        (void) metadata;
        (void) response;

        return false;
    }

protected:
    ExecCmdProvider(const libcyphal::presentation::Presentation& presentation, Server&& server)
        : alloc_{&presentation.memory()}
        , server_{std::move(server)}
        , response_timeout_{std::chrono::seconds{1}}
    {
        setupOnRequestCallback();
    }

private:
    void setupOnRequestCallback()
    {
        server_.setOnRequestCallback([this](const auto& arg, auto continuation) {
            //
            Response response{alloc_};
            if (!onCommand(arg.request.command, makeStringView(arg.request.parameter), arg.metadata, response))
            {
                response.status = Response::STATUS_BAD_COMMAND;
            }

            // There is nothing we can do about possible continuation failures - we just ignore them.
            // TODO: Introduce error handler at the node level.
            (void) continuation(arg.approx_now + response_timeout_, response);
        });
    }

    /// Makes a new string view from request string parameter.
    ///
    static cetl::string_view makeStringView(const Request::_traits_::TypeOf::parameter& container)
    {
        // No Lint and Sonar cpp:S3630 "reinterpret_cast" should not be used" b/c we need to access container raw data.
        // NOLINTNEXTLINE(*-pro-type-reinterpret-cast)
        return {reinterpret_cast<cetl::string_view::const_pointer>(container.data()), container.size()};  // NOSONAR
    }

    // MARK: Data members:

    Response::allocator_type alloc_;
    Server                   server_;
    libcyphal::Duration      response_timeout_;

};  // ExecCmdProvider

#endif  // COMMAND_PROVIDER_HPP_INCLUDED
