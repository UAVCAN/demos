// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#ifndef FILE_DOWNLOADER_HPP_INCLUDED
#define FILE_DOWNLOADER_HPP_INCLUDED

#include <cetl/pf17/cetlpf.hpp>
#include <libcyphal/presentation/client.hpp>
#include <libcyphal/presentation/presentation.hpp>
#include <libcyphal/presentation/response_promise.hpp>
#include <libcyphal/time_provider.hpp>
#include <libcyphal/transport/types.hpp>
#include <libcyphal/types.hpp>

#include <uavcan/file/Error_1_0.hpp>
#include <uavcan/file/GetInfo_0_2.hpp>
#include <uavcan/file/Read_1_1.hpp>
#include <uavcan/primitive/Unstructured_1_0.hpp>

#include <chrono>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <utility>

class FileDownloader final
{
public:
    /// Factory method to create a FileDownloader instance.
    ///
    static FileDownloader make(libcyphal::presentation::Presentation& presentation,
                               libcyphal::ITimeProvider&              time_provider)
    {
        return FileDownloader{presentation, time_provider};
    }

    FileDownloader(FileDownloader&& other) noexcept = default;
    ~FileDownloader()                               = default;

    FileDownloader(const FileDownloader&)                = delete;
    FileDownloader& operator=(const FileDownloader&)     = delete;
    FileDownloader& operator=(FileDownloader&&) noexcept = delete;

    bool start(const libcyphal::transport::NodeId remote_node_id, const cetl::string_view file_path)
    {
        get_info_promise_.reset();
        get_info_client_.reset();
        read_promise_.reset();
        read_client_.reset();

        state_.file_size        = 0;
        state_.file_progress    = 0;
        state_.file_path        = {file_path.data(), file_path.size()};
        state_.start_time       = time_provider_.now();
        state_.file_error.value = uavcan::file::Error_1_0::OK;

        get_info_client_ = makeClient<Svc::GetInfo>("GetInfo", remote_node_id);
        read_client_     = makeClient<Svc::Read>("Read", remote_node_id);
        if (!get_info_client_ || !read_client_)
        {
            state_.file_error.value = uavcan::file::Error_1_0::UNKNOWN_ERROR;
            return false;
        }

        read_request_.offset    = 0;
        read_request_.path.path = {file_path.begin(), file_path.end(), &presentation_.memory()};

        std::cout << "Getting file info (path='" << file_path << "')...\n";
        return initiateGetInfoRequest();
    }

private:
    using NodeId                 = libcyphal::transport::NodeId;
    using Presentation           = libcyphal::presentation::Presentation;
    using UnstructuredData       = uavcan::primitive::Unstructured_1_0;
    using ResponsePromiseFailure = libcyphal::presentation::ResponsePromiseFailure;

    template <typename T>
    struct SvcSpec : T
    {
        using Client  = libcyphal::presentation::ServiceClient<T>;
        using Promise = libcyphal::presentation::ResponsePromise<typename T::Response>;
        using Failure = typename Client::Failure;
    };
    struct Svc
    {
        using Read    = SvcSpec<uavcan::file::Read_1_1>;
        using GetInfo = SvcSpec<uavcan::file::GetInfo_0_2>;

    };  // Svc

    struct State
    {
        std::string             file_path;
        uavcan::file::Error_1_0 file_error;
        libcyphal::TimePoint    start_time;
        std::size_t             file_size{0};
        std::size_t             file_progress{0};
        int                     failed_requests{0};

    };  // State

    FileDownloader(Presentation& presentation, libcyphal::ITimeProvider& time_provider)
        : presentation_{presentation}
        , time_provider_{time_provider}
    {
    }

    template <typename Service>
    auto makeClient(const cetl::string_view role, const NodeId server_node_id)
        -> cetl::optional<typename Service::Client>
    {
        auto maybe_client = presentation_.makeClient<Service>(server_node_id);
        if (const auto* const failure = cetl::get_if<Presentation::MakeFailure>(&maybe_client))
        {
            (void) failure;
            std::cerr << "Can't make '" << role << "' client.\n";
            return cetl::nullopt;
        }
        return cetl::get<typename Service::Client>(std::move(maybe_client));
    }

    template <typename Service, typename Handler>
    bool makeRequest(  //
        const cetl::string_view                    role,
        cetl::optional<typename Service::Client>&  client,
        cetl::optional<typename Service::Promise>& promise,
        const typename Service::Request&           request,
        Handler&&                                  handler,
        const libcyphal::Duration                  timeout = std::chrono::seconds{1})
    {
        promise.reset();

        auto maybe_promise = client->request(time_provider_.now() + timeout, request);
        if (const auto* const failure = cetl::get_if<typename Service::Failure>(&maybe_promise))
        {
            (void) failure;
            state_.file_error.value = uavcan::file::Error_1_0::UNKNOWN_ERROR;
            std::cerr << "Can't make '" << role << "' request.\n";
            complete();
            return false;
        }

        promise.emplace(cetl::get<typename Service::Promise>(std::move(maybe_promise)));
        promise->setCallback(std::forward<Handler>(handler));
        return true;
    }

    bool initiateGetInfoRequest()
    {
        Svc::GetInfo::Request request{&presentation_.memory()};
        request.path.path = {state_.file_path.begin(), state_.file_path.end(), &presentation_.memory()};
        return makeRequest<Svc::GetInfo>("GetInfo",
                                         get_info_client_,
                                         get_info_promise_,
                                         request,
                                         [this](const auto& arg) {
                                             //
                                             handleGetInfoPromiseResult(arg.result);
                                         });
    }

    void handleGetInfoPromiseResult(const Svc::GetInfo::Promise::Result& result)
    {
        if (const auto* const failure = cetl::get_if<ResponsePromiseFailure>(&result))
        {
            (void) failure;
            state_.failed_requests++;
            if (state_.failed_requests >= MaxRetriesOnRequestFailure)
            {
                std::cerr << "'GetInfo' request failed (times=" << state_.failed_requests << ").\n";
                state_.file_error.value = uavcan::file::Error_1_0::UNKNOWN_ERROR;
                complete();
            }
            else
            {
                std::cerr << "'GetInfo' request failed (times=" << state_.failed_requests << "). Retrying…\n";
                initiateGetInfoRequest();
            }
            return;
        }
        state_.failed_requests = 0;
        const auto success     = cetl::get<Svc::GetInfo::Promise::Success>(result);

        get_info_promise_.reset();
        get_info_client_.reset();

        const auto& response = success.response;
        if (response._error.value == uavcan::file::Error_1_0::OK)
        {
            state_.file_size = response.size;
            std::cout << "Downloading (size=" << state_.file_size << ") ...\n";
            if (state_.file_size > 0)
            {
                state_.start_time = time_provider_.now();

                printProgress();
                initiateNextReadRequest();
                return;
            }

            state_.file_error.value = uavcan::file::Error_1_0::OK;
        }
        else
        {
            state_.file_error = response._error;
            std::cerr << "Can't get file info (err=" << response._error.value << ").\n";
        }

        complete();
    }

    bool initiateNextReadRequest()
    {
        return makeRequest<Svc::Read>("Read", read_client_, read_promise_, read_request_, [this](const auto& arg) {
            //
            handleReadPromiseResult(arg.result);
        });
    }

    void handleReadPromiseResult(Svc::Read::Promise::Result result)
    {
        if (const auto* const failure = cetl::get_if<ResponsePromiseFailure>(&result))
        {
            (void) failure;
            state_.failed_requests++;
            if (state_.failed_requests >= MaxRetriesOnRequestFailure)
            {
                std::cerr << "'Read' request failed (times=" << state_.failed_requests << ").\n";
                state_.file_error.value = uavcan::file::Error_1_0::UNKNOWN_ERROR;
                complete();
            }
            else
            {
                std::cerr << "'Read' request failed (times=" << state_.failed_requests << "). Retrying…\n";
                initiateNextReadRequest();
            }
            return;
        }
        state_.failed_requests = 0;

        const auto success = cetl::get<Svc::Read::Promise::Success>(std::move(result));

        const auto& response = success.response;
        if (response._error.value == uavcan::file::Error_1_0::OK)
        {
            const auto data_size = response.data.value.size();
            read_request_.offset += response.data.value.size();

            printProgress();

            // Are we done?
            if (data_size == UnstructuredData::_traits_::ArrayCapacity::value)
            {
                initiateNextReadRequest();
                return;
            }
        }
        else
        {
            state_.file_error = response._error;
            std::cerr << "Can't read file (err=" << response._error.value << ").\n";
        }
        complete();
    }

    void printProgress()
    {
        CETL_DEBUG_ASSERT(state_.file_size > 0, "");
        CETL_DEBUG_ASSERT(read_request_.offset <= state_.file_size, "");

        const auto progress = (read_request_.offset * 100U) / state_.file_size;
        CETL_DEBUG_ASSERT(progress <= 100U, "");

        // Print progress only if its integer % has changed (or in the beginning).
        if ((progress != state_.file_progress) || (read_request_.offset == 0))
        {
            state_.file_progress = progress;
            const auto duration  = time_provider_.now() - state_.start_time;
            if (const auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(duration).count())
            {
                const auto speed_kb_per_sec = (read_request_.offset * 1000000U) / (duration_us * 1024U);
                std::cout << "\r  progress " << progress << "% (speed=" << speed_kb_per_sec << "KB/s)            "
                          << std::flush;
            }
            else
            {
                std::cout << "\r  progress " << progress << "%" << std::flush;
            }
        }
    }

    void complete()
    {
        const auto duration = time_provider_.now() - state_.start_time;
        std::cout << "\nDownload completed (err=" << state_.file_error.value  //
                  << ", time=" << std::fixed << std::setprecision(6)          // NOLINT
                  << std::chrono::duration_cast<std::chrono::duration<double>>(duration).count() << "s).\n"
                  << std::flush;

        get_info_promise_.reset();
        get_info_client_.reset();
        read_promise_.reset();
        read_client_.reset();
    }

    // MARK: Data members:

    static constexpr int MaxRetriesOnRequestFailure = 10;

    Presentation&                         presentation_;
    libcyphal::ITimeProvider&             time_provider_;
    cetl::optional<Svc::GetInfo::Client>  get_info_client_;
    cetl::optional<Svc::GetInfo::Promise> get_info_promise_;
    cetl::optional<Svc::Read::Client>     read_client_;
    cetl::optional<Svc::Read::Promise>    read_promise_;
    Svc::Read::Request                    read_request_{&presentation_.memory()};
    State                                 state_;

};  // FileDownloader

#endif  // FILE_DOWNLOADER_HPP_INCLUDED
