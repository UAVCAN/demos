// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT

#ifndef PLATFORM_BSD_KQUEUE_SINGLE_THREADED_EXECUTOR_HPP_INCLUDED
#define PLATFORM_BSD_KQUEUE_SINGLE_THREADED_EXECUTOR_HPP_INCLUDED

#include "platform/posix/posix_executor_extension.hpp"
#include "platform/posix/posix_platform_error.hpp"

#include <cetl/pf17/cetlpf.hpp>
#include <cetl/rtti.hpp>
#include <libcyphal/errors.hpp>
#include <libcyphal/executor.hpp>
#include <libcyphal/platform/single_threaded_executor.hpp>
#include <libcyphal/types.hpp>

#include <sys/event.h>
#include <err.h>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

namespace platform
{
namespace bsd
{

/// @brief Defines BSD Linux platform specific single-threaded executor based on `kqueue` mechanism.
///
class KqueueSingleThreadedExecutor final : public libcyphal::platform::SingleThreadedExecutor,
                                           public posix::IPosixExecutorExtension
{
public:
    KqueueSingleThreadedExecutor()
        : kqueuefd_{kqueue()}
        , total_awaitables_{0}
    {
    }

    KqueueSingleThreadedExecutor(const KqueueSingleThreadedExecutor&)                = delete;
    KqueueSingleThreadedExecutor(KqueueSingleThreadedExecutor&&) noexcept            = delete;
    KqueueSingleThreadedExecutor& operator=(const KqueueSingleThreadedExecutor&)     = delete;
    KqueueSingleThreadedExecutor& operator=(KqueueSingleThreadedExecutor&&) noexcept = delete;

    ~KqueueSingleThreadedExecutor() override
    {
        if (kqueuefd_ >= 0)
        {
            ::close(kqueuefd_);
        }
    }

    cetl::optional<PollFailure> pollAwaitableResourcesFor(const cetl::optional<libcyphal::Duration> timeout) const override
    {
        CETL_DEBUG_ASSERT((total_awaitables_ > 0) || timeout,
                          "Infinite timeout without awaitables means that we will sleep forever.");

        if (total_awaitables_ == 0)
        {
            if (!timeout)
            {
                return libcyphal::ArgumentError{};
            }

            std::this_thread::sleep_for(*timeout);
            return cetl::nullopt;
        }

        // Make sure that timeout is within the range of `::epoll_wait()`'s `int` timeout parameter.
        // Any possible negative timeout will be treated as zero (return immediately from the `::epoll_wait`).
        //
        int clamped_timeout_ms = -1;  // "infinite" timeout
        if (timeout)
        {
            using PollDuration = std::chrono::milliseconds;

            clamped_timeout_ms = static_cast<int>(  //
                std::max(static_cast<PollDuration::rep>(0),
                         std::min(std::chrono::duration_cast<PollDuration>(*timeout).count(),
                                  static_cast<PollDuration::rep>(std::numeric_limits<int>::max()))));
        }

        std::array<struct kevent, MaxEpollEvents> evs{};
        struct timespec clamped_timeout{clamped_timeout_ms / 1000, 0};
        const int kqueue_result = kevent(kqueuefd_, NULL, 0, evs.data(), evs.size(), &clamped_timeout);
        if (kqueue_result < 0)
        {
            const auto err = errno;
            return libcyphal::transport::PlatformError{posix::PosixPlatformError{err}};
        }
        if (kqueue_result == 0)
        {
            return cetl::nullopt;
        }
        const auto kqueue_nfds = static_cast<std::size_t>(kqueue_result);

        const auto now_time = now();
        for (std::size_t index = 0; index < kqueue_nfds; ++index)
        {
            const struct kevent& ev = evs[index];
            if (auto* const cb_interface = static_cast<AwaitableNode*>(ev.udata))
            {
                cb_interface->schedule(Callback::Schedule::Once{now_time});
            }
        }

        return cetl::nullopt;
    }

protected:
    // MARK: - IPosixExecutorExtension

    CETL_NODISCARD Callback::Any registerAwaitableCallback(Callback::Function&&    function,
                                                           const Trigger::Variant& trigger) override
    {
        AwaitableNode new_cb_node{*this, std::move(function)};

        cetl::visit(  //
            cetl::make_overloaded(
                [&new_cb_node](const Trigger::Readable& readable) {
                    //
                    new_cb_node.setup(readable.fd, EVFILT_READ);
                },
                [&new_cb_node](const Trigger::Writable& writable) {
                    //
                    new_cb_node.setup(writable.fd, EVFILT_WRITE);
                }),
            trigger);

        insertCallbackNode(new_cb_node);
        return {std::move(new_cb_node)};
    }

    // MARK: - RTTI

    CETL_NODISCARD void* _cast_(const cetl::type_id& id) & noexcept override
    {
        if (id == IPosixExecutorExtension::_get_type_id_())
        {
            return static_cast<IPosixExecutorExtension*>(this);
        }
        return Base::_cast_(id);
    }
    CETL_NODISCARD const void* _cast_(const cetl::type_id& id) const& noexcept override
    {
        if (id == IPosixExecutorExtension::_get_type_id_())
        {
            return static_cast<const IPosixExecutorExtension*>(this);
        }
        return Base::_cast_(id);
    }

private:
    using Base = SingleThreadedExecutor;
    using Self = KqueueSingleThreadedExecutor;

    /// No Sonar cpp:S4963 b/c `AwaitableNode` supports move operation.
    ///
    class AwaitableNode final : public CallbackNode  // NOSONAR cpp:S4963
    {
    public:
        AwaitableNode(Self& executor, Callback::Function&& function)
            : CallbackNode{executor, std::move(function)}
            , fd_{-1}
            , events_{0}
        {
        }

        ~AwaitableNode() override
        {
            if (fd_ >= 0)
            {
                struct kevent ev;
                EV_SET(&ev, fd_, events_, EV_DELETE, NOTE_DELETE, 0, 0);
                kevent(getExecutor().kqueuefd_, &ev, 1, NULL, 0, NULL);
                getExecutor().total_awaitables_--;
            }
        }

        AwaitableNode(AwaitableNode&& other) noexcept
            : CallbackNode(std::move(other))
            , fd_{std::exchange(other.fd_, -1)}
            , events_{std::exchange(other.events_, 0)}
        {
            if (fd_ >= 0)
            {
                struct kevent ev;
                EV_SET(&ev, fd_, events_, EV_DELETE, NOTE_DELETE, 0, 0);
                kevent(getExecutor().kqueuefd_, &ev, 1, NULL, 0, NULL);
                EV_SET(&ev, fd_, events_, EV_ADD | EV_CLEAR, NOTE_WRITE, 0, this);
                kevent(getExecutor().kqueuefd_, &ev, 1, NULL, 0, NULL);
            }
        }

        AwaitableNode(const AwaitableNode&)                      = delete;
        AwaitableNode& operator=(const AwaitableNode&)           = delete;
        AwaitableNode& operator=(AwaitableNode&& other) noexcept = delete;

        int fd() const noexcept
        {
            return fd_;
        }

        std::uint32_t events() const noexcept
        {
            return events_;
        }

        void setup(const int fd, const std::uint32_t events) noexcept
        {
            CETL_DEBUG_ASSERT(fd >= 0, "");
            CETL_DEBUG_ASSERT(events != 0, "");

            fd_     = fd;
            events_ = events | EVFILT_VNODE;

            getExecutor().total_awaitables_++;
            struct kevent ev;
            EV_SET(&ev, fd, events_, EV_ADD | EV_CLEAR, NOTE_WRITE, 0, this);
            int ret = kevent(getExecutor().kqueuefd_, &ev, 1, NULL, 0, NULL);
            if (ret == -1)
                err(EXIT_FAILURE, "kevent register");
        }

    private:
        Self& getExecutor() noexcept
        {
            return static_cast<Self&>(executor());
        }

        // MARK: Data members:

        int           fd_;
        std::uint32_t events_;

    };  // AwaitableNode

    // MARK: - Data members:

    static constexpr int MaxEpollEvents = 16;

    int         kqueuefd_;
    std::size_t total_awaitables_;

};  // KqueueSingleThreadedExecutor

}  // namespace bsd
}  // namespace platform

#endif  // PLATFORM_BSD_KQUEUE_SINGLE_THREADED_EXECUTOR_HPP_INCLUDED
