// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT

#ifndef PLATFORM_LINUX_SINGLE_THREADED_EXECUTOR_HPP_INCLUDED
#define PLATFORM_LINUX_SINGLE_THREADED_EXECUTOR_HPP_INCLUDED

#ifdef PLATFORM_LINUX_TYPE_BSD
#include "platform/linux/kqueue_single_threaded_executor.hpp"
#else
#include "platform/linux/epoll_single_threaded_executor.hpp"
#endif

namespace platform
{
namespace Linux
{

#ifdef PLATFORM_LINUX_TYPE_BSD
using SingleThreadedExecutor = bsd::KqueueSingleThreadedExecutor;
#else
using SingleThreadedExecutor = debian::EpollSingleThreadedExecutor;
#endif

}  // namespace Linux
}  // namespace platform

#endif  // PLATFORM_LINUX_SINGLE_THREADED_EXECUTOR_HPP_INCLUDED
