// This software is distributed under the terms of the MIT License.
// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
// Copyright Amazon.com Inc. or its affiliates.
// SPDX-License-Identifier: MIT
// Author: Sergei Shirokov <sergei.shirokov@zubax.com>

#ifndef APPLICATION_HPP
#define APPLICATION_HPP

#include "platform/block_memory_resource.hpp"
#include "platform/linux/epoll_single_threaded_executor.hpp"
#include "platform/o1_heap_memory_resource.hpp"
#include "platform/storage.hpp"
#include "platform/string.hpp"

#include <cetl/pf17/cetlpf.hpp>
#include <libcyphal/application/registry/register.hpp>
#include <libcyphal/application/registry/registry_impl.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iterator>

/// The main application class.
///
/// Expected to be a singleton.
///
class Application final
{
public:
    // Defines max length of various strings.
    static constexpr std::size_t MaxIfaceLen = 64;
    static constexpr std::size_t MaxNodeDesc = 50;

    struct Regs
    {
        using Value = libcyphal::application::registry::IRegister::Value;

        template <std::size_t Footprint>
        using Register = libcyphal::application::registry::Register<Footprint>;

        /// Defines the footprint size of the type-erased register.
        /// The Footprint size is passed to internal unbounded variant
        /// which in turn should be big enough to store any register implementation.
        /// `12`-pointer size is a trade-off between memory usage and flexibility of what could be stored.
        /// Increase this value if you need to store more complex data (like more "big" register's lambdas).
        ///
        static constexpr std::size_t RegisterFootprint = sizeof(void*) * 12;

        /// Defines general purpose string parameter exposed as mutable register.
        ///
        template <std::size_t N>
        struct StringParam
        {
            StringParam(const libcyphal::application::registry::IRegister::Name     name,
                        libcyphal::application::registry::Registry&                 registry,
                        const cetl::string_view                                     initial_value,
                        const libcyphal::application::registry::IRegister::Options& options = {})
                : value_{initial_value}
                , memory_{registry.memory()}
                , register_{registry.route(
                      name,
                      [this] {
                          //
                          return makeStringValue(value_);
                      },
                      [this](const auto& value) -> cetl::optional<libcyphal::application::registry::SetError> {
                          //
                          if (value.is_string())
                          {
                              this->value_ = libcyphal::application::registry::makeStringView(value.get_string().value);
                              return cetl::nullopt;
                          }
                          return libcyphal::application::registry::SetError::Semantics;
                      },
                      options)}
            {
            }

            CETL_NODISCARD platform::String<N>& value()
            {
                return value_;
            }

        private:
            CETL_NODISCARD Value makeStringValue(const cetl::string_view sv) const
            {
                const Value::allocator_type allocator{&memory_};
                Value                       value{allocator};
                auto&                       str = value.set_string();
                std::copy(sv.begin(), sv.end(), std::back_inserter(str.value));
                return value;
            }

            platform::String<N>         value_;
            cetl::pmr::memory_resource& memory_;
            Register<RegisterFootprint> register_;

        };  // StringParam

        /// Defines general purpose uint16 array parameter exposed as mutable register.
        ///
        template <std::size_t N>
        struct Natural16Param
        {
            Natural16Param(const libcyphal::application::registry::IRegister::Name     name,
                           libcyphal::application::registry::Registry&                 registry,
                           const std::array<std::uint16_t, N>                          initial_value,
                           const libcyphal::application::registry::IRegister::Options& options = {})
                : value_{initial_value}
                , memory_{registry.memory()}
                , register_{registry.route(
                      name,
                      [this] { return makeNatural16Value(); },
                      [this](const auto& value) -> cetl::optional<libcyphal::application::registry::SetError> {
                          //
                          if (value.is_natural16())
                          {
                              const auto&       uint16s = value.get_natural16().value;
                              const std::size_t count   = std::min(uint16s.size(), value_.size());
                              for (std::size_t i = 0; i < count; ++i)
                              {
                                  value_[i] = uint16s[i];  // NOLINT
                              }
                              return cetl::nullopt;
                          }
                          return libcyphal::application::registry::SetError::Semantics;
                      },
                      options)}
            {
            }

            CETL_NODISCARD std::array<std::uint16_t, N>& value()
            {
                return value_;
            }

        private:
            CETL_NODISCARD Value makeNatural16Value() const
            {
                const Value::allocator_type allocator{&memory_};
                Value                       value{allocator};
                auto&                       uint16s = value.set_natural16();
                std::copy(value_.cbegin(), value_.cend(), std::back_inserter(uint16s.value));
                return value;
            }

            std::array<std::uint16_t, N> value_;
            cetl::pmr::memory_resource&  memory_;
            Register<RegisterFootprint>  register_;

        };  // Natural16Param

        Regs(platform::O1HeapMemoryResource&             o1_heap_mr,
             libcyphal::application::registry::Registry& registry,
             platform::BlockMemoryResource&              media_block_mr)
            : o1_heap_mr_{o1_heap_mr}
            , registry_{registry}
            , media_block_mr_{media_block_mr}
            , sys_info_mem_block_{registry.route("sys.info.mem.blk", [this] { return getSysInfoMemBlock(); })}
            , sys_info_mem_general_{registry.route("sys.info.mem.gen", [this] { return getSysInfoMemGeneral(); })}
        {
        }

    private:
        friend class Application;

        Value getSysInfoMemBlock() const;
        Value getSysInfoMemGeneral() const;

        platform::O1HeapMemoryResource&             o1_heap_mr_;
        libcyphal::application::registry::Registry& registry_;
        platform::BlockMemoryResource&              media_block_mr_;

        // clang-format off
        StringParam<MaxIfaceLen>    can_iface_   {  "uavcan.can.iface",         registry_,  {"vcan0"},      {true}};
        StringParam<MaxNodeDesc>    node_desc_   {  "uavcan.node.description",  registry_,  {NODE_NAME},    {true}};
        Natural16Param<1>           node_id_     {  "uavcan.node.id",           registry_,  {65535U},       {true}};
        StringParam<MaxIfaceLen>    udp_iface_   {  "uavcan.udp.iface",         registry_,  {"127.0.0.1"},  {true}};
        Natural16Param<2>           demo_u16s_   {  "demo.u16s",                registry_,  {0U, 0U},       {false}};
        Register<RegisterFootprint> sys_info_mem_block_;
        Register<RegisterFootprint> sys_info_mem_general_;
        // clang-format on

    };  // Regs

    struct IfaceParams
    {
        Regs::StringParam<MaxIfaceLen>& udp_iface;
        Regs::StringParam<MaxIfaceLen>& can_iface;
    };

    struct NodeParams
    {
        Regs::Natural16Param<1>&        id;
        Regs::StringParam<MaxNodeDesc>& description;
    };

    explicit Application(const char* const root_path);
    ~Application();

    Application(const Application&)            = delete;
    Application& operator=(const Application&) = delete;
    Application(Application&&)                 = delete;
    Application& operator=(Application&&)      = delete;

    CETL_NODISCARD platform::Linux::EpollSingleThreadedExecutor& executor() noexcept
    {
        return executor_;
    }

    CETL_NODISCARD platform::O1HeapMemoryResource& general_memory() noexcept
    {
        return o1_heap_mr_;
    }

    CETL_NODISCARD platform::BlockMemoryResource& media_block_memory() noexcept
    {
        return media_block_mr_;
    }

    CETL_NODISCARD libcyphal::application::registry::Registry& registry() noexcept
    {
        return registry_;
    }

    CETL_NODISCARD IfaceParams getIfaceParams() noexcept
    {
        return {regs_.udp_iface_, regs_.can_iface_};
    }

    CETL_NODISCARD NodeParams getNodeParams() noexcept
    {
        return {regs_.node_id_, regs_.node_desc_};
    }

    /// Returns the 128-bit unique-ID of the local node. This value is used in `uavcan.node.GetInfo.Response`.
    ///
    using UniqueId = std::array<std::uint8_t, 16>;  // NOLINT
    UniqueId getUniqueId();

private:
    // MARK: Data members:

    platform::Linux::EpollSingleThreadedExecutor executor_;
    platform::O1HeapMemoryResource               o1_heap_mr_;
    platform::O1HeapMemoryResource               o1_block_heap_mr_;
    platform::BlockMemoryResource                media_block_mr_;
    platform::storage::KeyValue                  storage_;
    libcyphal::application::registry::Registry   registry_;
    Regs                                         regs_;

};  // Application

#endif  // APPLICATION_HPP
