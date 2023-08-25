///                            ____                   ______            __          __
///                           / __ `____  ___  ____  / ____/_  ______  / /_  ____  / /
///                          / / / / __ `/ _ `/ __ `/ /   / / / / __ `/ __ `/ __ `/ /
///                         / /_/ / /_/ /  __/ / / / /___/ /_/ / /_/ / / / / /_/ / /
///                         `____/ .___/`___/_/ /_/`____/`__, / .___/_/ /_/`__,_/_/
///                             /_/                     /____/_/
///
/// This is a simple demo application that shows how to use LibUDPard. It is designed to be easily portable to any
/// baremetal embedded system; the Berkeley socket API will need to be replaced with whatever low-level UDP/IP stack
/// is used on the target platform. The demo application uses fixed-size block pools for dynamic memory management,
/// which is a common approach in deeply embedded systems. Applications where the ordinary heap is available can use
/// the standard malloc() and free() functions instead; or, if a hard real-time heap is needed, O1Heap may be used
/// instead: https://github.com/pavel-kirienko/o1heap.
///
/// The application performs dynamic node-ID allocation, subscribes to a subject and publishes to another subject.
/// Aside from that, it also publishes on the standard Heartbeat subject and responds to certain standard RPC requests.
///
/// The following BPF expression can be used to filter Cyphal/UDP traffic (e.g., in Wireshark):
///
///     udp and dst net 239.0.0.0 mask 255.0.0.0 and dst port 9382
///
/// This software is distributed under the terms of the MIT License.
/// Copyright (C) OpenCyphal Development Team  <opencyphal.org>
/// Copyright Amazon.com Inc. or its affiliates.
/// SPDX-License-Identifier: MIT
/// Author: Pavel Kirienko <pavel@opencyphal.org>

// For clock_gettime().
#define _DEFAULT_SOURCE  // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)

#include "register.h"
#include <udpard.h>
#include "memory_block.h"
#include "storage.h"

// DSDL-generated types.
#include <uavcan/node/Heartbeat_1_0.h>
#include <uavcan/node/GetInfo_1_0.h>
#include <uavcan/pnp/NodeIDAllocationData_2_0.h>
#include <uavcan/primitive/array/Real32_1_0.h>

// POSIX API.
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

// Standard library.
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <time.h>

/// By default, only the local loopback interface is used.
/// The connectivity can be changed after the node is launched via the register API.
/// LibUDPard natively supports non-redundant, doubly-redundant, and triply-redundant network interfaces
/// for fault tolerance.
#define DEFAULT_IFACE "127.0.0.1"

/// The maximum number of UDP datagrams enqueued in the TX queue at any given time.
#define TX_QUEUE_SIZE 50
/// The Cyphal/UDP specification recommends setting the TTL value of 16 hops.
#define UDP_TTL 16

/// This is used for sizing the memory pools for dynamic memory management.
/// We use a shared pool for both TX queues and for the RX buffers; the edge case is that we can have up to this
/// many items in the TX queue per iface or this many pending RX fragments per iface.
/// Remember that per the LibUDPard design, there is a dedicated TX pipeline per iface and shared RX pipelines for all
/// ifaces.
#define RESOURCE_LIMIT_PAYLOAD_FRAGMENTS ((TX_QUEUE_SIZE * UDPARD_NETWORK_INTERFACE_COUNT_MAX) + 50)
/// Each remote node emitting data on a given port that we are interested in requires us to allocate a small amount
/// of memory to keep certain state associated with that node. This is the maximum number of nodes we can handle.
#define RESOURCE_LIMIT_SESSIONS 1024

#define KILO 1000LL
#define MEGA (KILO * KILO)

typedef uint_least8_t byte_t;

/// Per the LibUDPard design, there is a dedicated TX pipeline per local network iface.
struct TxPipeline
{
    struct UdpardTx udpard_tx;
    int             socket_fd;
};

/// There is one RPC dispatcher in the entire application. It aggregates all RX RPC ports for all network ifaces.
/// The RPC dispatcher cannot be initialized unless the local node has a node-ID.
struct RPCDispatcher
{
    struct UdpardRxRPCDispatcher udpard_rpc_dispatcher;
    int                          socket_fd;
};

struct Publisher
{
    UdpardPortID        subject_id;
    enum UdpardPriority priority;
    UdpardMicrosecond   tx_timeout_usec;
    UdpardTransferID    transfer_id;
};

struct Subscriber
{
    struct UdpardRxSubscription subscription;
    int                         socket_fd[UDPARD_NETWORK_INTERFACE_COUNT_MAX];
};

struct ApplicationRegisters
{
    struct Register              node_id;           ///< uavcan.node.id             : natural16[1]
    struct Register              node_description;  ///< uavcan.node.description    : string
    struct Register              udp_iface;         ///< uavcan.udp.iface           : string
    struct Register              mem_info;          ///< A simple diagnostic register for viewing the memory usage.
    struct PublisherRegisterSet  pub_data;
    struct SubscriberRegisterSet sub_data;
};

struct Application
{
    UdpardMicrosecond started_at;

    /// This flag is raised when the node is requested to restart.
    bool restart_required;

    /// Common LibUDPard states.
    uint_fast8_t         iface_count;
    UdpardNodeID         local_node_id;
    struct TxPipeline    tx_pipeline[UDPARD_NETWORK_INTERFACE_COUNT_MAX];
    struct RPCDispatcher rpc_dispatcher;

    /// The local network interface addresses to use for this node.
    /// All communications are multicast, but multicast sockets need to be bound to a specific local address to
    /// tell the OS which ports to send/receive data via.
    struct in_addr ifaces[UDPARD_NETWORK_INTERFACE_COUNT_MAX];

    /// Publishers.
    struct Publisher pub_heartbeat;
    struct Publisher pub_pnp_node_id_allocation;
    struct Publisher pub_data;

    /// Subscribers.
    struct Subscriber sub_pnp_node_id_allocation;
    struct Subscriber sub_data;

    /// RPC servers.
    struct UdpardRxRPCPort srv_get_node_info;
    struct UdpardRxRPCPort srv_register_list;
    struct UdpardRxRPCPort srv_register_access;

    /// Registers.
    struct Register*            reg_root;  ///< The root of the register tree.
    struct ApplicationRegisters reg;
};

/// A deeply embedded system should sample a microsecond-resolution non-overflowing 64-bit timer.
/// Here is a simple non-blocking implementation as an example:
/// https://github.com/PX4/sapog/blob/601f4580b71c3c4da65cc52237e62a/firmware/src/motor/realtime/motor_timer.c#L233-L274
/// Mind the difference between monotonic time and wall time. Monotonic time never changes rate or makes leaps,
/// it is therefore impossible to synchronize with an external reference. Wall time can be synchronized and therefore
/// it may change rate or make leap adjustments. The two kinds of time serve completely different purposes.
static UdpardMicrosecond getMonotonicMicroseconds(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    {
        abort();
    }
    return (uint64_t) (ts.tv_sec * MEGA + ts.tv_nsec / KILO);
}

/// Returns the 128-bit unique-ID of the local node. This value is used in uavcan.node.GetInfo.Response and during the
/// plug-and-play node-ID allocation by uavcan.pnp.NodeIDAllocationData. The function is infallible.
static void getUniqueID(byte_t out[uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_])
{
    static byte_t uid[uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_];
    static bool   initialized = false;
    if (!initialized)
    {
        initialized = true;
        // A real hardware node would read its unique-ID from some hardware-specific source (typically stored in ROM).
        // This example is a software-only node, so we generate the UID at first launch and store it permanently.
        static const char* const Key  = ".unique_id";
        size_t                   size = uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_;
        if ((!storageGet(Key, &size, uid)) || (size != uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_))
        {
            // Populate the default; it is only used at the first run.
            for (size_t i = 0; i < uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_; i++)
            {
                uid[i] = (byte_t) rand();  // NOLINT
            }
            if (!storagePut(Key, uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_, uid))
            {
                abort();  // The node cannot function if the storage system is not available.
            }
        }
    }
    (void) memcpy(out, uid, sizeof(uid));
}

/// Helpers for emitting transfers over all available interfaces.
static void publish(struct Application* const app,
                    struct Publisher* const   pub,
                    const size_t              payload_size,
                    const void* const         payload)
{
    const UdpardMicrosecond deadline = getMonotonicMicroseconds() + pub->tx_timeout_usec;
    for (size_t i = 0; i < app->iface_count; i++)
    {
        (void) udpardTxPublish(&app->tx_pipeline[i].udpard_tx,
                               deadline,
                               pub->priority,
                               pub->subject_id,
                               &pub->transfer_id,
                               (struct UdpardPayload){.size = payload_size, .data = payload},
                               NULL);
    }
}

/// Invoked every second.
static void handle1HzLoop(struct Application* const app, const UdpardMicrosecond monotonic_time)
{
    const bool anonymous = app->local_node_id > UDPARD_NODE_ID_MAX;
    // Publish heartbeat every second unless the local node is anonymous. Anonymous nodes shall not publish heartbeat.
    if (!anonymous)
    {
        const uavcan_node_Heartbeat_1_0 heartbeat = {.uptime = (uint32_t) ((monotonic_time - app->started_at) / MEGA),
                                                     .mode   = {.value = uavcan_node_Mode_1_0_OPERATIONAL},
                                                     .health = {.value = uavcan_node_Health_1_0_NOMINAL},
                                                     .vendor_specific_status_code = 0};
        uint8_t                         serialized[uavcan_node_Heartbeat_1_0_SERIALIZATION_BUFFER_SIZE_BYTES_];
        size_t                          serialized_size = sizeof(serialized);
        const int8_t err = uavcan_node_Heartbeat_1_0_serialize_(&heartbeat, &serialized[0], &serialized_size);
        assert(err >= 0);
        if (err >= 0)
        {
            publish(app, &app->pub_heartbeat, serialized_size, &serialized[0]);
        }
    }
    else  // If we don't have a node-ID, obtain one by publishing allocation request messages until we get a response.
    {
        // The Specification says that the allocation request publication interval shall be randomized.
        // We implement randomization by calling rand() at fixed intervals and comparing it against some threshold.
        // There are other ways to do it, of course. See the docs in the Specification or in the DSDL definition here:
        // https://github.com/OpenCyphal/public_regulated_data_types/blob/master/uavcan/pnp/8165.NodeIDAllocationData.2.0.dsdl
        // Note that a high-integrity/safety-certified application is unlikely to be able to rely on this feature.
        if (rand() > RAND_MAX / 2)  // NOLINT
        {
            uavcan_pnp_NodeIDAllocationData_2_0 msg = {.node_id = {.value = UINT16_MAX}};
            getUniqueID(msg.unique_id);
            uint8_t      serialized[uavcan_pnp_NodeIDAllocationData_2_0_SERIALIZATION_BUFFER_SIZE_BYTES_];
            size_t       serialized_size = sizeof(serialized);
            const int8_t err = uavcan_pnp_NodeIDAllocationData_2_0_serialize_(&msg, &serialized[0], &serialized_size);
            assert(err >= 0);
            if (err >= 0)
            {
                // The response will arrive asynchronously eventually.
                publish(app, &app->pub_pnp_node_id_allocation, serialized_size, &serialized[0]);
            }
        }
    }
}

/// Invoked every 10 seconds.
static void handle01HzLoop(struct Application* const app, const UdpardMicrosecond monotonic_time)
{
    (void) app;
    (void) monotonic_time;
    // TODO FIXME
}

/// Sets up the instance of UdpardTx and configures the socket for it. Returns negative on error.
static int_fast32_t initTxPipeline(struct TxPipeline* const          self,
                                   const UdpardNodeID* const         local_node_id,
                                   const struct UdpardMemoryResource memory,
                                   const struct in_addr              local_iface)
{
    if (0 != udpardTxInit(&self->udpard_tx, local_node_id, TX_QUEUE_SIZE, memory))
    {
        return -EINVAL;
    }
    // Set up the TX socket for this iface.
    self->socket_fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (self->socket_fd < 0)
    {
        return -errno;
    }
    if (bind(self->socket_fd,
             (struct sockaddr*) &(struct sockaddr_in){.sin_family = AF_INET, .sin_addr = local_iface, .sin_port = 0},
             sizeof(struct sockaddr_in)) != 0)
    {
        return -errno;
    }
    if (fcntl(self->socket_fd, F_SETFL, O_NONBLOCK) != 0)
    {
        return -errno;
    }
    const int ttl = UDP_TTL;
    if (setsockopt(self->socket_fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl)) != 0)
    {
        return -errno;
    }
    if (setsockopt(self->socket_fd, IPPROTO_IP, IP_MULTICAST_IF, &local_iface, sizeof(local_iface)) != 0)
    {
        return -errno;
    }
    return 0;
}

static uavcan_register_Value_1_0 getRegisterSysInfoMem(struct Register* const self)
{
    (void) self;
    uavcan_register_Value_1_0 out = {0};
    uavcan_register_Value_1_0_select_empty_(&out);
    // TODO FIXME populate
    return out;
}

/// Enters all registers into the tree and initializes their default value.
/// The next step after this is to load the values from the non-volatile storage.
static void initRegisters(struct ApplicationRegisters* const reg, struct Register** const root)
{
    registerInit(&reg->node_id, root, (const char*[]){"uavcan", "node", "id", NULL});
    uavcan_register_Value_1_0_select_natural16_(&reg->node_id.value);
    reg->node_id.value.natural16.value.count       = 1;
    reg->node_id.value.natural16.value.elements[0] = UDPARD_NODE_ID_UNSET;
    reg->node_id.persistent                        = true;
    reg->node_id.remote_mutable                    = true;

    registerInit(&reg->node_description, root, (const char*[]){"uavcan", "node", "description", NULL});
    uavcan_register_Value_1_0_select_string_(&reg->node_description.value);  // Empty by default.
    reg->node_description.persistent     = true;
    reg->node_description.remote_mutable = true;

    registerInit(&reg->udp_iface, root, (const char*[]){"uavcan", "udp", "iface", NULL});
    uavcan_register_Value_1_0_select_string_(&reg->udp_iface.value);
    reg->udp_iface.persistent                = true;
    reg->udp_iface.remote_mutable            = true;
    reg->udp_iface.value._string.value.count = strlen(DEFAULT_IFACE);
    (void) memcpy(&reg->udp_iface.value._string.value.elements[0],
                  DEFAULT_IFACE,
                  reg->udp_iface.value._string.value.count);

    registerInit(&reg->mem_info, root, (const char*[]){"sys", "info", "mem", NULL});
    reg->mem_info.getter = &getRegisterSysInfoMem;

    registerInitPublisher(&reg->pub_data, root, "data", uavcan_primitive_array_Real32_1_0_FULL_NAME_AND_VERSION_);

    registerInitSubscriber(&reg->sub_data, root, "data", uavcan_primitive_array_Real32_1_0_FULL_NAME_AND_VERSION_);
}

/// Parse the addresses of the available local network interfaces from the given string.
/// In a deeply embedded system this may be replaced by some other networking APIs, like LwIP.
/// Invalid interface addresses are ignored; i.e., this is a best-effort parser.
/// Returns the number of valid ifaces found (which may be zero).
static uint_fast8_t parseNetworkIfaceAddresses(const uavcan_primitive_String_1_0* const in,
                                               struct in_addr out[UDPARD_NETWORK_INTERFACE_COUNT_MAX])
{
    uint_fast8_t count = 0;
    char         buf_z[uavcan_primitive_String_1_0_value_ARRAY_CAPACITY_ + 1];
    size_t       offset = 0;
    assert(in->value.count <= sizeof(buf_z));
    while ((offset < in->value.count) && (count < UDPARD_NETWORK_INTERFACE_COUNT_MAX))
    {
        // Copy chars from "in" into "buf_z" one by one until a whitespace character is found.
        size_t sz = 0;
        while ((offset < in->value.count) && (sz < (sizeof(buf_z) - 1)))
        {
            const char c = (char) in->value.elements[offset++];
            if ((c == ' ') || (c == '\t') || (c == '\r') || (c == '\n'))
            {
                break;
            }
            buf_z[sz++] = c;
        }
        buf_z[sz] = '\0';
        if (sz > 0)
        {
            const int      so    = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);  // Test the iface.
            struct in_addr iface = {0};
            if ((so >= 0) && (inet_pton(AF_INET, buf_z, &iface) == 1) &&
                (bind(so,
                      (struct sockaddr*) &(struct sockaddr_in){.sin_family = AF_INET, .sin_addr = iface, .sin_port = 0},
                      sizeof(struct sockaddr_in)) != 0) &&
                (setsockopt(so, IPPROTO_IP, IP_MULTICAST_IF, &iface, sizeof(struct in_addr)) != 0))
            {
                out[count++] = iface;
            }
            if (so >= 0)
            {
                (void) close(so);
            }
        }
    }
    return count;
}

/// This is designed for use with registerTraverse.
/// The context points to a size_t containing the number of registers loaded.
static void* regLoad(struct Register* const self, void* const context)
{
    assert((self != NULL) && (context != NULL));
    byte_t serialized[uavcan_register_Value_1_0_EXTENT_BYTES_];
    size_t sr_size = uavcan_register_Value_1_0_EXTENT_BYTES_;
    // Ignore non-persistent registers and those whose values are computed dynamically (can't be stored).
    // If the entry is not found or the stored value is invalid, the default value will be used.
    if (self->persistent && (self->getter == NULL) && storageGet(self->name, &sr_size, &serialized[0]) &&
        (uavcan_register_Value_1_0_deserialize_(&self->value, &serialized[0], &sr_size) >= 0))
    {
        ++(*(size_t*) context);
    }
    return NULL;
}

/// This is designed for use with registerTraverse.
/// The context points to a size_t containing the number of registers that could not be stored.
static void* regStore(struct Register* const self, void* const context)
{
    assert((self != NULL) && (context != NULL));
    if (self->persistent && self->remote_mutable)
    {
        byte_t     serialized[uavcan_register_Value_1_0_EXTENT_BYTES_];
        size_t     sr_size = uavcan_register_Value_1_0_EXTENT_BYTES_;
        const bool ok      = (uavcan_register_Value_1_0_serialize_(&self->value, serialized, &sr_size) >= 0) &&
                        storagePut(self->name, sr_size, &serialized[0]);
        if (!ok)
        {
            ++(*(size_t*) context);
        }
    }
    return NULL;
}

/// This is needed to implement node restarting. Remove this if running on an embedded system.
extern char** environ;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

int main(const int argc, char* const argv[])
{
    struct Application app = {.iface_count = 0, .local_node_id = UDPARD_NODE_ID_UNSET};
    // The first thing to do during the application initialization is to load the register values from the non-volatile
    // configuration storage. Non-volatile configuration is essential for most Cyphal nodes because it contains
    // information on how to reach the network and how to publish/subscribe to the subjects of interest.
    initRegisters(&app.reg, &app.reg_root);
    {
        size_t load_count = 0;
        (void) registerTraverse(app.reg_root, &regLoad, &load_count);
        (void) fprintf(stderr, "%zu registers loaded from the non-volatile storage\n", load_count);
    }
    // If we're running on a POSIX system, we can use the environment variables to override the loaded values.
    // There is a standard mapping between environment variable names and register names documented in the DSDL
    // definition of uavcan.register.Access; for example, "uavcan.node.id" --> "UAVCAN__NODE__ID".
    // This simple feature is left as an exercise to the reader. It is meaningless in a deeply embedded system though.
    //
    //  registerTraverse(app.reg, &regOverrideFromEnvironmentVariables, NULL);

    // Parse the iface addresses given via the standard iface register.
    app.iface_count = parseNetworkIfaceAddresses(&app.reg.udp_iface.value._string, &app.ifaces[0]);
    if (app.iface_count == 0)  // In case of error we fall back to the local loopback to keep the node reachable.
    {
        (void) fprintf(stderr, "Using the loopback iface because the iface register does not specify valid ifaces\n");
        app.iface_count = 1;
        app.ifaces[0]   = (struct in_addr){.s_addr = htonl(INADDR_LOOPBACK)};
    }

    // The block size values used here are derived from the sizes of the structs defined in LibUDPard and the MTU.
    // They may change when migrating between different versions of the library or when building the code for a
    // different platform, so it may be desirable to choose conservative values here (i.e. larger than necessary).
    MEMORY_BLOCK_ALLOCATOR_DEFINE(mem_session, 400, RESOURCE_LIMIT_SESSIONS);
    MEMORY_BLOCK_ALLOCATOR_DEFINE(mem_fragment, 88, RESOURCE_LIMIT_PAYLOAD_FRAGMENTS);
    MEMORY_BLOCK_ALLOCATOR_DEFINE(mem_payload, 2048, RESOURCE_LIMIT_PAYLOAD_FRAGMENTS);

    // Initialize the TX pipelines. We have one per local iface (unlike the RX pipelines which are shared).
    for (size_t i = 0; i < app.iface_count; i++)
    {
        const int_fast32_t result =
            initTxPipeline(&app.tx_pipeline[i],
                           &app.local_node_id,
                           (struct UdpardMemoryResource){.user_reference = &mem_payload,  // Shared pool.
                                                         .allocate       = &memoryBlockAllocate,
                                                         .deallocate     = &memoryBlockDeallocate},
                           app.ifaces[i]);
        if (result < 0)
        {
            (void) fprintf(stderr, "Failed to initialize TX pipeline for iface %zu: %li\n", i, -result);
            return 1;
        }
    }
    // Initialize the local node-ID. The register value may change at runtime; we don't want the change to take
    // effect until the node is restarted, so we initialize the local node-ID only once at startup.
    app.local_node_id = app.reg.node_id.value.natural16.value.elements[0];
    // Initialize the publishers. They are not dependent on the local node-ID value.
    // Heartbeat.
    app.pub_heartbeat.priority        = UdpardPriorityNominal;
    app.pub_heartbeat.subject_id      = uavcan_node_Heartbeat_1_0_FIXED_PORT_ID_;
    app.pub_heartbeat.tx_timeout_usec = 1 * MEGA;
    // PnP node-ID allocation.
    app.pub_pnp_node_id_allocation.priority        = UdpardPrioritySlow;
    app.pub_pnp_node_id_allocation.subject_id      = uavcan_pnp_NodeIDAllocationData_2_0_FIXED_PORT_ID_;
    app.pub_pnp_node_id_allocation.tx_timeout_usec = 1 * MEGA;
    // Data. This is not a fixed port-ID register, we load the values from the registers.
    app.pub_data.priority   = (app.reg.pub_data.priority.value.natural8.value.elements[0] <= UDPARD_PRIORITY_MAX)  //
                                  ? ((enum UdpardPriority) app.reg.pub_data.priority.value.natural8.value.elements[0])
                                  : UdpardPriorityOptional;
    app.pub_data.subject_id = app.reg.pub_data.base.id.value.natural16.value.elements[0];
    app.pub_data.tx_timeout_usec = 50 * KILO;

    // TODO
    (void) mem_session;
    (void) mem_fragment;

    // Main loop.
    app.started_at                       = getMonotonicMicroseconds();
    UdpardMicrosecond next_1_hz_iter_at  = app.started_at + MEGA;
    UdpardMicrosecond next_01_hz_iter_at = app.started_at + (MEGA * 10);
    while (!app.restart_required)
    {
        // Run a trivial scheduler polling the loops that run the business logic.
        const UdpardMicrosecond monotonic_time = getMonotonicMicroseconds();
        if (monotonic_time >= next_1_hz_iter_at)
        {
            next_1_hz_iter_at += MEGA;
            handle1HzLoop(&app, monotonic_time);
        }
        if (monotonic_time >= next_01_hz_iter_at)
        {
            next_01_hz_iter_at += (MEGA * 10);
            handle01HzLoop(&app, monotonic_time);
        }

        // Transmit pending frames from the prioritized TX queues managed by libudpard.
        for (size_t i = 0; i < app.iface_count; i++)
        {
            struct TxPipeline* const   pipe = &app.tx_pipeline[i];
            const struct UdpardTxItem* tqi  = udpardTxPeek(&pipe->udpard_tx);  // Find the highest-priority datagram.
            while (tqi != NULL)
            {
                // Attempt transmission only if the frame is not yet timed out while waiting in the TX queue.
                // Otherwise, just drop it and move on to the next one.
                if ((tqi->deadline_usec == 0) || (tqi->deadline_usec > monotonic_time))
                {
                    // A real-time embedded system should also enforce the transmission deadline here.
                    const ssize_t send_res = sendto(pipe->socket_fd,
                                                    tqi->datagram_payload.data,
                                                    tqi->datagram_payload.size,
                                                    MSG_DONTWAIT,
                                                    (struct sockaddr*) &(struct sockaddr_in){
                                                        .sin_family = AF_INET,
                                                        .sin_addr   = {htonl(tqi->destination.ip_address)},
                                                        .sin_port   = htons(tqi->destination.udp_port),
                                                    },
                                                    sizeof(struct sockaddr_in));
                    if (send_res < 0)
                    {
                        if ((errno == EAGAIN) || (errno == EWOULDBLOCK))
                        {
                            break;  // No more space in the TX buffer, try again later.
                        }
                        (void) fprintf(stderr, "Iface #%zu send error: %i [%s]\n", i, errno, strerror(errno));
                        // Datagram will be discarded.
                    }
                }
                udpardTxFree(pipe->udpard_tx.memory, udpardTxPop(&pipe->udpard_tx, tqi));
                tqi = udpardTxPeek(&pipe->udpard_tx);
            }
        }

        // Receive pending frames from the RX sockets of all network interfaces and feed them into the library.
        // Unblock early if TX sockets become writable and the TX queues are not empty.
        // FIXME TODO
        usleep(1000);
    }

    // Save registers immediately before restarting the node.
    // We don't access the storage during normal operation of the node because access is slow and is impossible to
    // perform without blocking; it also introduces undesirable complexities and complicates the failure modes.
    {
        size_t store_errors = 0;
        (void) registerTraverse(app.reg_root, &regStore, &store_errors);
        if (store_errors > 0)
        {
            (void) fprintf(stderr, "%zu registers could not be stored\n", store_errors);
        }
    }

    // It is recommended to postpone restart until all frames are sent though.
    (void) argc;
    puts("RESTART ");
    return -execve(argv[0], argv, environ);
}
