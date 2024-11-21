///                            ____                   ______            __          __
///                           / __ `____  ___  ____  / ____/_  ______  / /_  ____  / /
///                          / / / / __ `/ _ `/ __ `/ /   / / / / __ `/ __ `/ __ `/ /
///                         / /_/ / /_/ /  __/ / / / /___/ /_/ / /_/ / / / / /_/ / /
///                         `____/ .___/`___/_/ /_/`____/`__, / .___/_/ /_/`__,_/_/
///                             /_/                     /____/_/
///
/// A demo application showcasing the implementation of a UDRAL servo network service.
/// This application is intended to run on GNU/Linux but it is trivially adaptable to baremetal environments.
/// Please refer to the enclosed README for details.
///
/// This software is distributed under the terms of the MIT License.
/// Copyright (C) 2021 OpenCyphal <maintainers@opencyphal.org>
/// Author: Pavel Kirienko <pavel@opencyphal.org>

#include "socketcan.h"
#include "register.h"
#include <o1heap.h>

#include <uavcan/node/Heartbeat_1_0.h>
#include <uavcan/node/GetInfo_1_0.h>
#include <uavcan/node/ExecuteCommand_1_1.h>
#include <uavcan/node/port/List_0_1.h>
#include <uavcan/_register/Access_1_0.h>
#include <uavcan/_register/List_1_0.h>
#include <uavcan/pnp/NodeIDAllocationData_2_0.h>

#include <reg/udral/service/common/Readiness_0_1.h>
#include <reg/udral/service/actuator/common/_0_1.h>
#include <reg/udral/service/actuator/common/Feedback_0_1.h>
#include <reg/udral/service/actuator/common/Status_0_1.h>
#include <reg/udral/physics/dynamics/translation/LinearTs_0_1.h>
#include <reg/udral/physics/electricity/PowerTs_0_1.h>

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <unistd.h>

#define KILO 1000L
#define MEGA ((int64_t) KILO * KILO)

#define CAN_REDUNDANCY_FACTOR 1
/// For CAN FD the queue can be smaller.
#define CAN_TX_QUEUE_CAPACITY 100

/// We keep the state of the application here. Feel free to use static variables instead if desired.
typedef struct State
{
    CanardMicrosecond started_at;

    O1HeapInstance* heap;
    CanardInstance  canard;
    CanardTxQueue   canard_tx_queues[CAN_REDUNDANCY_FACTOR];

    /// The state of the business logic.
    struct
    {
        /// Whether the servo is supposed to actuate the load or to stay idle (safe low-power mode).
        struct
        {
            bool              armed;
            CanardMicrosecond last_update_at;
        } arming;

        /// Setpoint & motion profile (unsupported constraints are to be ignored).
        /// https://github.com/OpenCyphal/public_regulated_data_types/blob/master/reg/udral/service/actuator/servo/_.0.1.dsdl
        /// As described in the linked documentation, there are two kinds of servos supported: linear and rotary.
        /// Units per-kind are:   LINEAR                 ROTARY
        float position;      ///< [meter]                [radian]
        float velocity;      ///< [meter/second]         [radian/second]
        float acceleration;  ///< [(meter/second)^2]     [(radian/second)^2]
        float force;         ///< [newton]               [netwon*meter]
    } servo;

    /// These values are read from the registers at startup. You can also implement hot reloading if desired.
    /// The subjects of the servo network service are defined in the UDRAL data type definitions here:
    /// https://github.com/OpenCyphal/public_regulated_data_types/blob/master/reg/udral/service/actuator/servo/_.0.1.dsdl
    struct
    {
        struct
        {
            CanardPortID servo_feedback;  //< reg.udral.service.actuator.common.Feedback
            CanardPortID servo_status;    //< reg.udral.service.actuator.common.Status
            CanardPortID servo_power;     //< reg.udral.physics.electricity.PowerTs
            CanardPortID servo_dynamics;  //< (timestamped dynamics)
        } pub;
        struct
        {
            CanardPortID servo_setpoint;   //< (non-timestamped dynamics)
            CanardPortID servo_readiness;  //< reg.udral.service.common.Readiness
        } sub;
    } port_id;

    /// A transfer-ID is an integer that is incremented whenever a new message is published on a given subject.
    /// It is used by the protocol for deduplication, message loss detection, and other critical things.
    /// For CAN, each value can be of type uint8_t, but we use larger types for genericity and for statistical purposes,
    /// as large values naturally contain the number of times each subject was published to.
    struct
    {
        uint64_t uavcan_node_heartbeat;
        uint64_t uavcan_node_port_list;
        uint64_t uavcan_pnp_allocation;
        // Messages published synchronously can share the same transfer-ID:
        uint64_t servo_fast_loop;
        uint64_t servo_1Hz_loop;
    } next_transfer_id;
} State;

/// This flag is raised when the node is requested to restart.
static volatile bool g_restart_required = false;

/// A deeply embedded system should sample a microsecond-resolution non-overflowing 64-bit timer.
/// Here is a simple non-blocking implementation as an example:
/// https://github.com/PX4/sapog/blob/601f4580b71c3c4da65cc52237e62a/firmware/src/motor/realtime/motor_timer.c#L233-L274
/// Mind the difference between monotonic time and wall time. Monotonic time never changes rate or makes leaps,
/// it is therefore impossible to synchronize with an external reference. Wall time can be synchronized and therefore
/// it may change rate or make leap adjustments. The two kinds of time serve completely different purposes.
static CanardMicrosecond getMonotonicMicroseconds()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    {
        abort();
    }
    return (uint64_t) (ts.tv_sec * 1000000 + ts.tv_nsec / 1000);
}

// Returns the 128-bit unique-ID of the local node. This value is used in uavcan.node.GetInfo.Response and during the
// plug-and-play node-ID allocation by uavcan.pnp.NodeIDAllocationData. The function is infallible.
static void getUniqueID(uint8_t out[uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_])
{
    // A real hardware node would read its unique-ID from some hardware-specific source (typically stored in ROM).
    // This example is a software-only node, so we store the unique-ID in a (read-only) register instead.
    uavcan_register_Value_1_0 value = {0};
    uavcan_register_Value_1_0_select_unstructured_(&value);
    // Populate the default; it is only used at the first run if there is no such register.
    for (uint8_t i = 0; i < uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_; i++)
    {
        value.unstructured.value.elements[value.unstructured.value.count++] = (uint8_t) rand();  // NOLINT
    }
    registerRead("uavcan.node.unique_id", &value);
    assert(uavcan_register_Value_1_0_is_unstructured_(&value) &&
           value.unstructured.value.count == uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_);
    memcpy(&out[0], &value.unstructured.value, uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_);
}

typedef enum SubjectRole
{
    SUBJECT_ROLE_PUBLISHER,
    SUBJECT_ROLE_SUBSCRIBER,
} SubjectRole;

/// Reads the port-ID from the corresponding standard register. The standard register schema is documented in
/// the Cyphal Specification, section for the standard service uavcan.register.Access. You can also find it here:
/// https://github.com/OpenCyphal/public_regulated_data_types/blob/master/uavcan/register/384.Access.1.0.dsdl
/// A very hands-on demo is available in Python: https://pycyphal.readthedocs.io/en/stable/pages/demo.html
static CanardPortID getSubjectID(const SubjectRole role, const char* const port_name, const char* const type_name)
{
    // Deduce the register name from port name.
    const char* const role_name = (role == SUBJECT_ROLE_PUBLISHER) ? "pub" : "sub";
    char              register_name[uavcan_register_Name_1_0_name_ARRAY_CAPACITY_] = {0};
    snprintf(&register_name[0], sizeof(register_name), "uavcan.%s.%s.id", role_name, port_name);

    // Set up the default value. It will be used to populate the register if it doesn't exist.
    uavcan_register_Value_1_0 val = {0};
    uavcan_register_Value_1_0_select_natural16_(&val);
    val.natural16.value.count       = 1;
    val.natural16.value.elements[0] = UINT16_MAX;  // This means "undefined", per Specification, which is the default.

    // Read the register with defensive self-checks.
    registerRead(&register_name[0], &val);
    assert(uavcan_register_Value_1_0_is_natural16_(&val) && (val.natural16.value.count == 1));
    const uint16_t result = val.natural16.value.elements[0];

    // This part is NOT required but recommended by the Specification for enhanced introspection capabilities. It is
    // very cheap to implement so all implementations should do so. This register simply contains the name of the
    // type exposed at this port. It should be immutable, but it is not strictly required so in this implementation
    // we take shortcuts by making it mutable since it's behaviorally simpler in this specific case.
    snprintf(&register_name[0], sizeof(register_name), "uavcan.%s.%s.type", role_name, port_name);
    uavcan_register_Value_1_0_select_string_(&val);
    val._string.value.count = nunavutChooseMin(strlen(type_name), uavcan_primitive_String_1_0_value_ARRAY_CAPACITY_);
    memcpy(&val._string.value.elements[0], type_name, val._string.value.count);
    registerWrite(&register_name[0], &val);  // Unconditionally overwrite existing value because it's read-only.

    return result;
}

static void send(State* const                        state,
                 const CanardMicrosecond             tx_deadline_usec,
                 const CanardTransferMetadata* const metadata,
                 const size_t                        payload_size,
                 const void* const                   payload_data)
{
    for (uint8_t ifidx = 0; ifidx < CAN_REDUNDANCY_FACTOR; ifidx++)
    {
        const struct CanardPayload payload = {.size = payload_size, .data = payload_data};
        (void) canardTxPush(&state->canard_tx_queues[ifidx],
                            &state->canard,
                            tx_deadline_usec,
                            metadata,
                            payload);
    }
}

static void sendResponse(State* const                  state,
                         const CanardRxTransfer* const original_request_transfer,
                         const size_t                  payload_size,
                         const void* const             payload_data)
{
    CanardTransferMetadata meta = original_request_transfer->metadata;
    meta.transfer_kind          = CanardTransferKindResponse;
    send(state, original_request_transfer->timestamp_usec + MEGA, &meta, payload_size, payload_data);
}

/// Invoked at the rate of the fastest loop.
static void handleFastLoop(State* const state, const CanardMicrosecond monotonic_time)
{
    // Apply control inputs if armed.
    if (state->servo.arming.armed)
    {
        fprintf(stderr,
                "\rp=%.3f m    v=%.3f m/s    a=%.3f (m/s)^2    F=%.3f N    \r",
                (double) state->servo.position,
                (double) state->servo.velocity,
                (double) state->servo.acceleration,
                (double) state->servo.force);
    }
    else
    {
        fprintf(stderr, "\rDISARMED    \r");
    }

    const bool     anonymous         = state->canard.node_id > CANARD_NODE_ID_MAX;
    const uint64_t servo_transfer_id = state->next_transfer_id.servo_fast_loop++;

    // Publish feedback if the subject is enabled and the node is non-anonymous.
    if (!anonymous && (state->port_id.pub.servo_feedback <= CANARD_SUBJECT_ID_MAX))
    {
        reg_udral_service_actuator_common_Feedback_0_1 msg = {0};
        msg.heartbeat.readiness.value = state->servo.arming.armed ? reg_udral_service_common_Readiness_0_1_ENGAGED
                                                                  : reg_udral_service_common_Readiness_0_1_STANDBY;
        // If there are any hardware or configuration issues, report them here:
        msg.heartbeat.health.value = uavcan_node_Health_1_0_NOMINAL;
        // Serialize and publish the message:
        uint8_t      serialized[reg_udral_service_actuator_common_Feedback_0_1_SERIALIZATION_BUFFER_SIZE_BYTES_];
        size_t       serialized_size = sizeof(serialized);
        const int8_t err =
            reg_udral_service_actuator_common_Feedback_0_1_serialize_(&msg, &serialized[0], &serialized_size);
        assert(err >= 0);
        if (err >= 0)
        {
            const CanardTransferMetadata transfer = {
                .priority       = CanardPriorityHigh,
                .transfer_kind  = CanardTransferKindMessage,
                .port_id        = state->port_id.pub.servo_feedback,
                .remote_node_id = CANARD_NODE_ID_UNSET,
                .transfer_id    = (CanardTransferID) servo_transfer_id,
            };
            send(state, monotonic_time + 10 * KILO, &transfer, serialized_size, &serialized[0]);
        }
    }

    // Publish dynamics if the subject is enabled and the node is non-anonymous.
    if (!anonymous && (state->port_id.pub.servo_dynamics <= CANARD_SUBJECT_ID_MAX))
    {
        reg_udral_physics_dynamics_translation_LinearTs_0_1 msg = {0};
        // Our node does not synchronize its clock with the network, so we cannot timestamp our publications:
        msg.timestamp.microsecond = uavcan_time_SynchronizedTimestamp_1_0_UNKNOWN;
        // A real application would source these values from the hardware; we republish the setpoint for demo purposes.
        // TODO populate real values:
        msg.value.kinematics.position.meter                           = state->servo.position;
        msg.value.kinematics.velocity.meter_per_second                = state->servo.velocity;
        msg.value.kinematics.acceleration.meter_per_second_per_second = state->servo.acceleration;
        msg.value.force.newton                                        = state->servo.force;
        // Serialize and publish the message:
        uint8_t      serialized[reg_udral_physics_dynamics_translation_LinearTs_0_1_SERIALIZATION_BUFFER_SIZE_BYTES_];
        size_t       serialized_size = sizeof(serialized);
        const int8_t err =
            reg_udral_physics_dynamics_translation_LinearTs_0_1_serialize_(&msg, &serialized[0], &serialized_size);
        assert(err >= 0);
        if (err >= 0)
        {
            const CanardTransferMetadata transfer = {
                .priority       = CanardPriorityHigh,
                .transfer_kind  = CanardTransferKindMessage,
                .port_id        = state->port_id.pub.servo_dynamics,
                .remote_node_id = CANARD_NODE_ID_UNSET,
                .transfer_id    = (CanardTransferID) servo_transfer_id,
            };
            send(state, monotonic_time + 10 * KILO, &transfer, serialized_size, &serialized[0]);
        }
    }

    // Publish power if the subject is enabled and the node is non-anonymous.
    if (!anonymous && (state->port_id.pub.servo_power <= CANARD_SUBJECT_ID_MAX))
    {
        reg_udral_physics_electricity_PowerTs_0_1 msg = {0};
        // Our node does not synchronize its clock with the network, so we cannot timestamp our publications:
        msg.timestamp.microsecond = uavcan_time_SynchronizedTimestamp_1_0_UNKNOWN;
        // TODO populate real values:
        msg.value.current.ampere = 20.315F;
        msg.value.voltage.volt   = 51.3F;
        // Serialize and publish the message:
        uint8_t      serialized[reg_udral_physics_dynamics_translation_LinearTs_0_1_SERIALIZATION_BUFFER_SIZE_BYTES_];
        size_t       serialized_size = sizeof(serialized);
        const int8_t err = reg_udral_physics_electricity_PowerTs_0_1_serialize_(&msg, &serialized[0], &serialized_size);
        assert(err >= 0);
        if (err >= 0)
        {
            const CanardTransferMetadata transfer = {
                .priority       = CanardPriorityHigh,
                .transfer_kind  = CanardTransferKindMessage,
                .port_id        = state->port_id.pub.servo_power,
                .remote_node_id = CANARD_NODE_ID_UNSET,
                .transfer_id    = (CanardTransferID) servo_transfer_id,
            };
            send(state, monotonic_time + 10 * KILO, &transfer, serialized_size, &serialized[0]);
        }
    }
}

/// Invoked every second.
static void handle1HzLoop(State* const state, const CanardMicrosecond monotonic_time)
{
    const bool anonymous = state->canard.node_id > CANARD_NODE_ID_MAX;
    // Publish heartbeat every second unless the local node is anonymous. Anonymous nodes shall not publish heartbeat.
    if (!anonymous)
    {
        uavcan_node_Heartbeat_1_0 heartbeat = {0};
        heartbeat.uptime                    = (uint32_t) ((monotonic_time - state->started_at) / MEGA);
        heartbeat.mode.value                = uavcan_node_Mode_1_0_OPERATIONAL;
        const O1HeapDiagnostics heap_diag   = o1heapGetDiagnostics(state->heap);
        if (heap_diag.oom_count > 0)
        {
            heartbeat.health.value = uavcan_node_Health_1_0_CAUTION;
        }
        else
        {
            heartbeat.health.value = uavcan_node_Health_1_0_NOMINAL;
        }

        uint8_t      serialized[uavcan_node_Heartbeat_1_0_SERIALIZATION_BUFFER_SIZE_BYTES_];
        size_t       serialized_size = sizeof(serialized);
        const int8_t err = uavcan_node_Heartbeat_1_0_serialize_(&heartbeat, &serialized[0], &serialized_size);
        assert(err >= 0);
        if (err >= 0)
        {
            const CanardTransferMetadata transfer = {
                .priority       = CanardPriorityNominal,
                .transfer_kind  = CanardTransferKindMessage,
                .port_id        = uavcan_node_Heartbeat_1_0_FIXED_PORT_ID_,
                .remote_node_id = CANARD_NODE_ID_UNSET,
                .transfer_id    = (CanardTransferID) (state->next_transfer_id.uavcan_node_heartbeat++),
            };
            send(state,
                 monotonic_time + MEGA,  // Set transmission deadline 1 second, optimal for heartbeat.
                 &transfer,
                 serialized_size,
                 &serialized[0]);
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
            // Note that this will only work over CAN FD. If you need to run PnP over Classic CAN, use message v1.0.
            uavcan_pnp_NodeIDAllocationData_2_0 msg = {0};
            msg.node_id.value                       = UINT16_MAX;
            getUniqueID(msg.unique_id);
            uint8_t      serialized[uavcan_pnp_NodeIDAllocationData_2_0_SERIALIZATION_BUFFER_SIZE_BYTES_];
            size_t       serialized_size = sizeof(serialized);
            const int8_t err = uavcan_pnp_NodeIDAllocationData_2_0_serialize_(&msg, &serialized[0], &serialized_size);
            assert(err >= 0);
            if (err >= 0)
            {
                const CanardTransferMetadata transfer = {
                    .priority       = CanardPrioritySlow,
                    .transfer_kind  = CanardTransferKindMessage,
                    .port_id        = uavcan_pnp_NodeIDAllocationData_2_0_FIXED_PORT_ID_,
                    .remote_node_id = CANARD_NODE_ID_UNSET,
                    .transfer_id    = (CanardTransferID) (state->next_transfer_id.uavcan_pnp_allocation++),
                };
                send(state,  // The response will arrive asynchronously eventually.
                     monotonic_time + MEGA,
                     &transfer,
                     serialized_size,
                     &serialized[0]);
            }
        }
    }

    const uint64_t servo_transfer_id = state->next_transfer_id.servo_1Hz_loop++;

    if (!anonymous)
    {
        // Publish the servo status -- this is a low-rate message with low-severity diagnostics.
        reg_udral_service_actuator_common_Status_0_1 msg = {0};
        // TODO: POPULATE THE MESSAGE: temperature, errors, etc.
        uint8_t      serialized[reg_udral_service_actuator_common_Status_0_1_SERIALIZATION_BUFFER_SIZE_BYTES_];
        size_t       serialized_size = sizeof(serialized);
        const int8_t err =
            reg_udral_service_actuator_common_Status_0_1_serialize_(&msg, &serialized[0], &serialized_size);
        assert(err >= 0);
        if (err >= 0)
        {
            const CanardTransferMetadata transfer = {
                .priority       = CanardPriorityNominal,
                .transfer_kind  = CanardTransferKindMessage,
                .port_id        = state->port_id.pub.servo_status,
                .remote_node_id = CANARD_NODE_ID_UNSET,
                .transfer_id    = (CanardTransferID) servo_transfer_id,
            };
            send(state, monotonic_time + MEGA, &transfer, serialized_size, &serialized[0]);
        }
    }

    // Disarm automatically if the arming subject has not been updated in a while.
    if (state->servo.arming.armed && ((monotonic_time - state->servo.arming.last_update_at) >
                                      (uint64_t) (reg_udral_service_actuator_common___0_1_CONTROL_TIMEOUT * MEGA)))
    {
        state->servo.arming.armed = false;
        puts("Disarmed by timeout ");
    }
}

/// This is needed only for constructing uavcan_node_port_List_0_1.
static void fillSubscriptions(const CanardTreeNode* const               tree,  // NOLINT(misc-no-recursion)
                              uavcan_node_port_SubjectIDList_0_1* const obj)
{
    if (NULL != tree)
    {
        fillSubscriptions(tree->lr[0], obj);
        const CanardRxSubscription* crs = (const CanardRxSubscription*) tree;
        assert(crs->port_id <= CANARD_SUBJECT_ID_MAX);
        assert(obj->sparse_list.count < uavcan_node_port_SubjectIDList_0_1_sparse_list_ARRAY_CAPACITY_);
        obj->sparse_list.elements[obj->sparse_list.count++].value = crs->port_id;
        fillSubscriptions(tree->lr[1], obj);
    }
}

/// This is needed only for constructing uavcan_node_port_List_0_1.
static void fillServers(const CanardTreeNode* const               tree,  // NOLINT(misc-no-recursion)
                        uavcan_node_port_ServiceIDList_0_1* const obj)
{
    if (NULL != tree)
    {
        fillServers(tree->lr[0], obj);
        const CanardRxSubscription* crs = (const CanardRxSubscription*) tree;
        assert(crs->port_id <= CANARD_SERVICE_ID_MAX);
        (void) nunavutSetBit(&obj->mask_bitpacked_[0], sizeof(obj->mask_bitpacked_), crs->port_id, true);
        fillServers(tree->lr[1], obj);
    }
}

/// Invoked every 10 seconds.
static void handle01HzLoop(State* const state, const CanardMicrosecond monotonic_time)
{
    // Publish the recommended (not required) port introspection message. No point publishing it if we're anonymous.
    // The message is a bit heavy on the stack (about 2 KiB) but this is not a problem for a modern MCU.
    if (state->canard.node_id <= CANARD_NODE_ID_MAX)
    {
        uavcan_node_port_List_0_1 m = {0};
        uavcan_node_port_List_0_1_initialize_(&m);
        uavcan_node_port_SubjectIDList_0_1_select_sparse_list_(&m.publishers);
        uavcan_node_port_SubjectIDList_0_1_select_sparse_list_(&m.subscribers);

        // Indicate which subjects we publish to. Don't forget to keep this updated if you add new publications!
        {
            size_t* const cnt                                 = &m.publishers.sparse_list.count;
            m.publishers.sparse_list.elements[(*cnt)++].value = uavcan_node_Heartbeat_1_0_FIXED_PORT_ID_;
            m.publishers.sparse_list.elements[(*cnt)++].value = uavcan_node_port_List_0_1_FIXED_PORT_ID_;
            if (state->port_id.pub.servo_feedback <= CANARD_SUBJECT_ID_MAX)
            {
                m.publishers.sparse_list.elements[(*cnt)++].value = state->port_id.pub.servo_feedback;
            }
            if (state->port_id.pub.servo_status <= CANARD_SUBJECT_ID_MAX)
            {
                m.publishers.sparse_list.elements[(*cnt)++].value = state->port_id.pub.servo_status;
            }
            if (state->port_id.pub.servo_power <= CANARD_SUBJECT_ID_MAX)
            {
                m.publishers.sparse_list.elements[(*cnt)++].value = state->port_id.pub.servo_power;
            }
            if (state->port_id.pub.servo_dynamics <= CANARD_SUBJECT_ID_MAX)
            {
                m.publishers.sparse_list.elements[(*cnt)++].value = state->port_id.pub.servo_dynamics;
            }
        }

        // Indicate which servers and subscribers we implement.
        // We could construct the list manually but it's easier and more robust to just query libcanard for that.
        fillSubscriptions(state->canard.rx_subscriptions[CanardTransferKindMessage], &m.subscribers);
        fillServers(state->canard.rx_subscriptions[CanardTransferKindRequest], &m.servers);
        fillServers(state->canard.rx_subscriptions[CanardTransferKindResponse], &m.clients);  // For regularity.

        // Serialize and publish the message. Use a smaller buffer if you know that message is always small.
        uint8_t serialized[uavcan_node_port_List_0_1_SERIALIZATION_BUFFER_SIZE_BYTES_];
        size_t  serialized_size = uavcan_node_port_List_0_1_SERIALIZATION_BUFFER_SIZE_BYTES_;
        if (uavcan_node_port_List_0_1_serialize_(&m, &serialized[0], &serialized_size) >= 0)
        {
            const CanardTransferMetadata transfer = {
                .priority       = CanardPriorityOptional,  // Mind the priority.
                .transfer_kind  = CanardTransferKindMessage,
                .port_id        = uavcan_node_port_List_0_1_FIXED_PORT_ID_,
                .remote_node_id = CANARD_NODE_ID_UNSET,
                .transfer_id    = (CanardTransferID) (state->next_transfer_id.uavcan_node_port_list++),
            };
            send(state, monotonic_time + MEGA, &transfer, serialized_size, &serialized[0]);
        }
    }
}

/// https://github.com/OpenCyphal/public_regulated_data_types/blob/master/reg/udral/service/actuator/servo/_.0.1.dsdl
static void processMessageServoSetpoint(State* const                                                   state,
                                        const reg_udral_physics_dynamics_translation_Linear_0_1* const msg)
{
    state->servo.position     = msg->kinematics.position.meter;
    state->servo.velocity     = msg->kinematics.velocity.meter_per_second;
    state->servo.acceleration = msg->kinematics.acceleration.meter_per_second_per_second;
    state->servo.force        = msg->force.newton;
}

/// https://github.com/OpenCyphal/public_regulated_data_types/blob/master/reg/udral/service/common/Readiness.0.1.dsdl
static void processMessageServiceReadiness(State* const                                        state,
                                           const reg_udral_service_common_Readiness_0_1* const msg,
                                           const CanardMicrosecond                             monotonic_time)
{
    state->servo.arming.armed          = msg->value >= reg_udral_service_common_Readiness_0_1_ENGAGED;
    state->servo.arming.last_update_at = monotonic_time;
}

static void processMessagePlugAndPlayNodeIDAllocation(State* const                                     state,
                                                      const uavcan_pnp_NodeIDAllocationData_2_0* const msg)
{
    uint8_t uid[uavcan_node_GetInfo_Response_1_0_unique_id_ARRAY_CAPACITY_] = {0};
    getUniqueID(uid);
    if ((msg->node_id.value <= CANARD_NODE_ID_MAX) && (memcmp(uid, msg->unique_id, sizeof(uid)) == 0))
    {
        printf("Got PnP node-ID allocation: %d\n", msg->node_id.value);
        state->canard.node_id = (CanardNodeID) msg->node_id.value;
        // Store the value into the non-volatile storage.
        uavcan_register_Value_1_0 reg = {0};
        uavcan_register_Value_1_0_select_natural16_(&reg);
        reg.natural16.value.elements[0] = msg->node_id.value;
        reg.natural16.value.count       = 1;
        registerWrite("uavcan.node.id", &reg);
        // We no longer need the subscriber, drop it to free up the resources (both memory and CPU time).
        (void) canardRxUnsubscribe(&state->canard,
                                   CanardTransferKindMessage,
                                   uavcan_pnp_NodeIDAllocationData_2_0_FIXED_PORT_ID_);
    }
    // Otherwise, ignore it: either it is a request from another node or it is a response to another node.
}

static uavcan_node_ExecuteCommand_Response_1_1 processRequestExecuteCommand(
    const uavcan_node_ExecuteCommand_Request_1_1* req)
{
    uavcan_node_ExecuteCommand_Response_1_1 resp = {0};
    switch (req->command)
    {
    case uavcan_node_ExecuteCommand_Request_1_1_COMMAND_BEGIN_SOFTWARE_UPDATE:
    {
        char file_name[uavcan_node_ExecuteCommand_Request_1_1_parameter_ARRAY_CAPACITY_ + 1] = {0};
        memcpy(file_name, req->parameter.elements, req->parameter.count);
        file_name[req->parameter.count] = '\0';
        // TODO: invoke the bootloader with the specified file name. See https://github.com/Zubax/kocherga/
        printf("Firmware update request; filename: '%s' \n", &file_name[0]);
        resp.status = uavcan_node_ExecuteCommand_Response_1_1_STATUS_BAD_STATE;  // This is a stub.
        break;
    }
    case uavcan_node_ExecuteCommand_Request_1_1_COMMAND_FACTORY_RESET:
    {
        registerDoFactoryReset();
        resp.status = uavcan_node_ExecuteCommand_Response_1_1_STATUS_SUCCESS;
        break;
    }
    case uavcan_node_ExecuteCommand_Request_1_1_COMMAND_RESTART:
    {
        g_restart_required = true;
        resp.status        = uavcan_node_ExecuteCommand_Response_1_1_STATUS_SUCCESS;
        break;
    }
    case uavcan_node_ExecuteCommand_Request_1_1_COMMAND_STORE_PERSISTENT_STATES:
    {
        // If your registers are not automatically synchronized with the non-volatile storage, use this command
        // to commit them to the storage explicitly. Otherwise, it is safe to remove it.
        // In this demo, the registers are stored in files, so there is nothing to do.
        resp.status = uavcan_node_ExecuteCommand_Response_1_1_STATUS_SUCCESS;
        break;
    }
        // You can add vendor-specific commands here as well.
    default:
    {
        resp.status = uavcan_node_ExecuteCommand_Response_1_1_STATUS_BAD_COMMAND;
        break;
    }
    }
    return resp;
}

/// Performance notice: the register storage may be slow to access depending on its implementation (e.g., if it is
/// backed by an uncached filesystem). If your register storage implementation is slow, this may disrupt real-time
/// activities of the device. To avoid this, you can employ either measure:
///
/// - Load registers to memory at startup, synchronize with the storage at reboot/power-down.
///   To implement fast register access you can use https://github.com/pavel-kirienko/cavl.
///   See also uavcan.node.ExecuteCommand.COMMAND_STORE_PERSISTENT_STATES.
///
/// - If an RTOS is used (not a baremetal system), you can run a separate Cyphal processing task for
///   soft-real-time blocking operations (this approach is used in PX4).
///
/// - Document an operational limitation that the register interface should not be accessed while ENGAGED (armed).
///   Cyphal networks usually have no service traffic while the vehicle is operational.
///
static uavcan_register_Access_Response_1_0 processRequestRegisterAccess(const uavcan_register_Access_Request_1_0* req)
{
    char name[uavcan_register_Name_1_0_name_ARRAY_CAPACITY_ + 1] = {0};
    assert(req->name.name.count < sizeof(name));
    memcpy(&name[0], req->name.name.elements, req->name.name.count);
    name[req->name.name.count] = '\0';

    uavcan_register_Access_Response_1_0 resp = {0};

    // If we're asked to write a new value, do it now:
    if (!uavcan_register_Value_1_0_is_empty_(&req->value))
    {
        uavcan_register_Value_1_0_select_empty_(&resp.value);
        registerRead(&name[0], &resp.value);
        // If such register exists and it can be assigned from the request value:
        if (!uavcan_register_Value_1_0_is_empty_(&resp.value) && registerAssign(&resp.value, &req->value))
        {
            registerWrite(&name[0], &resp.value);
        }
    }

    // Regardless of whether we've just wrote a value or not, we need to read the current one and return it.
    // The client will determine if the write was successful or not by comparing the request value with response.
    uavcan_register_Value_1_0_select_empty_(&resp.value);
    registerRead(&name[0], &resp.value);

    // Currently, all registers we implement are mutable and persistent. This is an acceptable simplification,
    // but more advanced implementations will need to differentiate between them to support advanced features like
    // exposing internal states via registers, perfcounters, etc.
    resp._mutable   = true;
    resp.persistent = true;

    // Our node does not synchronize its time with the network so we can't populate the timestamp.
    resp.timestamp.microsecond = uavcan_time_SynchronizedTimestamp_1_0_UNKNOWN;

    return resp;
}

/// Constructs a response to uavcan.node.GetInfo which contains the basic information about this node.
static uavcan_node_GetInfo_Response_1_0 processRequestNodeGetInfo()
{
    uavcan_node_GetInfo_Response_1_0 resp = {0};
    resp.protocol_version.major           = CANARD_CYPHAL_SPECIFICATION_VERSION_MAJOR;
    resp.protocol_version.minor           = CANARD_CYPHAL_SPECIFICATION_VERSION_MINOR;

    // The hardware version is not populated in this demo because it runs on no specific hardware.
    // An embedded node like a servo would usually determine the version by querying the hardware.

    resp.software_version.major   = VERSION_MAJOR;
    resp.software_version.minor   = VERSION_MINOR;
    resp.software_vcs_revision_id = VCS_REVISION_ID;

    getUniqueID(resp.unique_id);

    // The node name is the name of the product like a reversed Internet domain name (or like a Java package).
    resp.name.count = strlen(NODE_NAME);
    memcpy(&resp.name.elements, NODE_NAME, resp.name.count);

    // The software image CRC and the Certificate of Authenticity are optional so not populated in this demo.
    return resp;
}

static void processReceivedTransfer(State* const state, const CanardRxTransfer* const transfer)
{
    if (transfer->metadata.transfer_kind == CanardTransferKindMessage)
    {
        size_t size = transfer->payload.size;
        if (transfer->metadata.port_id == state->port_id.sub.servo_setpoint)
        {
            reg_udral_physics_dynamics_translation_Linear_0_1 msg = {0};
            if (reg_udral_physics_dynamics_translation_Linear_0_1_deserialize_(&msg, transfer->payload.data, &size) >= 0)
            {
                processMessageServoSetpoint(state, &msg);
            }
        }
        else if (transfer->metadata.port_id == state->port_id.sub.servo_readiness)
        {
            reg_udral_service_common_Readiness_0_1 msg = {0};
            if (reg_udral_service_common_Readiness_0_1_deserialize_(&msg, transfer->payload.data, &size) >= 0)
            {
                processMessageServiceReadiness(state, &msg, transfer->timestamp_usec);
            }
        }
        else if (transfer->metadata.port_id == uavcan_pnp_NodeIDAllocationData_2_0_FIXED_PORT_ID_)
        {
            uavcan_pnp_NodeIDAllocationData_2_0 msg = {0};
            if (uavcan_pnp_NodeIDAllocationData_2_0_deserialize_(&msg, transfer->payload.data, &size) >= 0)
            {
                processMessagePlugAndPlayNodeIDAllocation(state, &msg);
            }
        }
        else
        {
            assert(false);  // Seems like we have set up a port subscription without a handler -- bad implementation.
        }
    }
    else if (transfer->metadata.transfer_kind == CanardTransferKindRequest)
    {
        if (transfer->metadata.port_id == uavcan_node_GetInfo_1_0_FIXED_PORT_ID_)
        {
            // The request object is empty so we don't bother deserializing it. Just send the response.
            const uavcan_node_GetInfo_Response_1_0 resp = processRequestNodeGetInfo();
            uint8_t      serialized[uavcan_node_GetInfo_Response_1_0_SERIALIZATION_BUFFER_SIZE_BYTES_];
            size_t       serialized_size = sizeof(serialized);
            const int8_t res = uavcan_node_GetInfo_Response_1_0_serialize_(&resp, &serialized[0], &serialized_size);
            if (res >= 0)
            {
                sendResponse(state, transfer, serialized_size, &serialized[0]);
            }
            else
            {
                assert(false);
            }
        }
        else if (transfer->metadata.port_id == uavcan_register_Access_1_0_FIXED_PORT_ID_)
        {
            uavcan_register_Access_Request_1_0 req  = {0};
            size_t                             size = transfer->payload.size;
            if (uavcan_register_Access_Request_1_0_deserialize_(&req, transfer->payload.data, &size) >= 0)
            {
                const uavcan_register_Access_Response_1_0 resp = processRequestRegisterAccess(&req);
                uint8_t serialized[uavcan_register_Access_Response_1_0_SERIALIZATION_BUFFER_SIZE_BYTES_];
                size_t  serialized_size = sizeof(serialized);
                if (uavcan_register_Access_Response_1_0_serialize_(&resp, &serialized[0], &serialized_size) >= 0)
                {
                    sendResponse(state, transfer, serialized_size, &serialized[0]);
                }
            }
        }
        else if (transfer->metadata.port_id == uavcan_register_List_1_0_FIXED_PORT_ID_)
        {
            uavcan_register_List_Request_1_0 req  = {0};
            size_t                           size = transfer->payload.size;
            if (uavcan_register_List_Request_1_0_deserialize_(&req, transfer->payload.data, &size) >= 0)
            {
                const uavcan_register_List_Response_1_0 resp = {.name = registerGetNameByIndex(req.index)};
                uint8_t serialized[uavcan_register_List_Response_1_0_SERIALIZATION_BUFFER_SIZE_BYTES_];
                size_t  serialized_size = sizeof(serialized);
                if (uavcan_register_List_Response_1_0_serialize_(&resp, &serialized[0], &serialized_size) >= 0)
                {
                    sendResponse(state, transfer, serialized_size, &serialized[0]);
                }
            }
        }
        else if (transfer->metadata.port_id == uavcan_node_ExecuteCommand_1_1_FIXED_PORT_ID_)
        {
            uavcan_node_ExecuteCommand_Request_1_1 req  = {0};
            size_t                                 size = transfer->payload.size;
            if (uavcan_node_ExecuteCommand_Request_1_1_deserialize_(&req, transfer->payload.data, &size) >= 0)
            {
                const uavcan_node_ExecuteCommand_Response_1_1 resp = processRequestExecuteCommand(&req);
                uint8_t serialized[uavcan_node_ExecuteCommand_Response_1_1_SERIALIZATION_BUFFER_SIZE_BYTES_];
                size_t  serialized_size = sizeof(serialized);
                if (uavcan_node_ExecuteCommand_Response_1_1_serialize_(&resp, &serialized[0], &serialized_size) >= 0)
                {
                    sendResponse(state, transfer, serialized_size, &serialized[0]);
                }
            }
        }
        else
        {
            assert(false);  // Seems like we have set up a port subscription without a handler -- bad implementation.
        }
    }
    else
    {
        assert(false);  // Bad implementation -- check your subscriptions.
    }
}

static void* canardAllocate(void* const user_reference, const size_t amount)
{
    O1HeapInstance* const heap = ((State*) user_reference)->heap;
    assert(o1heapDoInvariantsHold(heap));
    return o1heapAllocate(heap, amount);
}

static void canardDeallocate(void* const user_reference, const size_t amount, void* const pointer)
{
    (void) amount;
    O1HeapInstance* const heap = ((State*) user_reference)->heap;
    o1heapFree(heap, pointer);
}

extern char** environ;

int main(const int argc, char* const argv[])
{
    struct timespec ts;
    (void) clock_gettime(CLOCK_REALTIME, &ts);
    srand((unsigned) ts.tv_nsec);

    State state = {0};

    // A simple application like a servo node typically does not require more than 20 KiB of heap and 4 KiB of stack.
    // For the background and related theory refer to the following resources:
    // - https://github.com/OpenCyphal/libcanard/blob/master/README.md
    // - https://github.com/pavel-kirienko/o1heap/blob/master/README.md
    // - https://forum.opencyphal.org/t/uavcanv1-libcanard-nunavut-templates-memory-usage-concerns/1118/4
    _Alignas(O1HEAP_ALIGNMENT) static uint8_t heap_arena[1024 * 20] = {0};
    state.heap                                                      = o1heapInit(heap_arena, sizeof(heap_arena));
    assert(NULL != state.heap);
    struct CanardMemoryResource canard_memory = {
        .user_reference = &state,
        .deallocate     = canardDeallocate,
        .allocate       = canardAllocate,
    };

    // The libcanard instance requires the allocator for managing protocol states.
    state.canard                = canardInit(canard_memory);
    state.canard.user_reference = &state;  // Make the state reachable from the canard instance.

    // Restore the node-ID from the corresponding standard register. Default to anonymous.
    uavcan_register_Value_1_0 val = {0};
    uavcan_register_Value_1_0_select_natural16_(&val);
    val.natural16.value.count       = 1;
    val.natural16.value.elements[0] = UINT16_MAX;  // This means undefined (anonymous), per Specification/libcanard.
    registerRead("uavcan.node.id", &val);  // The names of the standard registers are regulated by the Specification.
    assert(uavcan_register_Value_1_0_is_natural16_(&val) && (val.natural16.value.count == 1));
    state.canard.node_id = (val.natural16.value.elements[0] > CANARD_NODE_ID_MAX)
                               ? CANARD_NODE_ID_UNSET
                               : (CanardNodeID) val.natural16.value.elements[0];

    // The description register is optional but recommended because it helps constructing/maintaining large networks.
    // It simply keeps a human-readable description of the node that should be empty by default.
    uavcan_register_Value_1_0_select_string_(&val);
    val._string.value.count = 0;
    registerRead("uavcan.node.description", &val);  // We don't need the value, we just need to ensure it exists.

    // The UDRAL cookie is used to mark nodes that are auto-configured by a specific auto-configuration authority.
    // We don't use this value, it is managed by remote nodes; our only responsibility is to persist it across reboots.
    // This register is entirely optional though; if not provided, the node will have to be configured manually.
    uavcan_register_Value_1_0_select_string_(&val);
    val._string.value.count = 0;  // The value should be empty by default, meaning that the node is not configured.
    registerRead("udral.pnp.cookie", &val);

    // Announce which UDRAL network services we support by populating appropriate registers. They are supposed to be
    // immutable (read-only), but in this simplified demo we don't support that, so we make them mutable (do fix this).
    uavcan_register_Value_1_0_select_string_(&val);
    strcpy((char*) val._string.value.elements, "servo");  // The prefix in port names like "servo.feedback", etc.
    val._string.value.count = strlen((const char*) val._string.value.elements);
    registerWrite("reg.udral.service.actuator.servo", &val);

    // Configure the transport by reading the appropriate standard registers.
    uavcan_register_Value_1_0_select_natural16_(&val);
    val.natural16.value.count       = 1;
    val.natural16.value.elements[0] = CANARD_MTU_CAN_FD;
    registerRead("uavcan.can.mtu", &val);
    assert(uavcan_register_Value_1_0_is_natural16_(&val) && (val.natural16.value.count == 1));
    // We also need the bitrate configuration register. In this demo we can't really use it but an embedded application
    // shall define "uavcan.can.bitrate" of type natural32[2]; the second value is zero/ignored if CAN FD not supported.
    const int sock[CAN_REDUNDANCY_FACTOR] = {
        socketcanOpen("vcan0", val.natural16.value.elements[0] > CANARD_MTU_CAN_CLASSIC)  //
    };
    for (uint8_t ifidx = 0; ifidx < CAN_REDUNDANCY_FACTOR; ifidx++)
    {
        if (sock[ifidx] < 0)
        {
            return -sock[ifidx];
        }
        state.canard_tx_queues[ifidx] =
            canardTxInit(CAN_TX_QUEUE_CAPACITY, val.natural16.value.elements[0], canard_memory);
    }

    // Load the port-IDs from the registers. You can implement hot-reloading at runtime if desired. Specification here:
    // https://github.com/OpenCyphal/public_regulated_data_types/blob/master/reg/udral/service/actuator/servo/_.0.1.dsdl
    // https://github.com/OpenCyphal/public_regulated_data_types/blob/master/reg/udral/README.md
    // As follows from the Specification, the register group name prefix can be arbitrary; here we just use "servo".
    // Publications:
    state.port_id.pub.servo_feedback =  // High-rate status information: all good or not, engaged or sleeping.
        getSubjectID(SUBJECT_ROLE_PUBLISHER,
                     "servo.feedback",
                     reg_udral_service_actuator_common_Feedback_0_1_FULL_NAME_AND_VERSION_);
    state.port_id.pub.servo_status =  // A low-rate high-level status overview: temperatures, fault flags, errors.
        getSubjectID(SUBJECT_ROLE_PUBLISHER,
                     "servo.status",
                     reg_udral_service_actuator_common_Status_0_1_FULL_NAME_AND_VERSION_);
    state.port_id.pub.servo_power =  // Electric power input measurements (voltage and current).
        getSubjectID(SUBJECT_ROLE_PUBLISHER,
                     "servo.power",
                     reg_udral_physics_electricity_PowerTs_0_1_FULL_NAME_AND_VERSION_);
    state.port_id.pub.servo_dynamics =  // Position/speed/acceleration/force feedback.
        getSubjectID(SUBJECT_ROLE_PUBLISHER,
                     "servo.dynamics",
                     reg_udral_physics_dynamics_translation_LinearTs_0_1_FULL_NAME_AND_VERSION_);
    // Subscriptions:
    state.port_id.sub.servo_setpoint =  // This message actually commands the servo setpoint with the motion profile.
        getSubjectID(SUBJECT_ROLE_SUBSCRIBER,
                     "servo.setpoint",
                     reg_udral_physics_dynamics_translation_Linear_0_1_FULL_NAME_AND_VERSION_);
    state.port_id.sub.servo_readiness =  // Arming subject: whether to act upon the setpoint or to stay idle.
        getSubjectID(SUBJECT_ROLE_SUBSCRIBER,
                     "servo.readiness",
                     reg_udral_service_common_Readiness_0_1_FULL_NAME_AND_VERSION_);

    // Set up subject subscriptions and RPC-service servers.
    // Message subscriptions:
    static const CanardMicrosecond servo_transfer_id_timeout = 100 * KILO;
    if (state.canard.node_id > CANARD_NODE_ID_MAX)
    {
        static CanardRxSubscription rx;
        const int8_t                res =  //
            canardRxSubscribe(&state.canard,
                              CanardTransferKindMessage,
                              uavcan_pnp_NodeIDAllocationData_2_0_FIXED_PORT_ID_,
                              uavcan_pnp_NodeIDAllocationData_2_0_EXTENT_BYTES_,
                              CANARD_DEFAULT_TRANSFER_ID_TIMEOUT_USEC,
                              &rx);
        if (res < 0)
        {
            return -res;
        }
    }
    if (state.port_id.sub.servo_setpoint <= CANARD_SUBJECT_ID_MAX)  // Do not subscribe if not configured.
    {
        static CanardRxSubscription rx;
        const int8_t                res =  //
            canardRxSubscribe(&state.canard,
                              CanardTransferKindMessage,
                              state.port_id.sub.servo_setpoint,
                              reg_udral_physics_dynamics_translation_Linear_0_1_EXTENT_BYTES_,
                              servo_transfer_id_timeout,
                              &rx);
        if (res < 0)
        {
            return -res;
        }
    }
    if (state.port_id.sub.servo_readiness <= CANARD_SUBJECT_ID_MAX)  // Do not subscribe if not configured.
    {
        static CanardRxSubscription rx;
        const int8_t                res =  //
            canardRxSubscribe(&state.canard,
                              CanardTransferKindMessage,
                              state.port_id.sub.servo_readiness,
                              reg_udral_service_common_Readiness_0_1_EXTENT_BYTES_,
                              servo_transfer_id_timeout,
                              &rx);
        if (res < 0)
        {
            return -res;
        }
    }
    // Service servers:
    {
        static CanardRxSubscription rx;
        const int8_t                res =  //
            canardRxSubscribe(&state.canard,
                              CanardTransferKindRequest,
                              uavcan_node_GetInfo_1_0_FIXED_PORT_ID_,
                              uavcan_node_GetInfo_Request_1_0_EXTENT_BYTES_,
                              CANARD_DEFAULT_TRANSFER_ID_TIMEOUT_USEC,
                              &rx);
        if (res < 0)
        {
            return -res;
        }
    }
    {
        static CanardRxSubscription rx;
        const int8_t                res =  //
            canardRxSubscribe(&state.canard,
                              CanardTransferKindRequest,
                              uavcan_node_ExecuteCommand_1_1_FIXED_PORT_ID_,
                              uavcan_node_ExecuteCommand_Request_1_1_EXTENT_BYTES_,
                              CANARD_DEFAULT_TRANSFER_ID_TIMEOUT_USEC,
                              &rx);
        if (res < 0)
        {
            return -res;
        }
    }
    {
        static CanardRxSubscription rx;
        const int8_t                res =  //
            canardRxSubscribe(&state.canard,
                              CanardTransferKindRequest,
                              uavcan_register_Access_1_0_FIXED_PORT_ID_,
                              uavcan_register_Access_Request_1_0_EXTENT_BYTES_,
                              CANARD_DEFAULT_TRANSFER_ID_TIMEOUT_USEC,
                              &rx);
        if (res < 0)
        {
            return -res;
        }
    }
    {
        static CanardRxSubscription rx;
        const int8_t                res =  //
            canardRxSubscribe(&state.canard,
                              CanardTransferKindRequest,
                              uavcan_register_List_1_0_FIXED_PORT_ID_,
                              uavcan_register_List_Request_1_0_EXTENT_BYTES_,
                              CANARD_DEFAULT_TRANSFER_ID_TIMEOUT_USEC,
                              &rx);
        if (res < 0)
        {
            return -res;
        }
    }

    // Now the node is initialized and we're ready to roll.
    state.started_at                           = getMonotonicMicroseconds();
    const CanardMicrosecond fast_loop_period   = MEGA / 50;
    CanardMicrosecond       next_fast_iter_at  = state.started_at + fast_loop_period;
    CanardMicrosecond       next_1_hz_iter_at  = state.started_at + MEGA;
    CanardMicrosecond       next_01_hz_iter_at = state.started_at + MEGA * 10;
    do
    {
        // Run a trivial scheduler polling the loops that run the business logic.
        CanardMicrosecond monotonic_time = getMonotonicMicroseconds();
        if (monotonic_time >= next_fast_iter_at)
        {
            next_fast_iter_at += fast_loop_period;
            handleFastLoop(&state, monotonic_time);
        }
        if (monotonic_time >= next_1_hz_iter_at)
        {
            next_1_hz_iter_at += MEGA;
            handle1HzLoop(&state, monotonic_time);
        }
        if (monotonic_time >= next_01_hz_iter_at)
        {
            next_01_hz_iter_at += MEGA * 10;
            handle01HzLoop(&state, monotonic_time);
        }

        // Manage CAN RX/TX per redundant interface.
        for (uint8_t ifidx = 0; ifidx < CAN_REDUNDANCY_FACTOR; ifidx++)
        {
            // Transmit pending frames from the prioritized TX queues managed by libcanard.
            CanardTxQueue* const     que = &state.canard_tx_queues[ifidx];
            const CanardTxQueueItem* tqi = canardTxPeek(que);  // Find the highest-priority frame.
            while (tqi != NULL)
            {
                // Attempt transmission only if the frame is not yet timed out while waiting in the TX queue.
                // Otherwise just drop it and move on to the next one.
                if ((tqi->tx_deadline_usec == 0) || (tqi->tx_deadline_usec > monotonic_time))
                {
                    const int16_t result = socketcanPush(sock[ifidx], &tqi->frame, 0);  // Non-blocking write attempt.
                    if (result == 0)
                    {
                        break;  // The queue is full, we will try again on the next iteration.
                    }
                    if (result < 0)
                    {
                        return -result;  // SocketCAN interface failure (link down?)
                    }
                }
                CanardTxQueueItem* const mut_tqi = canardTxPop(que, tqi);
                que->memory.deallocate(que->memory.user_reference, mut_tqi->allocated_size, mut_tqi);
                tqi = canardTxPeek(que);
            }

            // Process received frames by feeding them from SocketCAN to libcanard.
            // The order in which we handle the redundant interfaces doesn't matter -- libcanard can accept incoming
            // frames from any of the redundant interface in an arbitrary order.
            // The internal state machine will sort them out and remove duplicates automatically.
            CanardFrame   frame                  = {0};
            uint8_t       buf[CANARD_MTU_CAN_FD] = {0};
            const int16_t socketcan_result       = socketcanPop(sock[ifidx], &frame, NULL, sizeof(buf), buf, 0, NULL);
            if (socketcan_result == 0)  // The read operation has timed out with no frames, nothing to do here.
            {
                break;
            }
            if (socketcan_result < 0)  // The read operation has failed. This is not a normal condition.
            {
                return -socketcan_result;
            }
            // The SocketCAN adapter uses the wall clock for timestamping, but we need monotonic.
            // Wall clock can only be used for time synchronization.
            const CanardMicrosecond timestamp_usec = getMonotonicMicroseconds();
            CanardRxTransfer        transfer       = {0};
            const int8_t canard_result = canardRxAccept(&state.canard, timestamp_usec, &frame, ifidx, &transfer, NULL);
            if (canard_result > 0)
            {
                processReceivedTransfer(&state, &transfer);
                state.canard.memory.deallocate(state.canard.memory.user_reference,
                                               transfer.payload.allocated_size,
                                               transfer.payload.data);
            }
            else if ((canard_result == 0) || (canard_result == -CANARD_ERROR_OUT_OF_MEMORY))
            {
                (void) 0;  // The frame did not complete a transfer so there is nothing to do.
                // OOM should never occur if the heap is sized correctly. You can track OOM errors via heap API.
            }
            else
            {
                assert(false);  // No other error can possibly occur at runtime.
            }
        }
    } while (!g_restart_required);

    // It is recommended to postpone restart until all frames are sent though.
    (void) argc;
    puts("RESTART ");
    return -execve(argv[0], argv, environ);
}
