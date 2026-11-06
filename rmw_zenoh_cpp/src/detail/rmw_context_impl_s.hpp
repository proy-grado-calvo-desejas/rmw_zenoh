// Copyright 2024 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DETAIL__RMW_CONTEXT_IMPL_S_HPP_
#define DETAIL__RMW_CONTEXT_IMPL_S_HPP_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <zenoh.hxx>

#include "buffer_backend_context.hpp"
#include "graph_cache.hpp"
#include "rmw_node_data.hpp"
#include "session_lock.hpp"
#include "zenoh_utils.hpp"

#include "rmw/ret_types.h"
#include "rmw/types.h"

///=============================================================================
struct rmw_context_impl_s final
{
public:
  // Constructor that internally initializes the Zenoh session and other artifacts.
  // Throws an std::runtime_error if any of the initializations fail.
  // The construction will block until a Zenoh router is detected.
  // TODO(Yadunund): Make this a non-blocking call by checking for the Zenoh
  // router in a separate thread. Instead block when creating a node if router
  // check has not succeeded.
  rmw_context_impl_s(
    const std::size_t domain_id,
    const std::string & enclave);

  ~rmw_context_impl_s();

  // Get a copy of the enclave.
  std::string enclave() const;

  // Get the ROS domain id of this context.
  std::size_t domain_id() const;

  // Loan the Zenoh session.
  const std::shared_ptr<zenoh::Session> session() const;

  // Get an shm subsystem.
  const std::shared_ptr<rmw_zenoh_cpp::ShmContext> shm() const;

  // Get the graph guard condition.
  rmw_guard_condition_t * graph_guard_condition();

  // Get a unique id for a new entity.
  std::size_t get_next_entity_id();

  // Shutdown the Zenoh session.
  rmw_ret_t shutdown();

  // Check if the Zenoh session is shutdown.
  bool is_shutdown() const;

  // Returns true if the Zenoh session is valid.
  bool session_is_valid() const;

  /// Return a shared_ptr to the GraphCache stored in this context.
  std::shared_ptr<rmw_zenoh_cpp::GraphCache> graph_cache();

  /// Return a shared_ptr to the Serialization buffer pool stored in this context.
  std::shared_ptr<rmw_zenoh_cpp::BufferPool> serialization_buffer_pool();

  /// Create a NodeData and store it within this context. The NodeData can be
  /// retrieved using get_node().
  /// Returns false if parameters are invalid.
  bool create_node_data(
    const rmw_node_t * const node,
    const std::string & ns,
    const std::string & node_name);

  /// Retrieve the NodeData for a given rmw_node_t if present.
  std::shared_ptr<rmw_zenoh_cpp::NodeData> get_node_data(
    const rmw_node_t * const node);

  /// Delete the NodeData for a given rmw_node_t if present.
  void delete_node_data(const rmw_node_t * const node);

  /// Return a pointer to the per-context BufferBackendContext.
  rmw_zenoh_cpp::BufferBackendContext * buffer_backend_context();

  /// Acquire a shared (reader) lock on the Zenoh session.
  /// Every RMW entry point that reaches a session-owned Zenoh handle holds one
  /// of these for the duration of the call, so that reconfigure() -- which takes
  /// the matching exclusive lock -- cannot swap the session mid-call.
  [[nodiscard]]
  rmw_zenoh_cpp::SessionReadLock lock_session_shared() const;

  /// Return the json5 configuration the running Zenoh session was built from.
  std::string session_config() const;

  /// Return the wall-clock time in nanoseconds of the last successful
  /// reconfiguration, or 0 if the session has never been reconfigured.
  int64_t last_reconfigure_time() const;

  /// Tear the Zenoh session down and rebuild it from `config_json5`,
  /// re-declaring every ROS entity this context owns on the new session.
  /**
   * The rmw_node_t, rmw_publisher_t, rmw_subscription_t, rmw_service_t and
   * rmw_client_t pointers the application holds all stay valid: the objects
   * behind them are mutated in place rather than destroyed.
   *
   * The zenoh-level identity of every entity changes, because it is derived
   * from the session id. Remote peers therefore see every entity of this
   * process disappear and reappear, and matched/unmatched events fire
   * accordingly. Messages published or received during the window between
   * teardown and re-declaration are lost, and queued messages, unanswered
   * service requests and pending client replies are dropped.
   *
   * If the new session cannot be opened, the previous configuration is
   * restored. If that also fails, the context is left without a session and
   * subsequent RMW calls fail until a working configuration arrives.
   *
   * This call blocks until every in-flight RMW operation has finished.
   *
   * @param config_json5 The new zenoh configuration, in the same json5 format
   *   as config/DEFAULT_RMW_ZENOH_SESSION_CONFIG.json5.
   * @param error Set to a human readable reason when the return value is not
   *   RMW_RET_OK.
   */
  rmw_ret_t reconfigure(const std::string & config_json5, std::string & error);

  // Forward declaration
  class Data;

private:
  std::shared_ptr<Data> data_{nullptr};
};

#endif  // DETAIL__RMW_CONTEXT_IMPL_S_HPP_
