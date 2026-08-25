// Copyright 2026 Open Source Robotics Foundation, Inc.
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

#ifndef DETAIL__ADMIN_CHANNEL_HPP_
#define DETAIL__ADMIN_CHANNEL_HPP_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <zenoh.hxx>

namespace rmw_zenoh_cpp
{
/// The hermetic key space the session reconfiguration control channel lives in.
/**
 * This mirrors the "@ros2_lv" namespace used by liveliness tokens (see
 * docs/design.md). Zenoh wildcard matching never crosses between a key that
 * starts with "@ros2_admin" and a regular ROS topic key, so admin traffic can
 * never be delivered to a ROS subscription and a ROS publication can never be
 * mistaken for an admin message.
 */
extern const char ADMIN_SPACE[];

/// Key a session listens on for a new configuration:
///   @ros2_admin/<domain_id>/session/<session_id>/config
/// A publisher can target one session by its id, or every session in the domain
/// by publishing to "@ros2_admin/<domain_id>/session/**/config".
std::string admin_config_keyexpr(std::size_t domain_id, const std::string & session_id);

/// Key the outcome of each reconfiguration attempt is published on:
///   @ros2_admin/<domain_id>/session/<session_id>/ack
/// The session id in the key is the one in force *after* the attempt, so a
/// successful reconfiguration acks under its new id.
std::string admin_ack_keyexpr(std::size_t domain_id, const std::string & session_id);

/// Key a queryable answers with the configuration actually in force:
///   @ros2_admin/<domain_id>/session/<session_id>/current_config
std::string admin_current_config_keyexpr(std::size_t domain_id, const std::string & session_id);

///=============================================================================
/// The control channel through which a Zenoh router plugin pushes a new session
/// configuration into a running rmw_zenoh_cpp process.
/**
 * The subscriber callback runs on a thread owned by the very session that a
 * reconfiguration destroys, so it does no work beyond queueing the payload. A
 * dedicated worker thread owned by this class drains the queue and invokes the
 * reconfiguration callback, which is what actually rebuilds the session.
 */
class AdminChannel final
{
public:
  /// Invoked on the worker thread with a candidate json5 configuration.
  using ReconfigureCallback = std::function<void (const std::string &)>;

  /// @param domain_id The ROS domain id, which scopes the admin key space.
  /// @param on_config Invoked on the worker thread for each configuration
  ///   received. Never invoked concurrently with itself.
  AdminChannel(std::size_t domain_id, ReconfigureCallback on_config);

  ~AdminChannel();

  /// Declare the config subscriber and the current_config queryable on `session`.
  /// @param session The session to declare on.
  /// @param config_json5 The configuration `session` was built from; this is
  ///   what the current_config queryable serves.
  /// @param last_reconfigure_time_ns Wall-clock time of the reconfiguration that
  ///   produced `session`, or 0 if this is the process's first session.
  bool declare_on_session(
    const std::shared_ptr<zenoh::Session> & session,
    const std::string & config_json5,
    int64_t last_reconfigure_time_ns);

  /// Undeclare every Zenoh handle. Safe to call when nothing is declared.
  void undeclare();

  /// Start the reconfiguration worker thread. Idempotent.
  void start();

  /// Stop the worker thread and drop anything still queued. Idempotent.
  void stop();

  /// Publish the outcome of a reconfiguration attempt on the ack key.
  /// Does nothing if no session is currently declared on.
  void publish_ack(bool success, const std::string & error);

private:
  void run();

  // Maximum number of pending configurations. A plugin that pushes faster than
  // the process can rebuild its session is almost certainly misbehaving, so the
  // oldest pending entry is dropped rather than growing the queue without bound.
  static constexpr std::size_t MAX_PENDING_CONFIGS = 8;

  const std::size_t domain_id_;
  const ReconfigureCallback on_config_;

  // Guards the zenoh handles and the cached configuration below.
  mutable std::mutex mutex_;
  std::shared_ptr<zenoh::Session> session_;
  std::string session_id_;
  std::string config_json5_;
  int64_t last_reconfigure_time_ns_{0};
  std::optional<zenoh::Subscriber<void>> config_subscriber_;
  std::optional<zenoh::Queryable<void>> current_config_queryable_;

  // Guards the pending-configuration queue and the worker's running flag.
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::deque<std::string> queue_;
  bool running_{false};
  std::thread worker_;
};
}  // namespace rmw_zenoh_cpp

#endif  // DETAIL__ADMIN_CHANNEL_HPP_
