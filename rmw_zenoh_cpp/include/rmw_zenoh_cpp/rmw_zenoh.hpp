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

#ifndef RMW_ZENOH_CPP__RMW_ZENOH_HPP_
#define RMW_ZENOH_CPP__RMW_ZENOH_HPP_

#include <memory>
#include <string>

#include <zenoh.hxx>

#include "rmw/ret_types.h"
#include "rmw/types.h"
#include "rmw/visibility_control.h"

#ifdef __cplusplus

/// Get the Zenoh session associated with the given RMW context.
/// \param[in] context The RMW context.
/// \return A shared pointer to the Zenoh session, or nullptr if invalid.
RMW_PUBLIC
const std::shared_ptr<zenoh::Session>
rmw_zenoh_get_session(const rmw_context_t * context);

/// Get the json5 configuration the context's Zenoh session was built from.
/// \param[in] context The RMW context.
/// \return The configuration, or an empty string if the context is invalid.
RMW_PUBLIC
std::string
rmw_zenoh_get_session_config(const rmw_context_t * context);

/// Rebuild the context's Zenoh session from a new configuration.
/**
 * Tears the current Zenoh session down and opens a new one from
 * `config_json5`, re-declaring every ROS entity the context owns. The
 * rmw_node_t, rmw_publisher_t, rmw_subscription_t, rmw_service_t and
 * rmw_client_t pointers the application holds all stay valid.
 *
 * This is the same operation a Zenoh router plugin triggers by publishing to
 * `@ros2_admin/<domain_id>/session/<session_id>/config`; see
 * docs/design.md for the full contract, including the entity GID churn and the
 * data loss during the reconfiguration window.
 *
 * Blocks until every in-flight RMW operation on this context has finished.
 *
 * \param[in] context The RMW context.
 * \param[in] config_json5 The new Zenoh configuration, in the same json5
 *   format as config/DEFAULT_RMW_ZENOH_SESSION_CONFIG.json5.
 * \return RMW_RET_OK on success.
 *   RMW_RET_INVALID_ARGUMENT if the arguments are null or the configuration is
 *   not a valid Zenoh json5 document, in which case the running session is
 *   untouched.
 *   RMW_RET_ERROR if the session could not be rebuilt; the previous
 *   configuration is restored where possible. Call rmw_get_error_string() for
 *   the reason.
 */
RMW_PUBLIC
rmw_ret_t
rmw_zenoh_reconfigure_session(rmw_context_t * context, const char * config_json5);

#endif  // __cplusplus

#endif  // RMW_ZENOH_CPP__RMW_ZENOH_HPP_
