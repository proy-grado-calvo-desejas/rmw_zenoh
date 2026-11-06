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

#ifndef DETAIL__SESSION_LOCK_HPP_
#define DETAIL__SESSION_LOCK_HPP_

#include <shared_mutex>

namespace rmw_zenoh_cpp
{
/// A shared (reader) lock over the Zenoh session owned by an rmw_context_impl_s.
/**
 * Every RMW entry point that reaches a session-owned Zenoh handle -- a
 * publisher, a subscriber, a queryable, a querier, a liveliness token or the
 * graph cache -- holds one of these for the duration of the call.
 * rmw_context_impl_s::reconfigure() takes the matching exclusive lock while it
 * tears the session down and rebuilds it, so an in-flight call either completes
 * against the old session or blocks and then runs against the new one; it can
 * never observe a half-swapped session.
 *
 * Operations that only touch process-local state (message queues, wait set
 * condition variables, guard conditions) deliberately do *not* take this lock:
 * rmw_wait() in particular would otherwise hold it for the whole of its
 * timeout and stall reconfiguration.
 */
using SessionReadLock = std::shared_lock<std::shared_mutex>;
}  // namespace rmw_zenoh_cpp

#endif  // DETAIL__SESSION_LOCK_HPP_
