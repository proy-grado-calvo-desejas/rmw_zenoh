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

#include "admin_channel.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include <zenoh.hxx>

#include "logging_macros.hpp"

namespace rmw_zenoh_cpp
{
const char ADMIN_SPACE[] = "@ros2_admin";

namespace
{
std::string admin_session_prefix(std::size_t domain_id, const std::string & session_id)
{
  std::ostringstream oss;
  oss << ADMIN_SPACE << "/" << domain_id << "/session/" << session_id;
  return oss.str();
}

/// Escape a string for inclusion in a JSON string literal.
std::string json_escape(const std::string & value)
{
  std::string out;
  out.reserve(value.size() + 8);
  for (const char c : value) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out += c;
        break;
    }
  }
  return out;
}
}  // namespace

///=============================================================================
std::string admin_config_keyexpr(std::size_t domain_id, const std::string & session_id)
{
  return admin_session_prefix(domain_id, session_id) + "/config";
}

///=============================================================================
std::string admin_ack_keyexpr(std::size_t domain_id, const std::string & session_id)
{
  return admin_session_prefix(domain_id, session_id) + "/ack";
}

///=============================================================================
std::string admin_current_config_keyexpr(std::size_t domain_id, const std::string & session_id)
{
  return admin_session_prefix(domain_id, session_id) + "/current_config";
}

///=============================================================================
AdminChannel::AdminChannel(std::size_t domain_id, ReconfigureCallback on_config)
: domain_id_(domain_id),
  on_config_(std::move(on_config))
{
  // Do nothing.
}

///=============================================================================
AdminChannel::~AdminChannel()
{
  this->stop();
  this->undeclare();
}

///=============================================================================
bool AdminChannel::declare_on_session(
  const std::shared_ptr<zenoh::Session> & session,
  const std::string & config_json5,
  int64_t last_reconfigure_time_ns)
{
  if (session == nullptr) {
    return false;
  }

  const std::string session_id = session->get_zid().to_string();
  const std::string config_key = admin_config_keyexpr(domain_id_, session_id);
  const std::string current_config_key =
    admin_current_config_keyexpr(domain_id_, session_id);

  {
    std::lock_guard<std::mutex> lock(mutex_);
    session_ = session;
    session_id_ = session_id;
    config_json5_ = config_json5;
    last_reconfigure_time_ns_ = last_reconfigure_time_ns;
  }

  zenoh::ZResult result;
  // The callback runs on a thread owned by `session`, which is exactly what a
  // reconfiguration destroys, so it must not do the reconfiguration itself. It
  // queues the payload and returns; the worker thread does the work.
  auto subscriber = session->declare_subscriber(
    zenoh::KeyExpr(config_key),
    [this](const zenoh::Sample & sample) {
      std::string payload = sample.get_payload().as_string();
      if (payload.empty()) {
        RMW_ZENOH_LOG_WARN_NAMED(
          "rmw_zenoh_cpp",
          "[AdminChannel] Ignoring an empty configuration received on '%s'.",
          std::string(sample.get_keyexpr().as_string_view()).c_str());
        return;
      }

      RMW_ZENOH_LOG_INFO_NAMED(
        "rmw_zenoh_cpp",
        "[AdminChannel] Queued a new session configuration received on '%s'.",
        std::string(sample.get_keyexpr().as_string_view()).c_str());

      {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (!running_) {
          return;
        }
        while (queue_.size() >= MAX_PENDING_CONFIGS) {
          RMW_ZENOH_LOG_WARN_NAMED(
            "rmw_zenoh_cpp",
            "[AdminChannel] More than %zu configurations are pending; "
            "dropping the oldest.", MAX_PENDING_CONFIGS);
          queue_.pop_front();
        }
        queue_.push_back(std::move(payload));
      }
      queue_cv_.notify_one();
    },
    zenoh::closures::none,
    zenoh::Session::SubscriberOptions::create_default(),
    &result);
  if (result != Z_OK) {
    RMW_ZENOH_LOG_ERROR_NAMED(
      "rmw_zenoh_cpp",
      "[AdminChannel] Unable to subscribe to '%s'. This process cannot be "
      "reconfigured remotely.", config_key.c_str());
    std::lock_guard<std::mutex> lock(mutex_);
    session_.reset();
    return false;
  }

  auto queryable = session->declare_queryable(
    zenoh::KeyExpr(current_config_key),
    [this](const zenoh::Query & query) {
      std::string payload;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        std::ostringstream oss;
        oss << "{\"session_id\":\"" << json_escape(session_id_)
            << "\",\"last_reconfigure_time_ns\":" << last_reconfigure_time_ns_
            << ",\"config\":" << config_json5_ << "}";
        payload = oss.str();
      }
      auto options = zenoh::Query::ReplyOptions::create_default();
      options.encoding = zenoh::Encoding("application/json");
      zenoh::ZResult reply_result;
      query.reply(
        query.get_keyexpr(), zenoh::Bytes(std::move(payload)), std::move(options),
        &reply_result);
      if (reply_result != Z_OK) {
        RMW_ZENOH_LOG_ERROR_NAMED(
          "rmw_zenoh_cpp",
          "[AdminChannel] Unable to reply to a current_config query.");
      }
    },
    zenoh::closures::none,
    zenoh::Session::QueryableOptions::create_default(),
    &result);
  if (result != Z_OK) {
    // The config subscriber is the part that matters; a missing queryable only
    // costs the plugin its read-back, so carry on with a warning.
    RMW_ZENOH_LOG_WARN_NAMED(
      "rmw_zenoh_cpp",
      "[AdminChannel] Unable to declare the queryable on '%s'.",
      current_config_key.c_str());
  }

  std::lock_guard<std::mutex> lock(mutex_);
  config_subscriber_ = std::move(subscriber);
  if (result == Z_OK) {
    current_config_queryable_ = std::move(queryable);
  }

  RMW_ZENOH_LOG_INFO_NAMED(
    "rmw_zenoh_cpp",
    "[AdminChannel] Listening for session configurations on '%s'.", config_key.c_str());

  return true;
}

///=============================================================================
void AdminChannel::undeclare()
{
  std::optional<zenoh::Subscriber<void>> subscriber;
  std::optional<zenoh::Queryable<void>> queryable;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    subscriber = std::move(config_subscriber_);
    config_subscriber_.reset();
    queryable = std::move(current_config_queryable_);
    current_config_queryable_.reset();
    session_.reset();
  }

  zenoh::ZResult result;
  // undeclare() blocks until in-flight callbacks have finished, which is safe
  // here: the callbacks only touch the pending-configuration queue.
  if (subscriber.has_value()) {
    std::move(subscriber.value()).undeclare(&result);
    if (result != Z_OK) {
      RMW_ZENOH_LOG_ERROR_NAMED(
        "rmw_zenoh_cpp",
        "[AdminChannel] Unable to undeclare the config subscriber.");
    }
  }
  if (queryable.has_value()) {
    std::move(queryable.value()).undeclare(&result);
    if (result != Z_OK) {
      RMW_ZENOH_LOG_ERROR_NAMED(
        "rmw_zenoh_cpp",
        "[AdminChannel] Unable to undeclare the current_config queryable.");
    }
  }
}

///=============================================================================
void AdminChannel::start()
{
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (running_) {
      return;
    }
    running_ = true;
  }
  worker_ = std::thread(&AdminChannel::run, this);
}

///=============================================================================
void AdminChannel::stop()
{
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (!running_) {
      return;
    }
    running_ = false;
    queue_.clear();
  }
  queue_cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
}

///=============================================================================
void AdminChannel::publish_ack(bool success, const std::string & error)
{
  std::shared_ptr<zenoh::Session> session;
  std::string session_id;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    session = session_;
    session_id = session_id_;
  }
  if (session == nullptr) {
    return;
  }

  std::ostringstream oss;
  oss << "{\"session_id\":\"" << json_escape(session_id)
      << "\",\"success\":" << (success ? "true" : "false")
      << ",\"error\":\"" << json_escape(error) << "\"}";

  auto options = zenoh::Session::PutOptions::create_default();
  options.encoding = zenoh::Encoding("application/json");
  zenoh::ZResult result;
  session->put(
    zenoh::KeyExpr(admin_ack_keyexpr(domain_id_, session_id)),
    zenoh::Bytes(oss.str()),
    std::move(options),
    &result);
  if (result != Z_OK) {
    RMW_ZENOH_LOG_ERROR_NAMED(
      "rmw_zenoh_cpp",
      "[AdminChannel] Unable to publish the reconfiguration ack.");
  }
}

///=============================================================================
void AdminChannel::run()
{
  while (true) {
    std::string config;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this]() {return !running_ || !queue_.empty();});
      if (!running_) {
        return;
      }
      config = std::move(queue_.front());
      queue_.pop_front();
    }

    on_config_(config);
  }
}
}  // namespace rmw_zenoh_cpp
