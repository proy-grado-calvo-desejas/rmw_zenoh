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

#include "rmw_context_impl_s.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <zenoh.hxx>

#include "admin_channel.hpp"
#include "buffer_backend_context.hpp"
#include "buffer_backend_loader.hpp"
#include "graph_cache.hpp"
#include "guard_condition.hpp"
#include "identifier.hpp"
#include "logging_macros.hpp"
#include "rmw_node_data.hpp"
#include "session_lock.hpp"
#include "zenoh_config.hpp"

#include "rcpputils/scope_exit.hpp"
#include "rmw/error_handling.h"
#include "zenoh_utils.hpp"

// Megabytes of SHM to reserve.
// TODO(clalancette): Make this configurable, or get it from the configuration
#define SHM_BUFFER_SIZE_MB 10

// Zenoh config key for SHM transport optimization enabled
// When SHM is enabled, rmw_zenoh will use the same SHM segment
// than created for transport_optimization. Hence, it must be enabled.
static const char * CONFIG_KEY_SHM_TRANSPORT_OPTIM_ENABLED =
  "transport/shared_memory/transport_optimization/enabled";

// Zenoh config key for SHM pool size
static const char * CONFIG_KEY_SHM_POOL_SIZE =
  "transport/shared_memory/transport_optimization/pool_size";

// Zenoh config key for SHM threshold size
static const char * CONFIG_KEY_SHM_THRESHOLD_SIZE =
  "transport/shared_memory/transport_optimization/message_size_threshold";


// Upper bound on the number of attempts to find a zenoh router when rebuilding
// the session during a reconfiguration. ZENOH_ROUTER_CHECK_ATTEMPTS can ask the
// initial connection to wait indefinitely, but a reconfiguration runs while
// holding the exclusive session lock, so every RMW call in the process is
// blocked behind it. The user's setting is honoured up to this cap so that a
// configuration pointing at an unreachable router costs seconds rather than
// freezing the process.
static constexpr uint64_t MAX_RECONFIGURE_ROUTER_CHECK_ATTEMPTS = 10;

// Bundle all class members into a data struct which can be passed as a
// weak ptr to various threads for thread-safe access without capturing
// "this" ptr by reference.
class rmw_context_impl_s::Data final : public std::enable_shared_from_this<Data>
{
public:
  // Make a shared_ptr of Data.
  static std::shared_ptr<Data> make(
    std::size_t domain_id,
    const std::string & enclave)
  {
    auto data = std::shared_ptr<Data>(new Data(domain_id, enclave));
    data->init();
    return data;
  }

  // Shutdown the Zenoh session.
  rmw_ret_t shutdown()
  {
    rmw_ret_t ret = RMW_RET_OK;
    bool expected = false;
    if (!is_shutdown_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
      std::memory_order_relaxed))
    {
      return ret;
    }

    // Stop accepting reconfigurations first: the worker thread reaches into
    // everything below, so it must be joined before any of it goes away.
    if (admin_channel_ != nullptr) {
      admin_channel_->stop();
      admin_channel_->undeclare();
    }

    // Take the exclusive session lock so that shutdown cannot interleave with
    // an RMW call that is already using the session.
    std::unique_lock<std::shared_mutex> session_lock(session_mutex_);

    if (!undeclare_graph_subscriber()) {
      return RMW_RET_ERROR;
    }

    if (buffer_backend_context_) {
      rmw_zenoh_cpp::shutdown_buffer_backends(*buffer_backend_context_);
    }

    // Explicitly close the session before releasing our shared_ptr reference.
    // This calls wait_callbacks() internally (since zenoh commit e5db0ce), which waits
    // for all in-flight callbacks to finish. We call close() here while node-level entities
    // (subscriptions, publishers, etc.) are still alive — at shutdown time, the spin loop
    // has already exited so there are no active callbacks, and wait_callbacks() returns
    // immediately. The session is then marked as closed, so when the shared_ptr refcount
    // eventually reaches zero (after rcl destroys each handle in the normal teardown order),
    // the session destructor finds is_closed()==true and skips the blocking close() call.
    std::shared_ptr<zenoh::Session> session;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      session = std::move(session_);
      session_.reset();
    }
    if (session != nullptr) {
      session->close();
    }

    return RMW_RET_OK;
  }

  std::string enclave() const
  {
    return enclave_;
  }

  std::size_t domain_id() const
  {
    return domain_id_;
  }

  const std::shared_ptr<zenoh::Session> session() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return session_;
  }

  const std::shared_ptr<rmw_zenoh_cpp::ShmContext> shm() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return shm_;
  }

  rmw_guard_condition_t * graph_guard_condition()
  {
    return graph_guard_condition_.get();
  }

  std::size_t get_next_entity_id()
  {
    return next_entity_id_.fetch_add(1, std::memory_order_relaxed);
  }

  bool is_shutdown() const
  {
    return is_shutdown_.load(std::memory_order_acquire);
  }

  bool session_is_valid() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // session_ is null between the teardown and re-declare halves of a
    // reconfiguration, and stays null if the session could not be rebuilt.
    return session_ != nullptr && !session_->is_closed();
  }

  std::shared_ptr<rmw_zenoh_cpp::GraphCache> graph_cache()
  {
    return graph_cache_;
  }

  std::shared_ptr<rmw_zenoh_cpp::BufferPool> serialization_buffer_pool()
  {
    return serialization_buffer_pool_;
  }

  rmw_zenoh_cpp::BufferBackendContext * buffer_backend_context()
  {
    return buffer_backend_context_.get();
  }

  rmw_zenoh_cpp::SessionReadLock lock_session_shared() const
  {
    return rmw_zenoh_cpp::SessionReadLock(session_mutex_);
  }

  std::string session_config() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return session_config_;
  }

  int64_t last_reconfigure_time() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_reconfigure_time_;
  }

  bool create_node_data(
    const rmw_node_t * const node,
    const std::string & ns,
    const std::string & node_name)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (nodes_.count(node) > 0) {
      // Node already exists.
      return false;
    }

    // Check that the Zenoh session is still valid.
    if (session_ == nullptr || session_->is_closed()) {
      RMW_ZENOH_LOG_ERROR_NAMED(
        "rmw_zenoh_cpp",
        "Unable to create NodeData as Zenoh session is invalid.");
      return false;
    }

    auto node_data = rmw_zenoh_cpp::NodeData::make(
      node,
      this->get_next_entity_id(),
      session_,
      domain_id_,
      ns,
      node_name,
      enclave_);
    if (node_data == nullptr) {
      // Error already handled.
      return false;
    }

    auto node_insertion = nodes_.insert(std::make_pair(node, std::move(node_data)));
    if (!node_insertion.second) {
      return false;
    }

    return true;
  }

  std::shared_ptr<rmw_zenoh_cpp::NodeData> get_node_data(const rmw_node_t * const node)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto node_it = nodes_.find(node);
    if (node_it == nodes_.end()) {
      return nullptr;
    }
    return node_it->second;
  }

  void delete_node_data(const rmw_node_t * const node)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    nodes_.erase(node);
  }

  void update_graph_cache(const zenoh::Sample & sample_kind, const std::string & keystr)
  {
    switch (sample_kind.get_kind()) {
      case zenoh::SampleKind::Z_SAMPLE_KIND_PUT:
        graph_cache_->parse_put(keystr);
        break;
      case zenoh::SampleKind::Z_SAMPLE_KIND_DELETE:
        graph_cache_->parse_del(keystr);
        break;
      default:
        return;
    }

    // Trigger the ROS graph guard condition.
    rmw_ret_t rmw_ret = rmw_trigger_guard_condition(graph_guard_condition_.get());
    if (RMW_RET_OK != rmw_ret) {
      RMW_ZENOH_LOG_WARN_NAMED(
        "rmw_zenoh_cpp",
        "[update_graph_cache] Unable to trigger graph guard condition."
      );
    }
  }

  ///===========================================================================
  /// Tear down the current Zenoh session and rebuild it from `config_json5`.
  /// See rmw_context_impl_s::reconfigure() for the contract.
  rmw_ret_t reconfigure(const std::string & config_json5, std::string & error)
  {
    if (is_shutdown()) {
      error = "the context has been shut down";
      return RMW_RET_ERROR;
    }

    // Validate the payload before tearing anything down, so that a malformed
    // configuration costs nothing more than an error on the ack channel.
    {
      zenoh::ZResult result;
      zenoh::Config candidate = zenoh::Config::from_str(config_json5, &result);
      static_cast<void>(candidate);
      if (result != Z_OK) {
        error = "the payload is not a valid zenoh json5 configuration";
        RMW_ZENOH_LOG_ERROR_NAMED("rmw_zenoh_cpp", "[reconfigure] %s.", error.c_str());
        return RMW_RET_INVALID_ARGUMENT;
      }
    }

    // Block every RMW operation that touches the session for the duration.
    std::unique_lock<std::shared_mutex> session_lock(session_mutex_);

    if (is_shutdown()) {
      error = "the context has been shut down";
      return RMW_RET_ERROR;
    }

    std::string previous_config;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      previous_config = session_config_;
    }

    RMW_ZENOH_LOG_INFO_NAMED(
      "rmw_zenoh_cpp", "[reconfigure] Rebuilding the zenoh session.");

    // Step 1: snapshot the nodes and detach every entity they own. This runs
    // while the old session is still alive so that the undeclarations reach the
    // network and remote peers see the entities go away cleanly.
    std::vector<std::shared_ptr<rmw_zenoh_cpp::NodeData>> nodes;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      nodes.reserve(nodes_.size());
      for (const auto & entry : nodes_) {
        nodes.push_back(entry.second);
      }
    }
    for (const auto & node_data : nodes) {
      node_data->detach_from_session();
    }

    // Step 2: drop the context's own handles and the session itself.
    if (admin_channel_ != nullptr) {
      admin_channel_->undeclare();
    }
    static_cast<void>(undeclare_graph_subscriber());
    {
      std::shared_ptr<zenoh::Session> old_session;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        old_session = std::move(session_);
        session_.reset();
        shm_.reset();
      }
      if (old_session != nullptr) {
        old_session->close();
      }
    }

    // Step 3: open the new session, falling back to the previous configuration
    // if it cannot be opened.
    const std::optional<uint64_t> router_check_attempts = reconfigure_router_check_attempts();
    std::string open_error;
    std::optional<SessionBundle> bundle =
      open_session(config_json5, router_check_attempts, open_error);
    bool rolled_back = false;
    if (!bundle.has_value()) {
      RMW_ZENOH_LOG_ERROR_NAMED(
        "rmw_zenoh_cpp",
        "[reconfigure] Unable to open a session with the new configuration (%s). "
        "Rolling back to the previous configuration.", open_error.c_str());
      std::string rollback_error;
      bundle = open_session(previous_config, router_check_attempts, rollback_error);
      rolled_back = true;
      if (!bundle.has_value()) {
        error = "unable to open a session with the new configuration (" + open_error +
          ") and unable to restore the previous one (" + rollback_error +
          "); this context no longer has a session";
        RMW_ZENOH_LOG_ERROR_NAMED("rmw_zenoh_cpp", "[reconfigure] %s.", error.c_str());
        return RMW_RET_ERROR;
      }
    }

    const std::shared_ptr<zenoh::Session> session = bundle->session;
    int64_t reconfigure_time = rmw_zenoh_cpp::get_system_time_in_ns();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      session_ = session;
      shm_ = bundle->shm;
      session_config_ = bundle->config_json5;
      last_reconfigure_time_ = reconfigure_time;
    }

    // Step 4: re-key the graph cache onto the new session and repopulate it.
    // reset() removes every entity through the regular deletion path first, so
    // the application sees a matching unmatched event for everything that was
    // matched before.
    graph_cache_->reset(session->get_zid());
    if (!declare_graph_subscriber(session)) {
      error = "the session was rebuilt but the ROS graph subscriber could not be re-declared";
      RMW_ZENOH_LOG_ERROR_NAMED("rmw_zenoh_cpp", "[reconfigure] %s.", error.c_str());
      return RMW_RET_ERROR;
    }
    if (!prime_graph_cache(session)) {
      RMW_ZENOH_LOG_WARN_NAMED(
        "rmw_zenoh_cpp",
        "[reconfigure] Unable to query the ROS graph on the new session; the graph "
        "cache will fill in as liveliness tokens arrive.");
    }

    // Step 5: put every ROS entity back on the new session.
    bool ok = true;
    for (const auto & node_data : nodes) {
      ok = node_data->rebind(session) && ok;
    }

    // Step 6: re-declare the admin channel so the process can be reconfigured again.
    if (admin_channel_ != nullptr) {
      admin_channel_->declare_on_session(session, bundle->config_json5, reconfigure_time);
    }

    // The graph changed wholesale; wake anyone waiting on it.
    const rmw_ret_t trigger_ret = rmw_trigger_guard_condition(graph_guard_condition_.get());
    if (RMW_RET_OK != trigger_ret) {
      RMW_ZENOH_LOG_WARN_NAMED(
        "rmw_zenoh_cpp",
        "[reconfigure] Unable to trigger the graph guard condition.");
    }

    if (rolled_back) {
      error = "unable to open a session with the new configuration (" + open_error +
        "); the previous configuration was restored";
      return RMW_RET_ERROR;
    }
    if (!ok) {
      error = "the session was rebuilt but at least one ROS entity could not be re-declared";
      RMW_ZENOH_LOG_ERROR_NAMED("rmw_zenoh_cpp", "[reconfigure] %s.", error.c_str());
      return RMW_RET_ERROR;
    }

    RMW_ZENOH_LOG_INFO_NAMED(
      "rmw_zenoh_cpp",
      "[reconfigure] The zenoh session was rebuilt; new session id is %s.",
      session->get_zid().to_string().c_str());
    return RMW_RET_OK;
  }

  // Publish the outcome of a reconfiguration attempt on the admin ack key.
  void publish_reconfigure_ack(bool success, const std::string & error)
  {
    if (admin_channel_ != nullptr) {
      admin_channel_->publish_ack(success, error);
    }
  }

  // Destructor.
  ~Data()
  {
    auto ret = this->shutdown();
    nodes_.clear();
    static_cast<void>(ret);
  }

private:
  // How long to look for a zenoh router when rebuilding the session, honouring
  // the user's ZENOH_ROUTER_CHECK_ATTEMPTS up to
  // MAX_RECONFIGURE_ROUTER_CHECK_ATTEMPTS.
  static std::optional<uint64_t> reconfigure_router_check_attempts()
  {
    const std::optional<uint64_t> configured = rmw_zenoh_cpp::zenoh_router_check_attempts();
    if (!configured.has_value()) {
      return std::nullopt;
    }
    return std::min(configured.value(), MAX_RECONFIGURE_ROUTER_CHECK_ATTEMPTS);
  }

  // Everything that is created together with a zenoh session and has to be
  // replaced together with it.
  struct SessionBundle
  {
    std::shared_ptr<zenoh::Session> session;
    // Null when shared memory is disabled in the configuration.
    std::shared_ptr<rmw_zenoh_cpp::ShmContext> shm;
    // The configuration the session was actually opened with, after the
    // environment variable overrides below were applied. This is what is served
    // on the current_config key and what a failed reconfiguration rolls back to.
    std::string config_json5;
  };

  /// Open a zenoh session.
  /// @param config_json5 The configuration to use. std::nullopt loads it from
  ///   ZENOH_SESSION_CONFIG_URI or the packaged default, which is what the
  ///   process does at startup.
  /// @param router_check_attempts How many times to look for a zenoh router
  ///   before giving up and proceeding anyway. std::nullopt skips the check.
  /// @param error Set to a human readable reason on failure.
  static std::optional<SessionBundle> open_session(
    const std::optional<std::string> & config_json5,
    std::optional<uint64_t> router_check_attempts,
    std::string & error)
  {
    zenoh::ZResult result;

    std::optional<zenoh::Config> config;
    if (config_json5.has_value()) {
      zenoh::Config parsed = zenoh::Config::from_str(config_json5.value(), &result);
      if (result != Z_OK) {
        error = "the configuration is not a valid zenoh json5 document";
        return std::nullopt;
      }
      config = std::move(parsed);
    } else {
      config = rmw_zenoh_cpp::get_z_config(rmw_zenoh_cpp::ConfigurableEntity::Session);
      if (!config.has_value()) {
        error = "unable to load the zenoh session configuration";
        return std::nullopt;
      }
    }

    // Insert or replace some Config parameters based on environment variables.
    {
      // Handle SHM allocation size.
      if (const auto shm_alloc_size = rmw_zenoh_cpp::zenoh_shm_alloc_size() ) {
        RMW_ZENOH_LOG_INFO_NAMED(
            "rmw_zenoh_cpp",
            "ZENOH_SHM_ALLOC_SIZE=%d overriding the configuration key '%s'.",
            shm_alloc_size.value(), CONFIG_KEY_SHM_POOL_SIZE);
        config.value().insert_json5(
          CONFIG_KEY_SHM_POOL_SIZE,
          std::to_string(shm_alloc_size.value()));
      }

      // Handle SHM message size threshold.
      if (const auto shm_threshold = rmw_zenoh_cpp::zenoh_shm_message_size_threshold() ) {
        RMW_ZENOH_LOG_INFO_NAMED(
            "rmw_zenoh_cpp",
            "ZENOH_SHM_MESSAGE_SIZE_THRESHOLD=%d overriding the configuration key '%s'.",
            shm_threshold.value(), CONFIG_KEY_SHM_THRESHOLD_SIZE);
        config.value().insert_json5(
          CONFIG_KEY_SHM_THRESHOLD_SIZE,
          std::to_string(shm_threshold.value()));
      }
    }

    // Get shm configuration.
    bool shm_enabled = false;
    std::size_t shm_pool_size = 0;
    std::size_t msgsize_threshold = 512;
    {
      // Check is transport/shared_memory/enabled == true in Zenoh config
      std::string shm_enabled_val = config.value().get(Z_CONFIG_SHARED_MEMORY_KEY, &result);
      if (result == Z_OK) {
        shm_enabled = shm_enabled_val == "true" ? true : false;
      } else {
        RMW_ZENOH_LOG_ERROR_NAMED(
          "rmw_zenoh_cpp",
          "Not able to get %s from the config file",
          Z_CONFIG_SHARED_MEMORY_KEY);
      }

      // Check if transport/shared_memory/transport_optimization/enabled == true in Zenoh config
      // When SHM is enabled, rmw_zenoh will use the same SHM segment than created for
      // transport_optimization. Hence, we force transport_optimization to be enabled.
      if (shm_enabled) {
        std::string transport_optim_enabled_val =
          config.value().get(CONFIG_KEY_SHM_TRANSPORT_OPTIM_ENABLED, &result);
        if (result == Z_OK) {
          if (transport_optim_enabled_val != "true") {
            RMW_ZENOH_LOG_INFO_NAMED(
                "rmw_zenoh_cpp",
                "SHM is enabled but transport_optimization is not while required "
                "- overwritting '%s: true' in config.",
                CONFIG_KEY_SHM_TRANSPORT_OPTIM_ENABLED);
            config.value().insert_json5(
              CONFIG_KEY_SHM_TRANSPORT_OPTIM_ENABLED,
              "true");
          }
        } else {
          RMW_ZENOH_LOG_ERROR_NAMED(
            "rmw_zenoh_cpp",
            "Not able to get %s from the config file",
            CONFIG_KEY_SHM_TRANSPORT_OPTIM_ENABLED);
        }
      }

      std::string shm_size_threshold_val = config.value().get(
        CONFIG_KEY_SHM_THRESHOLD_SIZE,
        &result);
      if (result == Z_OK) {
        try {
          msgsize_threshold = std::stoull(shm_size_threshold_val);
        } catch (const std::invalid_argument & e) {
          RMW_ZENOH_LOG_ERROR_NAMED(
            "rmw_zenoh_cpp",
            "Not able to get %s from the config file: invalid argument: %s",
            CONFIG_KEY_SHM_THRESHOLD_SIZE,
            e.what()
          );
        } catch (const std::out_of_range & e) {
          RMW_ZENOH_LOG_ERROR_NAMED(
            "rmw_zenoh_cpp",
            "Not able to get %s from the config file: out of range: %s",
            CONFIG_KEY_SHM_THRESHOLD_SIZE,
            e.what()
          );
        }
      } else {
        RMW_ZENOH_LOG_ERROR_NAMED(
          "rmw_zenoh_cpp",
          "Not able to get %s from the config file",
          CONFIG_KEY_SHM_THRESHOLD_SIZE);
      }

      std::string shm_pool_size_val = config.value().get(
        CONFIG_KEY_SHM_POOL_SIZE,
        &result);
      if (result == Z_OK) {
        try {
          shm_pool_size = std::stoull(shm_pool_size_val);
        } catch (const std::invalid_argument & e) {
          RMW_ZENOH_LOG_ERROR_NAMED(
            "rmw_zenoh_cpp",
            "Not able to get %s from the config file: invalid argument: %s",
            CONFIG_KEY_SHM_POOL_SIZE,
            e.what()
          );
        } catch (const std::out_of_range & e) {
          RMW_ZENOH_LOG_ERROR_NAMED(
            "rmw_zenoh_cpp",
            "Not able to get %s from the config file: out of range: %s",
            CONFIG_KEY_SHM_POOL_SIZE,
            e.what()
          );
        }
      } else {
        RMW_ZENOH_LOG_ERROR_NAMED(
          "rmw_zenoh_cpp",
          "Not able to get %s from the config file",
          CONFIG_KEY_SHM_POOL_SIZE);
      }
    }

    // Snapshot the configuration as it will actually be used, so that a later
    // reconfiguration can roll back to exactly this.
    SessionBundle bundle;
    bundle.config_json5 = config.value().to_string();

    // Initialize the zenoh session.
    bundle.session = std::make_shared<zenoh::Session>(
      std::move(config.value()),
      zenoh::Session::SessionOptions::create_default(),
      &result);
    if (result != Z_OK) {
      error = "unable to open the zenoh session";
      return std::nullopt;
    }

    // Verify if the zenoh router is running if configured.
    if (router_check_attempts.has_value()) {
      uint64_t connection_attempts = 0;
      // Retry until the connection is successful.
      constexpr std::chrono::milliseconds sleep_time(1000);
      constexpr int64_t ticks_between_print(std::chrono::milliseconds(1000) / sleep_time);
      do {
        zenoh::ZResult check_result;
        const auto zids = bundle.session->get_routers_z_id(&check_result);
        if (check_result == Z_OK && !zids.empty()) {
          break;
        }
        if ((connection_attempts % ticks_between_print) == 0) {
          RMW_ZENOH_LOG_WARN_NAMED(
            "rmw_zenoh_cpp",
            "Unable to connect to a Zenoh router. "
            "Have you started a router with `ros2 run rmw_zenoh_cpp rmw_zenohd`?");
        }
        if (++connection_attempts >= router_check_attempts.value()) {
          RMW_ZENOH_LOG_WARN_NAMED(
            "rmw_zenoh_cpp",
            "Unable to connect to a Zenoh router after %zu attempt(s). "
            "Please ensure that a Zenoh router is running and can be reached. "
            "You may increase the number of attempts to check for a router by "
            "setting the ZENOH_ROUTER_CHECK_ATTEMPTS environment variable. "
            "Proceeding with initialization but other peers will not discover "
            "or receive data from peers in this session until a router is started.",
            router_check_attempts.value());
          break;
        }
        std::this_thread::sleep_for(sleep_time);
      } while(connection_attempts < router_check_attempts.value());
    }

    // Initialize the shm subsystem if shared_memory is enabled in the config
    if (shm_enabled) {
      RMW_ZENOH_LOG_DEBUG_NAMED("rmw_zenoh_cpp",
        "SHM is enabled - allocated size: %d - msg size threshold: %d",
        shm_pool_size,
        msgsize_threshold);

      bundle.shm = std::make_shared<rmw_zenoh_cpp::ShmContext>(
          msgsize_threshold
      );
    } else {
      RMW_ZENOH_LOG_DEBUG_NAMED("rmw_zenoh_cpp", "SHM is disabled");
    }

    return bundle;
  }

  // Private Constructor, to force the use of the make() method which returns a shared_ptr.
  Data(
    std::size_t domain_id,
    const std::string & enclave)
  : domain_id_(domain_id),
    enclave_(enclave),
    is_shutdown_(false),
    next_entity_id_(0),
    nodes_({}),
    liveliness_keyexpr_(rmw_zenoh_cpp::liveliness::subscription_token(domain_id))
  {
    std::string error;
    std::optional<SessionBundle> bundle = open_session(
      std::nullopt, rmw_zenoh_cpp::zenoh_router_check_attempts(), error);
    if (!bundle.has_value()) {
      throw std::runtime_error("Error setting up zenoh session: " + error);
    }
    session_ = bundle->session;
    shm_ = bundle->shm;
    session_config_ = bundle->config_json5;

    // Initialize the graph cache.
    graph_cache_ =
      std::make_shared<rmw_zenoh_cpp::GraphCache>(session_->get_zid());
    if (!prime_graph_cache(session_)) {
      throw std::runtime_error("Error getting liveliness. ");
    }

    graph_guard_condition_ = std::make_unique<rmw_guard_condition_t>();
    graph_guard_condition_->implementation_identifier = rmw_zenoh_cpp::rmw_zenoh_identifier;
    graph_guard_condition_->data = &guard_condition_data_;

    // Initialize the serialization buffer pool.
    serialization_buffer_pool_ = std::make_shared<rmw_zenoh_cpp::BufferPool>();

    // Initialize the buffer backend context.
    buffer_backend_context_ = std::make_unique<rmw_zenoh_cpp::BufferBackendContext>();
    rmw_zenoh_cpp::initialize_buffer_backends(*buffer_backend_context_);
  }

  // Query router/liveliness participants to get graph information from before
  // this session was started.
  bool prime_graph_cache(const std::shared_ptr<zenoh::Session> & session)
  {
    // We create a blocking channel that is unbounded, ie. `bound` = 0, to receive
    // replies for the z_liveliness_get() call. This is necessary as if the `bound`
    // is too low, the channel may starve the zenoh executor of its threads which
    // would lead to deadlocks when trying to receive replies and block the
    // execution here.
    // The blocking channel will return when the sender end is closed which is
    // the moment the query finishes.
    // The non-blocking fifo exists only for the use case where we don't want to
    // block the thread between responses (including the request termination response).
    // In general, unless we want to cooperatively schedule other tasks on the same
    // thread as reading the fifo, the blocking fifo will be more appropriate as
    // the code will be simpler, and if we're just going to spin over the non-blocking
    // reads until we obtain responses, we'll just be hogging CPU time by convincing
    // the OS that we're doing actual work when it could instead park the thread.

    // Intuitively use a FIFO channel rather than building it up with a closure and a
    // handler to FIFO channel
    zenoh::ZResult result;
    auto replies = session->liveliness_get(
      liveliness_keyexpr_,
      zenoh::channels::FifoChannel(SIZE_MAX - 1),
      zenoh::Session::LivelinessGetOptions::create_default(),
      &result);

    if (result != Z_OK) {
      RMW_ZENOH_LOG_ERROR_NAMED(
        "rmw_zenoh_cpp", "[rmw_context_impl_s] Error getting liveliness.");
      return false;
    }

    for (auto res = replies.recv(); std::holds_alternative<zenoh::Reply>(res);
      res = replies.recv())
    {
      const zenoh::Reply & reply = std::get<zenoh::Reply>(res);
      if (reply.is_ok()) {
        const auto & sample = reply.get_ok();
        graph_cache_->parse_put(std::string(sample.get_keyexpr().as_string_view()), true);
      } else {
        RMW_ZENOH_LOG_DEBUG_NAMED(
          "rmw_zenoh_cpp", "[rmw_context_impl_s] z_call received an invalid reply.\n");
      }
    }

    return true;
  }

  void init()
  {
    // This can't be in the constructor since shared_from_this() doesn't work there.
    if (!declare_graph_subscriber(session_)) {
      RMW_SET_ERROR_MSG("unable to create zenoh subscription");
      throw std::runtime_error("Unable to subscribe to ROS graph updates.");
    }

    // Bring up the reconfiguration control channel. A process that cannot
    // declare it still works, it just cannot be reconfigured remotely, so a
    // failure here is logged by the channel rather than fatal.
    std::weak_ptr<Data> data_wp = shared_from_this();
    admin_channel_ = std::make_unique<rmw_zenoh_cpp::AdminChannel>(
      domain_id_,
      [data_wp](const std::string & config_json5) {
        std::shared_ptr<Data> data = data_wp.lock();
        if (data == nullptr) {
          return;
        }
        std::string error;
        const rmw_ret_t ret = data->reconfigure(config_json5, error);
        data->publish_reconfigure_ack(ret == RMW_RET_OK, error);
      });
    admin_channel_->start();
    admin_channel_->declare_on_session(session_, session_config_, 0);
  }

  // Setup the liveliness subscriber that receives updates from the ROS graph
  // and updates the graph cache.
  bool declare_graph_subscriber(const std::shared_ptr<zenoh::Session> & session)
  {
    zenoh::Session::LivelinessSubscriberOptions sub_options =
      zenoh::Session::LivelinessSubscriberOptions::create_default();
    sub_options.history = true;

    std::weak_ptr<Data> context_impl_data_wp = weak_from_this();
    zenoh::ZResult result;
    auto subscriber = session->liveliness_declare_subscriber(
      liveliness_keyexpr_,
      [context_impl_data_wp](const zenoh::Sample & sample) {
        // Update the graph cache.
        std::shared_ptr<Data> context_impl_data = context_impl_data_wp.lock();
        if (context_impl_data == nullptr) {
          RMW_ZENOH_LOG_ERROR_NAMED("rmw_zenoh_cpp", "Unable to obtain context_impl.");
          return;
        }
        context_impl_data->update_graph_cache(
          sample,
          std::string(sample.get_keyexpr().as_string_view()));
      },
      zenoh::closures::none,
      std::move(sub_options),
      &result);

    if (result != Z_OK) {
      return false;
    }

    graph_subscriber_ = std::move(subscriber);
    return true;
  }

  // Undeclare the ROS graph liveliness subscriber if it is declared.
  // No synchronization is needed: undeclare blocks until in-flight callbacks
  // have finished.
  bool undeclare_graph_subscriber()
  {
    if (!graph_subscriber_.has_value()) {
      return true;
    }
    zenoh::ZResult result;
    std::move(graph_subscriber_).value().undeclare(&result);
    graph_subscriber_.reset();
    if (result != Z_OK) {
      RMW_ZENOH_LOG_ERROR_NAMED(
        "rmw_zenoh_cpp",
        "Unable to undeclare the liveliness token");
      return false;
    }
    return true;
  }

  // Mutex to lock when accessing members.
  mutable std::mutex mutex_;
  // Reader/writer lock over the zenoh session. RMW operations that touch
  // session-owned handles take it shared; reconfigure() takes it exclusively
  // while it swaps the session out from under them.
  mutable std::shared_mutex session_mutex_;
  // The ROS domain id of this context.
  std::size_t domain_id_;
  // Enclave, name used to find security artifacts in a sros2 keystore.
  std::string enclave_;
  // An owned session. Null between the teardown and re-declare halves of a
  // reconfiguration, and after shutdown.
  std::shared_ptr<zenoh::Session> session_;
  // The json5 configuration session_ was opened with. Kept so that a failed
  // reconfiguration can roll back to it and so that the admin channel can
  // report what is actually running.
  std::string session_config_;
  // Wall-clock time in nanoseconds of the last successful reconfiguration,
  // or 0 if the session has never been reconfigured.
  int64_t last_reconfigure_time_{0};
  // An optional SHM context that is initialized if SHM is enabled in the
  // zenoh session config.
  std::shared_ptr<rmw_zenoh_cpp::ShmContext> shm_;
  // Graph cache.
  std::shared_ptr<rmw_zenoh_cpp::GraphCache> graph_cache_;
  // Pool of serialization buffers.
  std::shared_ptr<rmw_zenoh_cpp::BufferPool> serialization_buffer_pool_;
  // ROS graph liveliness subscriber.
  // The graph_subscriber *must* exist in order for anything in this Data class,
  // and hence rmw_zenoh_cpp, to work.
  // However, zenoh::Subscriber does not have an empty constructor,
  // so just declaring this as a zenoh::Subscriber fails to compile.
  // We work around that by wrapping it in a std::optional,
  // so the std::optional gets constructed at Data constructor time,
  // and then we initialize graph_subscriber_ later. Note that the zenoh-cpp API
  // liveliness_declare_subscriber() throws an exception if it fails,
  // so this should all be safe to do.
  std::optional<zenoh::Subscriber<void>> graph_subscriber_;
  // The control channel a zenoh router plugin uses to push a new session
  // configuration into this process.
  std::unique_ptr<rmw_zenoh_cpp::AdminChannel> admin_channel_;
  // Equivalent to rmw_dds_common::Context's guard condition.
  // Guard condition that should be triggered when the graph changes.
  std::unique_ptr<rmw_guard_condition_t> graph_guard_condition_;
  // The GuardCondition data structure.
  rmw_zenoh_cpp::GuardCondition guard_condition_data_;
  // Shutdown flag.
  std::atomic<bool> is_shutdown_;
  // A counter to assign a local id for every entity created in this session.
  std::atomic<std::size_t> next_entity_id_;
  // Nodes created from this context.
  std::unordered_map<const rmw_node_t *, std::shared_ptr<rmw_zenoh_cpp::NodeData>> nodes_;

  zenoh::KeyExpr liveliness_keyexpr_;

  std::unique_ptr<rmw_zenoh_cpp::BufferBackendContext> buffer_backend_context_;
};

///=============================================================================
rmw_context_impl_s::rmw_context_impl_s(
  const std::size_t domain_id,
  const std::string & enclave)
{
  data_ = Data::make(domain_id, enclave);
}

///=============================================================================
rmw_context_impl_s::~rmw_context_impl_s()
{
  this->shutdown();
}

///=============================================================================
std::string rmw_context_impl_s::enclave() const
{
  return data_->enclave();
}

///=============================================================================
std::size_t rmw_context_impl_s::domain_id() const
{
  return data_->domain_id();
}

///=============================================================================
const std::shared_ptr<zenoh::Session> rmw_context_impl_s::session() const
{
  return data_->session();
}

///=============================================================================
const std::shared_ptr<rmw_zenoh_cpp::ShmContext> rmw_context_impl_s::shm() const
{
  return data_->shm();
}

///=============================================================================
rmw_guard_condition_t * rmw_context_impl_s::graph_guard_condition()
{
  return data_->graph_guard_condition();
}

///=============================================================================
std::size_t rmw_context_impl_s::get_next_entity_id()
{
  return data_->get_next_entity_id();
}

///=============================================================================
rmw_ret_t rmw_context_impl_s::shutdown()
{
  return data_->shutdown();
}

///=============================================================================
bool rmw_context_impl_s::is_shutdown() const
{
  return data_->is_shutdown();
}

///=============================================================================
bool rmw_context_impl_s::session_is_valid() const
{
  return data_->session_is_valid();
}

///=============================================================================
std::shared_ptr<rmw_zenoh_cpp::GraphCache> rmw_context_impl_s::graph_cache()
{
  return data_->graph_cache();
}

///=============================================================================
std::shared_ptr<rmw_zenoh_cpp::BufferPool> rmw_context_impl_s::serialization_buffer_pool()
{
  return data_->serialization_buffer_pool();
}

///=============================================================================
rmw_zenoh_cpp::BufferBackendContext * rmw_context_impl_s::buffer_backend_context()
{
  return data_->buffer_backend_context();
}

///=============================================================================
rmw_zenoh_cpp::SessionReadLock rmw_context_impl_s::lock_session_shared() const
{
  return data_->lock_session_shared();
}

///=============================================================================
std::string rmw_context_impl_s::session_config() const
{
  return data_->session_config();
}

///=============================================================================
int64_t rmw_context_impl_s::last_reconfigure_time() const
{
  return data_->last_reconfigure_time();
}

///=============================================================================
rmw_ret_t rmw_context_impl_s::reconfigure(const std::string & config_json5, std::string & error)
{
  return data_->reconfigure(config_json5, error);
}

///=============================================================================
bool rmw_context_impl_s::create_node_data(
  const rmw_node_t * const node,
  const std::string & ns,
  const std::string & node_name)
{
  return data_->create_node_data(node, ns, node_name);
}

///=============================================================================
std::shared_ptr<rmw_zenoh_cpp::NodeData> rmw_context_impl_s::get_node_data(
  const rmw_node_t * const node)
{
  return data_->get_node_data(node);
}

///=============================================================================
void rmw_context_impl_s::delete_node_data(const rmw_node_t * const node)
{
  data_->delete_node_data(node);
}
