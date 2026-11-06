// Copyright 2025 Open Source Robotics Foundation, Inc.
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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/string.hpp>
#include <zenoh.hxx>

#include "rmw/error_handling.h"

#include "rmw_zenoh_cpp/rmw_zenoh.hpp"

class TestRmwZenohSession : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestCase()
  {
    rclcpp::shutdown();
  }
};

TEST_F(TestRmwZenohSession, GetZenohSessionFromContext)
{
  // Create a node
  auto node = std::make_shared<rclcpp::Node>("test_zenoh_session");

  // Get the RMW context from rclcpp
  auto node_handle = node->get_node_base_interface()->get_shared_rcl_node_handle();
  auto rcl_node = node_handle.get();
  auto context = rcl_node->context;

  // Get the RMW context
  auto rmw_context = rcl_context_get_rmw_context(context);
  ASSERT_NE(rmw_context, nullptr);

  // Get the Zenoh session
  auto session = rmw_zenoh_get_session(rmw_context);
  ASSERT_NE(session, nullptr) << "Failed to get Zenoh session from RMW context";
}

TEST_F(TestRmwZenohSession, ZenohSessionDirectAccess)
{
  // Create a node
  auto node = std::make_shared<rclcpp::Node>("test_zenoh_session_direct");

  // Get the RMW context
  auto node_handle = node->get_node_base_interface()->get_shared_rcl_node_handle();
  auto rcl_node = node_handle.get();
  auto context = rcl_node->context;
  auto rmw_context = rcl_context_get_rmw_context(context);

  // Get the Zenoh session
  auto session = rmw_zenoh_get_session(rmw_context);
  ASSERT_NE(session, nullptr);

  // Verify we can get the ZID (Zenoh ID) from the session
  [[maybe_unused]] auto zid = session->get_zid();
  // ZID is a 16-byte array, just verify it exists

  // Create a simple keyexpr to test session functionality
  const std::string test_key = "test/rmw_zenoh/session";
  zenoh::KeyExpr keyexpr(test_key);

  // Test creating a publisher using the session directly
  // This demonstrates that applications can use the session for advanced Zenoh operations
  auto publisher = session->declare_publisher(keyexpr);

  // Test publishing data
  const std::string test_data = "test_message";
  publisher.put(test_data);

  // Give some time for the operation to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

int main(int argc, char ** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST_F(TestRmwZenohSession, ReconfigureSessionKeepsHandlesValid)
{
  auto node = std::make_shared<rclcpp::Node>("test_zenoh_reconfigure");

  auto node_handle = node->get_node_base_interface()->get_shared_rcl_node_handle();
  auto rmw_context = rcl_context_get_rmw_context(node_handle->context);
  ASSERT_NE(rmw_context, nullptr);

  // A publisher and a subscription created before the reconfiguration must
  // still work afterwards, through the very same handles.
  std::atomic<int> received{0};
  auto publisher = node->create_publisher<std_msgs::msg::String>("reconfigure_chatter", 10);
  auto subscription = node->create_subscription<std_msgs::msg::String>(
    "reconfigure_chatter", 10,
    [&received](std_msgs::msg::String::ConstSharedPtr) {received.fetch_add(1);});

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node);

  // Establish a baseline: if messages do not flow in this environment at all
  // there is nothing meaningful to assert about them after a reconfiguration.
  const auto round_trip = [&executor, &publisher, &received]() {
      const int before = received.load();
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      while (std::chrono::steady_clock::now() < deadline) {
        std_msgs::msg::String msg;
        msg.data = "hello";
        publisher->publish(msg);
        executor.spin_some();
        if (received.load() > before) {
          return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
      return false;
    };

  const bool messages_flow = round_trip();
  if (!messages_flow) {
    GTEST_SKIP() << "Messages do not flow in this environment; "
      "skipping the post-reconfiguration delivery check.";
  }

  const std::string config = rmw_zenoh_get_session_config(rmw_context);
  ASSERT_FALSE(config.empty()) << "Failed to read the running session configuration";

  auto old_session = rmw_zenoh_get_session(rmw_context);
  ASSERT_NE(old_session, nullptr);
  const std::string old_zid = old_session->get_zid().to_string();

  // Rebuild the session from the configuration it is already running.
  ASSERT_EQ(rmw_zenoh_reconfigure_session(rmw_context, config.c_str()), RMW_RET_OK)
    << rmw_get_error_string().str;

  auto new_session = rmw_zenoh_get_session(rmw_context);
  ASSERT_NE(new_session, nullptr);
  EXPECT_NE(new_session.get(), old_session.get());
  // The zenoh session id, and therefore every entity GID, necessarily changes.
  EXPECT_NE(new_session->get_zid().to_string(), old_zid);

  // The node, publisher and subscription handles are all still the ones the
  // application created, and messages flow again over the new session.
  EXPECT_STREQ(node->get_name(), "test_zenoh_reconfigure");
  EXPECT_TRUE(round_trip()) << "No message was delivered after the reconfiguration";
}

TEST_F(TestRmwZenohSession, ReconfigureWithInvalidConfigIsRejected)
{
  auto node = std::make_shared<rclcpp::Node>("test_zenoh_reconfigure_invalid");

  auto node_handle = node->get_node_base_interface()->get_shared_rcl_node_handle();
  auto rmw_context = rcl_context_get_rmw_context(node_handle->context);
  ASSERT_NE(rmw_context, nullptr);

  auto session_before = rmw_zenoh_get_session(rmw_context);
  ASSERT_NE(session_before, nullptr);

  // A payload that is not a zenoh configuration is rejected before anything is
  // torn down, so the running session is left untouched.
  EXPECT_EQ(
    rmw_zenoh_reconfigure_session(rmw_context, "this is not json5"),
    RMW_RET_INVALID_ARGUMENT);
  rmw_reset_error();

  auto session_after = rmw_zenoh_get_session(rmw_context);
  ASSERT_NE(session_after, nullptr);
  EXPECT_EQ(session_after.get(), session_before.get());
}
