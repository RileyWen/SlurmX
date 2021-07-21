#include "XdNodeKeeper.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/fiber/barrier.hpp>
#include <boost/interprocess/anonymous_shared_memory.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>

#include "SharedTestImpl/GlobalDefs.h"
#include "XdServer.h"
#include "slurmx/BoostInterprocessBarrier.h"
#include "slurmx/FdFunctions.h"

using testing::_;
using testing::AnyOf;
using testing::AtLeast;
using testing::Invoke;
using testing::InvokeWithoutArgs;
using testing::Sequence;

namespace bi = boost::interprocess;

#define RED "\033[0;31m"
#define RESET "\033[0m"

class MockCtlXdServer {
 public:
  MOCK_METHOD(void, XdNodeIsUpCb, (uint32_t, void*));
  MOCK_METHOD(void, XdNodeIsDownCb, (uint32_t, void*));
  MOCK_METHOD(void, XdNodeIsTempDownCb, (uint32_t, void*));
  MOCK_METHOD(void, XdNodeRecFromTempFailureCb, (uint32_t, void*));
};

class XdNodeKeeperTest : public ::testing::Test {
 public:
  void SetUp() override {
    m_mock_ctlxd_server_ = std::make_unique<MockCtlXdServer>();

    m_keeper_ = std::make_unique<CtlXd::XdNodeKeeper>();
    m_keeper_->SetNodeIsUpCb(
        std::bind(&MockCtlXdServer::XdNodeIsUpCb, m_mock_ctlxd_server_.get(),
                  std::placeholders::_1, std::placeholders::_2));
    m_keeper_->SetNodeIsDownCb(
        std::bind(&MockCtlXdServer::XdNodeIsDownCb, m_mock_ctlxd_server_.get(),
                  std::placeholders::_1, std::placeholders::_2));
    m_keeper_->SetNodeIsTempDownCb(std::bind(
        &MockCtlXdServer::XdNodeIsTempDownCb, m_mock_ctlxd_server_.get(),
        std::placeholders::_1, std::placeholders::_2));
    m_keeper_->SetNodeRecFromTempFailureCb(
        std::bind(&MockCtlXdServer::XdNodeRecFromTempFailureCb,
                  m_mock_ctlxd_server_.get(), std::placeholders::_1,
                  std::placeholders::_2));
  }

  void TearDown() override {
    if (m_keeper_) m_keeper_.reset();
    m_mock_ctlxd_server_.reset();
  }

  std::unique_ptr<MockCtlXdServer> m_mock_ctlxd_server_;
  std::unique_ptr<CtlXd::XdNodeKeeper> m_keeper_;
};

TEST_F(XdNodeKeeperTest, FailToConnect) {
  std::string server_addr{"127.0.0.1:50011"};

  resource_t res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  auto result_future = m_keeper_->RegisterXdNode(server_addr, &res, nullptr);
  CtlXd::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), false);
}

TEST_F(XdNodeKeeperTest, OneStub_OneAbortedServer) {
  TaskManager::GetInstance().Shutdown();
  TaskManager::GetInstance().Wait();

  using Xd::XdServer;
  using namespace std::chrono_literals;

  std::string server_addr{"127.0.0.1:50011"};

  resource_t res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  std::atomic_bool has_exited = false;

  auto xd_server = std::make_unique<XdServer>(server_addr, res);
  std::this_thread::sleep_for(1s);

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Up", index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Temp Down", index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Down", index);
        has_exited = true;
      }));

  Sequence seq;
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .Times(1)
      .InSequence(seq);

  auto result_future = m_keeper_->RegisterXdNode(server_addr, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  CtlXd::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 0);
  }

  std::this_thread::sleep_for(3s);
  xd_server->Shutdown();
  xd_server.reset();

  while (!has_exited) {
    std::this_thread::yield();
  }
}

// Note: we use sleep() here to provide synchronization. However, valgrind may
// slow the execution down too much and cause the test to fail.
//
// Todo: Consider to use barrier or condition variable.
//
// The server is gracefully shut down. In such case, a GOAWAY message will be
// send. READY -> IDLE transition is expected.
// e.g. Shutdown by the user command.
TEST_F(XdNodeKeeperTest, OneStub_OneTempDownServer) {
  TaskManager::GetInstance().Shutdown();
  TaskManager::GetInstance().Wait();

  using Xd::XdServer;
  using namespace std::chrono_literals;

  std::atomic_bool has_exited = false;

  std::string server_addr{"127.0.0.1:50011"};

  resource_t res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Up", index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Temp Down", index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeRecFromTempFailureCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} recovered from temporary failure", index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Down", index);
        has_exited = true;
      }));

  Sequence seq;
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeRecFromTempFailureCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .Times(1)
      .InSequence(seq);

  auto xd_server = std::make_unique<XdServer>(server_addr, res);
  std::this_thread::sleep_for(1s);

  auto result_future = m_keeper_->RegisterXdNode(server_addr, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  CtlXd::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 0);
  }

  std::this_thread::sleep_for(2s);
  xd_server->Shutdown();
  xd_server.reset();

  xd_server = std::make_unique<XdServer>(server_addr, res);
  std::this_thread::sleep_for(2s);
  xd_server->Shutdown();
  xd_server.reset();

  while (!has_exited) {
    std::this_thread::yield();
  }
}

// Note: Valgrind will conflict with pthread_barrier in such an abort case.
//  It may be caused by the bugs of pthread library or this test.
//  This test SHOULD be disabled when using valgrind.
//  See https://valgrind.org/docs/manual/hg-manual.html
//
// Todo: Check whether this case is buggy on pthread_barrier misuse. (using
//  Helgrind)
//
// The server is aborted. In such case, a GOAWAY message will never be
// send.
// e.g. Shutdown by power failure.
TEST_F(XdNodeKeeperTest, OneStub_OneTempAbortedServer) {
  using Xd::XdServer;
  using namespace std::chrono_literals;

  TaskManager::GetInstance().Shutdown();
  TaskManager::GetInstance().Wait();

  slurmx::SetCloseOnExecFromFd(STDERR_FILENO + 1);

  std::atomic_bool has_exited = false;

  boost::fibers::barrier xd_rec_barrier(2);

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Up", index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Temp Down", index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeRecFromTempFailureCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} recovered from temporary failure", index);
        xd_rec_barrier.wait();
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE("Node #{} is Down", index);
        has_exited = true;
      }));

  Sequence seq;
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeRecFromTempFailureCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .Times(1)
      .InSequence(seq);

  std::string server_addr{"127.0.0.1:50011"};

  resource_t res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  // Start server and client connects it.
  {
    struct TestIpc {
      TestIpc() : init_barrier(2), terminate_barrier(2) {}

      bi::barrier init_barrier;
      bi::barrier terminate_barrier;
    };

    bi::mapped_region region(bi::anonymous_shared_memory(sizeof(TestIpc)));
    TestIpc* ipc = new (region.get_address()) TestIpc;

    pid_t child_pid = fork();
    if (child_pid == 0) {  // Child
      auto xd_server = std::make_unique<XdServer>(server_addr, res);
      ipc->init_barrier.wait();

      ipc->terminate_barrier.wait();
      std::terminate();
    } else {
      ipc->init_barrier.wait();
      auto result_future =
          m_keeper_->RegisterXdNode(server_addr, &res, nullptr);
      result_future.wait();
      ASSERT_EQ(result_future.valid(), true);

      CtlXd::RegisterNodeResult result = result_future.get();
      EXPECT_EQ(result.allocated_node_index.has_value(), true);
      if (result.allocated_node_index.has_value()) {
        EXPECT_EQ(result.allocated_node_index.value(), 0);
      }

      ipc->terminate_barrier.wait();
      int stat;
      wait(&stat);
    }
    // ipc is destructed here.
  }

  // Restart server and wait for the client to reconnect.
  {
    struct TestIpc {
      TestIpc() : terminate_barrier(2) {}

      bi::barrier terminate_barrier;
    };

    bi::mapped_region region(bi::anonymous_shared_memory(sizeof(TestIpc)));
    TestIpc* ipc = new (region.get_address()) TestIpc;

    pid_t child_pid = fork();
    if (child_pid == 0) {  // Child
      auto xd_server = std::make_unique<XdServer>(server_addr, res);

      (void)xd_server.get();
      ipc->terminate_barrier.wait();
      std::terminate();
    } else {
      // Wait for the client to reconnect the server.
      xd_rec_barrier.wait();

      // Let child process terminate.
      ipc->terminate_barrier.wait();

      int stat;
      wait(&stat);
    }
    // ipc is destructed here.
  }

  while (!has_exited) {
    std::this_thread::yield();
  }
}

TEST_F(XdNodeKeeperTest, TwoStubs_TwoTempDownServers) {
  TaskManager::GetInstance().Shutdown();
  TaskManager::GetInstance().Wait();

  using Xd::XdServer;
  using namespace std::chrono_literals;
  using testing::AnyOf;

  std::string server_addr_0{"127.0.0.1:50011"};
  std::string server_addr_1{"127.0.0.1:50012"};

  resource_t res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  std::unique_ptr<boost::fibers::barrier> init_barrier_0;
  std::unique_ptr<boost::fibers::barrier> terminate_barrier_0;
  std::unique_ptr<boost::fibers::barrier> terminate_barrier_1;
  std::unique_ptr<boost::fibers::barrier> exit_barrier_all;

  std::atomic_uint disconnected_count = 0;
  std::atomic_uint exit_count = 0;

  init_barrier_0 = std::make_unique<boost::fibers::barrier>(2);
  terminate_barrier_0 = std::make_unique<boost::fibers::barrier>(2);
  terminate_barrier_1 = std::make_unique<boost::fibers::barrier>(2);

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(AnyOf(0, 1), _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE(RED "Node #{} is Up" RESET, index);

        if (index == 0) {
          terminate_barrier_0->wait();
        } else {
          terminate_barrier_1->wait();
        }
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(AnyOf(0, 1), _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE(RED "Node #{} is Temp Down" RESET, index);
        disconnected_count++;
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeRecFromTempFailureCb(0, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE(RED "Node #{} recovered from temporary failure" RESET,
                     index);
        terminate_barrier_0->wait();
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(AnyOf(0, 1), _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        SLURMX_TRACE(RED "Node #{} is Down" RESET, index);
        exit_count++;
      }));

  Sequence seq_0;
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeRecFromTempFailureCb(0, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .Times(1)
      .InSequence(seq_0);

  Sequence seq_1;
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(1, _))
      .Times(1)
      .InSequence(seq_1);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(1, _))
      .Times(1)
      .InSequence(seq_1);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(1, _))
      .Times(1)
      .InSequence(seq_1);

  std::thread t0([&] {
    auto xd_server = std::make_unique<XdServer>(server_addr_0, res);
    init_barrier_0->wait();

    // Wait for client stub 0 to connect.
    terminate_barrier_0->wait();
    xd_server->Shutdown();
  });

  // Wait for server 0 initialization.
  init_barrier_0->wait();

  auto result_future = m_keeper_->RegisterXdNode(server_addr_0, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  // Wait for Xd Node 0 registration result.
  CtlXd::RegisterNodeResult result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 0);
  }

  t0.join();

  std::thread t1([&] {
    auto xd_server = std::make_unique<XdServer>(server_addr_1, res);

    // Wait for client stub 1 to connect.
    terminate_barrier_1->wait();
    xd_server->Shutdown();
  });

  result_future = m_keeper_->RegisterXdNode(server_addr_1, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  // Wait for Xd Node 1 registration result.
  result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 1);
  }

  t1.join();

  // Wait for Xd Node 0,1 to encounter temporary failure.
  while (disconnected_count < 2) std::this_thread::yield();
  terminate_barrier_0 = std::make_unique<boost::fibers::barrier>(2);

  std::thread t0_restart([&] {
    auto xd_server = std::make_unique<XdServer>(server_addr_0, res);

    // Wait for client stub 0 to re-connect.
    terminate_barrier_0->wait();
    xd_server->Shutdown();
  });
  t0_restart.join();

  while (exit_count < 2) std::this_thread::yield();
}

TEST_F(XdNodeKeeperTest, CheckReuseOfSlot) {
  TaskManager::GetInstance().Shutdown();
  TaskManager::GetInstance().Wait();

  using Xd::XdServer;
  using namespace std::chrono_literals;

  std::string server_addr_0{"127.0.0.1:50011"};
  std::string server_addr_1{"127.0.0.1:50012"};
  std::string server_addr_2{"127.0.0.1:50013"};

  resource_t res;
  res.cpu_count = 10;
  res.memory_bytes = 1024 * 1024 * 1024;
  res.memory_sw_bytes = 1024 * 1024 * 1024;

  std::vector<std::unique_ptr<boost::fibers::barrier>> terminate_barriers;
  for (int i = 0; i < 3; i++)
    terminate_barriers.emplace_back(
        std::make_unique<boost::fibers::barrier>(2));

  auto restart_barrier_1 = std::make_unique<boost::fibers::barrier>(2);
  bool has_restarted_1 = false;
  uint start_count = 0;

  std::atomic_uint exit_count = 0;

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(_, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        ASSERT_THAT(index, AnyOf(0, 1, 2));

        SLURMX_TRACE(RED "Node #{} is Up" RESET, index);

        start_count++;
        if (start_count >= 3 && !has_restarted_1) {
          SLURMX_TRACE(RED "Terminate Node #1 ..." RESET);
          terminate_barriers[1]->wait();
        } else if (has_restarted_1) {
          for (auto&& i : {0, 1, 2}) terminate_barriers[i]->wait();
        }
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(_, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        ASSERT_THAT(index, AnyOf(0, 1, 2));
        SLURMX_TRACE(RED "Node #{} is Temp Down" RESET, index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeRecFromTempFailureCb(_, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        ASSERT_THAT(index, AnyOf(0, 1, 2));
        SLURMX_TRACE(RED "Node #{} recovered from temporary failure" RESET,
                     index);
      }));

  ON_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(_, _))
      .WillByDefault(Invoke([&](uint32_t index, void*) {
        ASSERT_THAT(index, AnyOf(0, 1, 2));
        SLURMX_TRACE(RED "Node #{} is Down" RESET, index);
        if (!has_restarted_1 && index == 1) {
          SLURMX_TRACE(RED "Restarting Node #1 ..." RESET);
          restart_barrier_1->wait();
          has_restarted_1 = true;
        } else if (has_restarted_1) {
          exit_count++;
        }
      }));

  Sequence seq_0;
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(0, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(0, _))
      .Times(1)
      .InSequence(seq_0);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(0, _))
      .Times(1)
      .InSequence(seq_0);

  Sequence seq_2;
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(2, _))
      .Times(1)
      .InSequence(seq_2);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(2, _))
      .Times(1)
      .InSequence(seq_2);
  EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(2, _))
      .Times(1)
      .InSequence(seq_2);

  Sequence seq_1;
  for (int i = 0; i < 2; i++) {
    EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsUpCb(1, _))
        .Times(1)
        .InSequence(seq_1);
    EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsTempDownCb(1, _))
        .Times(1)
        .InSequence(seq_1);
    EXPECT_CALL((*m_mock_ctlxd_server_), XdNodeIsDownCb(1, _))
        .Times(1)
        .InSequence(seq_1);
  }

  std::thread t0([&] {
    auto xd_server = std::make_unique<XdServer>(server_addr_0, res);
    terminate_barriers[0]->wait();
    xd_server->Shutdown();
  });

  // Server 0 and 2 serve as the slot occupier and they will be shut down at the
  // same time.
  std::thread t2([&] {
    auto xd_server = std::make_unique<XdServer>(server_addr_2, res);
    terminate_barriers[2]->wait();
    xd_server->Shutdown();
  });

  std::thread t1;
  t1 = std::thread([&] {
    auto xd_server = std::make_unique<XdServer>(server_addr_1, res);
    terminate_barriers[1]->wait();
    xd_server->Shutdown();
  });

  std::future<CtlXd::RegisterNodeResult> result_future;
  CtlXd::RegisterNodeResult result;

  result_future = m_keeper_->RegisterXdNode(server_addr_0, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 0);
  }

  result_future = m_keeper_->RegisterXdNode(server_addr_1, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 1);
  }

  result_future = m_keeper_->RegisterXdNode(server_addr_2, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 2);
  }

  t1.join();
  restart_barrier_1->wait();

  terminate_barriers[1] = std::make_unique<boost::fibers::barrier>(2);
  t1 = std::thread([&] {
    auto xd_server = std::make_unique<XdServer>(server_addr_1, res);
    terminate_barriers[1]->wait();
    xd_server->Shutdown();
  });

  result_future = m_keeper_->RegisterXdNode(server_addr_1, &res, nullptr);
  result_future.wait();
  ASSERT_EQ(result_future.valid(), true);

  result = result_future.get();
  EXPECT_EQ(result.allocated_node_index.has_value(), true);
  if (result.allocated_node_index.has_value()) {
    EXPECT_EQ(result.allocated_node_index.value(), 1);
  }

  while (exit_count < 3) {
    std::this_thread::yield();
  }

  t0.join();
  t1.join();
  t2.join();
}
