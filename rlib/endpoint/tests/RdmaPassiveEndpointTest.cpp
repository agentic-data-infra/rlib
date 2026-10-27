#include <chrono>
#include <cstring>
#include <functional>
#include <numeric>
#include <string>
#include <vector>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/system/ThreadName.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <gtest/gtest.h>

#include "rlib/endpoint/RdmaPassiveEndpointGroup.h"
#include "rlib/verbs/RdmaError.h"
#include "rlib/verbs/RdmaAddress.h"

namespace rlib {
namespace {

using namespace std::chrono_literals;
using Group = RdmaPassiveEndpointGroup<RdmaEndpoint>;

// What the server tells the client about its buffer.
struct BufferInfo {
  uint64_t addr;
  uint32_t length;
  uint32_t rkey;
};

folly::coro::Task<bool> co_eventually(std::function<bool()> condition) {
  for (int i = 0; i < 300; ++i) {
    if (condition()) {
      co_return true;
    }
    co_await folly::coro::sleep(10ms);
  }
  co_return condition();
}

// Polls one completion of the endpoint's CQ.
folly::coro::Task<ibv_wc> co_pollOne(RdmaEndpoint& endpoint) {
  ibv_wc wc{};
  std::span<ibv_wc> wcs{&wc, 1};
  int n = co_await endpoint.cqProvider()->co_poll(wcs);
  EXPECT_EQ(n, 1);
  co_return wc;
}

int postRecv(RdmaEndpoint& endpoint, const MemoryRegion& mr, uint32_t length) {
  ibv_sge sge{reinterpret_cast<uintptr_t>(mr.addr()), length, mr.lkey()};
  ibv_recv_wr wr{};
  wr.wr_id = 1;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  return endpoint.postRecv(wr);
}

int postSend(RdmaEndpoint& endpoint, const MemoryRegion& mr, uint32_t length) {
  ibv_sge sge{reinterpret_cast<uintptr_t>(mr.addr()), length, mr.lkey()};
  ibv_send_wr wr{};
  wr.wr_id = 2;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED;
  return endpoint.postSend(wr);
}

class RdmaPassiveEndpointTest : public testing::Test {
 protected:
  void SetUp() override {
    auto address = rdmaAddress();
    if (!address) {
      GTEST_SKIP() << "no RDMA device with an IPv4 address";
    }
    address_ = *address;
    serverGroup_ = Group::createDefaultGroup(16, 1, 32);
    clientGroup_ = Group::createDefaultGroup(16, 1, 32);
    // Senders retry while the receiver has not posted a receive yet.
    serverGroup_->connParam().rnrRetryCount = 7;
    clientGroup_->connParam().rnrRetryCount = 7;
    server_ = serverGroup_->createServerEndpoint();
    server_->bind(address_, 16);
  }

  void TearDown() override {
    if (clientGroup_) {
      folly::coro::blockingWait(clientGroup_->co_close());
      folly::coro::blockingWait(serverGroup_->co_close());
    }
  }

  // Accepts on the server while a client connects; returns both endpoints.
  std::pair<std::shared_ptr<RdmaEndpoint>, std::shared_ptr<RdmaEndpoint>>
  connect() {
    auto client = clientGroup_->createEndpoint();
    auto serverAddress = server_->srcAddr();
    auto [accepted, unit] =
        folly::coro::blockingWait(folly::coro::collectAll(
            server_->co_accept(), client->co_connect(serverAddress, 2000ms)));
    return {accepted, client};
  }

  SocketAddress address_;
  std::unique_ptr<Group> serverGroup_;
  std::unique_ptr<Group> clientGroup_;
  std::shared_ptr<RdmaServerEndpoint<RdmaEndpoint>> server_;
};

TEST_F(RdmaPassiveEndpointTest, ConnectsAcceptsAndCloses) {
  EXPECT_TRUE(server_->isBound());
  EXPECT_NE(server_->srcAddr().port(), 0);
  EXPECT_NE(server_->pd(), nullptr);

  auto [server, client] = connect();
  EXPECT_TRUE(server->isConnected());
  EXPECT_TRUE(client->isConnected());
  EXPECT_TRUE(server->isServerSide());
  EXPECT_FALSE(client->isServerSide());
  EXPECT_EQ(client->dstAddr(), server_->srcAddr());
  EXPECT_EQ(server->dstAddr(), client->srcAddr());
  // One protection domain per device.
  EXPECT_EQ(server->pd(), client->pd());
  EXPECT_EQ(server->pd(), server_->pd());
  EXPECT_NE(server->qp(), nullptr);
  EXPECT_EQ(client->cqProvider()->cqSize(), 32);

  folly::coro::blockingWait(client->co_close());
  folly::coro::blockingWait(server->co_close());
  // Idempotent.
  folly::coro::blockingWait(client->co_close());
}

TEST_F(RdmaPassiveEndpointTest, SendsAndReceives) {
  auto [server, client] = connect();
  std::vector<char> serverBuffer(4096), clientBuffer(4096);
  auto serverMr = server->registerMemory(serverBuffer.data(), 4096);
  auto clientMr = client->registerMemory(clientBuffer.data(), 4096);

  ASSERT_EQ(postRecv(*server, serverMr, 4096), 0);
  std::string message = "hello over rxe";
  std::memcpy(clientBuffer.data(), message.data(), message.size());
  ASSERT_EQ(postSend(*client, clientMr, message.size()), 0);

  auto [received, sent] = folly::coro::blockingWait(
      folly::coro::collectAll(co_pollOne(*server), co_pollOne(*client)));
  EXPECT_EQ(received.status, IBV_WC_SUCCESS);
  EXPECT_EQ(received.opcode, IBV_WC_RECV);
  EXPECT_EQ(received.byte_len, message.size());
  EXPECT_EQ(std::string(serverBuffer.data(), received.byte_len), message);
  EXPECT_EQ(sent.status, IBV_WC_SUCCESS);
  EXPECT_EQ(sent.opcode, IBV_WC_SEND);
  EXPECT_EQ(sent.wr_id, 2u);
}

TEST_F(RdmaPassiveEndpointTest, ReadsAndWritesRemoteMemory) {
  auto [server, client] = connect();
  constexpr uint32_t kSize = 64 << 10;
  std::vector<uint8_t> data(kSize);
  std::iota(data.begin(), data.end(), 0);
  auto dataMr = server->registerMemory(data.data(), kSize);
  BufferInfo info{reinterpret_cast<uintptr_t>(data.data()), kSize, dataMr.rkey()};

  // The server sends its buffer's address, length and key.
  BufferInfo serverInfo = info;
  BufferInfo clientInfo{};
  auto serverInfoMr = server->registerMemory(&serverInfo, sizeof(serverInfo));
  auto clientInfoMr = client->registerMemory(&clientInfo, sizeof(clientInfo));
  ASSERT_EQ(postRecv(*client, clientInfoMr, sizeof(clientInfo)), 0);
  ASSERT_EQ(postSend(*server, serverInfoMr, sizeof(serverInfo)), 0);
  auto [received, sent] = folly::coro::blockingWait(
      folly::coro::collectAll(co_pollOne(*client), co_pollOne(*server)));
  ASSERT_EQ(received.status, IBV_WC_SUCCESS);
  ASSERT_EQ(sent.status, IBV_WC_SUCCESS);
  EXPECT_EQ(clientInfo.addr, info.addr);
  EXPECT_EQ(clientInfo.rkey, info.rkey);

  std::vector<uint8_t> local(kSize);
  auto localMr = client->registerMemory(local.data(), kSize);

  // Read [1000, 5000) of the server's buffer into local[100...].
  int error = folly::coro::blockingWait(
      client->co_read(localMr, 100, 4000, clientInfo.addr + 1000, clientInfo.rkey));
  ASSERT_EQ(error, 0);
  EXPECT_EQ(std::memcmp(local.data() + 100, data.data() + 1000, 4000), 0);
  EXPECT_EQ(local[99], 0);

  // Write local[0, 3000) over [20000, 23000) of the server's buffer.
  std::fill(local.begin(), local.begin() + 3000, 0xab);
  error = folly::coro::blockingWait(
      client->co_write(localMr, 0, 3000, clientInfo.addr + 20000, clientInfo.rkey));
  ASSERT_EQ(error, 0);
  for (uint32_t i = 19990; i < 23010; ++i) {
    uint8_t expected = i >= 20000 && i < 23000 ? 0xab : static_cast<uint8_t>(i);
    ASSERT_EQ(data[i], expected) << i;
  }

  // Ranges outside the local region, and a bad key.
  error = folly::coro::blockingWait(
      client->co_read(localMr, kSize - 10, 11, clientInfo.addr, clientInfo.rkey));
  EXPECT_EQ(error, EINVAL);
  error = folly::coro::blockingWait(
      client->co_read(localMr, 0, 16, clientInfo.addr, clientInfo.rkey + 1));
  EXPECT_EQ(error, EIO);
}

TEST_F(RdmaPassiveEndpointTest, ClientDisconnects) {
  auto [server, client] = connect();
  folly::coro::blockingWait(client->co_close());
  EXPECT_TRUE(folly::coro::blockingWait(
      co_eventually([&server] { return server->isClosed(); })));
  EXPECT_FALSE(server->isConnected());
  folly::coro::blockingWait(server->co_close());
}

TEST_F(RdmaPassiveEndpointTest, ServerDisconnects) {
  auto [server, client] = connect();
  folly::coro::blockingWait(server->co_close());
  EXPECT_TRUE(folly::coro::blockingWait(
      co_eventually([&client] { return client->isClosed(); })));
  folly::coro::blockingWait(client->co_close());
}

TEST_F(RdmaPassiveEndpointTest, GroupCloseClosesLiveEndpoints) {
  auto [server, client] = connect();
  auto [server2, client2] = connect();
  folly::coro::blockingWait(clientGroup_->co_close());
  EXPECT_TRUE(clientGroup_->isClosed());
  EXPECT_TRUE(folly::coro::blockingWait(co_eventually([&] {
    return server->isClosed() && server2->isClosed();
  })));
  folly::coro::blockingWait(serverGroup_->co_close());
  EXPECT_TRUE(server_->isClosed());
  // Closing again, as the endpoints' owners still may, does nothing.
  folly::coro::blockingWait(client->co_close());
  folly::coro::blockingWait(server->co_close());
}

TEST_F(RdmaPassiveEndpointTest, CloseFailsPendingAccept) {
  auto accept = [&]() -> folly::coro::Task<void> {
    EXPECT_THROW(co_await server_->co_accept(), RdmaError);
  };
  auto close = [&]() -> folly::coro::Task<void> {
    co_await folly::coro::sleep(50ms);
    co_await server_->co_close();
  };
  folly::coro::blockingWait(folly::coro::collectAll(accept(), close()));
}

TEST_F(RdmaPassiveEndpointTest, ConnectToNoListenerFails) {
  auto client = clientGroup_->createEndpoint();
  auto serverAddress = server_->srcAddr();
  folly::coro::blockingWait(server_->co_close());
  try {
    folly::coro::blockingWait(client->co_connect(serverAddress, 2000ms));
    FAIL() << "connected";
  } catch (const RdmaError& e) {
    EXPECT_EQ(e.code().value(), ECONNREFUSED) << e.what();
  }
  EXPECT_TRUE(client->isClosed());
}

TEST_F(RdmaPassiveEndpointTest, ResumesOnCallersExecutor) {
  folly::CPUThreadPoolExecutor pool{
      1, std::make_shared<folly::NamedThreadFactory>("caller")};
  auto onPool = []() {
    return folly::getCurrentThreadName().value_or("").starts_with("caller");
  };
  auto client = clientGroup_->createEndpoint();
  auto serverAddress = server_->srcAddr();
  auto accept = [&]() -> folly::coro::Task<bool> {
    auto endpoint = co_await server_->co_accept();
    co_return onPool() && endpoint->isConnected();
  };
  auto connect = [&]() -> folly::coro::Task<bool> {
    co_await client->co_connect(serverAddress, 2000ms);
    co_return onPool();
  };
  auto close = [&]() -> folly::coro::Task<bool> {
    co_await client->co_close();
    co_return onPool();
  };
  auto [accepted, connected] = folly::coro::blockingWait(
      folly::coro::co_withExecutor(
          &pool, folly::coro::collectAll(accept(), connect())));
  EXPECT_TRUE(accepted);
  EXPECT_TRUE(connected);
  EXPECT_TRUE(folly::coro::blockingWait(
      folly::coro::co_withExecutor(&pool, close())));
}

TEST_F(RdmaPassiveEndpointTest, AcceptsManyConnections) {
  std::vector<std::shared_ptr<RdmaEndpoint>> endpoints;
  for (int i = 0; i < 8; ++i) {
    auto [server, client] = connect();
    EXPECT_TRUE(server->isConnected());
    endpoints.push_back(server);
    endpoints.push_back(client);
  }
}

} // namespace
} // namespace rlib
