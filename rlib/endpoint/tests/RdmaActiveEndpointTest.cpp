#include <chrono>
#include <cstring>
#include <numeric>
#include <string>
#include <vector>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/system/ThreadName.h>
#include <gtest/gtest.h>

#include "rlib/endpoint/RdmaActiveEndpointGroup.h"
#include "rlib/verbs/tests/TestUtil.h"

namespace rlib {
namespace {

using namespace std::chrono_literals;

constexpr int kRecvs = 16;
constexpr uint32_t kMessageSize = 256;

// Posts kRecvs receives in init(), whose completions go to the default
// dispatchCqEvent; has a registered send buffer.
class TestEndpoint : public RdmaActiveEndpoint {
 public:
  using RdmaActiveEndpoint::RdmaActiveEndpoint;

  // Sends 'size' bytes of the send buffer and waits for the completion.
  folly::coro::Task<ibv_wc> co_send(uint32_t size) {
    ibv_sge sge{
        reinterpret_cast<uintptr_t>(sendBuffer.data()), size, sendMr->lkey()};
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    co_return co_await co_postSend(wr);
  }

  std::vector<char> recvBuffer = std::vector<char>(kRecvs * kMessageSize);
  std::vector<char> sendBuffer = std::vector<char>(kMessageSize);
  std::optional<MemoryRegion> recvMr;
  std::optional<MemoryRegion> sendMr;

 protected:
  void init() override {
    recvMr.emplace(registerMemory(recvBuffer.data(), recvBuffer.size()));
    sendMr.emplace(registerMemory(sendBuffer.data(), sendBuffer.size()));
    for (int i = 0; i < kRecvs; ++i) {
      ibv_sge sge{
          reinterpret_cast<uintptr_t>(recvBuffer.data()) + i * kMessageSize,
          kMessageSize,
          recvMr->lkey()};
      ibv_recv_wr wr{};
      wr.wr_id = i;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      ASSERT_EQ(postRecv(wr), 0);
    }
  }
};

using Group = RdmaActiveEndpointGroup<TestEndpoint>;

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

// Parameter: polling CQ threads.
class RdmaActiveEndpointTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    auto address = test::rdmaAddress();
    if (!address) {
      GTEST_SKIP() << "no RDMA device with an IPv4 address";
    }
    serverGroup_ = makeGroup();
    clientGroup_ = makeGroup();
    server_ = serverGroup_->createServerEndpoint();
    server_->bind(*address, 64);
  }

  void TearDown() override {
    if (clientGroup_) {
      folly::coro::blockingWait(clientGroup_->co_close());
      folly::coro::blockingWait(serverGroup_->co_close());
    }
  }

  std::unique_ptr<Group> makeGroup() {
    auto group = std::make_unique<Group>(GetParam(), 256, 1, 2048);
    group->connParam().rnrRetryCount = 7;
    group->init(
        std::make_unique<RdmaDefaultEndpointFactory<TestEndpoint>>(*group));
    return group;
  }

  std::pair<std::shared_ptr<TestEndpoint>, std::shared_ptr<TestEndpoint>>
  connect() {
    auto client = clientGroup_->createEndpoint();
    auto serverAddress = server_->srcAddr();
    auto [accepted, unit] =
        folly::coro::blockingWait(folly::coro::collectAll(
            server_->co_accept(), client->co_connect(serverAddress, 2000ms)));
    return {accepted, client};
  }

  std::unique_ptr<Group> serverGroup_;
  std::unique_ptr<Group> clientGroup_;
  std::shared_ptr<RdmaServerEndpoint<TestEndpoint>> server_;
};

TEST_P(RdmaActiveEndpointTest, SendsAndReceivesThroughDispatchCqEvent) {
  auto [server, client] = connect();
  for (int i = 0; i < 5; ++i) {
    auto message = "message " + std::to_string(i);
    std::memcpy(client->sendBuffer.data(), message.data(), message.size());
    auto sent = folly::coro::blockingWait(client->co_send(message.size()));
    ASSERT_EQ(sent.status, IBV_WC_SUCCESS);
    EXPECT_EQ(sent.opcode, IBV_WC_SEND);

    auto received = folly::coro::blockingWait(server->co_nextCqEvent());
    ASSERT_EQ(received.status, IBV_WC_SUCCESS);
    EXPECT_EQ(received.opcode, IBV_WC_RECV);
    EXPECT_EQ(received.wr_id, static_cast<uint64_t>(i));
    EXPECT_EQ(
        std::string(
            server->recvBuffer.data() + i * kMessageSize, received.byte_len),
        message);
  }
}

TEST_P(RdmaActiveEndpointTest, RunsManyConcurrentReadsAndWrites) {
  auto [server, client] = connect();
  constexpr uint32_t kChunk = 4096;
  constexpr int kOps = 200;
  std::vector<uint8_t> data(kChunk * kOps);
  std::iota(data.begin(), data.end(), 0);
  auto dataMr = server->registerMemory(data.data(), data.size());

  BufferInfo info{
      reinterpret_cast<uintptr_t>(data.data()),
      static_cast<uint32_t>(data.size()),
      dataMr.rkey()};
  std::memcpy(server->sendBuffer.data(), &info, sizeof(info));
  ASSERT_EQ(
      folly::coro::blockingWait(server->co_send(sizeof(info))).status,
      IBV_WC_SUCCESS);
  auto received = folly::coro::blockingWait(client->co_nextCqEvent());
  ASSERT_EQ(received.status, IBV_WC_SUCCESS);
  BufferInfo remote;
  std::memcpy(&remote, client->recvBuffer.data(), sizeof(remote));

  // Reads of all chunks at once, into reversed positions.
  std::vector<uint8_t> local(data.size());
  auto localMr = client->registerMemory(local.data(), local.size());
  std::vector<folly::coro::Task<int>> reads;
  for (int i = 0; i < kOps; ++i) {
    reads.push_back(client->co_read(
        localMr, (kOps - 1 - i) * kChunk, kChunk, remote.addr + i * kChunk,
        remote.rkey));
  }
  auto readResults =
      folly::coro::blockingWait(folly::coro::collectAllRange(std::move(reads)));
  for (int i = 0; i < kOps; ++i) {
    ASSERT_EQ(readResults[i], 0) << i;
    ASSERT_EQ(
        std::memcmp(
            local.data() + (kOps - 1 - i) * kChunk,
            data.data() + i * kChunk,
            kChunk),
        0)
        << i;
  }

  // Writes of all chunks at once, each filled with its index.
  for (int i = 0; i < kOps; ++i) {
    std::memset(local.data() + i * kChunk, i, kChunk);
  }
  std::vector<folly::coro::Task<int>> writes;
  for (int i = 0; i < kOps; ++i) {
    writes.push_back(client->co_write(
        localMr, i * kChunk, kChunk, remote.addr + i * kChunk, remote.rkey));
  }
  auto writeResults = folly::coro::blockingWait(
      folly::coro::collectAllRange(std::move(writes)));
  for (int i = 0; i < kOps; ++i) {
    ASSERT_EQ(writeResults[i], 0) << i;
    for (uint32_t j = 0; j < kChunk; j += 512) {
      ASSERT_EQ(data[i * kChunk + j], static_cast<uint8_t>(i)) << i;
    }
  }

  EXPECT_EQ(
      folly::coro::blockingWait(
          client->co_read(localMr, 0, 16, remote.addr, remote.rkey + 1)),
      EIO);
}

TEST_P(RdmaActiveEndpointTest, EndpointsShareOneCqProcessorPerDevice) {
  constexpr int kEndpoints = 8;
  std::vector<std::shared_ptr<TestEndpoint>> servers, clients;
  for (int i = 0; i < kEndpoints; ++i) {
    auto [server, client] = connect();
    servers.push_back(server);
    clients.push_back(client);
  }
  for (int i = 1; i < kEndpoints; ++i) {
    EXPECT_EQ(clients[i]->cqProvider(), clients[0]->cqProvider());
    EXPECT_EQ(servers[i]->cqProvider(), servers[0]->cqProvider());
  }
  EXPECT_NE(clients[0]->cqProvider(), servers[0]->cqProvider());

  // Every client sends to its server at once; each server gets its own.
  std::vector<folly::coro::Task<ibv_wc>> sends;
  for (int i = 0; i < kEndpoints; ++i) {
    std::memset(clients[i]->sendBuffer.data(), 'a' + i, 8);
    sends.push_back(clients[i]->co_send(8));
  }
  for (auto& wc :
       folly::coro::blockingWait(folly::coro::collectAllRange(std::move(sends)))) {
    EXPECT_EQ(wc.status, IBV_WC_SUCCESS);
  }
  for (int i = 0; i < kEndpoints; ++i) {
    auto wc = folly::coro::blockingWait(servers[i]->co_nextCqEvent());
    ASSERT_EQ(wc.status, IBV_WC_SUCCESS);
    EXPECT_EQ(wc.byte_len, 8u);
    EXPECT_EQ(servers[i]->recvBuffer[0], 'a' + i);
  }
}

// Awaited receives that nothing satisfies.
folly::coro::Task<std::vector<ibv_wc>> co_pendingRecvs(
    TestEndpoint& endpoint,
    int count) {
  std::vector<folly::coro::Task<ibv_wc>> recvs;
  for (int i = 0; i < count; ++i) {
    recvs.push_back(folly::coro::co_invoke(
        [&endpoint]() -> folly::coro::Task<ibv_wc> {
          ibv_sge sge{
              reinterpret_cast<uintptr_t>(endpoint.sendBuffer.data()),
              kMessageSize,
              endpoint.sendMr->lkey()};
          ibv_recv_wr wr{};
          wr.sg_list = &sge;
          wr.num_sge = 1;
          co_return co_await endpoint.co_postRecv(wr);
        }));
  }
  co_return co_await folly::coro::collectAllRange(std::move(recvs));
}

void expectFlushed(const std::vector<ibv_wc>& wcs) {
  for (auto& wc : wcs) {
    EXPECT_EQ(wc.status, IBV_WC_WR_FLUSH_ERR);
  }
}

TEST_P(RdmaActiveEndpointTest, CloseFailsOutstandingOperations) {
  auto [server, client] = connect();
  auto close = [&]() -> folly::coro::Task<void> {
    co_await folly::coro::sleep(50ms);
    co_await client->co_close();
  };
  auto [wcs, unit] = folly::coro::blockingWait(
      folly::coro::collectAll(co_pendingRecvs(*client, 32), close()));
  ASSERT_EQ(wcs.size(), 32u);
  expectFlushed(wcs);
}

TEST_P(RdmaActiveEndpointTest, PeerCloseFailsOutstandingOperations) {
  auto [server, client] = connect();
  auto close = [&]() -> folly::coro::Task<void> {
    co_await folly::coro::sleep(50ms);
    co_await server->co_close();
  };
  auto [wcs, unit] = folly::coro::blockingWait(
      folly::coro::collectAll(co_pendingRecvs(*client, 32), close()));
  expectFlushed(wcs);
  EXPECT_TRUE(client->isClosed());
}

TEST_P(RdmaActiveEndpointTest, GroupCloseFailsOutstandingOperations) {
  auto [server, client] = connect();
  auto [server2, client2] = connect();
  auto close = [&]() -> folly::coro::Task<void> {
    co_await folly::coro::sleep(50ms);
    co_await clientGroup_->co_close();
  };
  auto [wcs, wcs2, unit] = folly::coro::blockingWait(folly::coro::collectAll(
      co_pendingRecvs(*client, 8), co_pendingRecvs(*client2, 8), close()));
  expectFlushed(wcs);
  expectFlushed(wcs2);
  EXPECT_TRUE(clientGroup_->isClosed());
  EXPECT_TRUE(folly::coro::blockingWait(co_eventually(
      [&] { return server->isClosed() && server2->isClosed(); })));
}

TEST_P(RdmaActiveEndpointTest, ResumesOnCallersExecutor) {
  auto [server, client] = connect();
  folly::CPUThreadPoolExecutor pool{
      2, std::make_shared<folly::NamedThreadFactory>("caller")};
  auto send = [&]() -> folly::coro::Task<bool> {
    bool onPool = true;
    for (int i = 0; i < 10; ++i) {
      auto wc = co_await client->co_send(8);
      onPool = onPool && wc.status == IBV_WC_SUCCESS &&
          folly::getCurrentThreadName().value_or("").starts_with("caller");
    }
    co_return onPool;
  };
  auto receive = [&]() -> folly::coro::Task<bool> {
    bool onPool = true;
    for (int i = 0; i < 10; ++i) {
      co_await server->co_nextCqEvent();
      onPool = onPool &&
          folly::getCurrentThreadName().value_or("").starts_with("caller");
    }
    co_return onPool;
  };
  auto [sent, received] = folly::coro::blockingWait(
      folly::coro::co_withExecutor(
          &pool, folly::coro::collectAll(send(), receive())));
  EXPECT_TRUE(sent);
  EXPECT_TRUE(received);
}

INSTANTIATE_TEST_SUITE_P(
    CqModes,
    RdmaActiveEndpointTest,
    testing::Values(false, true),
    [](const testing::TestParamInfo<bool>& info) {
      return info.param ? "Polling" : "Blocking";
    });

} // namespace
} // namespace rlib
