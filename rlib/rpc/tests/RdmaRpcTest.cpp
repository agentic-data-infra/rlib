#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include "rlib/rpc/RdmaRpcClientGroup.h"
#include "rlib/rpc/RdmaRpcServerGroup.h"
#include "rlib/verbs/RdmaAddress.h"

namespace rlib {
namespace {

using namespace std::chrono_literals;

// A value and up to 244 bytes of data.
struct TestMessage {
  static constexpr size_t kHeader = sizeof(uint64_t) + sizeof(uint32_t);
  static constexpr size_t kSize = 256;

  uint64_t value = 0;
  uint32_t length = 0;
  std::array<std::byte, kSize - kHeader> data{};

  size_t write(std::span<std::byte> out) const {
    size_t size = kHeader + length;
    if (size > out.size() || length > data.size()) {
      return std::max(size, out.size() + 1);
    }
    std::memcpy(out.data(), &value, sizeof(value));
    std::memcpy(out.data() + sizeof(value), &length, sizeof(length));
    std::memcpy(out.data() + kHeader, data.data(), length);
    return size;
  }

  void update(std::span<const std::byte> in) {
    std::memcpy(&value, in.data(), sizeof(value));
    std::memcpy(&length, in.data() + sizeof(value), sizeof(length));
    std::memcpy(data.data(), in.data() + kHeader, length);
  }
};

struct TestProtocol {
  using Request = TestMessage;
  using Response = TestMessage;
};

using Server = RdmaRpcServerGroup<TestProtocol>;
using Client = RdmaRpcClientGroup<TestProtocol>;
using ClientEndpoint = RdmaRpcClientEndpoint<TestProtocol>;

TestMessage makeRequest(uint64_t value, uint32_t length = 0) {
  TestMessage message;
  message.value = value;
  message.length = length;
  for (uint32_t i = 0; i < length; ++i) {
    message.data[i] = static_cast<std::byte>(value + i);
  }
  return message;
}

// Answers value + 1 with the request's data, after 'delay' and once 'gate'
// is open if 'useGate'.
class TestService : public RdmaRpcService<TestProtocol> {
 public:
  folly::coro::Task<void> co_process(
      RdmaRpcServerEvent<TestProtocol>& event) override {
    ++arrived;
    if (delay.count() > 0) {
      co_await folly::coro::sleep(delay);
    }
    if (useGate) {
      co_await gate;
    }
    event.response() = event.request();
    event.response().value = event.request().value + 1;
  }

  void open(RdmaRpcServerEndpoint<TestProtocol>& endpoint) override {
    ++opens;
    std::lock_guard lock{mutex};
    clusters.insert(endpoint.clusterId());
  }
  void close(RdmaRpcServerEndpoint<TestProtocol>&) override {
    ++closes;
  }

  std::chrono::milliseconds delay{0};
  bool useGate = false;
  folly::coro::Baton gate;
  std::atomic<int> arrived{0};
  std::atomic<int> opens{0};
  std::atomic<int> closes{0};
  std::mutex mutex;
  std::set<int> clusters;
};

folly::coro::Task<bool> co_eventually(std::function<bool()> condition) {
  for (int i = 0; i < 500; ++i) {
    if (condition()) {
      co_return true;
    }
    co_await folly::coro::sleep(10ms);
  }
  co_return condition();
}

void expectResponse(const TestMessage& response, const TestMessage& request) {
  EXPECT_EQ(response.value, request.value + 1);
  ASSERT_EQ(response.length, request.length);
  EXPECT_EQ(
      std::memcmp(response.data.data(), request.data.data(), request.length),
      0);
}

// Parameters: polling CQ threads, inline dispatch.
class RdmaRpcTest : public testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  void SetUp() override {
    address_ = rdmaAddress();
    if (!address_) {
      GTEST_SKIP() << "no RDMA device with an IPv4 address";
    }
  }

  void TearDown() override {
    service_.gate.post();
    if (client_) {
      folly::coro::blockingWait(client_->co_close());
    }
    if (server_) {
      folly::coro::blockingWait(server_->co_close());
    }
  }

  void start(RdmaRpcServerOptions serverOptions = {}, RdmaRpcOptions clientOptions = {}) {
    serverOptions.polling = std::get<0>(GetParam());
    serverOptions.inlineDispatch = std::get<1>(GetParam());
    server_ = Server::create(service_, serverOptions);
    server_->connParam().retryCount = 7;
    server_->connParam().rnrRetryCount = 7;
    server_->serve(*address_);
    client_ = Client::create(clientOptions);
    client_->connParam().retryCount = 7;
    client_->connParam().rnrRetryCount = 7;
  }

  std::shared_ptr<ClientEndpoint> connect() {
    auto endpoint = client_->createEndpoint();
    folly::coro::blockingWait(endpoint->co_connect(server_->address(), 2000ms));
    return endpoint;
  }

  static folly::coro::Task<void> co_check(
      ClientEndpoint& endpoint,
      uint64_t value,
      uint32_t length = 0) {
    auto request = makeRequest(value, length);
    TestMessage response;
    co_await endpoint.co_call(request, response);
    expectResponse(response, request);
  }

  std::optional<SocketAddress> address_;
  TestService service_;
  std::unique_ptr<Server> server_;
  std::unique_ptr<Client> client_;
};

TEST_P(RdmaRpcTest, AnswersACall) {
  start();
  auto endpoint = connect();
  folly::coro::blockingWait(co_check(*endpoint, 41));
  EXPECT_EQ(endpoint->messagesSent(), 1u);
  EXPECT_EQ(endpoint->messagesReceived(), 1u);
  EXPECT_EQ(endpoint->pendingCalls(), 0);
}

TEST_P(RdmaRpcTest, AnswersManySequentialCalls) {
  start();
  auto endpoint = connect();
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    for (int i = 0; i < 10000; ++i) {
      co_await co_check(*endpoint, i);
    }
  }());
  EXPECT_EQ(endpoint->messagesReceived(), 10000u);
}

TEST_P(RdmaRpcTest, AnswersManyConcurrentCallsFromManyThreads) {
  start();
  auto endpoint = connect();
  folly::CPUThreadPoolExecutor pool{4};
  std::vector<folly::coro::TaskWithExecutor<void>> calls;
  for (int i = 0; i < 1000; ++i) {
    calls.push_back(folly::coro::co_withExecutor(
        folly::getKeepAliveToken(pool), co_check(*endpoint, i)));
  }
  folly::coro::blockingWait(folly::coro::collectAllRange(std::move(calls)));
  EXPECT_EQ(endpoint->messagesReceived(), 1000u);
  EXPECT_EQ(endpoint->pendingCalls(), 0);
}

TEST_P(RdmaRpcTest, SendsMessagesInlineAndNot) {
  start();
  auto endpoint = connect();
  // 64 bytes go inline by default: header 4 + message 12 + data.
  for (uint32_t length : {0u, 1u, 47u, 48u, 49u, 100u, 244u}) {
    folly::coro::blockingWait(co_check(*endpoint, length, length));
  }
}

TEST_P(RdmaRpcTest, RefusesTooLargeRequests) {
  start();
  auto endpoint = connect();
  auto request = makeRequest(1);
  request.length = 245;
  TestMessage response;
  try {
    folly::coro::blockingWait(endpoint->co_call(request, response));
    FAIL() << "expected EMSGSIZE";
  } catch (const RdmaError& e) {
    EXPECT_EQ(e.code().value(), EMSGSIZE);
  }
  // The endpoint still works.
  folly::coro::blockingWait(co_check(*endpoint, 2));
  EXPECT_EQ(endpoint->pendingCalls(), 0);
}

TEST_P(RdmaRpcTest, StreamsCalls) {
  start();
  auto endpoint = connect();
  auto stream = endpoint->createStream();
  constexpr int kCalls = 100;
  std::vector<TestMessage> requests;
  std::vector<TestMessage> responses(kCalls);
  for (int i = 0; i < kCalls; ++i) {
    requests.push_back(makeRequest(i, 8));
  }
  folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
    // More requests than slots: co_request waits for slots, not responses.
    for (int i = 0; i < kCalls; ++i) {
      co_await stream->co_request(requests[i], responses[i]);
    }
    std::set<TestMessage*> taken;
    for (int i = 0; i < kCalls; ++i) {
      TestMessage* response = co_await stream->co_take();
      taken.insert(response);
    }
    EXPECT_EQ(taken.size(), static_cast<size_t>(kCalls));
  }());
  for (int i = 0; i < kCalls; ++i) {
    expectResponse(responses[i], requests[i]);
  }
  EXPECT_EQ(stream->poll(), nullptr);

  // poll() until the response is there.
  auto request = makeRequest(7);
  TestMessage response;
  folly::coro::blockingWait(stream->co_request(request, response));
  TestMessage* polled = nullptr;
  for (int i = 0; i < 1000000 && !polled; ++i) {
    polled = stream->poll();
  }
  ASSERT_EQ(polled, &response);
  expectResponse(response, request);
}

TEST_P(RdmaRpcTest, AnswersAfterTheServiceSuspends) {
  service_.delay = 1ms;
  start();
  auto endpoint = connect();
  std::vector<folly::coro::Task<void>> calls;
  for (int i = 0; i < 64; ++i) {
    calls.push_back(co_check(*endpoint, i));
  }
  folly::coro::blockingWait(folly::coro::collectAllRange(std::move(calls)));
}

TEST_P(RdmaRpcTest, QueuesResponsesWithoutASendSlot) {
  service_.useGate = true;
  RdmaRpcServerOptions options;
  options.sendQueue = 2;
  start(options);
  auto endpoint = connect();
  constexpr int kCalls = 16;
  std::vector<folly::coro::Task<void>> calls;
  for (int i = 0; i < kCalls; ++i) {
    calls.push_back(co_check(*endpoint, i));
  }
  // All responses at once, once all requests are in.
  auto release = [&]() -> folly::coro::Task<void> {
    EXPECT_TRUE(co_await co_eventually(
        [&] { return service_.arrived == kCalls; }));
    service_.gate.post();
  };
  folly::coro::blockingWait(folly::coro::collectAll(
      folly::coro::collectAllRange(std::move(calls)), release()));
  EXPECT_EQ(endpoint->pendingCalls(), 0);
}

TEST_P(RdmaRpcTest, ServesManyClientsOnSeveralProcessors) {
  RdmaRpcServerOptions options;
  options.cpus = {0, 1};
  start(options);
  constexpr int kClients = 4;
  std::vector<std::shared_ptr<ClientEndpoint>> endpoints;
  for (int i = 0; i < kClients; ++i) {
    endpoints.push_back(connect());
  }
  std::vector<folly::coro::Task<void>> clients;
  for (auto& endpoint : endpoints) {
    clients.push_back([](ClientEndpoint& endpoint) -> folly::coro::Task<void> {
      for (int i = 0; i < 200; ++i) {
        co_await co_check(endpoint, i);
      }
    }(*endpoint));
  }
  folly::coro::blockingWait(folly::coro::collectAllRange(std::move(clients)));
  EXPECT_EQ(service_.opens, kClients);
  {
    std::lock_guard lock{service_.mutex};
    EXPECT_EQ(service_.clusters, (std::set<int>{0, 1}));
  }
  // Server endpoints close when their clients do.
  for (auto& endpoint : endpoints) {
    folly::coro::blockingWait(endpoint->co_close());
  }
  EXPECT_TRUE(folly::coro::blockingWait(
      co_eventually([&] { return service_.closes == kClients; })));
  folly::coro::blockingWait(server_->co_close());
  EXPECT_EQ(service_.closes, kClients);
}

TEST_P(RdmaRpcTest, FailsCallsWhenTheClientCloses) {
  service_.useGate = true;
  start();
  auto endpoint = connect();
  constexpr int kCalls = 4;
  std::vector<folly::coro::Task<void>> calls;
  for (int i = 0; i < kCalls; ++i) {
    calls.push_back(co_check(*endpoint, i));
  }
  auto close = [&]() -> folly::coro::Task<void> {
    EXPECT_TRUE(co_await co_eventually(
        [&] { return service_.arrived == kCalls; }));
    co_await endpoint->co_close();
  };
  auto [results, unit] = folly::coro::blockingWait(folly::coro::collectAll(
      folly::coro::collectAllTryRange(std::move(calls)), close()));
  for (auto& result : results) {
    ASSERT_TRUE(result.hasException());
    auto* error = result.exception().get_exception<RdmaError>();
    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code().value(), ECONNRESET);
  }
  EXPECT_THROW(
      folly::coro::blockingWait(co_check(*endpoint, 1)), RdmaError);
}

TEST_P(RdmaRpcTest, FailsCallsWhenTheServerCloses) {
  service_.useGate = true;
  start();
  auto endpoint = connect();
  constexpr int kCalls = 4;
  std::vector<folly::coro::Task<void>> calls;
  for (int i = 0; i < kCalls; ++i) {
    calls.push_back(co_check(*endpoint, i));
  }
  auto close = [&]() -> folly::coro::Task<void> {
    EXPECT_TRUE(co_await co_eventually(
        [&] { return service_.arrived == kCalls; }));
    // The handlers finish once the connection is gone; their responses
    // are dropped.
    auto release = [&]() -> folly::coro::Task<void> {
      co_await folly::coro::sleep(100ms);
      service_.gate.post();
    };
    co_await folly::coro::collectAll(server_->co_close(), release());
  };
  auto [results, unit] = folly::coro::blockingWait(folly::coro::collectAll(
      folly::coro::collectAllTryRange(std::move(calls)), close()));
  for (auto& result : results) {
    ASSERT_TRUE(result.hasException());
    EXPECT_NE(result.exception().get_exception<RdmaError>(), nullptr);
  }
  EXPECT_EQ(service_.closes, 1);
}

INSTANTIATE_TEST_SUITE_P(
    PollingAndDispatch,
    RdmaRpcTest,
    testing::Combine(testing::Bool(), testing::Bool()),
    [](const auto& info) {
      return std::string{std::get<0>(info.param) ? "Polling" : "Blocking"} +
          (std::get<1>(info.param) ? "Inline" : "Executor");
    });

} // namespace
} // namespace rlib
