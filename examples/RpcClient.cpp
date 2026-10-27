// Calls rpc_server from several clients, each with its own connection, and
// prints the calls per second and round-trip latencies.
//
//   rpc_client --address=host:port [--clients=1] [--loop=100000]
//              [--mode=call|concurrent|stream] [--depth=16]
//              [--recv_queue=16] [--send_queue=0] [--max_inline=64]
//
// Modes: call makes one call at a time; concurrent runs --depth calling
// coroutines per client; stream keeps --depth requests in flight on a
// stream. Each client runs on its own thread.

#include <chrono>
#include <cstdio>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "examples/RpcExample.h"

DEFINE_string(address, "", "The server's address, host:port.");
DEFINE_int32(clients, 1, "Clients (connections), each on its own thread.");
DEFINE_int32(loop, 100000, "Calls per client.");
DEFINE_string(mode, "call", "call, concurrent or stream.");
DEFINE_int32(depth, 16, "Calls in flight per client (concurrent, stream).");
DEFINE_int32(recv_queue, 16, "Receives per connection.");
DEFINE_int32(send_queue, 0, "Send slots per connection; 0 for recv_queue.");
DEFINE_int32(max_inline, 64, "Largest message sent inline.");

namespace {

using namespace rlib;
using namespace rlib::examples;

folly::coro::Task<void> co_run() {
  RpcMode mode = parseMode(FLAGS_mode);
  RdmaRpcOptions options;
  options.recvQueue = FLAGS_recv_queue;
  options.sendQueue = FLAGS_send_queue;
  options.maxInline = FLAGS_max_inline;
  auto group = RpcClientGroup::create(options);
  group->connParam().retryCount = 7;
  group->connParam().rnrRetryCount = 7;

  auto address = SocketAddress::parse(FLAGS_address);
  std::vector<std::shared_ptr<RpcClientEndpoint>> endpoints;
  for (int i = 0; i < FLAGS_clients; ++i) {
    auto endpoint = group->createEndpoint();
    co_await endpoint->co_connect(address, std::chrono::seconds{1});
    endpoints.push_back(std::move(endpoint));
  }
  std::printf("%d clients connected to %s\n", FLAGS_clients,
              address.toString().c_str());

  folly::CPUThreadPoolExecutor threads{static_cast<size_t>(FLAGS_clients)};
  std::vector<folly::coro::TaskWithExecutor<RpcStats>> clients;
  for (auto& endpoint : endpoints) {
    clients.push_back(folly::coro::co_withExecutor(
        folly::getKeepAliveToken(threads),
        co_runClient(*endpoint, mode, FLAGS_loop, FLAGS_depth)));
  }
  auto start = std::chrono::steady_clock::now();
  auto results = co_await folly::coro::collectAllRange(std::move(clients));
  std::chrono::duration<double> seconds =
      std::chrono::steady_clock::now() - start;

  RpcStats total;
  for (auto& result : results) {
    total.merge(result);
  }
  std::printf(
      "%zu calls in %.2f s: %.0f calls/s, latency p50 %.1f us, p99 %.1f us, "
      "%d wrong answers\n",
      total.micros.size(), seconds.count(),
      total.micros.size() / seconds.count(), total.percentile(50),
      total.percentile(99), total.errors);

  co_await group->co_close();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::coro::blockingWait(co_run());
  return 0;
}
