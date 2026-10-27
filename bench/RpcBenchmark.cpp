// Round-trip latency and calls per second of RPCs (16-byte request and
// response, the examples' param + 1 service), with the server and 1, 2,
// 4, 8 and 16 clients in this process, each client on its own thread with
// its own connection:
//
//   rpc_benchmark [--address=host:port] [--polling] [--cpus=0,1]
//                 [--inline_dispatch] [--mode=call|concurrent|stream]
//                 [--depth=16] [--calls=20000] [--max_clients=16]
//
// Without --address it uses the first RDMA device's address.

#include <chrono>
#include <cstdio>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "examples/RpcExample.h"
#include "rlib/verbs/RdmaAddress.h"

DEFINE_string(address, "", "Server address; empty for the first RDMA device's.");
DEFINE_bool(polling, false, "Polling instead of blocking CQ threads.");
DEFINE_string(cpus, "", "Cores for the server's CQ threads, e.g. 0,1.");
DEFINE_bool(inline_dispatch, false, "Run the service on the CQ threads.");
DEFINE_string(mode, "call", "call, concurrent or stream.");
DEFINE_int32(depth, 16, "Calls in flight per client (concurrent, stream).");
DEFINE_int32(calls, 20000, "Calls per client.");
DEFINE_int32(max_clients, 16, "Most clients.");

namespace {

using namespace rlib;
using namespace rlib::examples;

folly::coro::Task<void> co_run() {
  auto address =
      FLAGS_address.empty() ? rdmaAddress() : SocketAddress::parse(FLAGS_address);
  if (!address) {
    std::printf("no RDMA device with an IPv4 address\n");
    co_return;
  }
  RpcMode mode = parseMode(FLAGS_mode);

  RpcService service;
  RdmaRpcServerOptions serverOptions;
  serverOptions.polling = FLAGS_polling;
  serverOptions.cpus = parseCpus(FLAGS_cpus);
  serverOptions.inlineDispatch = FLAGS_inline_dispatch;
  auto server = RpcServerGroup::create(service, serverOptions);
  // Soft-RoCE drops packets under load; retry instead of failing.
  server->connParam().retryCount = 7;
  server->connParam().rnrRetryCount = 7;
  server->serve(*address);

  auto client = RpcClientGroup::create();
  client->connParam().retryCount = 7;
  client->connParam().rnrRetryCount = 7;

  std::printf("mode %s, depth %d, server CQ threads %s%s%s\n",
              FLAGS_mode.c_str(), mode == RpcMode::kCall ? 1 : FLAGS_depth,
              FLAGS_polling ? "polling" : "blocking",
              FLAGS_cpus.empty() ? "" : (" on cpus " + FLAGS_cpus).c_str(),
              FLAGS_inline_dispatch ? ", inline dispatch" : "");
  std::printf("%8s %12s %10s %10s\n", "clients", "calls/s", "p50 us", "p99 us");
  for (int clients = 1; clients <= FLAGS_max_clients; clients *= 2) {
    std::vector<std::shared_ptr<RpcClientEndpoint>> endpoints;
    for (int i = 0; i < clients; ++i) {
      auto endpoint = client->createEndpoint();
      co_await endpoint->co_connect(server->address(), std::chrono::seconds{1});
      endpoints.push_back(std::move(endpoint));
    }
    folly::CPUThreadPoolExecutor threads{static_cast<size_t>(clients)};
    std::vector<folly::coro::TaskWithExecutor<RpcStats>> loops;
    for (auto& endpoint : endpoints) {
      loops.push_back(folly::coro::co_withExecutor(
          folly::getKeepAliveToken(threads),
          co_runClient(*endpoint, mode, FLAGS_calls, FLAGS_depth)));
    }
    auto start = std::chrono::steady_clock::now();
    auto results = co_await folly::coro::collectAllRange(std::move(loops));
    std::chrono::duration<double> seconds =
        std::chrono::steady_clock::now() - start;
    RpcStats total;
    for (auto& result : results) {
      total.merge(result);
    }
    if (total.errors) {
      std::printf("%d wrong answers\n", total.errors);
    }
    std::printf("%8d %12.0f %10.1f %10.1f\n", clients,
                total.micros.size() / seconds.count(), total.percentile(50),
                total.percentile(99));
    for (auto& endpoint : endpoints) {
      co_await endpoint->co_close();
    }
  }
  co_await client->co_close();
  co_await server->co_close();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::coro::blockingWait(co_run());
  return 0;
}
