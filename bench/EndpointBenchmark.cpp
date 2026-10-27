// Latency and bandwidth of active endpoints' one-sided operations, with a
// server and a client endpoint in this process:
//
//   - latency: one RDMA read or write of --latency_size bytes at a time;
//   - bandwidth: --depth reads or writes of --size bytes outstanding, for
//     --seconds.
//
//   endpoint_benchmark [--address=host:port] [--polling] [--size=65536]
//                      [--depth=16] [--seconds=2] [--latency_size=64]
//
// Without --address it uses the first RDMA device's address.

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "rlib/endpoint/RdmaActiveEndpointGroup.h"
#include "rlib/verbs/RdmaAddress.h"

DEFINE_string(address, "", "Server address; empty for the first RDMA device's.");
DEFINE_bool(polling, false, "Polling instead of blocking CQ threads.");
DEFINE_int32(size, 64 << 10, "Bytes per operation for bandwidth.");
DEFINE_int32(depth, 16, "Outstanding operations for bandwidth.");
DEFINE_double(seconds, 2, "Seconds per bandwidth measurement.");
DEFINE_int32(latency_size, 64, "Bytes per operation for latency.");
DEFINE_int32(latency_ops, 10000, "Operations per latency measurement.");

namespace {

using namespace rlib;
using Clock = std::chrono::steady_clock;
using Group = RdmaActiveEndpointGroup<RdmaActiveEndpoint>;

std::unique_ptr<Group> makeGroup() {
  int depth = std::max(FLAGS_depth, 1);
  auto group = std::make_unique<Group>(FLAGS_polling, depth, 1, 4 * depth);
  // Soft-RoCE drops packets under load; retry instead of failing.
  group->connParam().retryCount = 7;
  group->connParam().rnrRetryCount = 7;
  group->init(
      std::make_unique<RdmaDefaultEndpointFactory<RdmaActiveEndpoint>>(*group));
  return group;
}

struct Target {
  RdmaActiveEndpoint& endpoint;
  const MemoryRegion& local;
  uint64_t remoteAddr;
  uint32_t rkey;
};

folly::coro::Task<int> co_op(
    const Target& target,
    bool write,
    size_t offset,
    uint32_t size) {
  return write
      ? target.endpoint.co_write(
            target.local, offset, size, target.remoteAddr + offset, target.rkey)
      : target.endpoint.co_read(
            target.local, offset, size, target.remoteAddr + offset, target.rkey);
}

folly::coro::Task<void> co_latency(const Target& target, bool write) {
  std::vector<double> micros;
  micros.reserve(FLAGS_latency_ops);
  for (int i = 0; i < FLAGS_latency_ops; ++i) {
    auto start = Clock::now();
    int error = co_await co_op(target, write, 0, FLAGS_latency_size);
    if (error) {
      std::printf("operation failed: %d\n", error);
      co_return;
    }
    micros.push_back(
        std::chrono::duration<double, std::micro>(Clock::now() - start)
            .count());
  }
  std::sort(micros.begin(), micros.end());
  std::printf(
      "%-5s latency, %d B: median %.1f us, p99 %.1f us\n",
      write ? "write" : "read",
      FLAGS_latency_size,
      micros[micros.size() / 2],
      micros[micros.size() * 99 / 100]);
}

folly::coro::Task<void> co_bandwidth(const Target& target, bool write) {
  auto deadline = Clock::now() +
      std::chrono::duration_cast<Clock::duration>(
                      std::chrono::duration<double>(FLAGS_seconds));
  std::vector<int64_t> ops(FLAGS_depth);
  auto worker = [&](int slot) -> folly::coro::Task<void> {
    size_t offset = static_cast<size_t>(slot) * FLAGS_size;
    while (Clock::now() < deadline) {
      int error = co_await co_op(target, write, offset, FLAGS_size);
      if (error) {
        std::printf("operation failed: %d\n", error);
        co_return;
      }
      ++ops[slot];
    }
  };
  auto start = Clock::now();
  std::vector<folly::coro::Task<void>> workers;
  for (int i = 0; i < FLAGS_depth; ++i) {
    workers.push_back(worker(i));
  }
  co_await folly::coro::collectAllRange(std::move(workers));
  double seconds = std::chrono::duration<double>(Clock::now() - start).count();
  int64_t total = 0;
  for (auto n : ops) {
    total += n;
  }
  std::printf(
      "%-5s bandwidth, %d B x %d outstanding: %.2f GB/s, %.0f ops/s\n",
      write ? "write" : "read",
      FLAGS_size,
      FLAGS_depth,
      total * double(FLAGS_size) / seconds / 1e9,
      total / seconds);
}

folly::coro::Task<void> co_run(const SocketAddress& address) {
  auto serverGroup = makeGroup();
  auto clientGroup = makeGroup();
  auto server = serverGroup->createServerEndpoint();
  server->bind(address, 1);
  auto client = clientGroup->createEndpoint();
  auto serverAddress = server->srcAddr();
  auto [accepted, unit] = co_await folly::coro::collectAll(
      server->co_accept(),
      client->co_connect(serverAddress, std::chrono::seconds{2}));

  // The remote buffer's address and key would normally come in a message.
  size_t bytes = static_cast<size_t>(FLAGS_size) * FLAGS_depth;
  std::vector<char> remote(bytes), local(bytes);
  auto remoteMr = accepted->registerMemory(remote.data(), bytes);
  auto localMr = client->registerMemory(local.data(), bytes);
  Target target{
      *client,
      localMr,
      reinterpret_cast<uintptr_t>(remote.data()),
      remoteMr.rkey()};

  std::printf(
      "%s, CQ threads %s\n",
      serverAddress.toString().c_str(),
      FLAGS_polling ? "polling" : "blocking");
  for (bool write : {false, true}) {
    co_await co_latency(target, write);
  }
  for (bool write : {false, true}) {
    co_await co_bandwidth(target, write);
  }

  co_await clientGroup->co_close();
  co_await serverGroup->co_close();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  std::optional<SocketAddress> address;
  if (FLAGS_address.empty()) {
    address = rdmaAddress();
    if (!address) {
      std::fprintf(stderr, "no RDMA device with an IPv4 address\n");
      return 1;
    }
  } else {
    address = SocketAddress::parse(FLAGS_address);
  }
  folly::coro::blockingWait(co_run(*address));
  return 0;
}
