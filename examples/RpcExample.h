#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <folly/coro/Collect.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>

#include "rlib/rpc/RdmaRpcClientGroup.h"
#include "rlib/rpc/RdmaRpcServerGroup.h"

// The protocol, service and client loops of the RPC examples and benchmark.
namespace rlib::examples {

// A command, a parameter and the time the client sent it: 16 bytes.
struct RpcRequest {
  static constexpr size_t kSize = 16;

  int32_t cmd = 0;
  int32_t param = 0;
  int64_t time = 0;

  size_t write(std::span<std::byte> out) const {
    std::memcpy(out.data(), &cmd, 4);
    std::memcpy(out.data() + 4, &param, 4);
    std::memcpy(out.data() + 8, &time, 8);
    return kSize;
  }
  void update(std::span<const std::byte> in) {
    std::memcpy(&cmd, in.data(), 4);
    std::memcpy(&param, in.data() + 4, 4);
    std::memcpy(&time, in.data() + 8, 8);
  }
};

// An error code, the result and the request's time: 16 bytes.
struct RpcResponse {
  static constexpr size_t kSize = 16;

  int32_t error = 0;
  int32_t result = 0;
  int64_t time = 0;

  size_t write(std::span<std::byte> out) const {
    std::memcpy(out.data(), &error, 4);
    std::memcpy(out.data() + 4, &result, 4);
    std::memcpy(out.data() + 8, &time, 8);
    return kSize;
  }
  void update(std::span<const std::byte> in) {
    std::memcpy(&error, in.data(), 4);
    std::memcpy(&result, in.data() + 4, 4);
    std::memcpy(&time, in.data() + 8, 8);
  }
};

struct RpcProtocol {
  using Request = RpcRequest;
  using Response = RpcResponse;
};

using RpcServerGroup = RdmaRpcServerGroup<RpcProtocol>;
using RpcClientGroup = RdmaRpcClientGroup<RpcProtocol>;
using RpcClientEndpoint = RdmaRpcClientEndpoint<RpcProtocol>;

// Answers param + 1, after 'delay' if set.
class RpcService : public RdmaRpcService<RpcProtocol> {
 public:
  explicit RpcService(std::chrono::microseconds delay = {}) : delay_{delay} {}

  folly::coro::Task<void> co_process(
      RdmaRpcServerEvent<RpcProtocol>& event) override {
    if (delay_.count() > 0) {
      co_await folly::coro::sleep(delay_);
    }
    event.response().result = event.request().param + 1;
    event.response().time = event.request().time;
  }

 private:
  std::chrono::microseconds delay_;
};

// "2,3,5" as {2, 3, 5}.
inline std::vector<int> parseCpus(const std::string& list) {
  std::vector<int> cpus;
  size_t start = 0;
  while (start < list.size()) {
    size_t end = list.find(',', start);
    if (end == std::string::npos) {
      end = list.size();
    }
    cpus.push_back(std::stoi(list.substr(start, end - start)));
    start = end + 1;
  }
  return cpus;
}

inline int64_t nowNanos() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// How a client issues its calls:
//   kCall:       one co_call at a time;
//   kConcurrent: 'depth' coroutines calling at once;
//   kStream:     a stream with 'depth' requests in flight.
enum class RpcMode { kCall, kConcurrent, kStream };

inline RpcMode parseMode(const std::string& mode) {
  if (mode == "call") {
    return RpcMode::kCall;
  }
  if (mode == "concurrent") {
    return RpcMode::kConcurrent;
  }
  if (mode == "stream") {
    return RpcMode::kStream;
  }
  throw std::invalid_argument{"mode must be call, concurrent or stream"};
}

// What a client loop measured: round-trip latencies in microseconds.
struct RpcStats {
  std::vector<double> micros;
  int errors = 0;

  void add(const RpcRequest& request, const RpcResponse& response) {
    if (response.result != request.param + 1) {
      ++errors;
    }
    micros.push_back(static_cast<double>(nowNanos() - response.time) / 1000);
  }
  void merge(const RpcStats& other) {
    micros.insert(micros.end(), other.micros.begin(), other.micros.end());
    errors += other.errors;
  }
  // The p-th percentile (0..100); sorts.
  double percentile(double p) {
    if (micros.empty()) {
      return 0;
    }
    std::sort(micros.begin(), micros.end());
    size_t index = static_cast<size_t>(p / 100 * (micros.size() - 1));
    return micros[index];
  }
};

namespace detail {

inline folly::coro::Task<void> co_callLoop(
    RpcClientEndpoint& endpoint,
    int calls,
    RpcStats& stats) {
  RpcRequest request;
  RpcResponse response;
  for (int i = 0; i < calls; ++i) {
    request.param = i;
    request.time = nowNanos();
    co_await endpoint.co_call(request, response);
    stats.add(request, response);
  }
}

inline folly::coro::Task<void> co_streamLoop(
    RpcClientEndpoint& endpoint,
    int calls,
    int depth,
    RpcStats& stats) {
  auto stream = endpoint.createStream();
  // Request i goes with response i; the response says which request it
  // answers (result - 1).
  std::vector<RpcRequest> requests(depth);
  std::vector<RpcResponse> responses(depth);
  std::vector<int> free;
  for (int i = depth - 1; i >= 0; --i) {
    free.push_back(i);
  }
  int issued = 0;
  int taken = 0;
  while (taken < calls) {
    if (issued < calls && !free.empty()) {
      int slot = free.back();
      free.pop_back();
      requests[slot].param = issued++;
      requests[slot].time = nowNanos();
      co_await stream->co_request(requests[slot], responses[slot]);
      continue;
    }
    RpcResponse* response = co_await stream->co_take();
    int slot = static_cast<int>(response - responses.data());
    stats.add(requests[slot], *response);
    free.push_back(slot);
    ++taken;
  }
}

} // namespace detail

// Runs 'calls' calls on 'endpoint' as 'mode' says.
inline folly::coro::Task<RpcStats> co_runClient(
    RpcClientEndpoint& endpoint,
    RpcMode mode,
    int calls,
    int depth) {
  RpcStats stats;
  stats.micros.reserve(calls);
  depth = std::max(depth, 1);
  switch (mode) {
    case RpcMode::kCall:
      co_await detail::co_callLoop(endpoint, calls, stats);
      break;
    case RpcMode::kConcurrent: {
      std::vector<RpcStats> parts(depth);
      std::vector<folly::coro::Task<void>> loops;
      for (int i = 0; i < depth; ++i) {
        int share = calls / depth + (i < calls % depth ? 1 : 0);
        loops.push_back(detail::co_callLoop(endpoint, share, parts[i]));
      }
      co_await folly::coro::collectAllRange(std::move(loops));
      for (auto& part : parts) {
        stats.merge(part);
      }
      break;
    }
    case RpcMode::kStream:
      co_await detail::co_streamLoop(endpoint, calls, depth, stats);
      break;
  }
  co_return stats;
}

} // namespace rlib::examples
