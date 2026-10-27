#pragma once

// The endpoint the examples use: an active endpoint with three registered
// buffers, one each for RDMA, sending and receiving. It posts a receive in
// init(), so that a message sent right after connecting finds it.

#include <array>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>

#include <folly/coro/Task.h>

#include "rlib/endpoint/RdmaActiveEndpointGroup.h"

namespace rlib::examples {

class ExampleEndpoint : public RdmaActiveEndpoint {
 public:
  static constexpr uint32_t kBufferSize = 100;

  using RdmaActiveEndpoint::RdmaActiveEndpoint;

  // Sends the first 'length' bytes of the send buffer.
  folly::coro::Task<ibv_wc> co_send(uint32_t length) {
    ibv_sge sge{address(sendBuffer), length, sendMr->lkey()};
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    co_return co_await co_postSend(wr);
  }

  // Waits for the receive posted in init() (or by the last call) to
  // complete and posts the next one.
  folly::coro::Task<ibv_wc> co_receive() {
    auto wc = co_await co_nextCqEvent();
    postReceive();
    co_return wc;
  }

  std::array<char, kBufferSize> dataBuffer{};
  std::array<char, kBufferSize> sendBuffer{};
  std::array<char, kBufferSize> recvBuffer{};
  std::optional<MemoryRegion> dataMr;
  std::optional<MemoryRegion> sendMr;
  std::optional<MemoryRegion> recvMr;

 protected:
  void init() override {
    dataMr.emplace(registerMemory(dataBuffer.data(), kBufferSize));
    sendMr.emplace(registerMemory(sendBuffer.data(), kBufferSize));
    recvMr.emplace(registerMemory(recvBuffer.data(), kBufferSize));
    postReceive();
  }

 private:
  static uint64_t address(const std::array<char, kBufferSize>& buffer) {
    return reinterpret_cast<uintptr_t>(buffer.data());
  }

  void postReceive() {
    ibv_sge sge{address(recvBuffer), kBufferSize, recvMr->lkey()};
    ibv_recv_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    postRecv(wr);
  }
};

// The examples' group: blocking CQ threads, up to 128 work requests of up
// to 4 elements per QP, CQs of 256.
inline std::unique_ptr<RdmaActiveEndpointGroup<ExampleEndpoint>> makeGroup() {
  auto group = std::make_unique<RdmaActiveEndpointGroup<ExampleEndpoint>>(
      false, 128, 4, 256);
  group->connParam().retryCount = 2;
  group->connParam().rnrRetryCount = 7;
  group->init(
      std::make_unique<RdmaDefaultEndpointFactory<ExampleEndpoint>>(*group));
  return group;
}

// The address a server buffer is at, as the read example sends it.
struct BufferInfo {
  uint64_t addr;
  uint32_t length;
  uint32_t rkey;
};

} // namespace rlib::examples
