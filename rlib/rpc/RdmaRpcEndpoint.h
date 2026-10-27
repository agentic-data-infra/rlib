#pragma once

#include <infiniband/verbs.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <vector>

#include "rlib/endpoint/RdmaActiveEndpoint.h"
#include "rlib/rpc/RdmaRpcProtocol.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

// What RPC client and server endpoints share: one registered buffer split
// into recvQueue receive slots and sendSlots() send slots of kSlotSize
// bytes each. A message on the wire is [u32 ticket][payload], sent with
// SEND into a receive slot of the peer; all receives stay posted and are
// posted again once their message is consumed. Send slots are handed out
// and returned by the subclasses.
template <RdmaRpcProtocol P>
class RdmaRpcEndpoint : public RdmaActiveEndpoint {
 public:
  using Request = typename P::Request;
  using Response = typename P::Response;

  static constexpr size_t kHeaderSize = sizeof(uint32_t);
  static constexpr size_t kPayloadSize =
      std::max<size_t>(Request::kSize, Response::kSize);
  static constexpr size_t kSlotSize = kHeaderSize + kPayloadSize;

  RdmaRpcEndpoint(
      RdmaEndpointGroupBase& group,
      RdmaCmId id,
      bool serverSide,
      const RdmaRpcOptions& options)
      : RdmaActiveEndpoint{group, std::move(id), serverSide},
        options_{options},
        maxInline_{options.maxInline} {}

  uint64_t messagesSent() const {
    return messagesSent_;
  }
  uint64_t messagesReceived() const {
    return messagesReceived_;
  }
  const RdmaRpcOptions& options() const {
    return options_;
  }

  // What the device made of maxInline; set by the group with the QP.
  void setMaxInline(int maxInline) {
    maxInline_ = std::min(options_.maxInline, maxInline);
  }

 protected:
  // wr_ids: receive slot i is i, send slot i is kSendBit | i.
  static constexpr uint64_t kSendBit = uint64_t{1} << 32;

  // Registers the buffer and posts all receives.
  void init() override {
    size_t slots = options_.recvQueue + options_.sendSlots();
    buffer_.resize(slots * kSlotSize);
    mr_.emplace(registerMemory(buffer_.data(), buffer_.size()));
    for (int i = 0; i < options_.recvQueue; ++i) {
      if (int error = postRecvSlot(i)) {
        throwRdmaError("post receive", error);
      }
    }
  }

  std::byte* recvSlot(int index) {
    return buffer_.data() + static_cast<size_t>(index) * kSlotSize;
  }
  std::byte* sendSlot(int index) {
    return recvSlot(options_.recvQueue + index);
  }

  // The ticket and payload of a received message.
  uint32_t ticketOf(int recvIndex) {
    uint32_t ticket;
    std::memcpy(&ticket, recvSlot(recvIndex), sizeof(ticket));
    return ticket;
  }
  std::span<const std::byte> payloadOf(int recvIndex, uint32_t byteLen) {
    size_t length = byteLen > kHeaderSize ? byteLen - kHeaderSize : 0;
    return {recvSlot(recvIndex) + kHeaderSize, length};
  }

  void countReceived() {
    ++messagesReceived_;
  }

  int postRecvSlot(int index) {
    ibv_sge sge{
        reinterpret_cast<uintptr_t>(recvSlot(index)),
        static_cast<uint32_t>(kSlotSize),
        mr_->lkey()};
    ibv_recv_wr wr{};
    wr.wr_id = static_cast<uint64_t>(index);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    return postRecv(wr);
  }

  // Writes [ticket][message] into send slot 'index' and returns its
  // length. Throws RdmaError(EMSGSIZE) if the message does not fit.
  template <RdmaRpcMessage M>
  uint32_t writeSendSlot(int index, uint32_t ticket, const M& message) {
    std::byte* slot = sendSlot(index);
    std::memcpy(slot, &ticket, sizeof(ticket));
    size_t written =
        message.write(std::span<std::byte>{slot + kHeaderSize, kPayloadSize});
    if (written > kPayloadSize) {
      throw RdmaError{EMSGSIZE, "RPC message larger than its kSize"};
    }
    return static_cast<uint32_t>(kHeaderSize + written);
  }

  // Sends the first 'length' bytes of send slot 'index'; returns the error
  // of posting.
  int postSendSlot(int index, uint32_t length) {
    ibv_sge sge{
        reinterpret_cast<uintptr_t>(sendSlot(index)), length, mr_->lkey()};
    ibv_send_wr wr{};
    wr.wr_id = kSendBit | static_cast<uint64_t>(index);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    if (static_cast<int>(length) <= maxInline_) {
      wr.send_flags |= IBV_SEND_INLINE;
    }
    int error = postSend(wr);
    if (!error) {
      ++messagesSent_;
    }
    return error;
  }

  const RdmaRpcOptions options_;

 private:
  int maxInline_;
  std::vector<std::byte> buffer_;
  std::optional<MemoryRegion> mr_;
  std::atomic<uint64_t> messagesSent_{0};
  std::atomic<uint64_t> messagesReceived_{0};
};

} // namespace rlib
