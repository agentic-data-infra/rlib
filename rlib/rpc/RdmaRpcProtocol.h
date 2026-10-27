#pragma once

#include <concepts>
#include <cstddef>
#include <span>

namespace rlib {

// A message of an RPC protocol: a plain, default-constructible struct that
// serializes itself. write() writes the message into 'out' and returns the
// bytes written; if they do not fit, it returns more than out.size(),
// writing nothing past it. update() reads a message that write() wrote.
// kSize is the most bytes write() writes.
template <typename M>
concept RdmaRpcMessage = std::default_initializable<M> &&
    requires(
        const M& message,
        M& target,
        std::span<std::byte> out,
        std::span<const std::byte> in) {
      { M::kSize } -> std::convertible_to<size_t>;
      { message.write(out) } -> std::convertible_to<size_t>;
      target.update(in);
    };

// An RPC protocol: a request and a response message type, e.g.
//
//   struct EchoProtocol {
//     using Request = EchoMessage;
//     using Response = EchoMessage;
//   };
template <typename P>
concept RdmaRpcProtocol = RdmaRpcMessage<typename P::Request> &&
    RdmaRpcMessage<typename P::Response>;

// The options both RPC endpoint kinds share.
struct RdmaRpcOptions {
  // Messages (with their 4-byte header) up to this size are sent inline,
  // copied into the work request, if the device allows it.
  int maxInline = 64;
  // Receives each endpoint keeps posted, one message each.
  int recvQueue = 16;
  // Send slots per endpoint; 0 for recvQueue. A client has at most
  // min(recvQueue, sendQueue) calls in flight.
  int sendQueue = 0;

  int sendSlots() const {
    return sendQueue > 0 ? sendQueue : recvQueue;
  }
};

} // namespace rlib
