#pragma once

#include <cstdint>

#include <folly/coro/Task.h>

#include "rlib/rpc/RdmaRpcProtocol.h"

namespace rlib {

template <RdmaRpcProtocol P>
class RdmaRpcServerEndpoint;

// A request a server endpoint received, with room for its response. The
// endpoint sends the response once the service's co_process returns.
template <RdmaRpcProtocol P>
class RdmaRpcServerEvent {
 public:
  using Request = typename P::Request;
  using Response = typename P::Response;

  const Request& request() const {
    return request_;
  }
  Response& response() {
    return response_;
  }
  // Which of the client's calls it is.
  uint32_t ticket() const {
    return ticket_;
  }
  RdmaRpcServerEndpoint<P>& endpoint() const {
    return *endpoint_;
  }

 private:
  friend class RdmaRpcServerEndpoint<P>;

  Request request_;
  Response response_;
  uint32_t ticket_ = 0;
  // The receive slot the request arrived in.
  int index_ = 0;
  RdmaRpcServerEndpoint<P>* endpoint_ = nullptr;
};

// What an RPC server does with requests. co_process fills in the event's
// response; it may suspend (e.g. for I/O), and calls run concurrently, on
// the server group's executor or, with inlineDispatch, starting on the CQ
// thread. It should not throw: if it does, the error is logged and the
// response sent as it is. open and close run once per connection, when it
// is set up and when it is closed (on the connection manager thread or the
// closing coroutine's).
template <RdmaRpcProtocol P>
class RdmaRpcService {
 public:
  virtual ~RdmaRpcService() = default;

  virtual folly::coro::Task<void> co_process(RdmaRpcServerEvent<P>& event) = 0;
  virtual void open(RdmaRpcServerEndpoint<P>& /*endpoint*/) {}
  virtual void close(RdmaRpcServerEndpoint<P>& /*endpoint*/) {}
};

} // namespace rlib
