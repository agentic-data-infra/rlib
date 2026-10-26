#pragma once

#include <infiniband/verbs.h>

#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>

#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>

#include "rlib/endpoint/RdmaEndpoint.h"
#include "rlib/verbs/MemoryRegion.h"
#include "rlib/verbs/RdmaCmId.h"
#include "rlib/verbs/SocketAddress.h"

namespace rlib {

class RdmaEndpointGroupBase;
struct RdmaCmEvent;

// What RdmaServerEndpoint<C> has in common for all endpoint classes.
class RdmaServerEndpointBase {
 public:
  RdmaServerEndpointBase(RdmaEndpointGroupBase& group, RdmaCmId id);
  virtual ~RdmaServerEndpointBase() = default;

  RdmaServerEndpointBase(const RdmaServerEndpointBase&) = delete;
  RdmaServerEndpointBase& operator=(const RdmaServerEndpointBase&) = delete;

  // Binds to 'address' (port 0 picks one) and listens. Throws RdmaError.
  void bind(const SocketAddress& address, int backlog);

  // Stops listening: pending co_accept calls throw, and connection requests
  // not yet accepted are refused. Idempotent.
  folly::coro::Task<void> co_close();

  bool isBound() const;
  bool isClosed() const;

  SocketAddress srcAddr() const {
    return id_.localAddress();
  }
  RdmaCmId& cmId() {
    return id_;
  }
  // The device's protection domain, once bound to a device's address;
  // nullptr when bound to the any address.
  ibv_pd* pd() const {
    return pd_;
  }
  int endpointId() const {
    return endpointId_;
  }

  // Registers memory in pd(), like RdmaEndpoint::registerMemory. Throws
  // RdmaError.
  MemoryRegion registerMemory(void* addr, size_t length);

 protected:
  // The next connection request's endpoint, connected. Throws RdmaError.
  folly::coro::Task<std::shared_ptr<RdmaEndpoint>> co_acceptRaw();

 private:
  friend class RdmaEndpointGroupBase;

  enum class State { kInitialized, kReadyForAccept, kClosed };

  // From the group's connection manager thread.
  void dispatchCmEvent(const RdmaCmEvent& event);

  RdmaEndpointGroupBase& group_;
  const int endpointId_;
  RdmaCmId id_;
  ibv_pd* pd_ = nullptr;

  mutable std::mutex mutex_;
  State state_ = State::kInitialized;
  std::deque<std::shared_ptr<RdmaEndpoint>> requested_;
  // Posted when a request arrives or the server endpoint closes.
  folly::coro::Baton changed_;
};

// Accepts connections as endpoints of class C, made by the group's
// factory. One co_accept call at a time.
template <typename C>
class RdmaServerEndpoint : public RdmaServerEndpointBase {
 public:
  using RdmaServerEndpointBase::RdmaServerEndpointBase;

  // Waits for the next connection request and accepts it. Throws RdmaError.
  folly::coro::Task<std::shared_ptr<C>> co_accept() {
    auto endpoint = co_await co_acceptRaw();
    co_return std::static_pointer_cast<C>(std::move(endpoint));
  }
};

} // namespace rlib
