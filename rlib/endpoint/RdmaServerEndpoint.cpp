#include "rlib/endpoint/RdmaServerEndpoint.h"

#include <vector>

#include "rlib/endpoint/RdmaCmProcessor.h"
#include "rlib/endpoint/RdmaEndpointGroup.h"
#include "rlib/endpoint/RdmaEndpointProvider.h"
#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

RdmaServerEndpointBase::RdmaServerEndpointBase(
    RdmaEndpointGroupBase& group,
    RdmaCmId id)
    : group_{group}, endpointId_{group.nextId()}, id_{std::move(id)} {}

void RdmaServerEndpointBase::bind(const SocketAddress& address, int backlog) {
  {
    std::lock_guard lock{mutex_};
    if (state_ != State::kInitialized) {
      throw RdmaError{EINVAL, "bind: server endpoint already bound"};
    }
    state_ = State::kReadyForAccept;
  }
  id_.bindAddr(address);
  id_.listen(backlog);
  if (id_.verbs()) {
    pd_ = RdmaEndpointProvider::get().createProtectionDomain(id_.verbs());
  }
}

folly::coro::Task<std::shared_ptr<RdmaEndpoint>>
RdmaServerEndpointBase::co_acceptRaw() {
  std::shared_ptr<RdmaEndpoint> endpoint;
  while (!endpoint) {
    {
      std::lock_guard lock{mutex_};
      if (state_ != State::kReadyForAccept) {
        throw RdmaError{
            EINVAL, "accept: server endpoint not bound, or closed"};
      }
      if (!requested_.empty()) {
        endpoint = std::move(requested_.front());
        requested_.pop_front();
        break;
      }
    }
    co_await changed_;
    // Reset before checking again, so that no request is missed.
    changed_.reset();
  }
  co_await endpoint->co_accept();
  co_return endpoint;
}

folly::coro::Task<void> RdmaServerEndpointBase::co_close() {
  std::deque<std::shared_ptr<RdmaEndpoint>> requested;
  {
    std::lock_guard lock{mutex_};
    if (state_ == State::kClosed) {
      co_return;
    }
    state_ = State::kClosed;
    requested.swap(requested_);
  }
  changed_.post();
  for (auto& endpoint : requested) {
    co_await endpoint->co_close();
  }
  group_.unregisterServerEndpoint(id_.get());
  id_ = RdmaCmId{nullptr};
}

bool RdmaServerEndpointBase::isBound() const {
  std::lock_guard lock{mutex_};
  return state_ == State::kReadyForAccept;
}

bool RdmaServerEndpointBase::isClosed() const {
  std::lock_guard lock{mutex_};
  return state_ == State::kClosed;
}

MemoryRegion RdmaServerEndpointBase::registerMemory(void* addr, size_t length) {
  if (!pd_) {
    throw RdmaError{ENODEV, "registerMemory: not bound to a device"};
  }
  return MemoryRegion{pd_, addr, length};
}

void RdmaServerEndpointBase::dispatchCmEvent(const RdmaCmEvent& event) {
  if (event.type != RDMA_CM_EVENT_CONNECT_REQUEST) {
    return;
  }
  // Destroying the new id unaccepted refuses the request.
  RdmaCmId id{event.id};
  {
    std::lock_guard lock{mutex_};
    if (state_ != State::kReadyForAccept) {
      return;
    }
    std::shared_ptr<RdmaEndpoint> endpoint;
    try {
      endpoint = group_.createServerSideEndpoint(std::move(id));
    } catch (const std::exception& e) {
      RLIB_LOG("connection request: %s", e.what());
      return;
    }
    if (!endpoint) {
      return;
    }
    requested_.push_back(std::move(endpoint));
  }
  changed_.post();
}

} // namespace rlib
