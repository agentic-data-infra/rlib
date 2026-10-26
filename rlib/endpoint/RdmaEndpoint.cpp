#include "rlib/endpoint/RdmaEndpoint.h"

#include <folly/coro/Sleep.h>

#include "rlib/endpoint/RdmaCmProcessor.h"
#include "rlib/endpoint/RdmaEndpointGroup.h"
#include "rlib/endpoint/RdmaEndpointProvider.h"
#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

namespace {

// The errno for an event that fails a connection.
int eventError(const RdmaCmEvent& event) {
  switch (event.type) {
    case RDMA_CM_EVENT_REJECTED:
      return ECONNREFUSED;
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
      return ENODEV;
    default:
      return event.status < 0 ? -event.status : EHOSTUNREACH;
  }
}

} // namespace

RdmaEndpoint::RdmaEndpoint(
    RdmaEndpointGroupBase& group,
    RdmaCmId id,
    bool serverSide)
    : group_{group},
      endpointId_{group.nextId()},
      serverSide_{serverSide},
      id_{std::move(id)} {
  RLIB_LOG("new endpoint %d, server side %d", endpointId_, serverSide_);
}

RdmaEndpoint::~RdmaEndpoint() {
  if (qp_) {
    id_.destroyQp();
  }
}

folly::coro::Task<void> RdmaEndpoint::co_connect(
    SocketAddress destination,
    std::chrono::milliseconds timeout) {
  if (state() != State::kInitialized) {
    throw RdmaError{EISCONN, "connect: endpoint already connected"};
  }
  int timeoutMs = static_cast<int>(timeout.count());

  id_.resolveAddr(destination, timeoutMs);
  auto reached = co_await co_waitFor(State::kAddrResolved);
  check(reached, State::kAddrResolved, "connect: resolve address");

  id_.resolveRoute(timeoutMs);
  reached = co_await co_waitFor(State::kRouteResolved);
  check(reached, State::kRouteResolved, "connect: resolve route");

  group_.allocateResourcesRaw(*this);
  check(state(), State::kResourcesAllocated, "connect: allocate resources");

  auto param = connParam().toNative(id_.verbs());
  id_.connect(param);
  reached = co_await co_waitFor(State::kConnected);
  check(reached, State::kConnected, "connect");
}

folly::coro::Task<void> RdmaEndpoint::co_accept() {
  group_.allocateResourcesRaw(*this);
  check(state(), State::kResourcesAllocated, "accept: allocate resources");

  auto param = connParam().toNative(id_.verbs());
  id_.accept(param);
  auto reached = co_await co_waitFor(State::kConnected);
  check(reached, State::kConnected, "accept");
}

folly::coro::Task<void> RdmaEndpoint::co_close() {
  bool connected = false;
  {
    std::lock_guard lock{mutex_};
    if (closed_) {
      co_return;
    }
    closed_ = true;
    connected = state_ == State::kConnected;
  }
  RLIB_LOG("closing endpoint %d", endpointId_);
  if (connected) {
    try {
      id_.disconnect();
      // The peer answers at once (see dispatchCmEvent), unless it is gone.
      for (int i = 0; i < 100 && state() < State::kClosed; ++i) {
        co_await folly::coro::sleep(std::chrono::milliseconds{10});
      }
    } catch (const RdmaError& e) {
      RLIB_LOG("endpoint %d: %s", endpointId_, e.what());
    }
  }
  if (qp_) {
    co_await co_flush();
  }
  // Unregistered before the id is destroyed: a new id may get its address.
  group_.unregisterEndpoint(id_.get());
  std::lock_guard lock{mutex_};
  if (qp_) {
    id_.destroyQp();
    qp_ = nullptr;
  }
  id_ = RdmaCmId{nullptr};
}

void RdmaEndpoint::allocateResources() {
  if (resourcesAllocated_) {
    return;
  }
  pd_ = RdmaEndpointProvider::get().createProtectionDomain(id_.verbs());
  cqProvider_ = group_.createCqProviderRaw(*this);
  qp_ = group_.createQpProviderRaw(*this);
  resourcesAllocated_ = true;
  init();
  advance(State::kResourcesAllocated);
}

bool RdmaEndpoint::isConnected() const {
  return state() == State::kConnected;
}

bool RdmaEndpoint::isClosed() const {
  return state() >= State::kClosed;
}

MemoryRegion RdmaEndpoint::registerMemory(void* addr, size_t length) {
  return MemoryRegion{pd_, addr, length};
}

int RdmaEndpoint::postSend(ibv_send_wr& wrs, ibv_send_wr** badWr) {
  ibv_send_wr* bad = nullptr;
  return ibv_post_send(qp_, &wrs, badWr ? badWr : &bad);
}

int RdmaEndpoint::postRecv(ibv_recv_wr& wrs, ibv_recv_wr** badWr) {
  ibv_recv_wr* bad = nullptr;
  return ibv_post_recv(qp_, &wrs, badWr ? badWr : &bad);
}

folly::coro::Task<int> RdmaEndpoint::co_read(
    const MemoryRegion& local,
    size_t offset,
    uint32_t length,
    uint64_t remoteAddr,
    uint32_t rkey) {
  return co_rdma(IBV_WR_RDMA_READ, local, offset, length, remoteAddr, rkey);
}

folly::coro::Task<int> RdmaEndpoint::co_write(
    const MemoryRegion& local,
    size_t offset,
    uint32_t length,
    uint64_t remoteAddr,
    uint32_t rkey) {
  return co_rdma(IBV_WR_RDMA_WRITE, local, offset, length, remoteAddr, rkey);
}

folly::coro::Task<int> RdmaEndpoint::co_rdma(
    ibv_wr_opcode opcode,
    const MemoryRegion& local,
    size_t offset,
    uint32_t length,
    uint64_t remoteAddr,
    uint32_t rkey) {
  if (offset > local.length() || length > local.length() - offset) {
    co_return EINVAL;
  }
  ibv_sge sge{};
  sge.addr = reinterpret_cast<uintptr_t>(local.addr()) + offset;
  sge.length = length;
  sge.lkey = local.lkey();
  ibv_send_wr wr{};
  wr.wr_id = reinterpret_cast<uintptr_t>(&wr);
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = opcode;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = remoteAddr;
  wr.wr.rdma.rkey = rkey;
  if (int error = postSend(wr)) {
    co_return error;
  }
  ibv_wc wc{};
  std::span<ibv_wc> wcs{&wc, 1};
  while (true) {
    int n = co_await cqProvider_->co_poll(wcs);
    if (n < 0) {
      co_return EIO;
    }
    if (wc.wr_id == wr.wr_id) {
      co_return wc.status == IBV_WC_SUCCESS ? 0 : EIO;
    }
    RLIB_LOG("endpoint %d: dropped a completion", endpointId_);
  }
}

const RdmaConnParam& RdmaEndpoint::connParam() const {
  return group_.connParam();
}

void RdmaEndpoint::dispatchCmEvent(const RdmaCmEvent& event) {
  switch (event.type) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
      advance(State::kAddrResolved);
      break;
    case RDMA_CM_EVENT_ROUTE_RESOLVED:
      advance(State::kRouteResolved);
      break;
    case RDMA_CM_EVENT_ESTABLISHED:
      advance(State::kConnected);
      break;
    case RDMA_CM_EVENT_DISCONNECTED: {
      std::lock_guard lock{mutex_};
      if (!closed_ && state_ == State::kConnected) {
        // The peer disconnected: answer, so that its close need not wait.
        rdma_disconnect(id_.get());
      }
    }
      advance(State::kClosed);
      break;
    case RDMA_CM_EVENT_ADDR_ERROR:
    case RDMA_CM_EVENT_ROUTE_ERROR:
    case RDMA_CM_EVENT_CONNECT_ERROR:
    case RDMA_CM_EVENT_UNREACHABLE:
    case RDMA_CM_EVENT_REJECTED:
    case RDMA_CM_EVENT_DEVICE_REMOVAL:
      advance(State::kError, eventError(event));
      break;
    default:
      break;
  }
}

RdmaEndpoint::State RdmaEndpoint::state() const {
  std::lock_guard lock{mutex_};
  return state_;
}

void RdmaEndpoint::advance(State next, int error) {
  {
    std::lock_guard lock{mutex_};
    if (next <= state_) {
      return;
    }
    state_ = next;
    if (error) {
      error_ = error;
    }
  }
  stateChanged_.post();
}

folly::coro::Task<RdmaEndpoint::State> RdmaEndpoint::co_waitFor(
    State target) {
  while (true) {
    auto reached = state();
    if (reached >= target) {
      co_return reached;
    }
    co_await stateChanged_;
    // Reset before checking the state again, so that no change is missed.
    stateChanged_.reset();
  }
}

void RdmaEndpoint::check(State reached, State target, const char* what)
    const {
  if (reached == target) {
    return;
  }
  int error = ECONNABORTED;
  {
    std::lock_guard lock{mutex_};
    if (state_ == State::kError) {
      error = error_;
    } else if (state_ == State::kClosed) {
      error = ECONNRESET;
    }
  }
  throw RdmaError{error, what};
}

} // namespace rlib
