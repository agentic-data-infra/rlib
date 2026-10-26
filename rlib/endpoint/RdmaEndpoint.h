#pragma once

#include <infiniband/verbs.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>

#include "rlib/endpoint/RdmaCqProvider.h"
#include "rlib/verbs/MemoryRegion.h"
#include "rlib/verbs/RdmaCmId.h"
#include "rlib/verbs/RdmaConnParam.h"
#include "rlib/verbs/SocketAddress.h"

namespace rlib {

class RdmaEndpointGroupBase;
struct RdmaCmEvent;

// A reliable connected RDMA connection, made by a group: on the client side
// by RdmaEndpointGroup::createEndpoint() and co_connect, on the server side
// by RdmaServerEndpoint::co_accept. The group allocates its resources (a
// completion queue and a QP, through the group's hooks) while connecting.
//
// Applications derive their endpoint classes from it and create them in
// their group's RdmaEndpointFactory; init() runs once the resources exist.
//
// The control path (connecting, accepting) throws RdmaError; the data path
// (posting work requests, one-sided reads and writes) returns errors. Its
// coroutines resume on the awaiting coroutine's executor. One control call
// at a time.
class RdmaEndpoint {
 public:
  RdmaEndpoint(RdmaEndpointGroupBase& group, RdmaCmId id, bool serverSide);
  virtual ~RdmaEndpoint();

  RdmaEndpoint(const RdmaEndpoint&) = delete;
  RdmaEndpoint& operator=(const RdmaEndpoint&) = delete;

  // Resolves 'destination' (each of address and route within 'timeout'),
  // allocates the resources and connects. Throws RdmaError.
  folly::coro::Task<void> co_connect(
      SocketAddress destination,
      std::chrono::milliseconds timeout);

  // Disconnects (waiting up to a second for the disconnect to complete),
  // then releases the QP and the connection. Idempotent.
  folly::coro::Task<void> co_close();

  // Allocates the protection domain (shared by all endpoints on the
  // device), the completion queue and the QP, then calls init(). Called by
  // the group's allocateResources hook. Idempotent. Throws RdmaError.
  void allocateResources();

  bool isConnected() const;
  // Disconnected by either side, or failed.
  bool isClosed() const;

  SocketAddress srcAddr() const {
    return id_.localAddress();
  }
  SocketAddress dstAddr() const {
    return id_.peerAddress();
  }

  // Registers memory in the endpoint's protection domain for local write,
  // remote read and remote write. Throws RdmaError.
  MemoryRegion registerMemory(void* addr, size_t length);

  // Post chains of work requests (the data path); return 0 or the error,
  // with the first failed request in 'badWr' if given.
  int postSend(ibv_send_wr& wrs, ibv_send_wr** badWr = nullptr);
  int postRecv(ibv_recv_wr& wrs, ibv_recv_wr** badWr = nullptr);

  // One-sided RDMA read of [remoteAddr, remoteAddr + length) at the peer
  // into local.addr() + offset, and write the other way. They poll the
  // endpoint's completion queue until the request completes, so nothing
  // else may be outstanding on it. Return 0, EINVAL for a range outside
  // 'local', the error of posting, or EIO if the request failed.
  folly::coro::Task<int> co_read(
      const MemoryRegion& local,
      size_t offset,
      uint32_t length,
      uint64_t remoteAddr,
      uint32_t rkey);
  folly::coro::Task<int> co_write(
      const MemoryRegion& local,
      size_t offset,
      uint32_t length,
      uint64_t remoteAddr,
      uint32_t rkey);

  RdmaCmId& cmId() {
    return id_;
  }
  ibv_pd* pd() const {
    return pd_;
  }
  RdmaCqProvider* cqProvider() const {
    return cqProvider_.get();
  }
  ibv_qp* qp() const {
    return qp_;
  }
  bool isServerSide() const {
    return serverSide_;
  }
  int endpointId() const {
    return endpointId_;
  }
  const RdmaConnParam& connParam() const;

 protected:
  // Runs once the endpoint's resources are allocated, e.g. to register
  // memory and post receives.
  virtual void init() {}

 private:
  friend class RdmaEndpointGroupBase;
  friend class RdmaServerEndpointBase;

  // In order; waiting for a state ends at it or any later one.
  enum class State {
    kInitialized,
    kAddrResolved,
    kRouteResolved,
    kResourcesAllocated,
    kConnected,
    kClosed,
    kError,
  };

  // Accepts the connection request this endpoint was created for.
  folly::coro::Task<void> co_accept();
  // From the group's connection manager thread.
  void dispatchCmEvent(const RdmaCmEvent& event);

  State state() const;
  // Moves to 'next' if it is later than the current state.
  void advance(State next, int error = 0);
  folly::coro::Task<State> co_waitFor(State target);
  // Throws RdmaError unless 'reached' is 'target'.
  void check(State reached, State target, const char* what) const;
  folly::coro::Task<int> co_rdma(
      ibv_wr_opcode opcode,
      const MemoryRegion& local,
      size_t offset,
      uint32_t length,
      uint64_t remoteAddr,
      uint32_t rkey);

  RdmaEndpointGroupBase& group_;
  const int endpointId_;
  const bool serverSide_;
  RdmaCmId id_;
  ibv_pd* pd_ = nullptr;
  std::unique_ptr<RdmaCqProvider> cqProvider_;
  ibv_qp* qp_ = nullptr;
  bool resourcesAllocated_ = false;

  // Guards the state, and the id and QP against closing while the
  // connection manager thread uses them.
  mutable std::mutex mutex_;
  bool closed_ = false;
  State state_ = State::kInitialized;
  // The errno of the event that failed the connection.
  int error_ = 0;
  // Posted on every state change.
  folly::coro::Baton stateChanged_;
};

} // namespace rlib
