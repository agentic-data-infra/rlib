#pragma once

#include <infiniband/verbs.h>

#include <atomic>

#include <folly/coro/Task.h>
#include <folly/coro/UnboundedQueue.h>

#include "rlib/endpoint/RdmaEndpoint.h"

namespace rlib {

// An endpoint whose completions a group's CQ processor thread takes, so the
// application need not poll: co_postSend and co_postRecv resume the
// awaiting coroutine (on its executor) when their work requests complete,
// with any number outstanding. Completions of work requests posted with
// postSend/postRecv go to dispatchCqEvent.
//
// Made by RdmaActiveEndpointGroup; derive from it for application state.
class RdmaActiveEndpoint : public RdmaEndpoint {
 public:
  RdmaActiveEndpoint(RdmaEndpointGroupBase& group, RdmaCmId id, bool serverSide);

  // Posts a chain of work requests and waits for the last one to complete.
  // rlib sets the last request's wr_id and signals it; earlier requests
  // complete through dispatchCqEvent if signaled. Returns the completion
  // (status IBV_WC_SUCCESS, or the error, e.g. IBV_WC_WR_FLUSH_ERR when the
  // connection closed), or, if posting failed, status IBV_WC_GENERAL_ERR
  // with the error in vendor_err. Not after co_close.
  folly::coro::Task<ibv_wc> co_postSend(ibv_send_wr& wrs);
  folly::coro::Task<ibv_wc> co_postRecv(ibv_recv_wr& wrs);

  // As RdmaEndpoint's, without the limit: any number may be outstanding.
  folly::coro::Task<int> co_read(
      const MemoryRegion& local,
      size_t offset,
      uint32_t length,
      uint64_t remoteAddr,
      uint32_t rkey) override;
  folly::coro::Task<int> co_write(
      const MemoryRegion& local,
      size_t offset,
      uint32_t length,
      uint64_t remoteAddr,
      uint32_t rkey) override;

  // Completions of work requests posted with postSend/postRecv (e.g.
  // receives the application posts in init()), on the CQ processor's
  // thread: keep it short and do not block. By default they are queued for
  // co_nextCqEvent.
  virtual void dispatchCqEvent(const ibv_wc& wc);

  // The next completion that the default dispatchCqEvent queued; resumes
  // on the caller's executor. Cancellable.
  folly::coro::Task<ibv_wc> co_nextCqEvent();

  // Which of the group's CQ processors on the device the endpoint got, or
  // -1 before its resources are allocated.
  int clusterId() const;

 protected:
  // Moves the QP to the error state, waits for the outstanding co_post*
  // calls to complete (flushed), and unregisters the QP from the CQ
  // processor.
  folly::coro::Task<void> co_flush() override;

 private:
  folly::coro::Task<int> co_rdma(
      ibv_wr_opcode opcode,
      const MemoryRegion& local,
      size_t offset,
      uint32_t length,
      uint64_t remoteAddr,
      uint32_t rkey);

  std::atomic<int> outstanding_{0};
  folly::coro::UnboundedQueue<ibv_wc> cqEvents_;
};

} // namespace rlib
