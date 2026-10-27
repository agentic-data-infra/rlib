#include "rlib/endpoint/RdmaActiveEndpoint.h"

#include <chrono>
#include <cstring>

#include <folly/coro/Sleep.h>

#include "rlib/endpoint/RdmaCqProcessor.h"
#include "rlib/verbs/Log.h"

namespace rlib {

namespace {

ibv_wc postFailure(int error) {
  ibv_wc wc{};
  wc.status = IBV_WC_GENERAL_ERR;
  wc.vendor_err = static_cast<uint32_t>(error);
  return wc;
}

} // namespace

RdmaActiveEndpoint::RdmaActiveEndpoint(
    RdmaEndpointGroupBase& group,
    RdmaCmId id,
    bool serverSide)
    : RdmaEndpoint{group, std::move(id), serverSide} {}

folly::coro::Task<ibv_wc> RdmaActiveEndpoint::co_postSend(ibv_send_wr& wrs) {
  RdmaCompletionWaiter waiter{outstanding_};
  ibv_send_wr* last = &wrs;
  while (last->next) {
    last = last->next;
  }
  last->wr_id = waiter.wrId();
  last->send_flags |= IBV_SEND_SIGNALED;
  ++outstanding_;
  if (int error = postSend(wrs)) {
    --outstanding_;
    co_return postFailure(error);
  }
  co_await waiter.done;
  co_return waiter.wc;
}

folly::coro::Task<ibv_wc> RdmaActiveEndpoint::co_postRecv(ibv_recv_wr& wrs) {
  RdmaCompletionWaiter waiter{outstanding_};
  ibv_recv_wr* last = &wrs;
  while (last->next) {
    last = last->next;
  }
  last->wr_id = waiter.wrId();
  ++outstanding_;
  if (int error = postRecv(wrs)) {
    --outstanding_;
    co_return postFailure(error);
  }
  co_await waiter.done;
  co_return waiter.wc;
}

folly::coro::Task<int> RdmaActiveEndpoint::co_read(
    const MemoryRegion& local,
    size_t offset,
    uint32_t length,
    uint64_t remoteAddr,
    uint32_t rkey) {
  return co_rdma(IBV_WR_RDMA_READ, local, offset, length, remoteAddr, rkey);
}

folly::coro::Task<int> RdmaActiveEndpoint::co_write(
    const MemoryRegion& local,
    size_t offset,
    uint32_t length,
    uint64_t remoteAddr,
    uint32_t rkey) {
  return co_rdma(IBV_WR_RDMA_WRITE, local, offset, length, remoteAddr, rkey);
}

folly::coro::Task<int> RdmaActiveEndpoint::co_rdma(
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
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = opcode;
  wr.wr.rdma.remote_addr = remoteAddr;
  wr.wr.rdma.rkey = rkey;
  auto wc = co_await co_postSend(wr);
  if (wc.status == IBV_WC_GENERAL_ERR && wc.vendor_err) {
    co_return static_cast<int>(wc.vendor_err);
  }
  co_return wc.status == IBV_WC_SUCCESS ? 0 : EIO;
}

void RdmaActiveEndpoint::dispatchCqEvent(const ibv_wc& wc) {
  cqEvents_.enqueue(wc);
}

folly::coro::Task<ibv_wc> RdmaActiveEndpoint::co_nextCqEvent() {
  co_return co_await cqEvents_.dequeue();
}

int RdmaActiveEndpoint::clusterId() const {
  auto* processor = dynamic_cast<RdmaCqProcessorBase*>(cqProvider());
  return processor ? processor->clusterId() : -1;
}

folly::coro::Task<void> RdmaActiveEndpoint::co_flush() {
  ibv_qp_attr attr{};
  attr.qp_state = IBV_QPS_ERR;
  if (int error = ibv_modify_qp(qp(), &attr, IBV_QP_STATE)) {
    RLIB_LOG("ibv_modify_qp: %s", std::strerror(error));
  }
  // Flushed work requests complete promptly; give up after 10 seconds.
  for (int i = 0; i < 1000 && outstanding_ > 0; ++i) {
    co_await folly::coro::sleep(std::chrono::milliseconds{10});
  }
  if (outstanding_ > 0) {
    RLIB_LOG("endpoint %d: %d work requests did not complete",
             endpointId(), outstanding_.load());
  }
  if (auto* processor = dynamic_cast<RdmaCqProcessorBase*>(cqProvider())) {
    processor->unregisterQp(qp()->qp_num);
  }
}

} // namespace rlib
