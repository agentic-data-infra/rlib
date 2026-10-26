#pragma once

#include <infiniband/verbs.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include <folly/coro/BlockingWait.h>

#include "rlib/endpoint/RdmaActiveEndpoint.h"
#include "rlib/endpoint/RdmaCqProcessor.h"
#include "rlib/endpoint/RdmaEndpointGroup.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

// A group of RdmaActiveEndpoints (class C): all endpoints on a device share
// one completion queue, whose RdmaActiveCqProcessor thread wakes the
// endpoints' co_post* calls and hands other completions to their
// dispatchCqEvent. Call init() before use.
template <typename C>
class RdmaActiveEndpointGroup : public RdmaEndpointGroup<C> {
 public:
  // 'polling': the CQ threads poll all the time instead of sleeping on the
  // completion channel. Each QP takes up to maxWR send and receive work
  // requests of up to maxSge scatter/gather elements; each device's CQ
  // holds cqSize completions, for all its endpoints. 'cpu' (unless
  // negative) is the core to pin the CQ threads to.
  RdmaActiveEndpointGroup(
      bool polling,
      int maxWR,
      int maxSge,
      int cqSize,
      int cpu = -1)
      : polling_{polling},
        maxWR_{maxWR},
        maxSge_{maxSge},
        cqSize_{cqSize},
        cpu_{cpu} {}

  ~RdmaActiveEndpointGroup() override {
    // Endpoints flush through the CQ threads, so close them first.
    if (!this->isClosed()) {
      folly::coro::blockingWait(co_close());
    }
  }

  std::shared_ptr<RdmaCqProvider> createCqProvider(C& endpoint) override {
    ibv_context* verbs = endpoint.cmId().verbs();
    std::lock_guard lock{mutex_};
    auto& processor = processors_[verbs];
    if (!processor) {
      processor = std::make_shared<RdmaActiveCqProcessor<C>>(
          verbs, cqSize_, polling_, cpu_);
      processor->start();
    }
    return processor;
  }

  ibv_qp* createQpProvider(C& endpoint) override {
    auto* processor =
        static_cast<RdmaCqProcessorBase*>(endpoint.cqProvider());
    ibv_qp_init_attr attr{};
    attr.cap.max_recv_sge = maxSge_;
    attr.cap.max_recv_wr = maxWR_;
    attr.cap.max_send_sge = maxSge_;
    attr.cap.max_send_wr = maxWR_;
    attr.qp_type = IBV_QPT_RC;
    attr.recv_cq = processor->cq();
    attr.send_cq = processor->cq();
    ibv_qp* qp = endpoint.cmId().createQp(endpoint.pd(), attr);
    processor->registerQp(qp->qp_num, endpoint);
    return qp;
  }

  void allocateResources(C& endpoint) override {
    endpoint.allocateResources();
  }

  // Closes the endpoints, then stops the CQ threads. Idempotent.
  folly::coro::Task<void> co_close() override {
    co_await RdmaEndpointGroup<C>::co_close();
    std::lock_guard lock{mutex_};
    for (auto& [verbs, processor] : processors_) {
      processor->close();
    }
  }

  bool isPolling() const {
    return polling_;
  }
  int maxWR() const {
    return maxWR_;
  }
  int maxSge() const {
    return maxSge_;
  }
  int cqSize() const {
    return cqSize_;
  }

 private:
  const bool polling_;
  const int maxWR_;
  const int maxSge_;
  const int cqSize_;
  const int cpu_;
  std::mutex mutex_;
  std::unordered_map<ibv_context*, std::shared_ptr<RdmaActiveCqProcessor<C>>>
      processors_;
};

} // namespace rlib
