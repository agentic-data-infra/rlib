#pragma once

#include <infiniband/verbs.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <folly/coro/BlockingWait.h>

#include "rlib/endpoint/RdmaActiveEndpoint.h"
#include "rlib/endpoint/RdmaCqProcessor.h"
#include "rlib/endpoint/RdmaEndpointGroup.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

// A group of RdmaActiveEndpoints (class C) whose completions RdmaActive-
// CqProcessor threads take: they wake the endpoints' co_post* calls and
// hand other completions to their dispatchCqEvent. Each device gets one
// processor per entry of 'cpus', pinned to that core (or one unpinned
// processor if 'cpus' is empty), each with its own completion queue; new
// endpoints go to them round-robin. Call init() before use.
template <typename C>
class RdmaActiveEndpointGroup : public RdmaEndpointGroup<C> {
 public:
  // 'polling': the CQ threads poll all the time instead of sleeping on the
  // completion channel. Each QP takes up to maxWR send and receive work
  // requests of up to maxSge scatter/gather elements. Each processor's CQ
  // holds cqSize completions; its endpoints may need 2 * maxWR each, and
  // allocating an endpoint for which a full processor is next fails.
  RdmaActiveEndpointGroup(
      bool polling,
      int maxWR,
      int maxSge,
      int cqSize,
      std::vector<int> cpus = {})
      : polling_{polling},
        maxWR_{maxWR},
        maxSge_{maxSge},
        cqSize_{cqSize},
        cpus_{std::move(cpus)} {}

  ~RdmaActiveEndpointGroup() override {
    // Endpoints flush through the CQ threads, so close them first.
    if (!this->isClosed()) {
      folly::coro::blockingWait(co_close());
    }
  }

  // The device's next processor in turn.
  std::shared_ptr<RdmaCqProvider> createCqProvider(C& endpoint) override {
    ibv_context* verbs = endpoint.cmId().verbs();
    std::lock_guard lock{mutex_};
    auto& device = devices_[verbs];
    if (device.processors.empty()) {
      auto cpus = cpus_.empty() ? std::vector<int>{-1} : cpus_;
      for (size_t i = 0; i < cpus.size(); ++i) {
        auto processor = std::make_shared<RdmaActiveCqProcessor<C>>(
            verbs, cqSize_, polling_, cpus[i], static_cast<int>(i));
        processor->start();
        device.processors.push_back(std::move(processor));
      }
    }
    return device.processors[device.next++ % device.processors.size()];
  }

  // Throws RdmaError (ENOSPC if the endpoint's processor is full).
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
    if (!processor->registerQp(qp->qp_num, endpoint, 2 * maxWR_)) {
      endpoint.cmId().destroyQp();
      throw RdmaError{
          ENOSPC, "CQ processor full: raise cqSize or add processors"};
    }
    return qp;
  }

  void allocateResources(C& endpoint) override {
    endpoint.allocateResources();
  }

  // Closes the endpoints, then stops the CQ threads. Idempotent.
  folly::coro::Task<void> co_close() override {
    co_await RdmaEndpointGroup<C>::co_close();
    std::lock_guard lock{mutex_};
    for (auto& [verbs, device] : devices_) {
      for (auto& processor : device.processors) {
        processor->close();
      }
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
  const std::vector<int>& cpus() const {
    return cpus_;
  }

 private:
  struct Device {
    std::vector<std::shared_ptr<RdmaActiveCqProcessor<C>>> processors;
    size_t next = 0;
  };

  const bool polling_;
  const int maxWR_;
  const int maxSge_;
  const int cqSize_;
  const std::vector<int> cpus_;
  std::mutex mutex_;
  std::unordered_map<ibv_context*, Device> devices_;
};

} // namespace rlib
