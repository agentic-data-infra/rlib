#pragma once

#include <infiniband/verbs.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <folly/coro/Baton.h>

#include "rlib/endpoint/RdmaCqProvider.h"

namespace rlib {

class RdmaEndpoint;

// A coroutine waiting for the completion of a work request, typically on
// its frame. The work request's wr_id is wrId(): the waiter's address with
// the top bit set, which no application wr_id may have.
struct RdmaCompletionWaiter {
  static constexpr uint64_t kTag = uint64_t{1} << 63;

  explicit RdmaCompletionWaiter(std::atomic<int>& outstanding)
      : outstanding{outstanding} {}

  uint64_t wrId() const {
    return reinterpret_cast<uintptr_t>(this) | kTag;
  }

  // Decremented when the completion arrives.
  std::atomic<int>& outstanding;
  ibv_wc wc{};
  folly::coro::Baton done;
};

// What RdmaCqProcessor<C> has in common for all endpoint classes: a
// completion queue shared by the QPs of many endpoints, and a thread that
// takes its completions. Completions of waiters (RdmaCompletionWaiter) wake
// them; others go to dispatchCqEvent with the endpoint of their QP. The
// thread either polls the queue all the time (lowest latency, one core) or
// sleeps on the completion channel until a completion arrives.
class RdmaCqProcessorBase : public RdmaCqProvider {
 public:
  // 'cpu' (unless negative) is the core to pin the thread to; 'clusterId'
  // tells processors of the same group and device apart. Throws RdmaError.
  RdmaCqProcessorBase(
      ibv_context* verbs,
      int cqSize,
      bool polling,
      int cpu,
      int clusterId = 0);
  ~RdmaCqProcessorBase() override;

  void start();
  // Stops and joins the thread; idempotent.
  void close();

  // Registers a QP that may have up to 'cqEntries' completions in the
  // queue at a time; returns false, registering nothing, if the queue
  // cannot take them on top of the registered QPs'.
  bool registerQp(uint32_t qpNum, RdmaEndpoint& endpoint, int cqEntries);
  // After it returns, no more completions go to the QP's endpoint.
  void unregisterQp(uint32_t qpNum);

  bool isPolling() const {
    return polling_;
  }
  // The core the thread is pinned to, or -1.
  int cpu() const {
    return cpu_;
  }
  int clusterId() const {
    return clusterId_;
  }
  // The registered QPs.
  int qpCount() const;

 protected:
  // On the processor's thread, with the QP's endpoint registered.
  virtual void dispatchCqEvent(RdmaEndpoint& endpoint, const ibv_wc& wc) = 0;

 private:
  void run();
  void drain();
  void dispatch(const ibv_wc& wc);

  const bool polling_;
  const int cpu_;
  const int clusterId_;
  int stopFd_;
  std::atomic<bool> stop_{false};
  std::thread thread_;
  struct Qp {
    RdmaEndpoint* endpoint;
    int cqEntries;
  };
  // Held while dispatching, so that unregisterQp waits for it.
  mutable std::mutex mutex_;
  std::unordered_map<uint32_t, Qp> qps_;
  int cqEntries_ = 0;
};

// A CQ processor for endpoints of class C.
template <typename C>
class RdmaCqProcessor : public RdmaCqProcessorBase {
 public:
  using RdmaCqProcessorBase::RdmaCqProcessorBase;

  virtual void dispatchCqEvent(C& endpoint, const ibv_wc& wc) = 0;

 protected:
  void dispatchCqEvent(RdmaEndpoint& endpoint, const ibv_wc& wc) final {
    dispatchCqEvent(static_cast<C&>(endpoint), wc);
  }
};

// Hands completions to the endpoint's dispatchCqEvent (C is an
// RdmaActiveEndpoint).
template <typename C>
class RdmaActiveCqProcessor : public RdmaCqProcessor<C> {
 public:
  using RdmaCqProcessor<C>::RdmaCqProcessor;

  void dispatchCqEvent(C& endpoint, const ibv_wc& wc) override {
    endpoint.dispatchCqEvent(wc);
  }
};

} // namespace rlib
