#pragma once

#include <infiniband/verbs.h>

#include <memory>
#include <span>

#include <folly/coro/Task.h>

namespace rlib {

// Owns a completion queue and its completion channel.
class RdmaCqProvider {
 public:
  // Throws RdmaError.
  RdmaCqProvider(ibv_context* verbs, int cqSize);
  virtual ~RdmaCqProvider() = default;

  ibv_cq* cq() const {
    return cq_.get();
  }
  ibv_comp_channel* compChannel() const {
    return compChannel_.get();
  }
  int cqSize() const {
    return cqSize_;
  }

  // Polls up to wcs.size() completions without waiting; returns how many it
  // got, or a negative value if polling failed.
  int poll(std::span<ibv_wc> wcs);

  // Polls until there is at least one completion, rescheduling on the
  // current executor between empty polls; returns how many it got, or a
  // negative value if polling failed. Cancellable.
  folly::coro::Task<int> co_poll(std::span<ibv_wc> wcs);

 private:
  struct DestroyChannel {
    void operator()(ibv_comp_channel* channel) const;
  };
  struct DestroyCq {
    void operator()(ibv_cq* cq) const;
  };
  int cqSize_;
  // Declared first, destroyed after the CQ.
  std::unique_ptr<ibv_comp_channel, DestroyChannel> compChannel_;
  std::unique_ptr<ibv_cq, DestroyCq> cq_;
};

} // namespace rlib
