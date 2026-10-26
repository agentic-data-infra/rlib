#include "rlib/endpoint/RdmaCqProvider.h"

#include <cstring>

#include <folly/coro/CurrentExecutor.h>

#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

RdmaCqProvider::RdmaCqProvider(ibv_context* verbs, int cqSize)
    : cqSize_{cqSize}, compChannel_{ibv_create_comp_channel(verbs)} {
  if (!compChannel_) {
    throwRdmaError("ibv_create_comp_channel");
  }
  cq_.reset(ibv_create_cq(verbs, cqSize, nullptr, compChannel_.get(), 0));
  if (!cq_) {
    throwRdmaError("ibv_create_cq");
  }
}

int RdmaCqProvider::poll(std::span<ibv_wc> wcs) {
  return ibv_poll_cq(cq_.get(), static_cast<int>(wcs.size()), wcs.data());
}

folly::coro::Task<int> RdmaCqProvider::co_poll(std::span<ibv_wc> wcs) {
  while (true) {
    int n = poll(wcs);
    if (n != 0) {
      co_return n;
    }
    co_await folly::coro::co_reschedule_on_current_executor;
    co_await folly::coro::co_safe_point;
  }
}

void RdmaCqProvider::DestroyChannel::operator()(
    ibv_comp_channel* channel) const {
  if (int error = ibv_destroy_comp_channel(channel)) {
    RLIB_LOG("ibv_destroy_comp_channel: %s", std::strerror(error));
  }
}

void RdmaCqProvider::DestroyCq::operator()(ibv_cq* cq) const {
  if (int error = ibv_destroy_cq(cq)) {
    RLIB_LOG("ibv_destroy_cq: %s", std::strerror(error));
  }
}

} // namespace rlib
