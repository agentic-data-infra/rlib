#include "rlib/endpoint/RdmaCqProcessor.h"

#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <cstring>

#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

RdmaCqProcessorBase::RdmaCqProcessorBase(
    ibv_context* verbs,
    int cqSize,
    bool polling,
    int cpu)
    : RdmaCqProvider{verbs, cqSize},
      polling_{polling},
      cpu_{cpu},
      stopFd_{eventfd(0, EFD_CLOEXEC)} {
  if (stopFd_ < 0) {
    throwRdmaError("eventfd");
  }
  int fd = compChannel()->fd;
  int flags = fcntl(fd, F_GETFL);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    int error = errno;
    ::close(stopFd_);
    throwRdmaError("fcntl", error);
  }
}

RdmaCqProcessorBase::~RdmaCqProcessorBase() {
  close();
  ::close(stopFd_);
}

void RdmaCqProcessorBase::start() {
  thread_ = std::thread{[this] { run(); }};
}

void RdmaCqProcessorBase::close() {
  if (!thread_.joinable()) {
    return;
  }
  stop_ = true;
  uint64_t one = 1;
  if (write(stopFd_, &one, sizeof(one)) != sizeof(one)) {
    RLIB_LOG("eventfd write: %s", std::strerror(errno));
  }
  thread_.join();
}

void RdmaCqProcessorBase::registerQp(uint32_t qpNum, RdmaEndpoint& endpoint) {
  std::lock_guard lock{mutex_};
  qps_[qpNum] = &endpoint;
}

void RdmaCqProcessorBase::unregisterQp(uint32_t qpNum) {
  std::lock_guard lock{mutex_};
  qps_.erase(qpNum);
}

void RdmaCqProcessorBase::run() {
  if (cpu_ >= 0) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu_, &set);
    if (int error = pthread_setaffinity_np(pthread_self(), sizeof(set), &set)) {
      RLIB_LOG("pthread_setaffinity_np: %s", std::strerror(error));
    }
  }
  if (polling_) {
    while (!stop_.load(std::memory_order_relaxed)) {
      drain();
    }
    return;
  }
  if (int error = ibv_req_notify_cq(cq(), 0)) {
    RLIB_LOG("ibv_req_notify_cq: %s", std::strerror(error));
    return;
  }
  pollfd fds[2] = {{compChannel()->fd, POLLIN, 0}, {stopFd_, POLLIN, 0}};
  while (true) {
    // Completions may have arrived before the notification was requested.
    drain();
    if (::poll(fds, 2, -1) < 0) {
      if (errno == EINTR) {
        continue;
      }
      RLIB_LOG("poll: %s", std::strerror(errno));
      return;
    }
    if (fds[1].revents) {
      return;
    }
    ibv_cq* eventCq = nullptr;
    void* context = nullptr;
    if (ibv_get_cq_event(compChannel(), &eventCq, &context) == 0) {
      ibv_ack_cq_events(eventCq, 1);
      ibv_req_notify_cq(eventCq, 0);
    }
  }
}

void RdmaCqProcessorBase::drain() {
  std::array<ibv_wc, 32> wcs;
  while (true) {
    int n = poll(wcs);
    if (n < 0) {
      RLIB_LOG("ibv_poll_cq: %d", n);
      return;
    }
    for (int i = 0; i < n; ++i) {
      dispatch(wcs[i]);
    }
    if (n < static_cast<int>(wcs.size())) {
      return;
    }
  }
}

void RdmaCqProcessorBase::dispatch(const ibv_wc& wc) {
  if (wc.wr_id & RdmaCompletionWaiter::kTag) {
    auto* waiter = reinterpret_cast<RdmaCompletionWaiter*>(
        wc.wr_id & ~RdmaCompletionWaiter::kTag);
    waiter->wc = wc;
    waiter->outstanding.fetch_sub(1);
    // Resumes the waiter on its executor, not on this thread.
    waiter->done.post();
    return;
  }
  std::lock_guard lock{mutex_};
  auto it = qps_.find(wc.qp_num);
  if (it == qps_.end()) {
    RLIB_LOG("completion for unknown QP %u", wc.qp_num);
    return;
  }
  dispatchCqEvent(*it->second, wc);
}

} // namespace rlib
