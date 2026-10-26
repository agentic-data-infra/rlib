#include "rlib/endpoint/RdmaCmProcessor.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cstring>

#include "rlib/endpoint/RdmaEndpointGroup.h"
#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

RdmaCmProcessor::RdmaCmProcessor(RdmaEndpointGroupBase& group)
    : group_{group}, stopFd_{eventfd(0, EFD_CLOEXEC)} {
  if (stopFd_ < 0) {
    throwRdmaError("eventfd");
  }
  int flags = fcntl(channel_.fd(), F_GETFL);
  if (flags < 0 || fcntl(channel_.fd(), F_SETFL, flags | O_NONBLOCK) < 0) {
    int error = errno;
    ::close(stopFd_);
    throwRdmaError("fcntl", error);
  }
}

RdmaCmProcessor::~RdmaCmProcessor() {
  close();
  ::close(stopFd_);
}

void RdmaCmProcessor::start() {
  thread_ = std::thread{[this] { run(); }};
}

void RdmaCmProcessor::close() {
  if (!thread_.joinable()) {
    return;
  }
  uint64_t one = 1;
  if (write(stopFd_, &one, sizeof(one)) != sizeof(one)) {
    RLIB_LOG("eventfd write: %s", std::strerror(errno));
  }
  thread_.join();
}

RdmaCmId RdmaCmProcessor::createId() {
  return RdmaCmId{channel_};
}

void RdmaCmProcessor::run() {
  pollfd fds[2] = {{channel_.fd(), POLLIN, 0}, {stopFd_, POLLIN, 0}};
  while (true) {
    if (poll(fds, 2, -1) < 0) {
      if (errno == EINTR) {
        continue;
      }
      RLIB_LOG("poll: %s", std::strerror(errno));
      return;
    }
    if (fds[1].revents) {
      return;
    }
    rdma_cm_event* event = nullptr;
    while (rdma_get_cm_event(channel_.get(), &event) == 0) {
      RdmaCmEvent copy{event->event, event->id, event->listen_id, event->status};
      // Acknowledged first: destroying an id waits for its acknowledgements,
      // and dispatching may destroy one.
      rdma_ack_cm_event(event);
      RLIB_LOG("cm event %s", rdma_event_str(copy.type));
      group_.dispatchCmEvent(copy);
    }
  }
}

} // namespace rlib
