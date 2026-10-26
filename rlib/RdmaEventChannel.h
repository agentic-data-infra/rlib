#pragma once

#include <rdma/rdma_cma.h>

#include <memory>

namespace rlib {

// Owns an rdma_event_channel, the queue librdmacm reports connection events
// of its ids on. Move-only.
class RdmaEventChannel {
 public:
  // Throws RdmaError.
  RdmaEventChannel();

  rdma_event_channel* get() const {
    return channel_.get();
  }
  // Readable when an event is pending.
  int fd() const {
    return channel_->fd;
  }

 private:
  struct Destroy {
    void operator()(rdma_event_channel* channel) const;
  };
  std::unique_ptr<rdma_event_channel, Destroy> channel_;
};

} // namespace rlib
