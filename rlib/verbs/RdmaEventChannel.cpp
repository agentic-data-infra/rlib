#include "rlib/verbs/RdmaEventChannel.h"

#include "rlib/verbs/RdmaError.h"

namespace rlib {

RdmaEventChannel::RdmaEventChannel()
    : channel_{rdma_create_event_channel()} {
  if (!channel_) {
    throwRdmaError("rdma_create_event_channel");
  }
}

void RdmaEventChannel::Destroy::operator()(rdma_event_channel* channel) const {
  rdma_destroy_event_channel(channel);
}

} // namespace rlib
