#pragma once

#include <rdma/rdma_cma.h>

#include <thread>

#include "rlib/verbs/RdmaCmId.h"
#include "rlib/verbs/RdmaEventChannel.h"

namespace rlib {

class RdmaEndpointGroupBase;

// The fields of an rdma_cm_event that endpoints use, copied so that the
// event can be acknowledged before it is dispatched.
struct RdmaCmEvent {
  rdma_cm_event_type type;
  rdma_cm_id* id;
  // For RDMA_CM_EVENT_CONNECT_REQUEST, the listening id; 'id' is the new one.
  rdma_cm_id* listenId;
  int status;
};

// A group's connection manager: owns the group's event channel and a thread
// that dispatches its events to the group. close() wakes the thread through
// an eventfd and joins it.
class RdmaCmProcessor {
 public:
  // Throws RdmaError.
  explicit RdmaCmProcessor(RdmaEndpointGroupBase& group);
  ~RdmaCmProcessor();

  RdmaCmProcessor(const RdmaCmProcessor&) = delete;
  RdmaCmProcessor& operator=(const RdmaCmProcessor&) = delete;

  void start();
  // Stops and joins the thread; idempotent. Not from the thread itself.
  void close();

  // A new id on the channel. Throws RdmaError.
  RdmaCmId createId();

 private:
  void run();

  RdmaEndpointGroupBase& group_;
  RdmaEventChannel channel_;
  int stopFd_;
  std::thread thread_;
};

} // namespace rlib
