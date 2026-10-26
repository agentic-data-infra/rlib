#pragma once

#include <rdma/rdma_cma.h>

#include <memory>

#include "rlib/verbs/SocketAddress.h"

namespace rlib {

class RdmaEventChannel;

// Owns an rdma_cm_id, librdmacm's handle of a connection (or a listener).
// Destroying it blocks until its events are acknowledged, and leaves any QP
// on it alone. Move-only.
class RdmaCmId {
 public:
  // Creates an id whose events go to 'channel'; RDMA_PS_TCP gives reliable
  // connected QPs. Throws RdmaError.
  explicit RdmaCmId(
      RdmaEventChannel& channel,
      void* context = nullptr,
      rdma_port_space portSpace = RDMA_PS_TCP);

  // Takes ownership of 'id', e.g. the new id of a connection request.
  explicit RdmaCmId(rdma_cm_id* id) : id_{id} {}

  rdma_cm_id* get() const {
    return id_.get();
  }
  // The device the id is bound to, or nullptr before it is.
  ibv_context* verbs() const {
    return id_->verbs;
  }

  // Binds the id to a local address; port 0 picks a free port. An address
  // of an RDMA device's interface binds the id to that device too. Throws
  // RdmaError.
  void bindAddr(const SocketAddress& address);

  SocketAddress localAddress() const;

 private:
  struct Destroy {
    void operator()(rdma_cm_id* id) const;
  };
  std::unique_ptr<rdma_cm_id, Destroy> id_;
};

} // namespace rlib
