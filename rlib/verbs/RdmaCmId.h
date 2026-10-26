#pragma once

#include <rdma/rdma_cma.h>

#include <memory>

#include "rlib/verbs/SocketAddress.h"

namespace rlib {

class RdmaEventChannel;

// Owns an rdma_cm_id, librdmacm's handle of a connection (or a listener).
// Destroying it blocks until its events are acknowledged; destroy its QP
// first. Move-only.
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

  // Resolves 'destination' to a device and address, reporting
  // RDMA_CM_EVENT_ADDR_RESOLVED (or ..._ADDR_ERROR) on the id's channel.
  // Throws RdmaError.
  void resolveAddr(const SocketAddress& destination, int timeoutMs);
  // Resolves the route to the address, reporting
  // RDMA_CM_EVENT_ROUTE_RESOLVED (or ..._ROUTE_ERROR). Throws RdmaError.
  void resolveRoute(int timeoutMs);
  // Listens for connection requests on the bound address, reported as
  // RDMA_CM_EVENT_CONNECT_REQUEST with a new id. Throws RdmaError.
  void listen(int backlog);

  // Creates a QP on the id's device, owned by the id until destroyQp().
  // Throws RdmaError.
  ibv_qp* createQp(ibv_pd* pd, ibv_qp_init_attr& attr);
  void destroyQp();

  // Throw RdmaError.
  void connect(rdma_conn_param& param);
  void accept(rdma_conn_param& param);
  void disconnect();

  SocketAddress localAddress() const;
  SocketAddress peerAddress() const;

 private:
  struct Destroy {
    void operator()(rdma_cm_id* id) const;
  };
  std::unique_ptr<rdma_cm_id, Destroy> id_;
};

} // namespace rlib
