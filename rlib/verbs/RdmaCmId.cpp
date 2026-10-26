#include "rlib/verbs/RdmaCmId.h"

#include <cstring>

#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"
#include "rlib/verbs/RdmaEventChannel.h"

namespace rlib {

namespace {

rdma_cm_id* createId(
    RdmaEventChannel& channel,
    void* context,
    rdma_port_space portSpace) {
  rdma_cm_id* id = nullptr;
  if (rdma_create_id(channel.get(), &id, context, portSpace)) {
    throwRdmaError("rdma_create_id");
  }
  return id;
}

} // namespace

RdmaCmId::RdmaCmId(
    RdmaEventChannel& channel,
    void* context,
    rdma_port_space portSpace)
    : id_{createId(channel, context, portSpace)} {}

void RdmaCmId::bindAddr(const SocketAddress& address) {
  // rdma_bind_addr takes a non-const sockaddr but does not change it.
  SocketAddress copy = address;
  if (rdma_bind_addr(id_.get(), copy.addr())) {
    throwRdmaError("rdma_bind_addr");
  }
  RLIB_LOG("bound id to %s", localAddress().toString().c_str());
}

void RdmaCmId::resolveAddr(const SocketAddress& destination, int timeoutMs) {
  SocketAddress copy = destination;
  if (rdma_resolve_addr(id_.get(), nullptr, copy.addr(), timeoutMs)) {
    throwRdmaError("rdma_resolve_addr");
  }
}

void RdmaCmId::resolveRoute(int timeoutMs) {
  if (rdma_resolve_route(id_.get(), timeoutMs)) {
    throwRdmaError("rdma_resolve_route");
  }
}

void RdmaCmId::listen(int backlog) {
  if (rdma_listen(id_.get(), backlog)) {
    throwRdmaError("rdma_listen");
  }
}

ibv_qp* RdmaCmId::createQp(ibv_pd* pd, ibv_qp_init_attr& attr) {
  if (rdma_create_qp(id_.get(), pd, &attr)) {
    throwRdmaError("rdma_create_qp");
  }
  return id_->qp;
}

void RdmaCmId::destroyQp() {
  rdma_destroy_qp(id_.get());
}

void RdmaCmId::connect(rdma_conn_param& param) {
  if (rdma_connect(id_.get(), &param)) {
    throwRdmaError("rdma_connect");
  }
}

void RdmaCmId::accept(rdma_conn_param& param) {
  if (rdma_accept(id_.get(), &param)) {
    throwRdmaError("rdma_accept");
  }
}

void RdmaCmId::disconnect() {
  if (rdma_disconnect(id_.get())) {
    throwRdmaError("rdma_disconnect");
  }
}

SocketAddress RdmaCmId::localAddress() const {
  return SocketAddress::fromSockaddr(rdma_get_local_addr(id_.get()));
}

SocketAddress RdmaCmId::peerAddress() const {
  return SocketAddress::fromSockaddr(rdma_get_peer_addr(id_.get()));
}

void RdmaCmId::Destroy::operator()(rdma_cm_id* id) const {
  if (rdma_destroy_id(id)) {
    RLIB_LOG("rdma_destroy_id: %s", std::strerror(errno));
  }
}

} // namespace rlib
