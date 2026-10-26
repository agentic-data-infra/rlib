#include "rlib/RdmaCmId.h"

#include <cstring>

#include "rlib/Log.h"
#include "rlib/RdmaError.h"
#include "rlib/RdmaEventChannel.h"

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

SocketAddress RdmaCmId::localAddress() const {
  return SocketAddress::fromSockaddr(rdma_get_local_addr(id_.get()));
}

void RdmaCmId::Destroy::operator()(rdma_cm_id* id) const {
  if (rdma_destroy_id(id)) {
    RLIB_LOG("rdma_destroy_id: %s", std::strerror(errno));
  }
}

} // namespace rlib
