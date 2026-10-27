#pragma once

#include <infiniband/verbs.h>

#include <memory>

#include "rlib/endpoint/RdmaEndpointGroup.h"
#include "rlib/rpc/RdmaRpcClientEndpoint.h"

namespace rlib {

// A group of RPC client endpoints. Each endpoint has its own completion
// queue, which the coroutines calling on it poll (no CQ thread).
//
//   auto group = RdmaRpcClientGroup<EchoProtocol>::create();
//   auto endpoint = group->createEndpoint();
//   co_await endpoint->co_connect(address, 1s);
//   co_await endpoint->co_call(request, response);
template <RdmaRpcProtocol P>
class RdmaRpcClientGroup
    : public RdmaEndpointGroup<RdmaRpcClientEndpoint<P>> {
 public:
  using Endpoint = RdmaRpcClientEndpoint<P>;

  // A started group.
  static std::unique_ptr<RdmaRpcClientGroup> create(
      const RdmaRpcOptions& options = {}) {
    auto group = std::unique_ptr<RdmaRpcClientGroup>{
        new RdmaRpcClientGroup{options}};
    group->init(std::make_unique<Factory>(*group));
    return group;
  }

  std::shared_ptr<RdmaCqProvider> createCqProvider(Endpoint& endpoint) override {
    return std::make_shared<RdmaCqProvider>(
        endpoint.cmId().verbs(), options_.recvQueue + options_.sendSlots());
  }

  ibv_qp* createQpProvider(Endpoint& endpoint) override {
    ibv_cq* cq = endpoint.cqProvider()->cq();
    ibv_qp_init_attr attr{};
    attr.cap.max_recv_sge = 1;
    attr.cap.max_recv_wr = options_.recvQueue;
    attr.cap.max_send_sge = 1;
    attr.cap.max_send_wr = options_.sendSlots();
    attr.cap.max_inline_data = options_.maxInline;
    attr.qp_type = IBV_QPT_RC;
    attr.recv_cq = cq;
    attr.send_cq = cq;
    ibv_qp* qp = endpoint.cmId().createQp(endpoint.pd(), attr);
    endpoint.setMaxInline(static_cast<int>(attr.cap.max_inline_data));
    return qp;
  }

  void allocateResources(Endpoint& endpoint) override {
    endpoint.allocateResources();
  }

  const RdmaRpcOptions& options() const {
    return options_;
  }

 private:
  class Factory : public RdmaEndpointFactory<Endpoint> {
   public:
    explicit Factory(RdmaRpcClientGroup& group) : group_{group} {}
    std::shared_ptr<Endpoint> createEndpoint(RdmaCmId id, bool serverSide)
        override {
      return std::make_shared<Endpoint>(
          group_, std::move(id), serverSide, group_.options_);
    }

   private:
    RdmaRpcClientGroup& group_;
  };

  explicit RdmaRpcClientGroup(const RdmaRpcOptions& options)
      : options_{options} {}

  const RdmaRpcOptions options_;
};

} // namespace rlib
