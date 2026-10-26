#pragma once

#include <infiniband/verbs.h>

#include <memory>

#include "rlib/endpoint/RdmaEndpoint.h"
#include "rlib/endpoint/RdmaEndpointGroup.h"

namespace rlib {

// A group whose endpoints each get their own completion queue, which the
// application polls itself (RdmaCqProvider::poll and co_poll, through
// endpoint.cqProvider()). Resources are allocated while connecting.
template <typename C>
class RdmaPassiveEndpointGroup : public RdmaEndpointGroup<C> {
 public:
  // A started group of plain RdmaEndpoints.
  static std::unique_ptr<RdmaPassiveEndpointGroup<RdmaEndpoint>>
  createDefaultGroup(int maxWR, int maxSge, int cqSize) {
    auto group = std::make_unique<RdmaPassiveEndpointGroup<RdmaEndpoint>>(
        maxWR, maxSge, cqSize);
    group->init(std::make_unique<RawEndpointFactory>(*group));
    return group;
  }

  // Endpoints' QPs take up to maxWR send and receive work requests of up to
  // maxSge scatter/gather elements; their CQs hold cqSize completions.
  RdmaPassiveEndpointGroup(int maxWR, int maxSge, int cqSize)
      : maxWR_{maxWR}, maxSge_{maxSge}, cqSize_{cqSize} {}

  std::unique_ptr<RdmaCqProvider> createCqProvider(C& endpoint) override {
    return std::make_unique<RdmaCqProvider>(endpoint.cmId().verbs(), cqSize_);
  }

  ibv_qp* createQpProvider(C& endpoint) override {
    ibv_cq* cq = endpoint.cqProvider()->cq();
    ibv_qp_init_attr attr{};
    attr.cap.max_recv_sge = maxSge_;
    attr.cap.max_recv_wr = maxWR_;
    attr.cap.max_send_sge = maxSge_;
    attr.cap.max_send_wr = maxWR_;
    attr.qp_type = IBV_QPT_RC;
    attr.recv_cq = cq;
    attr.send_cq = cq;
    return endpoint.cmId().createQp(endpoint.pd(), attr);
  }

  void allocateResources(C& endpoint) override {
    endpoint.allocateResources();
  }

  int maxWR() const {
    return maxWR_;
  }
  int maxSge() const {
    return maxSge_;
  }
  int cqSize() const {
    return cqSize_;
  }

 private:
  class RawEndpointFactory : public RdmaEndpointFactory<RdmaEndpoint> {
   public:
    explicit RawEndpointFactory(RdmaEndpointGroupBase& group)
        : group_{group} {}
    std::shared_ptr<RdmaEndpoint> createEndpoint(RdmaCmId id, bool serverSide)
        override {
      return std::make_shared<RdmaEndpoint>(group_, std::move(id), serverSide);
    }

   private:
    RdmaEndpointGroupBase& group_;
  };

  int maxWR_;
  int maxSge_;
  int cqSize_;
};

} // namespace rlib
