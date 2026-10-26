#pragma once

#include <infiniband/verbs.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <folly/coro/Task.h>

#include "rlib/endpoint/RdmaCmProcessor.h"
#include "rlib/endpoint/RdmaCqProvider.h"
#include "rlib/endpoint/RdmaEndpoint.h"
#include "rlib/endpoint/RdmaEndpointFactory.h"
#include "rlib/endpoint/RdmaServerEndpoint.h"
#include "rlib/verbs/RdmaConnParam.h"

namespace rlib {

// What RdmaEndpointGroup<C> has in common for all endpoint classes: the
// connection manager thread, the endpoints by id, and connection
// parameters. Endpoints refer to their group, which must outlive them;
// co_close the group before destroying it.
class RdmaEndpointGroupBase {
 public:
  virtual ~RdmaEndpointGroupBase();

  RdmaEndpointGroupBase(const RdmaEndpointGroupBase&) = delete;
  RdmaEndpointGroupBase& operator=(const RdmaEndpointGroupBase&) = delete;

  // For connecting and accepting; set before.
  RdmaConnParam& connParam() {
    return connParam_;
  }
  const RdmaConnParam& connParam() const {
    return connParam_;
  }

  // Closes all endpoints and server endpoints, then stops the connection
  // manager. Idempotent.
  folly::coro::Task<void> co_close();
  bool isClosed() const {
    return closed_;
  }

  int nextId();

 protected:
  RdmaEndpointGroupBase();

  // Starts the connection manager thread, once the group is complete.
  void start();
  RdmaCmId createId();
  void registerEndpoint(std::shared_ptr<RdmaEndpoint> endpoint);
  void registerServerEndpoint(std::shared_ptr<RdmaServerEndpointBase> server);

  // The typed hooks of RdmaEndpointGroup<C>.
  virtual std::shared_ptr<RdmaEndpoint> createEndpointRaw(
      RdmaCmId id,
      bool serverSide) = 0;
  virtual std::unique_ptr<RdmaCqProvider> createCqProviderRaw(
      RdmaEndpoint& endpoint) = 0;
  virtual ibv_qp* createQpProviderRaw(RdmaEndpoint& endpoint) = 0;
  virtual void allocateResourcesRaw(RdmaEndpoint& endpoint) = 0;

 private:
  friend class RdmaCmProcessor;
  friend class RdmaEndpoint;
  friend class RdmaServerEndpointBase;

  void dispatchCmEvent(const RdmaCmEvent& event);
  // For a connection request to a server endpoint; nullptr (destroying
  // the id, which refuses the request) once closing.
  std::shared_ptr<RdmaEndpoint> createServerSideEndpoint(RdmaCmId id);
  void unregisterEndpoint(rdma_cm_id* id);
  void unregisterServerEndpoint(rdma_cm_id* id);

  RdmaConnParam connParam_;
  std::atomic<bool> closing_{false};
  std::atomic<bool> closed_{false};
  std::mutex mutex_;
  std::unordered_map<rdma_cm_id*, std::shared_ptr<RdmaEndpoint>> endpoints_;
  std::unordered_map<rdma_cm_id*, std::shared_ptr<RdmaServerEndpointBase>>
      serverEndpoints_;
  // Last: its thread stops before the rest is destroyed.
  RdmaCmProcessor cmProcessor_;
};

// A group of endpoints of the application's class C (an RdmaEndpoint),
// which share a connection manager thread and connection parameters.
// Subclasses decide how endpoints get their completion queue and QP and
// when resources are allocated (RdmaPassiveEndpointGroup: one CQ per
// endpoint, polled by the application). Call init() before use.
template <typename C>
class RdmaEndpointGroup : public RdmaEndpointGroupBase {
 public:
  // Starts the group with the factory for its endpoints.
  void init(std::unique_ptr<RdmaEndpointFactory<C>> factory) {
    factory_ = std::move(factory);
    start();
  }

  // A server endpoint, to bind and accept connections on. Throws RdmaError.
  std::shared_ptr<RdmaServerEndpoint<C>> createServerEndpoint() {
    auto server = std::make_shared<RdmaServerEndpoint<C>>(*this, createId());
    registerServerEndpoint(server);
    return server;
  }

  // A client endpoint, to co_connect. Throws RdmaError.
  std::shared_ptr<C> createEndpoint() {
    auto endpoint = factory_->createEndpoint(createId(), false);
    registerEndpoint(endpoint);
    return endpoint;
  }

  virtual std::unique_ptr<RdmaCqProvider> createCqProvider(C& endpoint) = 0;
  virtual ibv_qp* createQpProvider(C& endpoint) = 0;
  // Must end with endpoint.allocateResources().
  virtual void allocateResources(C& endpoint) = 0;

 protected:
  std::shared_ptr<RdmaEndpoint> createEndpointRaw(
      RdmaCmId id,
      bool serverSide) override {
    return factory_->createEndpoint(std::move(id), serverSide);
  }
  std::unique_ptr<RdmaCqProvider> createCqProviderRaw(
      RdmaEndpoint& endpoint) override {
    return createCqProvider(static_cast<C&>(endpoint));
  }
  ibv_qp* createQpProviderRaw(RdmaEndpoint& endpoint) override {
    return createQpProvider(static_cast<C&>(endpoint));
  }
  void allocateResourcesRaw(RdmaEndpoint& endpoint) override {
    allocateResources(static_cast<C&>(endpoint));
  }

 private:
  std::unique_ptr<RdmaEndpointFactory<C>> factory_;
};

} // namespace rlib
