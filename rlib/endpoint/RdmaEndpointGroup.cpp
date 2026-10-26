#include "rlib/endpoint/RdmaEndpointGroup.h"

#include <vector>

#include <folly/coro/BlockingWait.h>

#include "rlib/verbs/Log.h"

namespace rlib {

RdmaEndpointGroupBase::RdmaEndpointGroupBase() : cmProcessor_{*this} {}

RdmaEndpointGroupBase::~RdmaEndpointGroupBase() {
  if (!closing_) {
    // The subclasses are gone: no more events for their hooks.
    cmProcessor_.close();
    folly::coro::blockingWait(co_close());
  }
}

int RdmaEndpointGroupBase::nextId() {
  static std::atomic<int> next{0};
  return next++;
}

void RdmaEndpointGroupBase::start() {
  cmProcessor_.start();
}

RdmaCmId RdmaEndpointGroupBase::createId() {
  return cmProcessor_.createId();
}

void RdmaEndpointGroupBase::registerEndpoint(
    std::shared_ptr<RdmaEndpoint> endpoint) {
  std::lock_guard lock{mutex_};
  auto* id = endpoint->cmId().get();
  endpoints_[id] = std::move(endpoint);
}

void RdmaEndpointGroupBase::registerServerEndpoint(
    std::shared_ptr<RdmaServerEndpointBase> server) {
  std::lock_guard lock{mutex_};
  auto* id = server->cmId().get();
  serverEndpoints_[id] = std::move(server);
}

void RdmaEndpointGroupBase::unregisterEndpoint(rdma_cm_id* id) {
  std::lock_guard lock{mutex_};
  endpoints_.erase(id);
}

void RdmaEndpointGroupBase::unregisterServerEndpoint(rdma_cm_id* id) {
  std::lock_guard lock{mutex_};
  serverEndpoints_.erase(id);
}

std::shared_ptr<RdmaEndpoint> RdmaEndpointGroupBase::createServerSideEndpoint(
    RdmaCmId id) {
  if (closing_) {
    return nullptr;
  }
  auto endpoint = createEndpointRaw(std::move(id), true);
  registerEndpoint(endpoint);
  return endpoint;
}

void RdmaEndpointGroupBase::dispatchCmEvent(const RdmaCmEvent& event) {
  if (closed_) {
    return;
  }
  try {
    if (event.type == RDMA_CM_EVENT_CONNECT_REQUEST) {
      std::shared_ptr<RdmaServerEndpointBase> server;
      {
        std::lock_guard lock{mutex_};
        auto it = serverEndpoints_.find(event.listenId);
        if (it != serverEndpoints_.end()) {
          server = it->second;
        }
      }
      if (server) {
        server->dispatchCmEvent(event);
      } else {
        rdma_reject(event.id, nullptr, 0);
        rdma_destroy_id(event.id);
      }
      return;
    }
    std::shared_ptr<RdmaEndpoint> endpoint;
    {
      std::lock_guard lock{mutex_};
      auto it = endpoints_.find(event.id);
      if (it != endpoints_.end()) {
        endpoint = it->second;
      }
    }
    if (endpoint) {
      endpoint->dispatchCmEvent(event);
    }
  } catch (const std::exception& e) {
    RLIB_LOG("dispatching %s: %s", rdma_event_str(event.type), e.what());
  }
}

folly::coro::Task<void> RdmaEndpointGroupBase::co_close() {
  if (closing_.exchange(true)) {
    co_return;
  }
  std::vector<std::shared_ptr<RdmaEndpoint>> endpoints;
  std::vector<std::shared_ptr<RdmaServerEndpointBase>> servers;
  {
    std::lock_guard lock{mutex_};
    for (auto& [id, endpoint] : endpoints_) {
      endpoints.push_back(endpoint);
    }
    for (auto& [id, server] : serverEndpoints_) {
      servers.push_back(server);
    }
  }
  for (auto& endpoint : endpoints) {
    co_await endpoint->co_close();
  }
  for (auto& server : servers) {
    co_await server->co_close();
  }
  cmProcessor_.close();
  closed_ = true;
}

} // namespace rlib
