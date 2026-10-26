#pragma once

#include <memory>

#include "rlib/verbs/RdmaCmId.h"

namespace rlib {

class RdmaEndpointGroupBase;

// Creates a group's endpoints, of the application's endpoint class C (an
// RdmaEndpoint): for connect calls on the client side and for connection
// requests on the server side (serverSide true).
template <typename C>
class RdmaEndpointFactory {
 public:
  virtual ~RdmaEndpointFactory() = default;
  virtual std::shared_ptr<C> createEndpoint(RdmaCmId id, bool serverSide) = 0;
};

// Makes endpoints with C's constructor C(group, id, serverSide).
template <typename C>
class RdmaDefaultEndpointFactory : public RdmaEndpointFactory<C> {
 public:
  explicit RdmaDefaultEndpointFactory(RdmaEndpointGroupBase& group)
      : group_{group} {}

  std::shared_ptr<C> createEndpoint(RdmaCmId id, bool serverSide) override {
    return std::make_shared<C>(group_, std::move(id), serverSide);
  }

 private:
  RdmaEndpointGroupBase& group_;
};

} // namespace rlib
