#pragma once

#include <memory>

#include "rlib/verbs/RdmaCmId.h"

namespace rlib {

// Creates a group's endpoints, of the application's endpoint class C (an
// RdmaEndpoint): for connect calls on the client side and for connection
// requests on the server side (serverSide true).
template <typename C>
class RdmaEndpointFactory {
 public:
  virtual ~RdmaEndpointFactory() = default;
  virtual std::shared_ptr<C> createEndpoint(RdmaCmId id, bool serverSide) = 0;
};

} // namespace rlib
