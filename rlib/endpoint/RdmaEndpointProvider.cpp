#include "rlib/endpoint/RdmaEndpointProvider.h"

#include "rlib/verbs/RdmaError.h"

namespace rlib {

RdmaEndpointProvider& RdmaEndpointProvider::get() {
  // Never destroyed: endpoints may outlive static destructors.
  static auto* provider = new RdmaEndpointProvider;
  return *provider;
}

ibv_pd* RdmaEndpointProvider::createProtectionDomain(ibv_context* verbs) {
  if (!verbs) {
    throw RdmaError{ENODEV, "protection domain: id not bound to a device"};
  }
  std::lock_guard lock{mutex_};
  auto& pd = pds_[verbs];
  if (!pd) {
    pd = std::make_unique<ProtectionDomain>(verbs);
  }
  return pd->get();
}

} // namespace rlib
