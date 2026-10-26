#pragma once

#include <infiniband/verbs.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include "rlib/verbs/ProtectionDomain.h"

namespace rlib {

// The process-wide protection domains, one per device, which all endpoints
// on a device share so that memory registered once works with all of them.
// They live until the process exits.
class RdmaEndpointProvider {
 public:
  static RdmaEndpointProvider& get();

  // The protection domain of the device 'verbs', allocated on first use.
  // Throws RdmaError (ENODEV for a null device).
  ibv_pd* createProtectionDomain(ibv_context* verbs);

 private:
  RdmaEndpointProvider() = default;

  std::mutex mutex_;
  std::unordered_map<ibv_context*, std::unique_ptr<ProtectionDomain>> pds_;
};

} // namespace rlib
