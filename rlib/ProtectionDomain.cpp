#include "rlib/ProtectionDomain.h"

#include <cstring>

#include "rlib/Log.h"
#include "rlib/RdmaError.h"

namespace rlib {

ProtectionDomain::ProtectionDomain(ibv_context* verbs)
    : pd_{ibv_alloc_pd(verbs)} {
  if (!pd_) {
    throwRdmaError("ibv_alloc_pd");
  }
}

void ProtectionDomain::Deallocate::operator()(ibv_pd* pd) const {
  if (int error = ibv_dealloc_pd(pd)) {
    RLIB_LOG("ibv_dealloc_pd: %s", std::strerror(error));
  }
}

} // namespace rlib
