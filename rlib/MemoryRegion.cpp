#include "rlib/MemoryRegion.h"

#include <cstring>

#include "rlib/Log.h"
#include "rlib/RdmaError.h"

namespace rlib {

MemoryRegion::MemoryRegion(ibv_pd* pd, void* addr, size_t length, int access)
    : mr_{ibv_reg_mr(pd, addr, length, access)} {
  if (!mr_) {
    throwRdmaError("ibv_reg_mr");
  }
}

void MemoryRegion::Deregister::operator()(ibv_mr* mr) const {
  if (int error = ibv_dereg_mr(mr)) {
    RLIB_LOG("ibv_dereg_mr: %s", std::strerror(error));
  }
}

} // namespace rlib
