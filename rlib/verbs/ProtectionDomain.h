#pragma once

#include <infiniband/verbs.h>

#include <memory>

namespace rlib {

// Owns an ibv_pd, which memory regions and QPs of a device are allocated
// in. Move-only.
class ProtectionDomain {
 public:
  // Throws RdmaError.
  explicit ProtectionDomain(ibv_context* verbs);

  ibv_pd* get() const {
    return pd_.get();
  }

 private:
  struct Deallocate {
    void operator()(ibv_pd* pd) const;
  };
  std::unique_ptr<ibv_pd, Deallocate> pd_;
};

} // namespace rlib
