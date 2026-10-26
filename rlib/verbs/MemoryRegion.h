#pragma once

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace rlib {

// Owns an ibv_mr: memory registered with a device, which local work
// requests name by lkey() and remote peers by addr() and rkey(). Move-only;
// the memory must outlive it.
class MemoryRegion {
 public:
  // Local write, remote read and remote write.
  static constexpr int kDefaultAccess = IBV_ACCESS_LOCAL_WRITE |
      IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

  // Registers [addr, addr + length) in 'pd'. Throws RdmaError.
  MemoryRegion(
      ibv_pd* pd,
      void* addr,
      size_t length,
      int access = kDefaultAccess);

  ibv_mr* get() const {
    return mr_.get();
  }
  void* addr() const {
    return mr_->addr;
  }
  size_t length() const {
    return mr_->length;
  }
  uint32_t lkey() const {
    return mr_->lkey;
  }
  uint32_t rkey() const {
    return mr_->rkey;
  }

 private:
  struct Deregister {
    void operator()(ibv_mr* mr) const;
  };
  std::unique_ptr<ibv_mr, Deregister> mr_;
};

} // namespace rlib
