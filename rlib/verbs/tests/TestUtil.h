#pragma once

#include <infiniband/verbs.h>

#include <memory>

namespace rlib::test {

struct CloseDevice {
  void operator()(ibv_context* context) const {
    ibv_close_device(context);
  }
};
using Device = std::unique_ptr<ibv_context, CloseDevice>;

// Opens the first RDMA device (rxe0 with Soft-RoCE), or returns nullptr if
// there is none. Tests that need one GTEST_SKIP without it.
Device openFirstDevice();

} // namespace rlib::test
