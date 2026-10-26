#pragma once

#include <infiniband/verbs.h>

#include <memory>
#include <optional>

#include "rlib/verbs/SocketAddress.h"

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

// The address (port 0) of the first IPv4 interface an RDMA device runs on,
// e.g. the NIC under rxe0, or nullopt if there is none.
std::optional<SocketAddress> rdmaAddress();

} // namespace rlib::test
