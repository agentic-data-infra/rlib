#pragma once

#include <optional>

#include "rlib/verbs/SocketAddress.h"

namespace rlib {

// The address (port 0) of the first IPv4 interface an RDMA device runs on,
// e.g. the NIC under Soft-RoCE's rxe0, or nullopt if there is none.
std::optional<SocketAddress> rdmaAddress();

} // namespace rlib
