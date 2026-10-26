#pragma once

#include <rdma/rdma_cma.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace rlib {

// The parameters of rdma_connect and rdma_accept. Everything defaults to 0
// except the RDMA read and atomic depths, which default to the device's
// maximum (max_qp_rd_atom). With retryCount 0, a lost packet fails the
// connection; raise it (e.g. to 2) on lossy networks.
struct RdmaConnParam {
  // Outstanding RDMA reads and atomics this side issues; nullopt: the
  // device's maximum.
  std::optional<uint8_t> initiatorDepth;
  // Outstanding RDMA reads and atomics this side serves; nullopt: the
  // device's maximum.
  std::optional<uint8_t> responderResources;
  uint8_t flowControl = 0;
  // Ignored when accepting.
  uint8_t retryCount = 0;
  uint8_t rnrRetryCount = 0;
  // Sent with the connection request or reply; librdmacm copies it. Must
  // stay alive until toNative's result is used.
  std::span<const std::byte> privateData;

  // The rdma_conn_param for an id bound to the device 'verbs' (which is
  // only queried if a depth is nullopt). Throws std::invalid_argument for
  // private data over 255 bytes, RdmaError if the device query fails.
  rdma_conn_param toNative(ibv_context* verbs) const;
};

} // namespace rlib
