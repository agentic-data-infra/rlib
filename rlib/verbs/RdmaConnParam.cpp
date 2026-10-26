#include "rlib/verbs/RdmaConnParam.h"

#include <stdexcept>

#include "rlib/verbs/RdmaError.h"

namespace rlib {

rdma_conn_param RdmaConnParam::toNative(ibv_context* verbs) const {
  if (privateData.size() > UINT8_MAX) {
    throw std::invalid_argument{"RdmaConnParam: private data over 255 bytes"};
  }
  uint8_t deviceDepth = 0;
  if (!initiatorDepth || !responderResources) {
    ibv_device_attr attr{};
    if (int error = ibv_query_device(verbs, &attr)) {
      throwRdmaError("ibv_query_device", error);
    }
    deviceDepth = static_cast<uint8_t>(attr.max_qp_rd_atom);
  }
  rdma_conn_param param{};
  param.private_data = privateData.data();
  param.private_data_len = static_cast<uint8_t>(privateData.size());
  param.initiator_depth = initiatorDepth.value_or(deviceDepth);
  param.responder_resources = responderResources.value_or(deviceDepth);
  param.flow_control = flowControl;
  param.retry_count = retryCount;
  param.rnr_retry_count = rnrRetryCount;
  return param;
}

} // namespace rlib
