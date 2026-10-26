#include "rlib/verbs/RdmaConnParam.h"

#include <vector>

#include <gtest/gtest.h>

#include "rlib/verbs/tests/TestUtil.h"

namespace rlib {
namespace {

TEST(RdmaConnParamTest, HasDefaults) {
  RdmaConnParam param;
  EXPECT_FALSE(param.initiatorDepth);
  EXPECT_FALSE(param.responderResources);
  EXPECT_EQ(param.flowControl, 0);
  EXPECT_EQ(param.retryCount, 0);
  EXPECT_EQ(param.rnrRetryCount, 0);
  EXPECT_TRUE(param.privateData.empty());
}

TEST(RdmaConnParamTest, ConvertsExplicitValuesWithoutDevice) {
  std::vector<std::byte> data(10);
  RdmaConnParam param{
      .initiatorDepth = 4,
      .responderResources = 8,
      .flowControl = 1,
      .retryCount = 2,
      .rnrRetryCount = 7,
      .privateData = data,
  };
  auto native = param.toNative(nullptr);
  EXPECT_EQ(native.initiator_depth, 4);
  EXPECT_EQ(native.responder_resources, 8);
  EXPECT_EQ(native.flow_control, 1);
  EXPECT_EQ(native.retry_count, 2);
  EXPECT_EQ(native.rnr_retry_count, 7);
  EXPECT_EQ(native.private_data, data.data());
  EXPECT_EQ(native.private_data_len, 10);
  EXPECT_EQ(native.srq, 0);
  EXPECT_EQ(native.qp_num, 0u);
}

TEST(RdmaConnParamTest, RejectsLongPrivateData) {
  std::vector<std::byte> data(256);
  RdmaConnParam param{
      .initiatorDepth = 1, .responderResources = 1, .privateData = data};
  EXPECT_THROW(param.toNative(nullptr), std::invalid_argument);
}

TEST(RdmaConnParamTest, DefaultsDepthsToDeviceMaximum) {
  auto device = test::openFirstDevice();
  if (!device) {
    GTEST_SKIP() << "no RDMA device";
  }
  ibv_device_attr attr{};
  ASSERT_EQ(ibv_query_device(device.get(), &attr), 0);
  auto native = RdmaConnParam{}.toNative(device.get());
  EXPECT_GT(attr.max_qp_rd_atom, 0);
  EXPECT_EQ(native.initiator_depth, attr.max_qp_rd_atom);
  EXPECT_EQ(native.responder_resources, attr.max_qp_rd_atom);
}

} // namespace
} // namespace rlib
