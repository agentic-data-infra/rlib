#include "rlib/verbs/RdmaCmId.h"

#include <utility>

#include <gtest/gtest.h>

#include "rlib/verbs/RdmaEventChannel.h"
#include "rlib/verbs/tests/TestUtil.h"

namespace rlib {
namespace {

TEST(RdmaCmIdTest, CreatesAndDestroysChannelsAndIds) {
  RdmaEventChannel channel;
  EXPECT_NE(channel.get(), nullptr);
  EXPECT_GE(channel.fd(), 0);

  int context = 0;
  RdmaCmId id{channel, &context};
  EXPECT_EQ(id.get()->channel, channel.get());
  EXPECT_EQ(id.get()->context, &context);
  EXPECT_EQ(id.get()->ps, RDMA_PS_TCP);
  EXPECT_EQ(id.verbs(), nullptr);

  RdmaCmId moved{std::move(id)};
  EXPECT_EQ(id.get(), nullptr);
  EXPECT_EQ(moved.get()->context, &context);

  RdmaEventChannel otherChannel{std::move(channel)};
  EXPECT_EQ(channel.get(), nullptr);
}

TEST(RdmaCmIdTest, BindsToAnyAddress) {
  RdmaEventChannel channel;
  RdmaCmId id{channel};
  id.bindAddr(SocketAddress{});
  auto local = id.localAddress();
  EXPECT_EQ(local.host(), "0.0.0.0");
  EXPECT_NE(local.port(), 0);
}

TEST(RdmaCmIdTest, BindsToDevice) {
  auto address = test::rdmaAddress();
  if (!address) {
    GTEST_SKIP() << "no RDMA device with an IPv4 address";
  }
  RdmaEventChannel channel;
  RdmaCmId id{channel};
  id.bindAddr(*address);
  EXPECT_NE(id.verbs(), nullptr);
  EXPECT_EQ(id.localAddress().host(), address->host());
}

} // namespace
} // namespace rlib
