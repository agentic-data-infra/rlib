#include "rlib/verbs/MemoryRegion.h"

#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "rlib/verbs/ProtectionDomain.h"
#include "rlib/verbs/RdmaError.h"
#include "rlib/verbs/tests/TestUtil.h"

namespace rlib {
namespace {

TEST(MemoryRegionTest, RegistersMemory) {
  auto device = test::openFirstDevice();
  if (!device) {
    GTEST_SKIP() << "no RDMA device";
  }
  ProtectionDomain pd{device.get()};
  std::vector<char> buffer(1 << 20);
  MemoryRegion mr{pd.get(), buffer.data(), buffer.size()};
  EXPECT_EQ(mr.addr(), buffer.data());
  EXPECT_EQ(mr.length(), buffer.size());
  EXPECT_EQ(mr.get()->pd, pd.get());
  EXPECT_NE(mr.lkey(), 0u);
  EXPECT_NE(mr.rkey(), 0u);

  // Moving keeps the registration; the moved-from region owns nothing.
  auto lkey = mr.lkey();
  MemoryRegion moved{std::move(mr)};
  EXPECT_EQ(mr.get(), nullptr);
  EXPECT_EQ(moved.lkey(), lkey);

  MemoryRegion other{pd.get(), buffer.data(), 4096, IBV_ACCESS_LOCAL_WRITE};
  moved = std::move(other);
  EXPECT_EQ(moved.length(), 4096u);
}

TEST(MemoryRegionTest, ThrowsOnFailure) {
  auto device = test::openFirstDevice();
  if (!device) {
    GTEST_SKIP() << "no RDMA device";
  }
  ProtectionDomain pd{device.get()};
  char byte = 0;
  // Remote write access requires local write access.
  try {
    MemoryRegion mr{pd.get(), &byte, 1, IBV_ACCESS_REMOTE_WRITE};
    FAIL() << "registered";
  } catch (const RdmaError& e) {
    EXPECT_EQ(e.code().value(), EINVAL);
    EXPECT_NE(std::string{e.what()}.find("ibv_reg_mr"), std::string::npos);
  }
}

} // namespace
} // namespace rlib
