#include "rlib/verbs/SocketAddress.h"

#include <netinet/in.h>

#include <gtest/gtest.h>

namespace rlib {
namespace {

TEST(SocketAddressTest, ParsesIpv4) {
  auto address = SocketAddress::parse("10.1.2.3:1919");
  EXPECT_EQ(address.family(), AF_INET);
  EXPECT_EQ(address.host(), "10.1.2.3");
  EXPECT_EQ(address.port(), 1919);
  EXPECT_EQ(address.length(), sizeof(sockaddr_in));
  EXPECT_EQ(address.toString(), "10.1.2.3:1919");
}

TEST(SocketAddressTest, ParsesIpv6InBrackets) {
  auto address = SocketAddress::parse("[fe80::1]:80");
  EXPECT_EQ(address.family(), AF_INET6);
  EXPECT_EQ(address.host(), "fe80::1");
  EXPECT_EQ(address.port(), 80);
  EXPECT_EQ(address.length(), sizeof(sockaddr_in6));
  EXPECT_EQ(address.toString(), "[fe80::1]:80");
}

TEST(SocketAddressTest, ResolvesHostNames) {
  auto address = SocketAddress::parse("localhost:7");
  EXPECT_EQ(address.port(), 7);
  EXPECT_TRUE(address.host() == "127.0.0.1" || address.host() == "::1");
}

TEST(SocketAddressTest, DefaultIsIpv4Any) {
  SocketAddress address;
  EXPECT_EQ(address.toString(), "0.0.0.0:0");
  EXPECT_EQ(address, SocketAddress::parse("0.0.0.0:0"));
}

TEST(SocketAddressTest, RejectsMalformedInput) {
  for (const char* input :
       {"10.1.2.3", "10.1.2.3:", ":80", "10.1.2.3:65536", "10.1.2.3:8x",
        "10.1.2.3:-1", "fe80::1:80", "[]:80", "no.such.host.invalid:80"}) {
    EXPECT_THROW(SocketAddress::parse(input), std::invalid_argument) << input;
  }
}

TEST(SocketAddressTest, RoundTripsThroughSockaddr) {
  for (const char* input : {"192.168.0.1:4420", "[::1]:4420"}) {
    auto address = SocketAddress::parse(input);
    auto copy = SocketAddress::fromSockaddr(address.addr());
    EXPECT_EQ(copy, address);
    EXPECT_EQ(copy.toString(), input);
  }
  EXPECT_NE(
      SocketAddress::parse("192.168.0.1:1"),
      SocketAddress::parse("192.168.0.1:2"));
}

TEST(SocketAddressTest, RejectsOtherFamilies) {
  sockaddr addr{};
  addr.sa_family = AF_UNIX;
  EXPECT_THROW(SocketAddress::fromSockaddr(&addr), std::invalid_argument);
}

} // namespace
} // namespace rlib
