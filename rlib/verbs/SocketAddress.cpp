#include "rlib/verbs/SocketAddress.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>

#include <charconv>
#include <cstring>
#include <stdexcept>

namespace rlib {

namespace {

const sockaddr_in& v4(const sockaddr_storage& s) {
  return reinterpret_cast<const sockaddr_in&>(s);
}

const sockaddr_in6& v6(const sockaddr_storage& s) {
  return reinterpret_cast<const sockaddr_in6&>(s);
}

[[noreturn]] void invalid(std::string_view hostPort, const char* why) {
  throw std::invalid_argument{
      "bad address '" + std::string{hostPort} + "': " + why};
}

} // namespace

SocketAddress::SocketAddress() {
  storage_.ss_family = AF_INET;
}

SocketAddress SocketAddress::parse(std::string_view hostPort) {
  auto colon = hostPort.rfind(':');
  if (colon == std::string_view::npos) {
    invalid(hostPort, "no port");
  }
  auto host = hostPort.substr(0, colon);
  auto portString = hostPort.substr(colon + 1);
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  } else if (host.find(':') != std::string_view::npos) {
    invalid(hostPort, "IPv6 hosts go in brackets");
  }
  if (host.empty()) {
    invalid(hostPort, "no host");
  }
  uint16_t port = 0;
  auto [end, error] = std::from_chars(
      portString.data(), portString.data() + portString.size(), port);
  if (portString.empty() || error != std::errc{} ||
      end != portString.data() + portString.size()) {
    invalid(hostPort, "bad port");
  }

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  std::string hostString{host};
  if (int rc = getaddrinfo(hostString.c_str(), nullptr, &hints, &result)) {
    invalid(hostPort, gai_strerror(rc));
  }
  SocketAddress address;
  std::memcpy(&address.storage_, result->ai_addr, result->ai_addrlen);
  freeaddrinfo(result);
  if (address.family() == AF_INET) {
    reinterpret_cast<sockaddr_in&>(address.storage_).sin_port = htons(port);
  } else {
    reinterpret_cast<sockaddr_in6&>(address.storage_).sin6_port = htons(port);
  }
  return address;
}

SocketAddress SocketAddress::fromSockaddr(const sockaddr* addr) {
  SocketAddress address;
  if (addr->sa_family == AF_INET) {
    std::memcpy(&address.storage_, addr, sizeof(sockaddr_in));
  } else if (addr->sa_family == AF_INET6) {
    std::memcpy(&address.storage_, addr, sizeof(sockaddr_in6));
  } else {
    throw std::invalid_argument{
        "unsupported address family " + std::to_string(addr->sa_family)};
  }
  return address;
}

uint16_t SocketAddress::port() const {
  return ntohs(family() == AF_INET ? v4(storage_).sin_port
                                   : v6(storage_).sin6_port);
}

std::string SocketAddress::host() const {
  char buffer[INET6_ADDRSTRLEN];
  const void* addr = family() == AF_INET
      ? static_cast<const void*>(&v4(storage_).sin_addr)
      : static_cast<const void*>(&v6(storage_).sin6_addr);
  inet_ntop(family(), addr, buffer, sizeof(buffer));
  return buffer;
}

std::string SocketAddress::toString() const {
  auto port = std::to_string(this->port());
  return family() == AF_INET6 ? "[" + host() + "]:" + port
                              : host() + ":" + port;
}

socklen_t SocketAddress::length() const {
  return family() == AF_INET ? sizeof(sockaddr_in) : sizeof(sockaddr_in6);
}

bool operator==(const SocketAddress& a, const SocketAddress& b) {
  return a.family() == b.family() &&
      std::memcmp(a.addr(), b.addr(), a.length()) == 0;
}

} // namespace rlib
