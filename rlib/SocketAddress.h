#pragma once

#include <sys/socket.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace rlib {

// An IPv4 or IPv6 address and port, what librdmacm binds, listens on and
// connects to. A value type around a sockaddr_storage.
class SocketAddress {
 public:
  // The IPv4 any address, port 0.
  SocketAddress();

  // Parses "host:port", where host is an IPv4 address, an IPv6 address in
  // brackets ("[::1]:1234") or a host name, which is resolved (blocking).
  // Throws std::invalid_argument for malformed input or an unknown host.
  static SocketAddress parse(std::string_view hostPort);

  // Copies an AF_INET or AF_INET6 sockaddr. Throws std::invalid_argument for
  // other families.
  static SocketAddress fromSockaddr(const sockaddr* addr);

  int family() const {
    return storage_.ss_family;
  }
  uint16_t port() const;
  // The numeric host, without brackets.
  std::string host() const;
  // "host:port", with IPv6 hosts in brackets.
  std::string toString() const;

  const sockaddr* addr() const {
    return reinterpret_cast<const sockaddr*>(&storage_);
  }
  sockaddr* addr() {
    return reinterpret_cast<sockaddr*>(&storage_);
  }
  socklen_t length() const;

  friend bool operator==(const SocketAddress& a, const SocketAddress& b);

 private:
  sockaddr_storage storage_{};
};

} // namespace rlib
