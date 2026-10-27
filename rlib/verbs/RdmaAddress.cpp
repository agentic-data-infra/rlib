#include "rlib/verbs/RdmaAddress.h"

#include <ifaddrs.h>
#include <net/if.h>

#include "rlib/verbs/RdmaCmId.h"
#include "rlib/verbs/RdmaError.h"
#include "rlib/verbs/RdmaEventChannel.h"

namespace rlib {

std::optional<SocketAddress> rdmaAddress() {
  ifaddrs* interfaces = nullptr;
  if (getifaddrs(&interfaces)) {
    return std::nullopt;
  }
  std::optional<SocketAddress> found;
  RdmaEventChannel channel;
  for (auto* i = interfaces; i && !found; i = i->ifa_next) {
    if (!i->ifa_addr || i->ifa_addr->sa_family != AF_INET ||
        (i->ifa_flags & IFF_LOOPBACK)) {
      continue;
    }
    auto address = SocketAddress::fromSockaddr(i->ifa_addr);
    RdmaCmId id{channel};
    try {
      id.bindAddr(address);
    } catch (const RdmaError&) {
      continue;
    }
    if (id.verbs()) {
      found = address;
    }
  }
  freeifaddrs(interfaces);
  return found;
}

} // namespace rlib
