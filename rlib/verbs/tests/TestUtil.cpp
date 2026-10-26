#include "rlib/verbs/tests/TestUtil.h"

#include <ifaddrs.h>
#include <net/if.h>

#include "rlib/verbs/RdmaCmId.h"
#include "rlib/verbs/RdmaError.h"
#include "rlib/verbs/RdmaEventChannel.h"

namespace rlib::test {

Device openFirstDevice() {
  int count = 0;
  ibv_device** devices = ibv_get_device_list(&count);
  if (!devices) {
    return nullptr;
  }
  Device device{count > 0 ? ibv_open_device(devices[0]) : nullptr};
  ibv_free_device_list(devices);
  return device;
}

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

} // namespace rlib::test
