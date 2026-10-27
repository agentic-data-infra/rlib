#include "rlib/verbs/tests/TestUtil.h"

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

} // namespace rlib::test
