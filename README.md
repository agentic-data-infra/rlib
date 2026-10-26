# rlib
RDMA endpoints for C++20 coroutines.

rlib is going to be a C++ library for RDMA with two APIs, both
`folly::coro` based and both in namespace `rlib`:

- **Endpoints** (`RdmaEndpoint`, `RdmaServerEndpoint`, `RdmaActiveEndpoint`,
  `RdmaEndpointGroup`, ...), directly on libibverbs and librdmacm, posting
  native `ibv_send_wr` chains. Connecting, accepting and closing will be
  `co_connect`, `co_accept` and `co_close`, and posted work requests will
  resume their coroutine when they complete.
- **RPC** (e.g. `RdmaRpcEndpoint`), on top of the endpoints.

The code is in three layers, each a Buck target that depends only on the one
below it:

```
rlib/rpc       //rlib/rpc:rpc            RPC API                (planned)
   |
rlib/endpoint  //rlib/endpoint:endpoint  endpoint API           (next PRs)
   |
rlib/verbs     //rlib/verbs:verbs        RAII and value types   (today)
   |
libibverbs, librdmacm
```

Tests live next to the code they test (`rlib/verbs/tests`); `rlib/tests` has
the shared test main.

Today there is the bottom layer, `rlib/verbs`, the building blocks the
endpoints are made of:

- `SocketAddress`: an IPv4 or IPv6 address and port, parsed from
  `"host:port"` (`"[::1]:1234"` for IPv6) and converted to and from
  `sockaddr`.
- `RdmaConnParam`: the parameters of `rdma_connect` and `rdma_accept`.
- `RdmaEventChannel`, `RdmaCmId`, `ProtectionDomain` and `MemoryRegion`: own
  an `rdma_event_channel`, `rdma_cm_id`, `ibv_pd` and `ibv_mr`. They are
  move-only and release their handle when destroyed.
- `RdmaError`: what the control path (setup, connection management) throws
  when a verbs or rdma_cm call fails, a `std::system_error` with the call's
  errno.

```cpp
#include "rlib/verbs/MemoryRegion.h"
#include "rlib/verbs/ProtectionDomain.h"
#include "rlib/verbs/RdmaCmId.h"
#include "rlib/verbs/RdmaEventChannel.h"

rlib::RdmaEventChannel channel;
rlib::RdmaCmId id{channel};
id.bindAddr(rlib::SocketAddress::parse("10.0.0.1:0"));  // an RDMA NIC's address

rlib::ProtectionDomain pd{id.verbs()};
std::vector<char> buffer(1 << 20);
rlib::MemoryRegion mr{pd.get(), buffer.data(), buffer.size()};
// mr.lkey() for local work requests, mr.addr() and mr.rkey() for peers
```

Passive endpoints (`RdmaEndpoint`, `RdmaServerEndpoint`, endpoint groups)
come next, then active endpoints, then RPC.

## Building

rlib builds with [Buck2](https://buck2.build) and GCC 12, like
[Kiln](https://github.com/agentic-data-infra/kiln). It depends on folly (with
folly::coro) as Velox's setup script builds it, built with GCC 12 into
`/opt/deps-gcc12` (Kiln's `scripts/build_deps_gcc12.sh`); on rdma-core
(`libibverbs-dev`, `librdmacm-dev`); and, for tests, on the system gtest
(`libgtest-dev`).

Prerequisites: GCC 12 (`g++-12`), cmake, ninja, and buck2 on `PATH`.

```
# Generate third-party/generated/deps.bzl from folly (--prefix for another place).
scripts/gen_third_party.py

buck2 build //...
buck2 test //...
```

Re-run `scripts/gen_third_party.py` whenever folly is rebuilt. rlib logs
nothing by default; build with `-DRLIB_ENABLE_LOG` for `RLIB_LOG`'s lines on
stderr.

## Soft-RoCE

Without an RDMA NIC, rlib runs on Soft-RoCE, the kernel's software RoCE
driver (`rdma_rxe`) on top of an ordinary network interface. Set it up with

```
scripts/setup-rxe.sh          # or scripts/setup-rxe.sh <netdev>
```

which installs rdma-core and its tools (and, on cloud kernels like GCP's, the
`linux-modules-extra` package that has `rdma_rxe`), loads the driver and adds
the device `rxe0`. Run it again after a reboot. Connect to the interface's
address, not `127.0.0.1`: loopback has no RDMA device. On a GCP VM (kernel
6.8, `ens3`) `ib_send_bw` measures about 880 MB/s over rxe.

## Testing

```
buck2 test //...
```

Tests that need an RDMA device (memory registration, binding to a device)
skip themselves when there is none; run `scripts/setup-rxe.sh` first to run
them all. Registering memory pins it, up to the locked memory limit
(`ulimit -l`).

## License

Apache 2.0, see [LICENSE](LICENSE).
