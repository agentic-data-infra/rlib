# rlib
RDMA endpoints for C++20 coroutines.

rlib is a C++ library for RDMA with two APIs, both `folly::coro` based and
both in namespace `rlib`:

- **Endpoints** (`RdmaEndpoint`, `RdmaServerEndpoint`, `RdmaEndpointGroup`,
  ...), directly on libibverbs and librdmacm, posting native `ibv_send_wr`
  chains. Connecting, accepting and closing are `co_connect`, `co_accept`
  and `co_close`. Passive endpoints leave polling their completion queues
  to the application; active endpoints (`RdmaActiveEndpoint`) resume the
  coroutine that posted a work request when it completes.
- **RPC** (e.g. `RdmaRpcEndpoint`), on top of the endpoints, later.

The code is in three layers, each a Buck target that depends only on the one
below it:

```
rlib/rpc       //rlib/rpc:rpc            RPC API                (planned)
   |
rlib/endpoint  //rlib/endpoint:endpoint  endpoint API
   |
rlib/verbs     //rlib/verbs:verbs        RAII and value types
   |
libibverbs, librdmacm
```

Tests live next to the code they test (`rlib/verbs/tests`,
`rlib/endpoint/tests`); `rlib/tests` has the shared test main.

## Endpoints

A group (`RdmaEndpointGroup<C>`) makes endpoints of the application's class
`C`, derived from `RdmaEndpoint`, with an `RdmaEndpointFactory<C>`. Its
connection manager thread handles connection events for all of them, and
its hooks decide how an endpoint gets its completion queue and QP
(`createCqProvider`, `createQpProvider`) and when they are allocated
(`allocateResources`). `RdmaPassiveEndpointGroup<C>` gives each endpoint its
own completion queue, which the application polls (`cqProvider()->poll`,
or `co_poll`, which yields to the executor between empty polls). All
endpoints on a device share one protection domain, so memory registered
once works with all of them.

```cpp
#include "rlib/endpoint/RdmaPassiveEndpointGroup.h"

using Group = rlib::RdmaPassiveEndpointGroup<rlib::RdmaEndpoint>;
// Up to 16 work requests of 1 scatter/gather element each, CQs of 32.
auto group = Group::createDefaultGroup(16, 1, 32);

// Server.
auto server = group->createServerEndpoint();
server->bind(rlib::SocketAddress::parse("10.0.0.1:1919"), /*backlog=*/16);
std::shared_ptr<rlib::RdmaEndpoint> endpoint = co_await server->co_accept();

// Client.
auto endpoint = group->createEndpoint();
co_await endpoint->co_connect(rlib::SocketAddress::parse("10.0.0.1:1919"), 1s);

// Either side.
auto mr = endpoint->registerMemory(buffer, size);
endpoint->postSend(wr);                        // a native ibv_send_wr chain
int n = co_await endpoint->cqProvider()->co_poll(wcs);
int error = co_await endpoint->co_read(mr, offset, length, remoteAddr, rkey);
co_await endpoint->co_close();
co_await group->co_close();                    // closes what is still open
```

### Active endpoints

In an `RdmaActiveEndpointGroup<C>` (`C` derived from `RdmaActiveEndpoint`),
all endpoints on a device share one completion queue, and a CQ processor
thread takes its completions: it either polls all the time (lowest latency,
one core) or sleeps on the completion channel. `co_postSend` and
`co_postRecv` post a chain of work requests and resume the awaiting
coroutine, on its executor, with the completion of the last one; any
number may be outstanding, and no allocation is made per operation (the
waiter lives in the coroutine frame and its address is the wr_id, with the
top bit set: application wr_ids must leave it clear). Completions of work
requests posted with plain `postSend`/`postRecv`, e.g. receives posted in
`init()`, go to `dispatchCqEvent`, which by default queues them for
`co_nextCqEvent`. Closing an endpoint fails its outstanding operations
(`IBV_WC_WR_FLUSH_ERR`) instead of leaving them hanging.

```cpp
#include "rlib/endpoint/RdmaActiveEndpointGroup.h"

using Group = rlib::RdmaActiveEndpointGroup<rlib::RdmaActiveEndpoint>;
auto group = std::make_unique<Group>(/*polling=*/false, /*maxWR=*/128,
                                     /*maxSge=*/4, /*cqSize=*/1024);
group->init(std::make_unique<
    rlib::RdmaDefaultEndpointFactory<rlib::RdmaActiveEndpoint>>(*group));

auto endpoint = group->createEndpoint();
co_await endpoint->co_connect(address, 1s);
ibv_wc wc = co_await endpoint->co_postSend(wr);   // resumes on completion
std::vector<folly::coro::Task<int>> reads;        // many at once
for (...) {
  reads.push_back(endpoint->co_read(mr, offset, length, remoteAddr, rkey));
}
co_await folly::coro::collectAllRange(std::move(reads));
```

### Errors and threads

Connecting, accepting and closing throw `RdmaError`; the data path
(`postSend`, `postRecv`, `co_read`, `co_write`) returns errors. Coroutines
resume on the awaiting coroutine's executor, not on the connection manager
or CQ thread. Closing disconnects; the peer's endpoint sees `isClosed()` and
answers the disconnect at once.

`RdmaConnParam` defaults `retryCount` and `rnrRetryCount` to 0, so a lost
packet, or a send that finds no receive posted, fails the connection;
Soft-RoCE drops packets under load, so set them (e.g. to 7) there.

### Examples and benchmark

`examples/` has two client/server pairs on active endpoints: a message and
its answer (`send_recv_server`, `send_recv_client`), and RDMA reads of a
buffer whose address and key the server sends (`read_server`,
`read_client`).

```
buck2 run //examples:read_server -- --address=10.0.0.1:1919 &
buck2 run //examples:read_client -- --address=10.0.0.1:1919
buck2 run //bench:endpoint_benchmark -- [--polling]
```

The benchmark connects two active endpoints in one process and measures
one-sided reads and writes: latency one at a time (64 B), and bandwidth
with 16 outstanding (64 KiB). On a GCP VM with Soft-RoCE (where perftest's
`ib_read_bw` and `ib_write_bw` get 0.62 and 0.87 GB/s):

| CQ threads | read latency | write latency | read bandwidth | write bandwidth |
|------------|--------------|---------------|----------------|-----------------|
| blocking   | 20-25 us     | 9 us          | 0.63 GB/s      | 0.87 GB/s       |
| polling    | 10-18 us     | 3 us          | 0.64 GB/s      | 0.90 GB/s       |

## Verbs

`rlib/verbs` has the building blocks the endpoints are made of:

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
