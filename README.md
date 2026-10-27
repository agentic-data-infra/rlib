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
- **RPC** (`RdmaRpcClientGroup`, `RdmaRpcServerGroup`, `RdmaRpcService`),
  on top of the endpoints: fixed-size request/response messages over
  SEND/RECV, with many calls in flight per connection.

The code is in three layers, each a Buck target that depends only on the one
below it:

```
rlib/rpc       //rlib/rpc:rpc            RPC API
   |
rlib/endpoint  //rlib/endpoint:endpoint  endpoint API
   |
rlib/verbs     //rlib/verbs:verbs        RAII and value types
   |
libibverbs, librdmacm
```

Tests live next to the code they test (`rlib/verbs/tests`,
`rlib/endpoint/tests`, `rlib/rpc/tests`); `rlib/tests` has the shared test main.

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
CQ processor threads take the endpoints' completions: each has its own
completion queue and either polls it all the time (lowest latency, one core)
or sleeps on its completion channel. By default each device gets one
processor, shared by all endpoints; with `cpus`, it gets one per listed
core, pinned to it, and endpoints go to them round-robin (`clusterId()`
says which). A processor's CQ (`cqSize`) must have room for 2 * `maxWR`
completions per endpoint; allocating an endpoint for a full processor fails
with ENOSPC. `co_postSend` and
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
                                     /*maxSge=*/4, /*cqSize=*/4096,
                                     /*cpus=*/std::vector<int>{2, 3});
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

## RPC

A protocol is a request and a response type, each a small struct that
serializes itself (`RdmaRpcMessage`: `kSize`, `write`, `update`). A server
group accepts connections and hands each request to the service's
`co_process`; a client endpoint's `co_call` sends a request and resumes with
the response.

```cpp
#include "rlib/rpc/RdmaRpcClientGroup.h"
#include "rlib/rpc/RdmaRpcServerGroup.h"

struct Message {
  static constexpr size_t kSize = 16;
  uint64_t a = 0, b = 0;
  size_t write(std::span<std::byte> out) const;   // returns bytes written
  void update(std::span<const std::byte> in);
};
struct Protocol {
  using Request = Message;
  using Response = Message;
};

class Service : public rlib::RdmaRpcService<Protocol> {
  folly::coro::Task<void> co_process(
      rlib::RdmaRpcServerEvent<Protocol>& event) override {
    event.response().a = event.request().a + 1;   // may co_await
    co_return;
  }
};

// Server.
Service service;
rlib::RdmaRpcServerOptions options;
options.cpus = {2, 3};                            // CQ threads
auto server = rlib::RdmaRpcServerGroup<Protocol>::create(service, options);
server->serve(rlib::SocketAddress::parse("10.0.0.1:1919"));

// Client.
auto client = rlib::RdmaRpcClientGroup<Protocol>::create();
auto endpoint = client->createEndpoint();
co_await endpoint->co_connect(address, 1s);
Message request, response;
co_await endpoint->co_call(request, response);    // from any number of coroutines

auto stream = endpoint->createStream();           // responses as they arrive
co_await stream->co_request(request, response);
Message* done = co_await stream->co_take();
```

Each connection has `recvQueue` receives posted and `sendQueue` send slots
in one registered buffer; a message is a 4-byte ticket and the payload,
sent inline when it fits `maxInline`. A client has up to
min(`recvQueue`, `sendQueue`) calls in flight (more wait for a slot); its
endpoints have no thread: the calling coroutines poll the completion queue,
one at a time. A server's connections share its CQ processors (`cpus`,
`clusterSize` connections each), and `co_process` runs on the group's
executor, or starts on the CQ thread with `inlineDispatch`. A server
connection works on up to `recvQueue` requests at once; keep the clients'
calls in flight within that, or they wait for receiver-not-ready retries.
Responses for which no send slot is free are queued. Failed calls, and calls
on a closed or failed connection, throw `RdmaError`.

### RPC examples and benchmark

`examples/RpcExample.h` has a 16-byte request (command, parameter, send
time) and response, a service that answers parameter + 1 (after
`--delay_us`, if given), and client loops in three modes: `call` (one
`co_call` at a time), `concurrent` (`--depth` coroutines calling on one
connection) and `stream` (`--depth` requests in flight on a stream).
`rpc_server` serves it; `rpc_client` runs `--clients` connections, each on
its own thread, and prints calls per second and round-trip latencies.

```
buck2 run //examples:rpc_server -- --address=10.0.0.1:1919 [--cpus=2,3] [--polling] &
buck2 run //examples:rpc_client -- --address=10.0.0.1:1919 --clients=4 --mode=stream
buck2 run //bench:rpc_benchmark -- [--polling] [--inline_dispatch] [--mode=call]
```

The benchmark runs the server and 1 to 16 clients in one process, each
client on its own thread making one call at a time. On a 16-vCPU GCP VM
with Soft-RoCE:

| clients | calls/s (p50 / p99 us) | with `--inline_dispatch` | with `--polling` |
|--------:|------------------------|--------------------------|------------------|
| 1       | 87k (11 / 20)          | 136k (6 / 15)            | 77k (13 / 20)    |
| 2       | 183k (11 / 19)         | 194k (10 / 14)           | 139k (15 / 22)   |
| 4       | 305k (12 / 23)         | 204k (19 / 25)           | 306k (12 / 23)   |
| 8       | 396k (19 / 35)         | 207k (38 / 46)           | 427k (17 / 50)   |
| 16      | 98k (73 / 2990)        | 140k (104 / 121)         | 45k (202 / 3317) |

Client endpoints poll their completion queue while calls are pending, so
each client keeps a core busy; at 16 clients they take all cores from the
server's threads and from Soft-RoCE, which runs in the kernel. Inline
dispatch has no service threads to compete. With a hardware NIC the clients
compete only with the server.

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
