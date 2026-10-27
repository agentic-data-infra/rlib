// An RPC server answering param + 1 (see RpcExample.h) until interrupted.
//
//   rpc_server --address=host:port [--cpus=2,3] [--polling]
//              [--inline_dispatch] [--delay_us=0] [--threads=0]
//              [--recv_queue=16] [--send_queue=0] [--max_inline=64]
//
// The address is an RDMA device's (e.g. the NIC under Soft-RoCE).

#include <csignal>
#include <cstdio>

#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "examples/RpcExample.h"

DEFINE_string(address, "", "The address to listen on, host:port.");
DEFINE_string(cpus, "", "Cores for CQ threads, e.g. 2,3; one unpinned if empty.");
DEFINE_bool(polling, false, "Polling instead of blocking CQ threads.");
DEFINE_bool(inline_dispatch, false, "Run the service on the CQ threads.");
DEFINE_int32(delay_us, 0, "Microseconds the service waits before answering.");
DEFINE_int32(threads, 0, "Service threads; 0 for one per core.");
DEFINE_int32(recv_queue, 16, "Receives per connection.");
DEFINE_int32(send_queue, 0, "Send slots per connection; 0 for recv_queue.");
DEFINE_int32(max_inline, 64, "Largest message sent inline.");

using namespace rlib;
using namespace rlib::examples;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  RpcService service{std::chrono::microseconds{FLAGS_delay_us}};
  RdmaRpcServerOptions options;
  options.cpus = parseCpus(FLAGS_cpus);
  options.polling = FLAGS_polling;
  options.inlineDispatch = FLAGS_inline_dispatch;
  options.threads = FLAGS_threads;
  options.recvQueue = FLAGS_recv_queue;
  options.sendQueue = FLAGS_send_queue;
  options.maxInline = FLAGS_max_inline;
  auto server = RpcServerGroup::create(service, options);
  // Soft-RoCE drops packets under load; retry instead of failing.
  server->connParam().retryCount = 7;
  server->connParam().rnrRetryCount = 7;
  server->serve(SocketAddress::parse(FLAGS_address));
  std::printf("serving on %s\n", server->address().toString().c_str());

  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  int signal = 0;
  sigwait(&signals, &signal);

  std::printf("closing\n");
  folly::coro::blockingWait(server->co_close());
  return 0;
}
