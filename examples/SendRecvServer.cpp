// Accepts one connection, receives a message and answers it.
//
//   send_recv_server --address=host:port
//
// The address is an RDMA device's (e.g. the NIC under Soft-RoCE).

#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "examples/ExampleEndpoint.h"

DEFINE_string(address, "", "The address to listen on, host:port.");

namespace {

using namespace rlib;
using namespace rlib::examples;

folly::coro::Task<void> co_run() {
  auto group = makeGroup();
  auto server = group->createServerEndpoint();
  server->bind(SocketAddress::parse(FLAGS_address), 10);
  std::printf("server bound to %s\n", server->srcAddr().toString().c_str());

  auto endpoint = co_await server->co_accept();
  std::printf(
      "connection from %s accepted\n", endpoint->dstAddr().toString().c_str());

  auto received = co_await endpoint->co_receive();
  std::printf(
      "message from the client: %.*s\n",
      static_cast<int>(received.byte_len),
      endpoint->recvBuffer.data());

  std::string reply = "Hello from the server";
  std::memcpy(endpoint->sendBuffer.data(), reply.data(), reply.size());
  auto sent = co_await endpoint->co_send(reply.size());
  std::printf("reply sent, status %s\n", ibv_wc_status_str(sent.status));

  co_await endpoint->co_close();
  co_await server->co_close();
  co_await group->co_close();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::coro::blockingWait(co_run());
  return 0;
}
