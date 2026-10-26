// Connects to send_recv_server, sends a message and receives the answer.
//
//   send_recv_client --address=host:port

#include <chrono>

#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "examples/ExampleEndpoint.h"

DEFINE_string(address, "", "The server's address, host:port.");

namespace {

using namespace rlib;
using namespace rlib::examples;

folly::coro::Task<void> co_run() {
  auto group = makeGroup();
  auto endpoint = group->createEndpoint();
  co_await endpoint->co_connect(
      SocketAddress::parse(FLAGS_address), std::chrono::seconds{1});
  std::printf("connected to %s\n", endpoint->dstAddr().toString().c_str());

  std::string message = "Hello from the client";
  std::memcpy(endpoint->sendBuffer.data(), message.data(), message.size());
  auto sent = co_await endpoint->co_send(message.size());
  std::printf("message sent, status %s\n", ibv_wc_status_str(sent.status));

  auto received = co_await endpoint->co_receive();
  std::printf(
      "message from the server: %.*s\n",
      static_cast<int>(received.byte_len),
      endpoint->recvBuffer.data());

  co_await endpoint->co_close();
  co_await group->co_close();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::coro::blockingWait(co_run());
  return 0;
}
