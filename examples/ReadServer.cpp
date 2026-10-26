// Accepts one connection, puts a text into a registered buffer and sends
// the client the buffer's address and key; the client then reads the text
// with RDMA reads. Waits for the client's final message before closing.
//
//   read_server --address=host:port

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
  std::printf("connection accepted\n");

  auto text = "This is an RDMA read on key " +
      std::to_string(endpoint->dataMr->rkey()) + "!";
  std::snprintf(
      endpoint->dataBuffer.data(), ExampleEndpoint::kBufferSize, "%s",
      text.c_str());
  BufferInfo info{
      reinterpret_cast<uintptr_t>(endpoint->dataBuffer.data()),
      ExampleEndpoint::kBufferSize,
      endpoint->dataMr->rkey()};
  std::memcpy(endpoint->sendBuffer.data(), &info, sizeof(info));
  co_await endpoint->co_send(sizeof(info));
  std::printf("buffer address and key sent\n");

  co_await endpoint->co_receive();
  std::printf("final message received\n");

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
