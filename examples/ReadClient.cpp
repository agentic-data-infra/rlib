// Connects to read_server, receives the address and key of its buffer, and
// reads the buffer with RDMA reads of 10, 20, ..., 100 bytes. Then sends a
// final message.
//
//   read_client --address=host:port

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

  co_await endpoint->co_receive();
  BufferInfo info;
  std::memcpy(&info, endpoint->recvBuffer.data(), sizeof(info));
  std::printf(
      "server buffer at %#lx, length %u, key %u\n",
      static_cast<unsigned long>(info.addr),
      info.length,
      info.rkey);

  for (uint32_t length = 10; length <= info.length; length += 10) {
    endpoint->dataBuffer.fill(0);
    int error = co_await endpoint->co_read(
        *endpoint->dataMr, 0, length, info.addr, info.rkey);
    if (error) {
      std::printf("read failed: %s\n", std::strerror(error));
      break;
    }
    std::printf(
        "read %3u bytes: %.*s\n",
        length,
        static_cast<int>(length),
        endpoint->dataBuffer.data());
  }

  std::string done = "done";
  std::memcpy(endpoint->sendBuffer.data(), done.data(), done.size());
  co_await endpoint->co_send(done.size());

  co_await endpoint->co_close();
  co_await group->co_close();
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  folly::coro::blockingWait(co_run());
  return 0;
}
