#pragma once

#include <infiniband/verbs.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <folly/coro/AsyncScope.h>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "rlib/endpoint/RdmaActiveEndpointGroup.h"
#include "rlib/rpc/RdmaRpcServerEndpoint.h"
#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

struct RdmaRpcServerOptions : RdmaRpcOptions {
  // CQ threads poll all the time instead of sleeping.
  bool polling = false;
  // One CQ processor per listed core, pinned to it (one unpinned if empty);
  // connections go to them round-robin.
  std::vector<int> cpus;
  // Connections each CQ processor takes; more are refused.
  int clusterSize = 64;
  // Start co_process on the CQ thread, for services that answer at once
  // (or quickly hop to their own executor), saving a thread switch.
  bool inlineDispatch = false;
  // Where co_process runs; by default the group's own pool of 'threads'
  // threads (0: one per core).
  folly::Executor::KeepAlive<> executor;
  int threads = 0;
};

// An RPC server: accepts connections at an address and hands their
// requests to a service.
//
//   EchoService service;
//   auto group = RdmaRpcServerGroup<EchoProtocol>::create(service);
//   group->serve(address);
//   ...
//   co_await group->co_close();
template <RdmaRpcProtocol P>
class RdmaRpcServerGroup
    : public RdmaActiveEndpointGroup<RdmaRpcServerEndpoint<P>> {
  using Base = RdmaActiveEndpointGroup<RdmaRpcServerEndpoint<P>>;

 public:
  using Endpoint = RdmaRpcServerEndpoint<P>;

  // A started group; 'service' must outlive it.
  static std::unique_ptr<RdmaRpcServerGroup> create(
      RdmaRpcService<P>& service,
      RdmaRpcServerOptions options = {}) {
    auto group = std::unique_ptr<RdmaRpcServerGroup>{
        new RdmaRpcServerGroup{service, std::move(options)}};
    group->init(std::make_unique<Factory>(*group));
    return group;
  }

  ~RdmaRpcServerGroup() override {
    // Before the scope and executor go.
    if (!closing_) {
      folly::coro::blockingWait(co_close());
    }
  }

  // Binds to 'address' (port 0 picks one) and accepts connections until
  // closed. Throws RdmaError.
  void serve(const SocketAddress& address, int backlog = 64) {
    server_ = this->createServerEndpoint();
    server_->bind(address, backlog);
    scope_.add(folly::coro::co_withExecutor(
        context_.executor, co_acceptLoop(server_)));
  }

  // The address serve() bound to.
  SocketAddress address() const {
    return server_->srcAddr();
  }

  ibv_qp* createQpProvider(Endpoint& endpoint) override {
    auto* processor =
        static_cast<RdmaCqProcessorBase*>(endpoint.cqProvider());
    ibv_qp_init_attr attr{};
    attr.cap.max_recv_sge = 1;
    attr.cap.max_recv_wr = options_.recvQueue;
    attr.cap.max_send_sge = 1;
    attr.cap.max_send_wr = options_.sendSlots();
    attr.cap.max_inline_data = options_.maxInline;
    attr.qp_type = IBV_QPT_RC;
    attr.recv_cq = processor->cq();
    attr.send_cq = processor->cq();
    ibv_qp* qp = endpoint.cmId().createQp(endpoint.pd(), attr);
    endpoint.setMaxInline(static_cast<int>(attr.cap.max_inline_data));
    if (!processor->registerQp(qp->qp_num, endpoint, entriesPerEndpoint())) {
      endpoint.cmId().destroyQp();
      throw RdmaError{ENOSPC, "CQ processor full: raise clusterSize"};
    }
    return qp;
  }

  // Stops accepting, closes the connections (dropping responses not yet
  // sent), stops the CQ threads and waits for running handlers.
  // Idempotent.
  folly::coro::Task<void> co_close() override {
    if (closing_.exchange(true)) {
      co_return;
    }
    if (server_) {
      co_await server_->co_close();
    }
    co_await Base::co_close();
    co_await scope_.joinAsync();
  }

  const RdmaRpcServerOptions& options() const {
    return options_;
  }

 private:
  class Factory : public RdmaEndpointFactory<Endpoint> {
   public:
    explicit Factory(RdmaRpcServerGroup& group) : group_{group} {}
    std::shared_ptr<Endpoint> createEndpoint(RdmaCmId id, bool serverSide)
        override {
      return std::make_shared<Endpoint>(
          group_, std::move(id), serverSide, group_.options_, group_.context_);
    }

   private:
    RdmaRpcServerGroup& group_;
  };

  RdmaRpcServerGroup(RdmaRpcService<P>& service, RdmaRpcServerOptions options)
      : Base{
            options.polling,
            std::max(options.recvQueue, options.sendSlots()),
            1,
            (options.recvQueue + options.sendSlots()) * options.clusterSize,
            options.cpus},
        options_{std::move(options)},
        ownedExecutor_{
            options_.executor ? nullptr : makeExecutor(options_.threads)},
        context_{
            service,
            options_.executor ? options_.executor
                              : folly::getKeepAliveToken(ownedExecutor_.get()),
            scope_,
            options_.inlineDispatch} {}

  static std::unique_ptr<folly::CPUThreadPoolExecutor> makeExecutor(
      int threads) {
    if (threads <= 0) {
      threads = static_cast<int>(
          std::max(1u, std::thread::hardware_concurrency()));
    }
    return std::make_unique<folly::CPUThreadPoolExecutor>(threads);
  }

  int entriesPerEndpoint() const {
    return options_.recvQueue + options_.sendSlots();
  }

  static folly::coro::Task<void> co_acceptLoop(
      std::shared_ptr<RdmaServerEndpoint<Endpoint>> server) {
    while (true) {
      try {
        co_await server->co_accept();
      } catch (const RdmaError& e) {
        if (server->isClosed()) {
          break;
        }
        RLIB_LOG("RPC server: accept: %s", e.what());
      }
    }
  }

  const RdmaRpcServerOptions options_;
  // Destroyed last, once nothing runs on it.
  std::unique_ptr<folly::CPUThreadPoolExecutor> ownedExecutor_;
  folly::coro::AsyncScope scope_;
  const RdmaRpcServerContext<P> context_;
  std::shared_ptr<RdmaServerEndpoint<Endpoint>> server_;
  std::atomic<bool> closing_{false};
};

} // namespace rlib
