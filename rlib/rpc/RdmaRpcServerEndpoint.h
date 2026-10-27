#pragma once

#include <infiniband/verbs.h>

#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <vector>

#include <folly/Executor.h>
#include <folly/coro/AsyncScope.h>
#include <folly/coro/Task.h>
#include <folly/executors/InlineExecutor.h>

#include "rlib/rpc/RdmaRpcEndpoint.h"
#include "rlib/rpc/RdmaRpcService.h"
#include "rlib/verbs/Log.h"

namespace rlib {

// What a server group gives its endpoints.
template <RdmaRpcProtocol P>
struct RdmaRpcServerContext {
  RdmaRpcService<P>& service;
  // Where handlers and closing run.
  folly::Executor::KeepAlive<> executor;
  folly::coro::AsyncScope& scope;
  // Start handlers on the CQ thread instead of the executor.
  bool inlineDispatch;
};

// The server side of an RPC connection, made by RdmaRpcServerGroup. Each
// request that arrives (on the CQ processor's thread) is handed to the
// service's co_process as an event; its response is sent once that
// returns, or as soon as a send slot is free. Receive slot i has event i,
// and the slot is posted again once the response is on its way, so a
// connection has at most recvQueue requests in progress; a client with
// more in flight waits (receiver-not-ready retries) until one is done.
// The endpoint closes itself when the client disconnects.
template <RdmaRpcProtocol P>
class RdmaRpcServerEndpoint : public RdmaRpcEndpoint<P> {
  using Base = RdmaRpcEndpoint<P>;
  using Event = RdmaRpcServerEvent<P>;

 public:
  RdmaRpcServerEndpoint(
      RdmaEndpointGroupBase& group,
      RdmaCmId id,
      bool serverSide,
      const RdmaRpcOptions& options,
      const RdmaRpcServerContext<P>& context)
      : Base{group, std::move(id), serverSide, options},
        context_{context},
        events_(options.recvQueue) {
    for (int i = 0; i < options.recvQueue; ++i) {
      events_[i].index_ = i;
      events_[i].endpoint_ = this;
    }
    for (int i = options.sendSlots() - 1; i >= 0; --i) {
      freeSlots_.push_back(i);
    }
  }

  // On the CQ processor's thread.
  void dispatchCqEvent(const ibv_wc& wc) override {
    if (wc.status != IBV_WC_SUCCESS) {
      if (wc.status != IBV_WC_WR_FLUSH_ERR) {
        RLIB_LOG("RPC server endpoint %d: completion %s", this->endpointId(),
                 ibv_wc_status_str(wc.status));
      }
      return;
    }
    if (wc.wr_id & Base::kSendBit) {
      sendCompleted(static_cast<int>(wc.wr_id & ~Base::kSendBit));
      return;
    }
    int index = static_cast<int>(wc.wr_id);
    Event& event = events_[index];
    event.ticket_ = this->ticketOf(index);
    event.request_.update(this->payloadOf(index, wc.byte_len));
    event.response_ = typename P::Response{};
    this->countReceived();
    auto self = std::static_pointer_cast<RdmaRpcServerEndpoint>(
        this->shared_from_this());
    folly::Executor::KeepAlive<> executor = context_.inlineDispatch
        ? folly::getKeepAliveToken(folly::InlineExecutor::instance())
        : context_.executor;
    context_.scope.add(folly::coro::co_withExecutor(
        std::move(executor), co_handle(std::move(self), &event)));
  }

  // Responses waiting for a send slot.
  size_t queuedResponses() const {
    std::lock_guard lock{mutex_};
    return queued_.size();
  }

 protected:
  void init() override {
    Base::init();
    {
      std::lock_guard lock{mutex_};
      opened_ = true;
    }
    context_.service.open(*this);
  }

  // Responses still to send are dropped.
  folly::coro::Task<void> co_flush() override {
    co_await Base::co_flush();
    bool wasOpen = false;
    {
      std::lock_guard lock{mutex_};
      wasOpen = opened_ && !closed_;
      closed_ = true;
      queued_.clear();
    }
    if (wasOpen) {
      context_.service.close(*this);
    }
  }

  void onDisconnected() override {
    auto self = std::static_pointer_cast<RdmaRpcServerEndpoint>(
        this->shared_from_this());
    context_.scope.add(folly::coro::co_withExecutor(
        context_.executor, co_closeEndpoint(std::move(self))));
  }

 private:
  static folly::coro::Task<void> co_handle(
      std::shared_ptr<RdmaRpcServerEndpoint> self,
      Event* event) {
    try {
      co_await self->context_.service.co_process(*event);
    } catch (const std::exception& e) {
      RLIB_LOG("RPC service: %s", e.what());
    }
    self->sendResponse(*event);
  }

  static folly::coro::Task<void> co_closeEndpoint(
      std::shared_ptr<RdmaRpcServerEndpoint> self) {
    co_await self->co_close();
  }

  void sendResponse(Event& event) {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return;
    }
    if (freeSlots_.empty()) {
      queued_.push_back(&event);
      return;
    }
    int slot = freeSlots_.back();
    freeSlots_.pop_back();
    send(slot, event);
  }

  void sendCompleted(int slot) {
    std::lock_guard lock{mutex_};
    if (closed_) {
      return;
    }
    if (queued_.empty()) {
      freeSlots_.push_back(slot);
      return;
    }
    Event* event = queued_.front();
    queued_.pop_front();
    send(slot, *event);
  }

  // With the mutex held: sends the event's response from 'slot'. The
  // event's receive is posted again first, so that the client's next
  // request finds it.
  void send(int slot, Event& event) {
    uint32_t length = 0;
    try {
      length = this->writeSendSlot(slot, event.ticket_, event.response_);
    } catch (const RdmaError& e) {
      RLIB_LOG("RPC server endpoint %d: %s", this->endpointId(), e.what());
    }
    // The event may be reused from here on.
    if (int error = this->postRecvSlot(event.index_)) {
      RLIB_LOG("RPC server endpoint %d: post receive: %d", this->endpointId(),
               error);
    }
    int error = length ? this->postSendSlot(slot, length) : EMSGSIZE;
    if (error) {
      RLIB_LOG("RPC server endpoint %d: send failed: %d", this->endpointId(),
               error);
      freeSlots_.push_back(slot);
    }
  }

  const RdmaRpcServerContext<P>& context_;
  std::vector<Event> events_;
  mutable std::mutex mutex_;
  bool opened_ = false;
  bool closed_ = false;
  std::vector<int> freeSlots_;
  std::deque<Event*> queued_;
};

} // namespace rlib
