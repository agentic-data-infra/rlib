#pragma once

#include <infiniband/verbs.h>

#include <array>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Task.h>

#include "rlib/rpc/RdmaRpcEndpoint.h"
#include "rlib/verbs/Log.h"
#include "rlib/verbs/RdmaError.h"

namespace rlib {

template <RdmaRpcProtocol P>
class RdmaRpcStream;

// The client side of an RPC connection. co_call sends a request and resumes
// with the response; any number of coroutines may call at once, with at
// most min(recvQueue, sendQueue) calls in flight (more wait for a slot).
//
// The endpoint has its own completion queue and no thread: coroutines
// waiting for a call poll it themselves, one at a time, yielding to their
// executor between empty polls; the others wait until their call completes
// or it is their turn to poll. A call's send slot is free again once both
// its request was sent and its response arrived. Calls allocate nothing:
// their state lives in the coroutine frame and a fixed slot table.
//
// A failed or flushed completion fails all pending calls with RdmaError,
// as does closing the endpoint; later calls throw too.
template <RdmaRpcProtocol P>
class RdmaRpcClientEndpoint : public RdmaRpcEndpoint<P> {
  using Base = RdmaRpcEndpoint<P>;

 public:
  using Request = typename P::Request;
  using Response = typename P::Response;

  RdmaRpcClientEndpoint(
      RdmaEndpointGroupBase& group,
      RdmaCmId id,
      bool serverSide,
      const RdmaRpcOptions& options)
      : Base{group, std::move(id), serverSide, options},
        calls_(options.sendSlots()) {
    int slots = std::min(options.recvQueue, options.sendSlots());
    for (int i = slots - 1; i >= 0; --i) {
      freeSlots_.push_back(i);
    }
  }

  // Sends 'request' and waits for the response, which it reads into
  // 'response'. Throws RdmaError (EMSGSIZE if the request does not fit).
  folly::coro::Task<void> co_call(const Request& request, Response& response) {
    Waiter waiter;
    int slot = co_await co_acquireSlot(waiter);
    start(slot, request, response, &waiter, nullptr);
    co_await co_waitUntil(waiter, [&waiter] { return waiter.done; }, false);
    if (waiter.error) {
      throw RdmaError{waiter.error, "RPC call failed"};
    }
  }

  // A stream of calls on this endpoint whose responses are taken in the
  // order they arrive. It must outlive its calls.
  std::unique_ptr<RdmaRpcStream<P>> createStream() {
    return std::make_unique<RdmaRpcStream<P>>(*this);
  }

  // Calls in flight.
  int pendingCalls() const {
    std::lock_guard lock{mutex_};
    return std::min(this->options_.recvQueue, this->options_.sendSlots()) -
        static_cast<int>(freeSlots_.size());
  }

 protected:
  // Fails the pending calls once the QP is down.
  folly::coro::Task<void> co_flush() override {
    co_await Base::co_flush();
    std::lock_guard lock{mutex_};
    fail(ECONNRESET);
  }

 private:
  friend class RdmaRpcStream<P>;

  // A coroutine waiting for a slot, its call or its turn to poll.
  struct Waiter {
    folly::coro::Baton baton;
    Waiter* prev = nullptr;
    Waiter* next = nullptr;
    bool parked = false;
    // A slot handed to the waiter, or -1.
    int slot = -1;
    bool done = false;
    int error = 0;
  };

  // A list of parked waiters, linked through their frames.
  struct WaiterList {
    Waiter* head = nullptr;
    Waiter* tail = nullptr;

    void push(Waiter& w) {
      w.prev = tail;
      w.next = nullptr;
      (tail ? tail->next : head) = &w;
      tail = &w;
    }
    void remove(Waiter& w) {
      (w.prev ? w.prev->next : head) = w.next;
      (w.next ? w.next->prev : tail) = w.prev;
      w.prev = w.next = nullptr;
    }
  };

  struct Completion {
    Response* response;
    int error;
  };

  struct Call {
    bool active = false;
    bool sent = false;
    bool received = false;
    Response* response = nullptr;
    Waiter* waiter = nullptr;
    RdmaRpcStream<P>* stream = nullptr;
  };

  folly::coro::Task<int> co_acquireSlot(Waiter& waiter) {
    co_await co_waitUntil(
        waiter,
        [this, &waiter] {
          if (waiter.slot < 0 && !freeSlots_.empty() && !broken_) {
            waiter.slot = freeSlots_.back();
            freeSlots_.pop_back();
          }
          return waiter.slot >= 0 || broken_ != 0;
        },
        true);
    if (waiter.slot < 0) {
      throw RdmaError{broken_, "RPC endpoint failed or closed"};
    }
    co_return waiter.slot;
  }

  // Sends the request of a call in 'slot'; for a stream if 'stream'.
  void start(
      int slot,
      const Request& request,
      Response& response,
      Waiter* waiter,
      RdmaRpcStream<P>* stream) {
    {
      std::lock_guard lock{mutex_};
      calls_[slot] = Call{true, false, false, &response, waiter, stream};
    }
    int error = 0;
    try {
      error = this->postSendSlot(
          slot, this->writeSendSlot(slot, static_cast<uint32_t>(slot), request));
    } catch (const RdmaError&) {
      std::lock_guard lock{mutex_};
      calls_[slot].active = false;
      releaseSlot(slot);
      throw;
    }
    if (error) {
      std::lock_guard lock{mutex_};
      calls_[slot].active = false;
      releaseSlot(slot);
      throw RdmaError{error, "RPC call: post send"};
    }
  }

  // Waits until 'ready' (called with the mutex held) returns true, polling
  // the CQ whenever nobody else does.
  template <typename Ready>
  folly::coro::Task<void> co_waitUntil(Waiter& waiter, Ready ready, bool forSlot) {
    while (true) {
      bool poll = false;
      {
        std::lock_guard lock{mutex_};
        if (waiter.parked) {
          (forSlot ? parkedForSlot_ : parked_).remove(waiter);
          waiter.parked = false;
        }
        if (ready()) {
          wakePoller();
          break;
        }
        if (!polling_) {
          polling_ = true;
          poll = true;
        } else {
          (forSlot ? parkedForSlot_ : parked_).push(waiter);
          waiter.parked = true;
        }
      }
      if (poll) {
        int n = pollOnce();
        {
          std::lock_guard lock{mutex_};
          polling_ = false;
        }
        if (n == 0) {
          co_await folly::coro::co_reschedule_on_current_executor;
        }
        continue;
      }
      co_await waiter.baton;
      waiter.baton.reset();
    }
  }

  // Polls once if nobody else does; for RdmaRpcStream::poll.
  void tryPoll() {
    {
      std::lock_guard lock{mutex_};
      if (polling_) {
        return;
      }
      polling_ = true;
    }
    pollOnce();
    std::lock_guard lock{mutex_};
    polling_ = false;
    wakePoller();
  }

  // With the mutex held: if nobody polls, wakes a parked waiter to.
  void wakePoller() {
    if (polling_) {
      return;
    }
    if (parked_.head) {
      parked_.head->baton.post();
    } else if (parkedForSlot_.head) {
      parkedForSlot_.head->baton.post();
    }
  }

  // Only the poller calls it.
  int pollOnce() {
    std::array<ibv_wc, 16> wcs;
    int n = this->cqProvider()->poll(wcs);
    if (n < 0) {
      std::lock_guard lock{mutex_};
      fail(EIO);
      return 0;
    }
    for (int i = 0; i < n; ++i) {
      dispatch(wcs[i]);
    }
    return n;
  }

  void dispatch(const ibv_wc& wc) {
    if (wc.status != IBV_WC_SUCCESS) {
      std::lock_guard lock{mutex_};
      if (wc.status != IBV_WC_WR_FLUSH_ERR) {
        RLIB_LOG("RPC client: completion %s", ibv_wc_status_str(wc.status));
      }
      fail(wc.status == IBV_WC_WR_FLUSH_ERR ? ECONNRESET : EIO);
      return;
    }
    if (wc.wr_id & Base::kSendBit) {
      int slot = static_cast<int>(wc.wr_id & ~Base::kSendBit);
      std::lock_guard lock{mutex_};
      if (broken_ || !calls_[slot].active) {
        return;
      }
      calls_[slot].sent = true;
      maybeFree(slot);
      return;
    }
    int index = static_cast<int>(wc.wr_id);
    uint32_t ticket = this->ticketOf(index);
    // Under the lock: fail() may complete the call from another thread.
    std::lock_guard lock{mutex_};
    if (broken_) {
      return;
    }
    if (ticket >= calls_.size() || !calls_[ticket].active ||
        calls_[ticket].received) {
      RLIB_LOG("RPC client: response for unknown ticket %u", ticket);
      fail(EPROTO);
      return;
    }
    Call& call = calls_[ticket];
    call.response->update(this->payloadOf(index, wc.byte_len));
    this->countReceived();
    if (int error = this->postRecvSlot(index)) {
      fail(error);
      return;
    }
    call.received = true;
    complete(call, 0);
    maybeFree(static_cast<int>(ticket));
  }

  // With the mutex held.
  void complete(Call& call, int error) {
    if (call.waiter) {
      call.waiter->done = true;
      call.waiter->error = error;
      call.waiter->baton.post();
    } else if (call.stream) {
      call.stream->completed(Completion{call.response, error});
    }
  }

  // With the mutex held.
  void maybeFree(int slot) {
    Call& call = calls_[slot];
    if (call.sent && call.received) {
      call.active = false;
      releaseSlot(slot);
    }
  }

  // With the mutex held: hands the slot to a waiter or frees it.
  void releaseSlot(int slot) {
    if (Waiter* waiter = parkedForSlot_.head) {
      parkedForSlot_.remove(*waiter);
      waiter->parked = false;
      waiter->slot = slot;
      waiter->baton.post();
      return;
    }
    freeSlots_.push_back(slot);
  }

  // With the mutex held: fails all pending calls and wakes all waiters.
  void fail(int error) {
    if (broken_) {
      return;
    }
    broken_ = error;
    for (auto& call : calls_) {
      if (call.active && !call.received) {
        complete(call, error);
      }
      call.active = false;
    }
    for (Waiter* w = parked_.head; w; w = w->next) {
      w->baton.post();
    }
    for (Waiter* w = parkedForSlot_.head; w; w = w->next) {
      w->baton.post();
    }
  }

  mutable std::mutex mutex_;
  bool polling_ = false;
  // The errno that failed the endpoint, or 0.
  int broken_ = 0;
  std::vector<int> freeSlots_;
  std::vector<Call> calls_;
  WaiterList parked_;
  WaiterList parkedForSlot_;
};

// Calls whose responses are taken in the order they arrive: co_request
// sends a request without waiting for its response, co_take returns the
// next response. Made by RdmaRpcClientEndpoint::createStream; one
// co_take at a time.
template <RdmaRpcProtocol P>
class RdmaRpcStream {
  using Endpoint = RdmaRpcClientEndpoint<P>;

 public:
  using Request = typename P::Request;
  using Response = typename P::Response;

  explicit RdmaRpcStream(Endpoint& endpoint) : endpoint_{endpoint} {}

  // Sends 'request'; its response goes into 'response', which must stay
  // alive until co_take returns it. Waits only for a free slot. Throws
  // RdmaError.
  folly::coro::Task<void> co_request(
      const Request& request,
      Response& response) {
    typename Endpoint::Waiter waiter;
    int slot = co_await endpoint_.co_acquireSlot(waiter);
    endpoint_.start(slot, request, response, nullptr, this);
  }

  // The next response to arrive (the object given to co_request). Throws
  // RdmaError if its call failed.
  folly::coro::Task<Response*> co_take() {
    typename Endpoint::Waiter waiter;
    co_await endpoint_.co_waitUntil(
        waiter,
        [this, &waiter] {
          bool ready = !completed_.empty() || endpoint_.broken_ != 0;
          taker_ = ready ? nullptr : &waiter;
          return ready;
        },
        false);
    co_return take();
  }

  // The next response if one has arrived (polling the CQ once if nobody
  // else is), or nullptr. Throws RdmaError if its call failed.
  Response* poll() {
    {
      std::lock_guard lock{endpoint_.mutex_};
      if (!completed_.empty()) {
        return takeLocked();
      }
    }
    endpoint_.tryPoll();
    std::lock_guard lock{endpoint_.mutex_};
    return completed_.empty() ? nullptr : takeLocked();
  }

 private:
  friend Endpoint;

  // With the endpoint's mutex held.
  void completed(typename Endpoint::Completion completion) {
    completed_.push_back(completion);
    if (taker_) {
      taker_->baton.post();
    }
  }

  Response* take() {
    std::lock_guard lock{endpoint_.mutex_};
    if (completed_.empty()) {
      throw RdmaError{endpoint_.broken_, "RPC endpoint failed or closed"};
    }
    return takeLocked();
  }

  Response* takeLocked() {
    auto completion = completed_.front();
    completed_.pop_front();
    if (completion.error) {
      throw RdmaError{completion.error, "RPC call failed"};
    }
    return completion.response;
  }

  Endpoint& endpoint_;
  std::deque<typename Endpoint::Completion> completed_;
  // The parked co_take, if any.
  typename Endpoint::Waiter* taker_ = nullptr;
};

} // namespace rlib
