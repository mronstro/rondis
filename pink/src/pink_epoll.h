// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_SRC_PINK_EPOLL_H_
#define PINK_SRC_PINK_EPOLL_H_
#include <queue>
#include <vector>
#ifdef __APPLE__
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#else
#include "sys/epoll.h"
#endif

#include "pink/src/pink_item.h"
#include "slash/include/slash_mutex.h"

namespace pink {

struct PinkFiredEvent {
  int fd;
  int mask;
};

class PinkEpoll {
 public:
  static const int kUnlimitedQueue = -1;
  static const int kRead = 1;
  static const int kWrite = 2;
  static const int kError = 4;

  PinkEpoll(int queue_limit = kUnlimitedQueue);
  ~PinkEpoll();
  int PinkAddEvent(const int fd, const int mask);
  int PinkDelEvent(const int fd, int mask);
  int PinkModEvent(const int fd, const int old_mask, const int mask);

  int PinkPoll(const int timeout);

  PinkFiredEvent *firedevent() const { return firedevent_; }

  int notify_receive_fd() {
    return notify_receive_fd_;
  }
  int notify_send_fd() {
    return notify_send_fd_;
  }
  PinkItem notify_queue_pop();

  bool Register(const PinkItem& it, bool force);
  bool Deregister(const PinkItem& it) { return false; }

 private:
  int epfd_;
#ifdef __APPLE__
  std::vector<struct kevent> events_;
#else
  std::vector<struct epoll_event> events_;
#endif
  PinkFiredEvent *firedevent_;

  /*
   * The PbItem queue is the fd queue, receive from dispatch thread
   */
  int queue_limit_;
  slash::Mutex notify_queue_protector_;
  std::queue<PinkItem> notify_queue_;

  /*
   * These two fd receive the notify from dispatch thread
   */
  int notify_receive_fd_;
  int notify_send_fd_;
};

}  // namespace pink
#endif  // PINK_SRC_PINK_EPOLL_H_
