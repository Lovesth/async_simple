//
// Created by xmh on 25-2-13.
//

#ifndef IOCONTEXT_H
#define IOCONTEXT_H

#include <sys/epoll.h>
#include "async_simple/executors/SimpleExecutor.h"

class Socket;

class IoContext {
public:
    IoContext(int maxThreads = 10, int maxEvents = 100);
    IoContext(const IoContext &other) = delete;
    IoContext &operator=(const IoContext &other) = delete;
    IoContext(IoContext &&other);

    IoContext &operator=(IoContext &&other);
    ~IoContext();
    void run();

public:
    int epoll_fd_;
    int maxThreads_;
    int maxEvents_;
    async_simple::executors::SimpleExecutor *executor_;
    epoll_event *eventPool_;
};

#endif  // IOCONTEXT_H