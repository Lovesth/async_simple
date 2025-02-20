//
// Created by xmh on 25-2-20.
//

#include "Socket.h"
#include <sys/socket.h>
#include "IoContext.h"

Socket::Socket(int domain, int type, int protocol, uint32_t listen_events,
               IoContext *io_context)
    : io_context_(io_context), listen_events_(listen_events) {
    fd_ = ::socket(domain, type, protocol);
    if (fd_ == -1) {
        std::cerr << "Error creating socket" << std::endl;
        exit(-1);
    }
    if (!addEvents(listen_events_) || !attach2IoContext()) {
        std::cerr << "Error attached to io_context" << std::endl;
        exit(-1);
    }
}
Socket::~Socket() {
    if (fd_ != -1) {
        fd_ = -1;
        ::close(fd_);
    }
}
bool Socket::attach2IoContext() {
    if (!io_context_ || io_context_->epoll_fd_ == -1 ||
        io_context_->maxEvents_ <= 0 || io_context_->maxThreads_ <= 0)
        return false;
    const int epoll_fd = io_context_->epoll_fd_;
    epoll_event event{};
    event.events = listen_events_;
    event.data.ptr = this;
    const int ret = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd_, &event);
    return (ret != -1);
}
bool Socket::addEvents(const uint32_t events) {
    async_simple::coro::ScopedSpinLock Lock(io_state_lock_);
    if ((listen_events_ & events) == events) {
        return true;
    }
    listen_events_ |= events;
    epoll_event e_event{};
    e_event.events = listen_events_;
    const auto res =
        epoll_ctl(io_context_->epoll_fd_, EPOLL_CTL_MOD, fd_, &e_event);
    return res != -1;
}
bool Socket::removeEvents(uint32_t events) {
    async_simple::coro::ScopedSpinLock Lock(io_state_lock_);
    auto removed_events = listen_events_ & events;
    if (!removed_events)
        return true;
    listen_events_ ^= removed_events;
    epoll_event e_event{};
    e_event.events = listen_events_;
    const auto res =
        epoll_ctl(io_context_->epoll_fd_, EPOLL_CTL_MOD, fd_, &e_event);
    return res != -1;
}
