//
// Created by xmh on 25-2-13.
//

#ifndef ASYNC_SIMPLE_HOOK_SYS_CALL_H
#define ASYNC_SIMPLE_HOOK_SYS_CALL_H

#include <sys/epoll.h>
#include <sys/socket.h>
#include "async_simple/coro/Lazy.h"
#include "Socket.h"

// 假设Socket::fd_已经是no_block模式
async_simple::coro::Lazy<int> connect(Socket *sock, sockaddr *serverAdder) {
    int ret = ::connect(sock->fd_, serverAdder, sizeof(sockaddr));
    if (ret == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (sock->addEvents(EPOLLOUT)) {
            auto events = co_await SendAwaiter(sock);
            (void)events;
            if (sock->fd_ != -1) {
                // TODO 判断是否是ET模式
                ret = ::connect(sock->fd_, serverAdder, sizeof(sockaddr));
            }
        }
    }
    co_return ret;
}

async_simple::coro::Lazy<int> send(Socket *sock, void *buffer, size_t len) {
    int ret = ::send(sock->fd_, buffer, len, 0);
    if (ret == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (sock->addEvents(EPOLLOUT)) {
            auto events = co_await SendAwaiter(sock);
            (void)events;
            if (sock->fd_ != -1) {
                // TODO 判断是否是ET模式
                ret = ::send(sock->fd_, buffer, len, 0);
            }
        }
    }
    co_return ret;
}

async_simple::coro::Lazy<int> recv(Socket *sock, void *buffer, size_t len) {
    int ret = ::recv(sock->fd_, buffer, len, 0);
    if (ret == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (sock->addEvents(EPOLLIN)) {
            auto events = co_await RecvAwaiter(sock);
            (void)events;
            if (sock->fd_ != -1) {
                // TODO 判断是否是ET模式
                // TODO 出错时关闭fd
                ret = ::recv(sock->fd_, buffer, len, 0);
            }
        }
    }
    co_return ret;
}

async_simple::coro::Lazy<int> accept(Socket *sock) {
    struct sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    int ret = ::accept(sock->fd_, reinterpret_cast<sockaddr *>(&addr), &len);
    if (ret == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if (sock->addEvents(EPOLLOUT)) {
            auto events = co_await RecvAwaiter(sock);
            (void)events;
            if (sock->fd_ != -1) {
                // TODO 判断是否是ET模式
                // TODO 出错时关闭fd
                ret = ::accept(sock->fd_, reinterpret_cast<sockaddr *>(&addr), &len);
            }
        }
    }
    co_return ret;
}

#endif  // ASYNC_SIMPLE_HOOK_SYS_CALL_H
