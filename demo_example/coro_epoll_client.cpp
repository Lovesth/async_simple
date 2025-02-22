//
// Created by xmh on 25-2-21.
//
#include <netinet/in.h>

#include <arpa/inet.h>
#include <asio/detail/socket_ops.hpp>
#include "HookSysCall.hpp"

async_simple::coro::Lazy<> client_send(IoContext *io_context, std::string host,
                                       int port, int nClients = 1024,
                                       int nRound = 1024) {
    auto executor_ = co_await async_simple::CurrentExecutor{};
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) != 1) {
        std::cerr << "Error convert string to addr!" << std::endl;
        co_return;
    }
    server_addr.sin_port = htons(port);

    // 创建nClients个客户端
    for (int i = 0; i < nClients; ++i) {
        executor_->schedule([server_addr, io_context, nRound]() mutable -> void {
            auto func = [server_addr, io_context, nRound]() -> async_simple::coro::Lazy<> {
                Socket sock(AF_INET, SOCK_STREAM, 0, io_context);
                auto res = co_await connect(&sock, reinterpret_cast<sockaddr*>(const_cast<sockaddr_in *>(&server_addr)));
                if (res == -1) {
                    co_return;
                }
                char buffer[1024] = "Hello, this is coro_epoll_client";
                for (int j=0; j<nRound; ++j) {
                    int send_bytes{0};
                    while (send_bytes < 1024) {
                        auto tmp = co_await send(&sock, buffer+send_bytes, sizeof(buffer)-send_bytes);
                        if (tmp <= 0) {
                            co_return;
                        }
                        send_bytes += tmp;
                    }
                    int recv_bytes{0};
                    while (recv_bytes < 1024) {
                        auto tmp = co_await recv(&sock, buffer+recv_bytes, sizeof(buffer)-recv_bytes);
                        if (tmp <= 0) {
                            co_return;
                        }
                        recv_bytes += tmp;
                    }
                }

            };
            func().start([](auto&&){});
        });
    }
}

int main() {
    async_simple::executors::SimpleExecutor executor{32};
    IoContext io_context(100, &executor);

    auto t = std::jthread(&IoContext::run, &io_context);
    client_send(&io_context, "127.0.0.1", 8080, 1024, 1024)
        .directlyStart([](auto &&) {}, &executor);
    return 0;
}