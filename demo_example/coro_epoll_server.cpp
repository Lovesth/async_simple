//
// Created by xmh on 25-2-19.
//
#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/executors/SimpleExecutor.h>
#include <netinet/in.h>
#include <memory>
#include <thread>
#include "HookSysCall.hpp"

async_simple::coro::Lazy<> echo_server(Socket* server_sock) {
    auto executor_ = co_await async_simple::CurrentExecutor{};
    auto io_context = server_sock->io_context_;
    async_simple::logicAssert(executor_ != nullptr,
                              "executor is not allowed to be nullptr here!");
    while (server_sock->fd_ != -1) {
        auto fd = co_await accept(server_sock);
        // auto sock = std::make_shared<Socket>();
        if (fd == -1)
            continue;
        executor_->schedule([fd, io_context] {
            auto func = [fd, io_context] -> async_simple::coro::Lazy<> {
                char buffer[1024] = {0};
                Socket sock(fd, io_context);
                while (true) {
                    auto recv_len = co_await recv(&sock, buffer, sizeof(buffer));
                    
                }
                co_return;
            };
            func().start([](auto&&) {});
        });
    }
}

int main() {
    // 1. 创建 Socket
    int server_fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (server_fd == -1) {
        std::cerr << "Failed to create server socket!" << std::endl;
        return -1;
    }

    // 2. 设置地址复用
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 3. 设置地址和端口
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);

    // 4. 绑定地址和端口号到套接字
    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(sockaddr)) ==
        -1) {
        std::cerr << "Failed to bind!" << std::endl;
        return -1;
    }

    // 监听模式设置
    if (listen(server_fd, 2048) == -1) {
        std::cerr << "Listen failed!" << std::endl;
        return -1;
    }

    //
    async_simple::executors::SimpleExecutor executor{32};
    IoContext io_context(100, &executor);
    Socket server_sock(server_fd, &io_context);

    echo_server(&server_sock).directlyStart([](auto&&) {}, &executor);

    auto t = std::jthread(IoContext::run, &io_context);
    return 0;
}