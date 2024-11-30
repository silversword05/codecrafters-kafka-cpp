#pragma once

#include <arpa/inet.h>
#include <bits/stdc++.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

struct Fd {
    explicit Fd(int _fd) : fd(_fd) {}
    Fd() = default;
    ~Fd();

    // Delete copy operations
    Fd(const Fd &) = delete;
    Fd &operator=(const Fd &) = delete;

    // Add move operations
    Fd(Fd &&other) noexcept : fd(other.fd) { other.fd = -1; }
    Fd &operator=(Fd &&other) noexcept;

    void setFd(int _fd) { fd = _fd; }
    int getFd() const { return fd; }
    operator int() const { return fd; }

  private:
    int fd = -1;
};

struct TCPManager {
    TCPManager() = default;

    static struct sockaddr_in getSocketAddr() {
        struct sockaddr_in server_addr {
            .sin_family = AF_INET, .sin_port = htons(9092),
        };
        server_addr.sin_addr.s_addr = INADDR_ANY;
        return server_addr;
    }

    void createSocketAndListen();
    Fd acceptConnections() const;

    void writeBufferOnClientFd(const Fd &client_fd,
                               const auto &response_message) const;

    bool readBufferFromClientFd(
        const Fd &client_fd,
        const std::function<void(const char *, const size_t)> &func) const;

    void addClientThread(std::jthread &&client_thread) {
        client_threads.push_back(std::move(client_thread));
    }

  private:
    Fd server_fd;
    std::vector<std::jthread> client_threads;
};