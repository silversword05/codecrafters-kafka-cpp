#include "TCPManager.h"

#include <bits/stdc++.h>
#include <unistd.h>

Fd::~Fd() {
    if (fd != -1) {
        std::cout << "Closing file descriptor " << fd << "\n";
        close(fd);
    }
}

void TCPManager::createSocketAndListen() {
    server_fd.setFd(socket(AF_INET, SOCK_STREAM, 0));
    if (server_fd < 0) {
        throw std::runtime_error("Failed to create server socket: ");
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
        close(server_fd);
        throw std::runtime_error("setsockopt failed: ");
    }

    struct sockaddr_in server_addr = getSocketAddr();

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr),
             sizeof(server_addr)) != 0) {
        close(server_fd);
        throw std::runtime_error("Failed to bind to port 9092");
    }

    std::cout << "Waiting for a client to connect...\n";

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        throw std::runtime_error("listen failed");
    }

    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    std::cerr << "Logs from your program will appear here!\n";
}

Fd TCPManager::acceptConnections() {
    struct sockaddr_in client_addr {};
    socklen_t client_addr_len = sizeof(client_addr);

    struct sockaddr *addr = reinterpret_cast<struct sockaddr *>(&client_addr);
    Fd client_fd(accept(server_fd, addr, &client_addr_len));

    if (client_fd < 0) {
        throw std::runtime_error("Failed to accept connection: ");
    }

    std::cout << "Client connected\n";
    return client_fd;
}

void TCPManager::writeBufferOnClientFd(const Fd &client_fd,
                                       std::string_view buffer,
                                       const uint32_t corellation_id) {

    uint32_t msgLen = htonl(buffer.size());

    // Write message Length
    if (write(client_fd, &msgLen, sizeof(msgLen)) < 0) {
        throw std::runtime_error("Failed to write msgLen to client: ");
    }

    // Write correlation id
    if (write(client_fd, &corellation_id, sizeof(corellation_id)) < 0) {
        throw std::runtime_error("Failed to write msgLen to client: ");
    }

    // Write message
    if (write(client_fd, buffer.data(), buffer.size()) < 0) {
        throw std::runtime_error("Failed to write buffer to client: ");
    }
}
