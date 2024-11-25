#include "TCPManager.h"

#include <bits/stdc++.h>
#include <unistd.h>

Fd::~Fd() {
    if (fd != -1) {
        std::cout << "Closing file descriptor " << fd << "\n";
        close(fd);
    } else {
        std::cout << "File descriptor already closed" << fd << "\n ";
    }
}

void RequestMessage::fromBuffer(const char *buffer, size_t buffer_size) {
    if (buffer_size < sizeof(RequestMessage)) {
        throw std::runtime_error("Buffer size is too small");
    }

    message_size = ntohl(*reinterpret_cast<const uint32_t *>(buffer));
    request_api_key = ntohs(*reinterpret_cast<const int16_t *>(buffer + 4));
    request_api_version = ntohs(*reinterpret_cast<const int16_t *>(buffer + 6));
    corellation_id = ntohl(*reinterpret_cast<const int32_t *>(buffer + 8));
}

std::string RequestMessage::toString() const {
    return "RequestMessage{message_size=" + std::to_string(message_size) +
           ", request_api_key=" + std::to_string(request_api_key) +
           ", request_api_version=" + std::to_string(request_api_version) +
           ", corellation_id=" + std::to_string(corellation_id) + "}";
}

std::string_view ResponseMessage::toBuffer() const {
    char buffer[sizeof(ResponseMessage)];

    *reinterpret_cast<uint32_t *>(buffer) = htonl(message_size);
    *reinterpret_cast<int32_t *>(buffer + 4) = htonl(corellation_id);
    *reinterpret_cast<int32_t *>(buffer + 8) = htons(error_code);

    return std::string_view(buffer, sizeof(buffer));
}

std::string ResponseMessage::toString() const {
    return "ResponseMessage{message_size=" + std::to_string(message_size) +
           ", corellation_id=" + std::to_string(corellation_id) +
           ", error_code=" + std::to_string(error_code) + "}";
}

void TCPManager::createSocketAndListen() {
    server_fd.setFd(socket(AF_INET, SOCK_STREAM, 0));
    if (server_fd < 0) {
        perror("socket failed: ");
        throw std::runtime_error("Failed to create server socket: ");
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
        0) {
        perror("setsockopt failed: ");
        close(server_fd);
        throw std::runtime_error("setsockopt failed: ");
    }

    struct sockaddr_in server_addr = getSocketAddr();

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr),
             sizeof(server_addr)) != 0) {
        perror("bind failed: ");
        close(server_fd);
        throw std::runtime_error("Failed to bind to port 9092");
    }

    std::cout << "Waiting for a client to connect...\n";

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        perror("listen failed: ");
        close(server_fd);
        throw std::runtime_error("listen failed");
    }

    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    std::cerr << "Logs from your program will appear here!\n";
}

Fd TCPManager::acceptConnections() const {
    struct sockaddr_in client_addr {};
    socklen_t client_addr_len = sizeof(client_addr);

    struct sockaddr *addr = reinterpret_cast<struct sockaddr *>(&client_addr);
    Fd client_fd(accept(server_fd, addr, &client_addr_len));

    if (client_fd < 0) {
        perror("accept failed: ");
        throw std::runtime_error("Failed to accept connection: ");
    }

    std::cout << "Client connected\n";
    return client_fd;
}

void TCPManager::writeBufferOnClientFd(
    const Fd &client_fd, const ResponseMessage &response_message) const {

    std::cout << "Sending msg to client: " << response_message.toString()
              << "\n";

    std::string_view buffer = response_message.toBuffer();

    // Write message Length
    if (send(client_fd, buffer.data(), buffer.size(), 0) < 0) {
        perror("send failed: ");
        throw std::runtime_error("Failed to send msgLen to client: ");
    }

    std::cout << "Message sent to client\n";
}

RequestMessage TCPManager::readBufferFromClientFd(const Fd &client_fd) const {
    RequestMessage request_message;

    constexpr size_t buffer_size = sizeof(RequestMessage);
    char buffer[buffer_size];
    ssize_t bytes_received = recv(client_fd, buffer, buffer_size, 0);

    if (bytes_received < 0) {
        perror("recv failed: ");
        throw std::runtime_error("Failed to read from client: ");
    }

    request_message.fromBuffer(buffer, buffer_size);

    std::cout << "Message received from client: " << request_message.toString()
              << "\n";
    return request_message;
}

KafkaApis::KafkaApis(const Fd &_client_fd, const TCPManager &_tcp_manager)
    : client_fd(_client_fd), tcp_manager(_tcp_manager) {}

void KafkaApis::checkApiVersions() const {
    RequestMessage request_message =
        tcp_manager.readBufferFromClientFd(client_fd);

    ResponseMessage response_message{
        .message_size = 0,
        .corellation_id = request_message.corellation_id,
        .error_code = 0,
    };

    if (request_message.request_api_version < 0 ||
        request_message.request_api_version > 4) {
        response_message.error_code = UNSUPPORTED_VERSION;
        std::cout << "Unsupported version: "
                  << request_message.request_api_version << "\n";
    }

    tcp_manager.writeBufferOnClientFd(client_fd, response_message);
}