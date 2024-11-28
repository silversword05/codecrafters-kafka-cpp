#include "TCPManager.h"
#include <chrono>
#include <iostream>
#include <thread>

struct Response {
    int32_t length, correlation_id;
};

int main(int argc, char *argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    TCPManager tcp_manager;
    tcp_manager.createSocketAndListen();

    Fd client_fd = tcp_manager.acceptConnections();
    KafkaApis kafka_apis(client_fd, tcp_manager);

    tcp_manager.readBufferFromClientFd(
        client_fd, [&kafka_apis](const char *buf, const size_t buf_size) {
            kafka_apis.classifyRequest(buf, buf_size);
        });

    // Hack to keep the program running for a while so that netcat can read the
    //  buffer
    // using namespace std::chrono_literals;
    // std::this_thread::sleep_for(1000ms);

    return 0;
}

// #include <arpa/inet.h>
// #include <cstdint>
// #include <cstring>
// #include <iostream>
// #include <netinet/tcp.h>
// #include <stdexcept>
// #include <sys/socket.h>
// #include <sys/types.h>
// #include <unistd.h>
// #include <vector>
// const int16_t UNSUPPORTED_VERSION = 35;
// const int16_t SUCCESS = 0;
// // RAII wrapper for file descriptor
// struct ScopedFd {
//   int fd;
//   explicit ScopedFd(int fd_) : fd(fd_) {}
//   ~ScopedFd() {
//     if (fd >= 0)
//       close(fd);
//   }
//   // Delete copy operations
//   ScopedFd(const ScopedFd &) = delete;
//   ScopedFd &operator=(const ScopedFd &) = delete;
//   // Add move operations
//   ScopedFd(ScopedFd &&other) noexcept : fd(other.fd) { other.fd = -1; }
//   ScopedFd &operator=(ScopedFd &&other) noexcept {
//     if (this != &other) {
//       if (fd >= 0)
//         close(fd);
//       fd = other.fd;
//       other.fd = -1;
//     }
//     return *this;
//   }
//   operator int() const { return fd; }
// };
// // Network IO functions
// template <typename T> T read_network_number(int fd) {
//   T value;
//   if (read(fd, &value, sizeof(value)) != sizeof(value)) {
//     throw std::runtime_error("Failed to read from socket");
//   }
//   if constexpr (sizeof(T) == 1)
//     return value;
//   if constexpr (sizeof(T) == 2)
//     return ntohs(value);
//   if constexpr (sizeof(T) == 4)
//     return ntohl(value);
//   throw std::runtime_error("Unsupported number size");
// }
// template <typename T> void write_network_number(int fd, T value) {
//   T network_value;
//   if constexpr (sizeof(T) == 1)
//     network_value = value;
//   if constexpr (sizeof(T) == 2)
//     network_value = htons(value);
//   if constexpr (sizeof(T) == 4)
//     network_value = htonl(value);
//   if (write(fd, &network_value, sizeof(network_value)) !=
//       sizeof(network_value)) {
//     throw std::runtime_error("Failed to write to socket");
//   }
// }
// void read_and_discard(int fd, size_t bytes) {
//   std::vector<uint8_t> buffer(bytes);
//   if (bytes > 0 &&
//       read(fd, buffer.data(), bytes) != static_cast<ssize_t>(bytes)) {
//     throw std::runtime_error("Failed to read and discard bytes");
//   }
// }
// void read_compact_string(int fd) {
//   // In Kafka's compact string format, the length is encoded as unsigned
//   varint
//   // and includes the length byte itself, so we need to subtract 1
//   auto length = read_network_number<uint8_t>(fd) - 1;
//   if (length > 0) {
//     read_and_discard(fd, length);
//   }
// }
// struct Request {
//   int32_t message_size;
//   int16_t api_key;
//   int16_t api_version;
//   int32_t correlation_id;
// };
// Request read_request_header(int fd) {
//   Request req{};
//   req.message_size = read_network_number<int32_t>(fd);
//   req.api_key = read_network_number<int16_t>(fd);
//   req.api_version = read_network_number<int16_t>(fd);
//   req.correlation_id = read_network_number<int32_t>(fd);
//   return req;
// }
// void send_response(int fd, int32_t correlation_id, int16_t error_code) {

//   std::vector<unsigned char> response(19); // Total size is 19 bytes
//   int pos = 0;

//   // Correlation ID (4 bytes)
//   int32_t network_correlation_id = htonl(correlation_id);
//   std::memcpy(response.data() + pos, &network_correlation_id, 4);
//   pos += 4;
//   // Error code (2 bytes)
//   int16_t network_error_code = htons(error_code);
//   std::memcpy(response.data() + pos, &network_error_code, 2);
//   pos += 2;
//   if (error_code == SUCCESS) {
//     // Number of API keys (1 byte)
//     response[pos++] = 2; // 2 API keys
//     // API Key 18 (ApiVersions)
//     int16_t api_key = htons(18);
//     std::memcpy(response.data() + pos, &api_key, 2);
//     pos += 2;
//     // Min version (2 bytes)
//     int16_t min_version = htons(3);
//     std::memcpy(response.data() + pos, &min_version, 2);
//     pos += 2;
//     // Max version (2 bytes)
//     int16_t max_version = htons(4);
//     std::memcpy(response.data() + pos, &max_version, 2);
//     pos += 2;
//     // Tagged fields (1 byte)
//     response[pos++] = 0;
//     // Throttle time (4 bytes)
//     int32_t throttle_time = htonl(0);
//     std::memcpy(response.data() + pos, &throttle_time, 4);
//     pos += 4;
//     // Final tagged fields (1 byte)
//     response[pos] = 0;
//   }
//   // Calculate and send size prefix
//   int32_t response_size = response.size();
//   int32_t size_network = htonl(response_size);
//   if (write(fd, &size_network, sizeof(size_network)) != sizeof(size_network))
//   {
//     throw std::runtime_error("Failed to write response size");
//   }
//   // Send the response body
//   if (write(fd, response.data(), response.size()) !=
//       static_cast<ssize_t>(response.size())) {
//     throw std::runtime_error("Failed to write response body");
//   }
//   // Flush and close write side
//   int optval = 1;
//   setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
//   shutdown(fd, SHUT_WR);
// }

// ScopedFd create_server_socket(uint16_t port) {
//   ScopedFd server_fd{socket(AF_INET, SOCK_STREAM, 0)};
//   if (server_fd < 0) {
//     throw std::runtime_error("Failed to create server socket");
//   }
//   int reuse = 1;
//   if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse))
//   <
//       0) {
//     throw std::runtime_error("Failed to set socket option");
//   }
//   sockaddr_in server_addr{};
//   server_addr.sin_family = AF_INET;
//   server_addr.sin_addr.s_addr = INADDR_ANY;
//   server_addr.sin_port = htons(port);
//   if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr),
//            sizeof(server_addr)) != 0) {
//     throw std::runtime_error("Failed to bind to port");
//   }
//   if (listen(server_fd, 5) != 0) {
//     throw std::runtime_error("Failed to listen");
//   }
//   return server_fd;
// }
// int main() {
//   std::cout << std::unitbuf;
//   std::cerr << std::unitbuf;
//   try {
//     auto server_fd = create_server_socket(9092);
//     std::cout << "Waiting for a client to connect...\n";
//     while (true) {
//       sockaddr_in client_addr{};
//       socklen_t client_addr_len = sizeof(client_addr);
//       ScopedFd client_fd{
//           accept(server_fd, reinterpret_cast<struct sockaddr
//           *>(&client_addr),
//                  &client_addr_len)};
//       if (client_fd < 0) {
//         std::cerr << "Failed to accept connection\n";
//         continue;
//       }
//       std::cout << "Client connected\n";
//       try {
//         auto request = read_request_header(client_fd);
//         // Read client ID (compact string)
//         read_compact_string(client_fd);
//         // Read tags count
//         read_network_number<uint8_t>(client_fd);
//         // Check if API version is supported (0-4)
//         int16_t error_code =
//             (request.api_version >= 0 && request.api_version <= 4)
//                 ? SUCCESS
//                 : UNSUPPORTED_VERSION;
//         // Send response with correlation ID and error code
//         send_response(client_fd, request.correlation_id, error_code);
//       } catch (const std::exception &e) {
//         std::cerr << "Error handling client: " << e.what() << '\n';
//       }
//     }
//   } catch (const std::exception &e) {
//     std::cerr << "Error: " << e.what() << '\n';
//     return 1;
//   }
//   return 0;
// }
