#include <arpa/inet.h>
#include <netdb.h>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>

struct Fd {
    explicit Fd(int _fd) : fd(_fd) {}
    Fd() = default;
    ~Fd();

    void setFd(int _fd) { fd = _fd; }
    int getFd() const { return fd; }
    operator int() const { return fd; }

  private:
    int fd = -1;
};

struct RequestMessage {
    uint32_t message_size{};
    int16_t request_api_key{};
    int16_t request_api_version{};
    int32_t corellation_id{};

    void fromBuffer(const char *buffer, size_t buffer_size);
    std::string toString() const;
};

struct ResponseMessage {
    uint32_t message_size{};
    int32_t corellation_id{};

    std::string_view toBuffer() const;
    std::string toString() const;
};

struct TCPManager {
    TCPManager() = default;

    struct sockaddr_in getSocketAddr() {
        struct sockaddr_in server_addr {
            .sin_family = AF_INET, .sin_port = htons(9092),
        };
        server_addr.sin_addr.s_addr = INADDR_ANY;
        return server_addr;
    }

    void createSocketAndListen();
    Fd acceptConnections();
    void writeBufferOnClientFd(const Fd &client_fd,
                               const ResponseMessage &response_message);
    RequestMessage readBufferFromClientFd(const Fd &client_fd);

  private:
    Fd server_fd;
};