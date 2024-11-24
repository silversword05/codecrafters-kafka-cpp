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
    void writeBufferOnClientFd(const Fd &client_fd, std::string_view buffer,
                               const uint32_t corellation_id);

  private:
    Fd server_fd;
};