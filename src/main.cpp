#include "TCPManager.h"
#include <iostream>

int main(int argc, char *argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    TCPManager tcp_manager;
    tcp_manager.createSocketAndListen();

    Fd client_fd = tcp_manager.acceptConnections();

    tcp_manager.writeBufferOnClientFd(client_fd, "Hello, World!", 7);
    return 0;
}
