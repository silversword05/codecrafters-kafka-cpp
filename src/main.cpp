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

    kafka_apis.checkApiVersions();

    // Hack to keep the program running for a while so that netcat can read the
    //  buffer
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1000ms);

    return 0;
}
