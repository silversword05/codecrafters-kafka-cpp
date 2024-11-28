#include "TCPManager.h"

namespace {

std::function<void(int)> shutdown_handler;
void signal_handler(int signal) { shutdown_handler(signal); }

bool handleClient(const Fd &client_fd, const TCPManager &tcp_manager) {
    KafkaApis kafka_apis(client_fd, tcp_manager);

    return tcp_manager.readBufferFromClientFd(
        client_fd, [&kafka_apis](const char *buf, const size_t buf_size) {
            kafka_apis.classifyRequest(buf, buf_size);
        });
}

} // namespace

int main(int argc, char *argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    try {
        TCPManager tcp_manager;
        tcp_manager.createSocketAndListen();

        shutdown_handler = [&tcp_manager](int signal) {
            std::cout << "Caught signal " << signal << '\n';
            tcp_manager.~TCPManager();
            exit(1);
        };

        signal(SIGINT, signal_handler);

        while (true) {
            Fd client_fd = tcp_manager.acceptConnections();
            tcp_manager.addClientThread(
                std::jthread([_client_fd = Fd(std::move(client_fd)),
                              &tcp_manager](std::stop_token st) {
                    std::cout << "Attaching a new client thread " << _client_fd
                              << " \n";

                    while (!st.stop_requested()) {
                        try {
                            bool done = handleClient(_client_fd, tcp_manager);
                            if (done == false) { /// Client disconnected
                                break;
                            }
                        } catch (const std::exception &e) {
                            std::cerr << "Error handling client: " << e.what()
                                      << '\n';
                        }
                    }

                    shutdown(_client_fd, SHUT_RDWR);
                    std::cout << "Client thread stopped\n";
                }));
        }

    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}