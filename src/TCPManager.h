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

#pragma pack(push, 1)
struct Header {
    uint32_t message_size{};
};

template <size_t N> struct NullableString {
    using lenT = std::conditional_t<N == 1, int8_t, int16_t>;
    static_assert(N == 1 || N == 2,
                  "NullableString Len only supports 1 or 2 bytes");

    static constexpr size_t LEN_SIZE = sizeof(lenT);
    std::string value = "";

    static NullableString fromBuffer(const char *buffer, size_t buffer_size);
    std::string_view toString() const;
    std::string toBuffer() const;
    size_t size() const;
};

struct TaggedFields {
    uint8_t fieldCount = 0;
    std::string toString() const;

    std::string toBuffer() const;
    void fromBuffer(const char *buffer, size_t buffer_size);
};

struct RequestHeader : Header {
    constexpr static uint32_t MIN_HEADER_SIZE = 14;

    int16_t request_api_key{};
    int16_t request_api_version{};
    int32_t corellation_id{};
    NullableString<2> client_id{};
    TaggedFields tagged_fields{};

    static RequestHeader fromBuffer(const char *buffer, size_t buffer_size);
    void fromBufferLocal(const char *buffer, size_t buffer_size);
    std::string toString() const;
    size_t requestHeaderSize() const;
};

struct ApiVersionsRequestMessage : RequestHeader {
    static ApiVersionsRequestMessage fromBuffer(const char *buffer,
                                                size_t buffer_size);
    std::string toString() const;
};

struct DescribeTopicPartitionsRequest : RequestHeader {
    uint8_t array_length{}; // The length of the topics array + 1

    struct Topic {
        NullableString<1> topic_name{};
        TaggedFields tagged_fields{};

        static Topic fromBuffer(const char *buffer, size_t buffer_size);
        void fromBufferLocal(const char *buffer, size_t buffer_size);
        std::string toString() const;
        size_t size() const;
    };

    std::vector<Topic> topics{};
    uint32_t responsePartitionLimit = 0;
    uint8_t cursor = 0;
    TaggedFields tagged_field{};

    static DescribeTopicPartitionsRequest fromBuffer(const char *buffer,
                                                     size_t buffer_size);
    std::string toString() const;
};

constexpr size_t MAX_BUFFER_SIZE = 1024;

struct ResponseHeader : Header {
    int32_t corellation_id{};
};

struct ApiVersionsResponseMessage : ResponseHeader {
    int16_t error_code{};
    uint8_t api_keys_count{};

    struct ApiKey {
        int16_t api_key{};
        int16_t min_version{};
        int16_t max_version{};
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    ApiKey api_key1{};
    ApiKey api_key2{};

    int32_t throttle_time = 0;
    TaggedFields tagged_fields{};

    std::string toBuffer() const;
    std::string toString() const;
};

struct DescribeTopicPartitionsResponse : ResponseHeader {
    TaggedFields tagged_fields{};
    int32_t throttle_time = 0;

    uint8_t array_length{}; // The length of the topics array + 1

    struct Topic {
        int16_t error_code{};
        NullableString<1> topic_name{};
        std::array<char, 16> topic_id{};
        bool boolInternal;
        uint8_t array_length{}; // The length of the topics array + 1
        std::array<char, 4> authorizedOperations{};
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
        size_t size() const;
    };

    std::vector<Topic> topics{};
    uint8_t cursor = 0;
    TaggedFields tagged_field{};

    std::string toBuffer() const;
    std::string toString() const;
};

#pragma pack(pop)

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

struct KafkaApis {
    KafkaApis(const Fd &_client_fd, const TCPManager &_tcp_manager);
    ~KafkaApis() = default;

    static constexpr uint32_t UNSUPPORTED_VERSION = 35;
    static constexpr uint16_t UNKNOWN_TOPIC_OR_PARTITION = 3;

    static constexpr uint16_t API_VERSIONS_REQUEST = 18;
    static constexpr uint16_t DESCRIBE_TOPIC_PARTITIONS_REQUEST = 75;

    void classifyRequest(const char *buf, const size_t buf_size) const;
    void checkApiVersions(const char *buf, const size_t buf_size) const;
    void describeTopicPartitions(const char *buf, const size_t buf_size) const;

  private:
    const Fd &client_fd;
    const TCPManager &tcp_manager;
};