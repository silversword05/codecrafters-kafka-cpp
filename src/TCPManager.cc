#include "TCPManager.h"

#include <netinet/tcp.h>
#include <unistd.h>

namespace {
void hexdump(const void *data, size_t size) {
    const unsigned char *bytes = static_cast<const unsigned char *>(data);

    for (size_t i = 0; i < size; ++i) {
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(bytes[i]) << " ";

        if ((i + 1) % 16 == 0) {
            std::cout << std::endl;
        }
    }

    std::cout << std::endl;
}

template <std::integral T> std::string toBuffer(const T &t) {
    std::string buffer;
    buffer.resize(sizeof(T));

    if constexpr (sizeof(T) == 1) {
        *reinterpret_cast<T *>(buffer.data()) = t;
    } else if constexpr (sizeof(T) == 2) {
        *reinterpret_cast<T *>(buffer.data()) = htons(t);
    } else if constexpr (sizeof(T) == 4) {
        *reinterpret_cast<T *>(buffer.data()) = htonl(t);
    } else {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4,
                      "Unsupported size");
    }

    return buffer;
}

template <std::integral T> T fromBuffer(const char *buffer) {
    if constexpr (sizeof(T) == 1) {
        return *reinterpret_cast<const T *>(buffer);
    } else if constexpr (sizeof(T) == 2) {
        return ntohs(*reinterpret_cast<const T *>(buffer));
    } else if constexpr (sizeof(T) == 4) {
        return ntohl(*reinterpret_cast<const T *>(buffer));
    } else {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4,
                      "Unsupported size");
    }
}
} // namespace

Fd &Fd::operator=(Fd &&other) noexcept {
    if (this != &other) {
        if (fd >= 0)
            close(fd);
        fd = other.fd;
        other.fd = -1;
    }
    return *this;
}

Fd::~Fd() {
    if (fd != -1) {
        std::cout << "Closing file descriptor " << fd << "\n";
        close(fd);
    } else {
        std::cout << "File descriptor already closed " << fd << "\n";
    }
    fd = -1;
}

template <size_t N>
NullableString<N> NullableString<N>::fromBuffer(const char *buffer,
                                                size_t buffer_size) {
    if (buffer_size < LEN_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }
    NullableString<N> nullable_string;

    lenT length = ::fromBuffer<lenT>(buffer);
    if constexpr (N == 1) {
        length -= 1; /* Topic Name size + 1 */
    }

    if (length == -1) {
        return nullable_string;
    }

    nullable_string.value = std::string(buffer + sizeof(lenT), length);
    return nullable_string;
}

template <size_t N> size_t NullableString<N>::size() const {
    return LEN_SIZE + value.size();
}

template <size_t N> std::string_view NullableString<N>::toString() const {
    return value;
}

template <size_t N> std::string NullableString<N>::toBuffer() const {
    std::string buffer;
    if constexpr (N == 1) {
        buffer.append(::toBuffer<uint8_t>(value.size() + 1));
    } else {
        buffer.append(::toBuffer<uint16_t>(value.size()));
    }
    buffer.append(value);
    return buffer;
}

std::string TaggedFields::toBuffer() const {
    std::string buffer;
    buffer.append(::toBuffer(fieldCount));
    return buffer;
}

void TaggedFields::fromBuffer(const char *buffer, size_t buffer_size) {
    if (buffer_size < sizeof(fieldCount)) {
        throw std::runtime_error("Buffer size is too small");
    }

    fieldCount = ::fromBuffer<uint8_t>(buffer);
}

std::string TaggedFields::toString() const {
    return "TaggedFields{fieldCount=" + std::to_string(fieldCount) + "}";
}

void RequestHeader::fromBufferLocal(const char *buffer, size_t buffer_size) {
    if (buffer_size < RequestHeader::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

#define READ(field)                                                            \
    field = ::fromBuffer<decltype(RequestHeader::field)>(                      \
        buffer + offsetof(RequestHeader, field));

    READ(message_size);
    READ(request_api_key);
    READ(request_api_version);
    READ(corellation_id);

    client_id = NullableString<2>::fromBuffer(
        buffer + offsetof(RequestHeader, client_id),
        buffer_size - offsetof(RequestHeader, client_id));

    tagged_fields.fromBuffer(buffer + offsetof(RequestHeader, client_id) +
                                 client_id.value.size(),
                             buffer_size - offsetof(RequestHeader, client_id) -
                                 client_id.value.size());

#undef READ
}

RequestHeader RequestHeader::fromBuffer(const char *buffer,
                                        size_t buffer_size) {
    RequestHeader request_header;
    request_header.fromBufferLocal(buffer, buffer_size);
    return request_header;
}

size_t RequestHeader::requestHeaderSize() const {
    return offsetof(RequestHeader, client_id) + client_id.size() +
           sizeof(TaggedFields);
}

ApiVersionsRequestMessage
ApiVersionsRequestMessage::fromBuffer(const char *buffer, size_t buffer_size) {
    if (buffer_size < ApiVersionsRequestMessage::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

    ApiVersionsRequestMessage api_versions_request_message;
    api_versions_request_message.fromBufferLocal(buffer, buffer_size);
    return api_versions_request_message;
}

void DescribeTopicPartitionsRequest::Topic::fromBufferLocal(
    const char *buffer, size_t buffer_size) {
    if (buffer_size < NullableString<1>::LEN_SIZE + sizeof(TaggedFields)) {
        throw std::runtime_error("Buffer size is too small");
    }

    topic_name = NullableString<1>::fromBuffer(buffer, buffer_size);
    tagged_fields.fromBuffer(buffer + topic_name.size(),
                             buffer_size - topic_name.size());
}

DescribeTopicPartitionsRequest::Topic
DescribeTopicPartitionsRequest::Topic::fromBuffer(const char *buffer,
                                                  size_t buffer_size) {
    Topic topic;
    topic.fromBufferLocal(buffer, buffer_size);
    return topic;
}

size_t DescribeTopicPartitionsRequest::Topic::size() const {
    return topic_name.size() + sizeof(TaggedFields);
}

DescribeTopicPartitionsRequest
DescribeTopicPartitionsRequest::fromBuffer(const char *buffer,
                                           size_t buffer_size) {
    if (buffer_size < DescribeTopicPartitionsRequest::MIN_HEADER_SIZE) {
        throw std::runtime_error("Buffer size is too small");
    }

    auto bufCpy = buffer;

    DescribeTopicPartitionsRequest describe_topic_partitions_request;
    describe_topic_partitions_request.fromBufferLocal(buffer, buffer_size);
    buffer = buffer + describe_topic_partitions_request.requestHeaderSize();
    buffer_size =
        buffer_size - describe_topic_partitions_request.requestHeaderSize();

    describe_topic_partitions_request.array_length =
        ::fromBuffer<uint8_t>(buffer);
    buffer = buffer + sizeof(uint8_t);
    buffer_size = buffer_size - sizeof(uint8_t);

    for (size_t i = 0; i < describe_topic_partitions_request.array_length - 1;
         ++i) {
        DescribeTopicPartitionsRequest::Topic topic =
            DescribeTopicPartitionsRequest::Topic::fromBuffer(buffer,
                                                              buffer_size);
        describe_topic_partitions_request.topics.push_back(topic);
        buffer = buffer + topic.size();
        buffer_size = buffer_size - topic.size();
    }

    describe_topic_partitions_request.responsePartitionLimit =
        ::fromBuffer<uint32_t>(buffer);
    buffer = buffer + sizeof(uint32_t);
    buffer_size = buffer_size - sizeof(uint32_t);

    describe_topic_partitions_request.cursor = ::fromBuffer<uint8_t>(buffer);
    buffer = buffer + sizeof(uint8_t);
    buffer_size = buffer_size - sizeof(uint8_t);

    describe_topic_partitions_request.tagged_fields.fromBuffer(buffer,
                                                               buffer_size);
    return describe_topic_partitions_request;
}

#pragma diagnostic(pop)

std::string DescribeTopicPartitionsRequest::Topic::toString() const {
    return "Topic{topic_name=" + std::string(topic_name.toString()) +
           ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsRequest::toString() const {
    std::string topics_str = "Topics{";
    for (const auto &topic : topics) {
        topics_str += topic.toString() + ", ";
    }
    topics_str += "}";

    return "DescribeTopicPartitionsRequest{" + RequestHeader::toString() +
           ", array_length=" + std::to_string(array_length) +
           ", topics=" + topics_str + ", responsePartitionLimit=" +
           std::to_string(responsePartitionLimit) +
           ", cursor=" + std::to_string(cursor) +
           ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string RequestHeader::toString() const {
    return "RequestHeader{message_size=" + std::to_string(message_size) +
           ", request_api_key=" + std::to_string(request_api_key) +
           ", request_api_version=" + std::to_string(request_api_version) +
           ", corellation_id=" + std::to_string(corellation_id) +
           ", client_id=" + std::string(client_id.toString()) + "}";
}

std::string ApiVersionsRequestMessage::toString() const {
    return "ApiVersionsRequestMessage{" + RequestHeader::toString() + "}";
}

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

std::string ApiVersionsResponseMessage::ApiKey::toBuffer() const {
    std::string buffer;

    buffer.append(::toBuffer(api_key));
    buffer.append(::toBuffer(min_version));
    buffer.append(::toBuffer(max_version));
    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

std::string ApiVersionsResponseMessage::ApiKey::toString() const {
    return "ApiKey{api_key=" + std::to_string(api_key) +
           ", min_version=" + std::to_string(min_version) +
           ", max_version=" + std::to_string(max_version) +
           ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string ApiVersionsResponseMessage::toBuffer() const {
    std::string buffer = "XXXX"; // These are 4 bytes for message_size

    buffer.append(::toBuffer(corellation_id));
    buffer.append(::toBuffer(error_code));
    buffer.append(::toBuffer(api_keys_count));

    buffer.append(api_key1.toBuffer());
    buffer.append(api_key2.toBuffer());

    buffer.append(::toBuffer(throttle_time));
    buffer.append(tagged_fields.toBuffer());

    // Update the message size
    *reinterpret_cast<uint32_t *>(buffer.data()) = htonl(buffer.size() - 4);

    return buffer;
}

std::string DescribeTopicPartitionsResponse::Topic::toBuffer() const {
    std::string buffer;
    buffer.append(::toBuffer(error_code));

    buffer.append(topic_name.toBuffer());
    buffer.append(topic_id.data(), topic_id.size());

    std::cout << "buffer size before bool internal: " << buffer.size()
              << std::endl;

    buffer.append(::toBuffer<uint8_t>((boolInternal) ? 1 : 0));
    buffer.append(::toBuffer(array_length));

    buffer.append(authorizedOperations.data(), authorizedOperations.size());
    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

size_t DescribeTopicPartitionsResponse::Topic::size() const {
    return sizeof(error_code) + topic_name.size() + topic_id.size() +
           sizeof(boolInternal) + sizeof(array_length) +
           authorizedOperations.size() + sizeof(tagged_field.fieldCount);
}

std::string DescribeTopicPartitionsResponse::toBuffer() const {
    std::string buffer = "XXXX"; // These are 4 bytes for message_size

    buffer.append(::toBuffer(corellation_id));
    buffer.append(tagged_fields.toBuffer());
    buffer.append(::toBuffer(throttle_time));
    buffer.append(::toBuffer(array_length));

    for (const auto &topic : topics) {
        buffer.append(topic.toBuffer());
    }

    buffer.append(::toBuffer(cursor));
    buffer.append(tagged_field.toBuffer());

    // Update the message size
    *reinterpret_cast<uint32_t *>(buffer.data()) = htonl(buffer.size() - 4);

    return buffer;
}

#pragma diagnostic(pop)

std::string ApiVersionsResponseMessage::toString() const {
    return "ApiVersionsResponseMessage{message_size=" +
           std::to_string(message_size) +
           ", corellation_id=" + std::to_string(corellation_id) +
           ", error_code=" + std::to_string(error_code) +
           ", api_keys_count=" + std::to_string(api_keys_count) +
           ", api_key1=" + api_key1.toString() +
           ", api_key2=" + api_key2.toString() +
           ", throttle_time=" + std::to_string(throttle_time) +
           ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsResponse::Topic::toString() const {
    return "Topic{error_code=" + std::to_string(error_code) +
           ", topic_name=" + std::string(topic_name.toString()) +
           ", boolInternal=" + std::to_string(boolInternal) +
           ", array_length=" + std::to_string(array_length) +
           ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsResponse::toString() const {
    std::string topics_str = "Topics{";
    for (const auto &topic : topics) {
        topics_str += topic.toString() + ", ";
    }
    topics_str += "}";

    return "DescribeTopicPartitionsResponse{message_size=" +
           std::to_string(message_size) +
           ", corellation_id=" + std::to_string(corellation_id) +
           ", tagged_fields=" + tagged_fields.toString() +
           ", throttle_time=" + std::to_string(throttle_time) +
           ", array_length=" + std::to_string(array_length) +
           ", topics=" + topics_str + ", cursor=" + std::to_string(cursor) +
           ", tagged_fields=" + tagged_field.toString() + "}";
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

void TCPManager::writeBufferOnClientFd(const Fd &client_fd,
                                       const auto &response_message) const {

    std::cout << "Sending msg to client: " << response_message.toString()
              << "\n";

    std::string buffer = response_message.toBuffer();

    // Write message Length
    if (send(client_fd, buffer.data(), sizeof(uint32_t), 0) !=
        sizeof(uint32_t)) {
        perror("send failed: ");
        throw std::runtime_error("Failed to send msgLen to client: ");
    }

    if (send(client_fd, buffer.data() + 4, buffer.size() - 4, 0) !=
        buffer.size() - 4) {
        perror("send failed: ");
        throw std::runtime_error("Failed to send msgLen to client: ");
    }

    std::cout << "Message sent to client: " << buffer.size() << " bytes\n";

    // Just flush the write buffer
    int optval = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
}

bool TCPManager::readBufferFromClientFd(
    const Fd &client_fd,
    const std::function<void(const char *, const size_t)> &func) const {
    char buffer[MAX_BUFFER_SIZE];
    size_t bytes_received = recv(client_fd, buffer, MAX_BUFFER_SIZE, 0);

    if (bytes_received < 0) {
        perror("recv failed: ");
        throw std::runtime_error("Failed to read from client: ");
    }

    if (bytes_received == 0) {
        std::cout << "Client disconnected\n";
        return false;
    }

    std::cout << "Received " << bytes_received << " bytes from client\n";
    func(buffer, bytes_received);
    return true;
}

KafkaApis::KafkaApis(const Fd &_client_fd, const TCPManager &_tcp_manager)
    : client_fd(_client_fd), tcp_manager(_tcp_manager) {}

void KafkaApis::classifyRequest(const char *buf, const size_t buf_size) const {
    RequestHeader request_header = RequestHeader::fromBuffer(buf, buf_size);

    std::cout << "Received Request: " << request_header.toString() << "\n";

    switch (request_header.request_api_key) {
    case API_VERSIONS_REQUEST:
        checkApiVersions(buf, buf_size);
        break;
    case DESCRIBE_TOPIC_PARTITIONS_REQUEST:
        describeTopicPartitions(buf, buf_size);
        break;
    default:
        std::cout << "Unsupported API key: " << request_header.request_api_key
                  << "\n";
    }
}

void KafkaApis::checkApiVersions(const char *buf, const size_t buf_size) const {
    ApiVersionsRequestMessage request_message =
        ApiVersionsRequestMessage::fromBuffer(buf, buf_size);

    std::cout << "Received API Versions Request: " << request_message.toString()
              << "\n";

    ApiVersionsResponseMessage api_versions_response_message;
    api_versions_response_message.corellation_id =
        request_message.corellation_id;

    if (request_message.request_api_version < 0 ||
        request_message.request_api_version > 4) {
        api_versions_response_message.error_code = UNSUPPORTED_VERSION;
        std::cout << "Unsupported version: "
                  << request_message.request_api_version << "\n";
    } else {
        std::cout << "Supported version: "
                  << request_message.request_api_version << "\n";
        api_versions_response_message.api_keys_count = 3;

        api_versions_response_message.api_key1.api_key = API_VERSIONS_REQUEST;
        api_versions_response_message.api_key1.min_version = 3;
        api_versions_response_message.api_key1.max_version = 4;

        api_versions_response_message.api_key2.api_key =
            DESCRIBE_TOPIC_PARTITIONS_REQUEST;
        api_versions_response_message.api_key2.min_version = 0;
        api_versions_response_message.api_key2.max_version = 0;
    }

    tcp_manager.writeBufferOnClientFd(client_fd, api_versions_response_message);
}

void KafkaApis::describeTopicPartitions(const char *buf,
                                        const size_t buf_size) const {
    DescribeTopicPartitionsRequest request_message =
        DescribeTopicPartitionsRequest::fromBuffer(buf, buf_size);

    std::cout << "Received Describe Topic Partitions Request: "
              << request_message.toString() << "\n";

    DescribeTopicPartitionsResponse describe_topic_partitions_response;

    describe_topic_partitions_response.corellation_id =
        request_message.corellation_id;
    describe_topic_partitions_response.array_length =
        request_message.topics.size() + 1;

    for (const auto &topic : request_message.topics) {
        DescribeTopicPartitionsResponse::Topic topic1;
        topic1.error_code = UNKNOWN_TOPIC_OR_PARTITION;
        topic1.topic_name = topic.topic_name;
        topic1.array_length = 1; // Empty partitions array
        topic1.authorizedOperations = {0, 0, 0x0d, char(0xf8)};
        topic1.tagged_fields = topic.tagged_fields;

        describe_topic_partitions_response.topics.push_back(topic1);
    }

    describe_topic_partitions_response.cursor = request_message.cursor;
    describe_topic_partitions_response.tagged_field =
        request_message.tagged_fields;

    tcp_manager.writeBufferOnClientFd(client_fd,
                                      describe_topic_partitions_response);
}