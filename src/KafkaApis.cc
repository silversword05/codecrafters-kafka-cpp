#include "KafkaApis.h"
#include "MessageDefs.h"

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