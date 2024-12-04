#include "MessageDefs.h"

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

class IosFlagSaver {
  public:
    explicit IosFlagSaver(std::ostream &_ios) : ios(_ios), f(_ios.flags()) {}
    ~IosFlagSaver() { ios.flags(f); }

    IosFlagSaver(const IosFlagSaver &rhs) = delete;
    IosFlagSaver &operator=(const IosFlagSaver &rhs) = delete;

  private:
    std::ostream &ios;
    std::ios::fmtflags f;
};

void hexdump(const void *data, size_t size) {
    IosFlagSaver iosfs(std::cout);
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

template <CompactArrayT T> consteval size_t CompactArray<T>::unitSize() {
    if constexpr (std::is_integral_v<T>) {
        return sizeof(T);
    } else {
        static_assert(std::same_as<T, std::array<char, 16>>,
                      "Unsupported type");
        return std::size(T());
    }
}

template <CompactArrayT T> size_t CompactArray<T>::size() const {
    return sizeof(uint8_t) + CompactArray<T>::unitSize() * values.size();
}

template <CompactArrayT T>
void CompactArray<T>::fromBuffer(const char *buffer, size_t buffer_size) {
    if (buffer_size < sizeof(uint8_t)) {
        throw std::runtime_error("Buffer size is too small");
    }

    uint8_t array_length = ::fromBuffer<uint8_t>(buffer);
    buffer += sizeof(uint8_t);
    buffer_size -= sizeof(uint8_t);

    for (uint8_t i = 0; i < array_length - 1; ++i) {
        if constexpr (std::is_integral_v<T>) {
            values.push_back(::fromBuffer<T>(buffer));
        } else {
            static_assert(std::same_as<T, std::array<char, 16>>,
                          "Unsupported type");
            T value;
            std::copy_n(buffer, value.size(), value.begin());
            values.push_back(value);
        }

        buffer += unitSize();
        buffer_size -= unitSize();
    }
}

template <CompactArrayT T> std::string CompactArray<T>::toBuffer() const {
    std::string buffer;
    buffer.append(::toBuffer<uint8_t>(values.size() + 1));

    for (const auto &value : values) {
        if constexpr (std::is_integral_v<T>) {
            buffer.append(::toBuffer(value));
        } else {
            static_assert(std::same_as<T, std::array<char, 16>>,
                          "Unsupported type");
            buffer.append(value.data(), value.size());
        }
    }
    return buffer;
}

auto zigzagDecode(auto x) { return (x >> 1) ^ (-(x & 1)); }

void VariableInt::fromBuffer(const char *buffer) {
    int shift = 0;

    while (true) {
        uint8_t byte = *buffer;
        buffer++;
        int_size++;
        value |= (int32_t)(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            break;
        }
        shift += 7;
    }

    value = zigzagDecode(value);
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
        buffer.append(::toBuffer<lenT>(value.size() + 1));
    } else {
        buffer.append(::toBuffer<lenT>(value.size()));
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

std::string DescribeTopicPartitionsResponse::Partition::toBuffer() const {
    std::string buffer;

    buffer.append(::toBuffer(error_code));
    buffer.append(::toBuffer(partition_id));
    buffer.append(::toBuffer(leader_id));
    buffer.append(::toBuffer(leader_epoch));

    buffer.append(replica_array.toBuffer());
    buffer.append(in_sync_replica_array.toBuffer());
    buffer.append(eligible_leader_replica_array.toBuffer());
    buffer.append(last_known_elr_array.toBuffer());
    buffer.append(offline_replica_array.toBuffer());

    buffer.append(tagged_fields.toBuffer());
    return buffer;
}

std::string DescribeTopicPartitionsResponse::Topic::toBuffer() const {
    std::string buffer;
    buffer.append(::toBuffer(error_code));

    buffer.append(topic_name.toBuffer());
    buffer.append(topic_uuid.data(), topic_uuid.size());

    buffer.append(::toBuffer<uint8_t>((boolInternal) ? 1 : 0));
    buffer.append(::toBuffer<uint8_t>(partitions.size() + 1));

    for (const auto &partition : partitions) {
        buffer.append(partition.toBuffer());
    }

    buffer.append(authorizedOperations.data(), authorizedOperations.size());
    buffer.append(tagged_fields.toBuffer());

    return buffer;
}

std::string DescribeTopicPartitionsResponse::toBuffer() const {
    std::string buffer = "XXXX"; // These are 4 bytes for message_size

    buffer.append(::toBuffer(corellation_id));
    buffer.append(tagged_fields.toBuffer());
    buffer.append(::toBuffer(throttle_time));
    buffer.append(::toBuffer<uint8_t>(topics.size() + 1));

    for (const auto &topic : topics) {
        buffer.append(topic.toBuffer());
    }

    buffer.append(::toBuffer(cursor));
    buffer.append(tagged_field.toBuffer());

    // Update the message size
    *reinterpret_cast<uint32_t *>(buffer.data()) = htonl(buffer.size() - 4);

    return buffer;
}

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

std::string DescribeTopicPartitionsResponse::Partition::toString() const {
    return "Partition{error_code=" + std::to_string(error_code) +
           ", partition_id=" + std::to_string(partition_id) +
           ", leader_id=" + std::to_string(leader_id) +
           ", leader_epoch=" + std::to_string(leader_epoch) +
           ", replica_array=" + replica_array.toString() +
           ", in_sync_replica_array=" + in_sync_replica_array.toString() +
           ", eligible_leader_replica_array=" +
           eligible_leader_replica_array.toString() +
           ", last_known_elr_array=" + last_known_elr_array.toString() +
           ", offline_replica_array=" + offline_replica_array.toString() +
           ", tagged_fields=" + tagged_fields.toString() + "}";
}

std::string DescribeTopicPartitionsResponse::Topic::toString() const {
    std::string partitions_str = "Partitions{";
    for (const auto &partition : partitions) {
        partitions_str += partition.toString() + ", ";
    }
    partitions_str += "}";

    return "Topic{error_code=" + std::to_string(error_code) +
           ", topic_name=" + std::string(topic_name.toString()) +
           ", topic_id=" + charArrToHex(topic_uuid) +
           ", boolInternal=" + std::to_string(boolInternal) +
           ", array_length=" + std::to_string(partitions.size() + 1) +
           ", partitions=" + partitions_str +
           ", authorizedOperations=" + charArrToHex(authorizedOperations) +
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
           ", array_length=" + std::to_string(topics.size() + 1) +
           ", topics=" + topics_str + ", cursor=" + std::to_string(cursor) +
           ", tagged_fields=" + tagged_field.toString() + "}";
}

#pragma diagnostic(pop)

template struct NullableString<1>;
template struct NullableString<2>;
template struct CompactArray<uint32_t>;
template struct CompactArray<std::array<char, 16>>;