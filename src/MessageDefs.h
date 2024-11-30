#pragma once

#include <arpa/inet.h>
#include <bits/stdc++.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

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