#pragma once

#include <arpa/inet.h>
#include <bits/stdc++.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

#pragma pack(push, 1)

template <typename T>
concept CompactArrayT =
    std::integral<T> || std::same_as<T, std::array<char, 16>>;

template <CompactArrayT T> struct CompactArray {
    std::vector<T> values;

    static consteval size_t unitSize();
    size_t size() const;
    void fromBuffer(const char *buffer, size_t buffer_size);
    std::string toBuffer() const;
    std::string toString() const;
};

struct VariableInt {
    int32_t value = 0;
    size_t int_size = 0;

    void fromBuffer(const char *buffer);
    size_t size() const { return int_size; }
    operator int() const { return value; }
};

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

    struct Partition {
        uint16_t error_code{};
        uint32_t partition_id{};
        uint32_t leader_id{};
        uint32_t leader_epoch{};
        CompactArray<uint32_t> replica_array;
        CompactArray<uint32_t> in_sync_replica_array;
        CompactArray<uint32_t> eligible_leader_replica_array;
        CompactArray<uint32_t> last_known_elr_array;
        CompactArray<uint32_t> offline_replica_array;
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    struct Topic {
        int16_t error_code{};
        NullableString<1> topic_name{};
        std::array<char, 16> topic_uuid{};
        bool boolInternal = false;
        std::vector<Partition> partitions{};
        std::array<char, 4> authorizedOperations{};
        TaggedFields tagged_fields{};

        std::string toBuffer() const;
        std::string toString() const;
    };

    std::vector<Topic> topics{};
    uint8_t cursor = 0;
    TaggedFields tagged_field{};

    std::string toBuffer() const;
    std::string toString() const;
};

#pragma pack(pop)

void hexdump(const void *data, size_t size);

#pragma GCC diagnostic ignored "-Winvalid-offsetof"

#if __BIG_ENDIAN__
#define htonll(x) (x)
#define ntohll(x) (x)
#else
#define htonll(x) ((((uint64_t)htonl(x & 0xFFFFFFFF)) << 32) + htonl(x >> 32))
#define ntohll(x) ((((uint64_t)ntohl(x & 0xFFFFFFFF)) << 32) + ntohl(x >> 32))
#endif

template <std::integral T> std::string toBuffer(const T &t) {
    std::string buffer;
    buffer.resize(sizeof(T));

    if constexpr (sizeof(T) == 1) {
        *reinterpret_cast<T *>(buffer.data()) = t;
    } else if constexpr (sizeof(T) == 2) {
        *reinterpret_cast<T *>(buffer.data()) = htons(t);
    } else if constexpr (sizeof(T) == 4) {
        *reinterpret_cast<T *>(buffer.data()) = htonl(t);
    } else if constexpr (sizeof(T) == 8) {
        *reinterpret_cast<T *>(buffer.data()) = htonll(t);
    } else {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                          sizeof(T) == 8,
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
    } else if constexpr (sizeof(T) == 8) {
        return ntohll(*reinterpret_cast<const T *>(buffer));
    } else {
        static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 ||
                          sizeof(T) == 8,
                      "Unsupported size");
    }
}

template <size_t N> std::string charArrToHex(std::array<char, N> arr) {
#define HEX(x) std::setw(2) << std::setfill('0') << std::hex << (int)(x)
    std::stringstream ss;
    for (char c : arr)
        ss << HEX(c) << " ";
    std::string str = ss.str();
    return str;
#undef HEX
}

#pragma diagnostic(pop)