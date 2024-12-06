#pragma once

#include "MessageDefs.h"
#include "TCPManager.h"

#pragma pack(push, 1)

struct Key {
    VariableInt length;
    std::string key;

    size_t size() const;
    void fromBuffer(const char *buffer, size_t buffer_size);
    std::string toString() const;
};

struct Value {
    static constexpr uint8_t PARTITION_RECORD = 3;
    static constexpr uint8_t TOPIC_RECORD = 2;

    struct PartitionRecord {
        uint32_t partition_id{};
        std::array<char, 16> topic_uuid{};
        CompactArray<uint32_t> replica_array;
        CompactArray<uint32_t> in_sync_replica_array;
        CompactArray<uint32_t> removing_replica_array;
        CompactArray<uint32_t> adding_replica_array;
        uint32_t leader_id{};
        uint32_t leader_epoch{};
        uint32_t partition_epoch{};
        CompactArray<std::array<char, 16>> directories_array;
        uint8_t tagged_fields_count{};

        PartitionRecord() noexcept = default;
        size_t size() const;
        void fromBuffer(const char *buffer, size_t buffer_size);
        std::string toString() const;
    };

    struct TopicRecord {
        NullableString<1> topic_name;
        std::array<char, 16> topic_uuid;
        uint8_t tagged_fields_count{};

        TopicRecord() noexcept = default;
        size_t size() const;
        void fromBuffer(const char *buffer, size_t buffer_size);
        std::string toString() const;
    };

    struct NoRecord {
        NoRecord() noexcept = default;
        std::string toString() const { return "NoRecord{}"; }
    };

    using RecordT = std::variant<NoRecord, PartitionRecord, TopicRecord>;

    VariableInt length{};
    uint8_t frame_version{};
    uint8_t record_type{};
    uint8_t record_version{};
    RecordT record;

    size_t size() const;
    void fromBuffer(const char *buffer, size_t buffer_size);
    std::string toString() const;
};

struct Record {
    VariableInt length{};
    uint8_t attributes{};
    int8_t timestamp_delta{};
    int8_t offset_delta{};
    Key key;
    Value value;
    uint8_t headers_count{};

    size_t size() const;
    void fromBuffer(const char *buffer, size_t buffer_size);
    std::string toString() const;
};

struct RecordBatch {
    uint64_t base_offset{};
    uint32_t batch_length{};
    uint32_t partition_leader_epoch{};
    uint8_t magic{};
    uint32_t crc{};
    uint16_t attributes{};
    uint32_t last_offset_delta{};
    uint64_t base_timestamp{};
    uint64_t max_timestamp{};
    int64_t producer_id{};
    int16_t producer_epoch{};
    int32_t base_sequence{};
    std::vector<Record> records;

    size_t size() const;
    void fromBuffer(const char *buffer, size_t buffer_size);
    std::string toString() const;
};

#pragma pack(pop)

struct ClusterMetadata {
    ClusterMetadata() { readClusterMetadata(); }

    std::unordered_map<std::string, std::array<char, 16>> topic_name_uuid_map;
    std::map<std::array<char, 16>, std::vector<Value::PartitionRecord>>
        topic_uuid_partition_id_map;

    static inline const std::string medata_file =
        "/tmp/kraft-combined-logs/__cluster_metadata-0/"
        "00000000000000000000.log";

  private:
    void readClusterMetadata();
    void waitForFileToExist() const;
};

struct KafkaApis {
    KafkaApis(const Fd &, const TCPManager &, const ClusterMetadata &);
    ~KafkaApis() = default;

    static constexpr uint32_t UNSUPPORTED_VERSION = 35;
    static constexpr uint16_t UNKNOWN_TOPIC_OR_PARTITION = 3;
    static constexpr uint16_t NO_ERROR = 0;
    static constexpr uint16_t UNKNOWN_TOPIC = 100;

    static constexpr uint16_t API_VERSIONS_REQUEST = 18;
    static constexpr uint16_t DESCRIBE_TOPIC_PARTITIONS_REQUEST = 75;
    static constexpr uint16_t FETCH_REQUEST = 1;

    void classifyRequest(const char *buf, const size_t buf_size) const;
    void checkApiVersions(const char *buf, const size_t buf_size) const;
    void describeTopicPartitions(const char *buf, const size_t buf_size) const;
    void fetchTopicMessages(const char *buf, const size_t buf_size) const;

  private:
    const Fd &client_fd;
    const TCPManager &tcp_manager;
    const ClusterMetadata &cluster_metadata;
};