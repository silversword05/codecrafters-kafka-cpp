#pragma once

#include "TCPManager.h"

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