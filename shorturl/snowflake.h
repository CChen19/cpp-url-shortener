#ifndef SHORTURL_SNOWFLAKE_H
#define SHORTURL_SNOWFLAKE_H

#include <atomic>
#include <cstdint>
#include <mutex>

class SnowflakeIdGenerator {
public:
    // Two processes minting ids in the same second must not collide: the id
    // layout carries 10 worker-id bits between timestamp and sequence.
    // Default worker id is derived from the pid, so two instances on one host
    // disagree; a config value (snowflake.worker_id) pins it for strict
    // deployments. Values outside 0..1023 are clamped.
    static void set_worker_id(long id);

    uint64_t next_id();

private:
    static const uint64_t kEpochSeconds = 1767225600ULL; // 2026-01-01 00:00:00 UTC
    static const uint64_t kSequenceBits = 12;
    static const uint64_t kWorkerBits = 10;
    static const uint64_t kMaxSequence = (1ULL << kSequenceBits) - 1;
    static const uint64_t kMaxWorkerId = (1ULL << kWorkerBits) - 1;
    static const uint64_t kMaxTimestampDelta = (1ULL << 29) - 1;

    uint64_t current_seconds() const;
    uint64_t wait_next_second(uint64_t last_second) const;

    static std::atomic<uint64_t> s_worker_id_;

    std::mutex mutex_;
    uint64_t last_second_ = 0;
    uint64_t sequence_ = 0;
};

#endif
