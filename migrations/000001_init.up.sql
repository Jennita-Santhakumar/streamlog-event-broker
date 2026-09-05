CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE topics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    partitions INT NOT NULL DEFAULT 3,
    replication_factor INT NOT NULL DEFAULT 3,
    retention_ms BIGINT NOT NULL DEFAULT 86400000,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Physical partition metadata. leader_broker/replicas/isr are recorded to match
-- the PRD's schema, but this build is intentionally single-broker (see README
-- "Design Decisions & Trade-offs"), so every partition's leader is broker_id=1
-- and replicas/isr are always [1].
CREATE TABLE partitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_number INT NOT NULL,
    leader_broker INT NOT NULL DEFAULT 1,
    replicas INT[] NOT NULL DEFAULT ARRAY[1],
    isr INT[] NOT NULL DEFAULT ARRAY[1],
    UNIQUE (topic_id, partition_number)
);

-- Message bytes live in on-disk segment log files, not this table (see
-- internal/broker/segment.go). This table indexes where each message's bytes
-- physically live so a consumer read-by-offset can locate the right segment
-- file + byte position without scanning the log.
CREATE TABLE message_index (
    id BIGSERIAL PRIMARY KEY,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition INT NOT NULL,
    "offset" BIGINT NOT NULL,
    segment_base_offset BIGINT NOT NULL,
    byte_position BIGINT NOT NULL,
    byte_length INT NOT NULL,
    key VARCHAR(255),
    headers JSONB,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_id, partition, "offset")
);

CREATE INDEX idx_message_index_topic_partition ON message_index(topic_id, partition, "offset");

CREATE TABLE consumer_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, topic_id)
);

CREATE TABLE consumer_offsets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id UUID NOT NULL REFERENCES consumer_groups(id) ON DELETE CASCADE,
    partition INT NOT NULL,
    "offset" BIGINT NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (group_id, partition)
);

CREATE INDEX idx_offsets_group_partition ON consumer_offsets(group_id, partition);

CREATE TABLE dlq (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_topic VARCHAR(255) NOT NULL,
    original_partition INT NOT NULL,
    original_offset BIGINT NOT NULL,
    reason TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
