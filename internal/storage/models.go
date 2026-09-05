package storage

import "time"

type Topic struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	Partitions        int       `json:"partitions"`
	ReplicationFactor int       `json:"replication_factor"`
	RetentionMs       int64     `json:"retention_ms"`
	CreatedAt         time.Time `json:"created_at"`
}

type MessageIndexRow struct {
	Offset            int64             `json:"offset"`
	SegmentBaseOffset int64             `json:"-"`
	BytePosition      int64             `json:"-"`
	ByteLength        int32             `json:"-"`
	Key               string            `json:"key"`
	Headers           map[string]string `json:"headers,omitempty"`
	Timestamp         int64             `json:"timestamp"`
}

type DLQRow struct {
	ID                string    `json:"id"`
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	Reason            string    `json:"reason"`
	FailedAt          time.Time `json:"failed_at"`
}

type ConsumerLagRow struct {
	GroupName       string `json:"group_name"`
	TopicName       string `json:"topic_name"`
	Partition       int    `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	LatestOffset    int64  `json:"latest_offset"`
	Lag             int64  `json:"lag"`
}
