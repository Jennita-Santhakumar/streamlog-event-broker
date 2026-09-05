package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrAlreadyExists = errors.New("already exists")

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgxpool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := s.pool.Exec(ctx, sql, args...)
	return err
}

func (s *Store) CreateTopic(ctx context.Context, name string, partitions, replication int, retentionMs int64) (*Topic, error) {
	var t Topic
	err := s.pool.QueryRow(ctx, `
		INSERT INTO topics (name, partitions, replication_factor, retention_ms)
		VALUES ($1, $2, $3, $4)
		RETURNING id, name, partitions, replication_factor, retention_ms, created_at
	`, name, partitions, replication, retentionMs).Scan(&t.ID, &t.Name, &t.Partitions, &t.ReplicationFactor, &t.RetentionMs, &t.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, ErrAlreadyExists
		}
		return nil, err
	}

	batch := &pgx.Batch{}
	for i := 0; i < partitions; i++ {
		batch.Queue(`INSERT INTO partitions (topic_id, partition_number) VALUES ($1, $2)`, t.ID, i)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < partitions; i++ {
		if _, err := br.Exec(); err != nil {
			return nil, err
		}
	}

	return &t, nil
}

func (s *Store) GetTopicByName(ctx context.Context, name string) (*Topic, error) {
	var t Topic
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, partitions, replication_factor, retention_ms, created_at
		FROM topics WHERE name = $1
	`, name).Scan(&t.ID, &t.Name, &t.Partitions, &t.ReplicationFactor, &t.RetentionMs, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &t, nil
}

func (s *Store) ListTopics(ctx context.Context) ([]Topic, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, partitions, replication_factor, retention_ms, created_at
		FROM topics ORDER BY created_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var topics []Topic
	for rows.Next() {
		var t Topic
		if err := rows.Scan(&t.ID, &t.Name, &t.Partitions, &t.ReplicationFactor, &t.RetentionMs, &t.CreatedAt); err != nil {
			return nil, err
		}
		topics = append(topics, t)
	}
	return topics, rows.Err()
}

func (s *Store) DeleteTopic(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM topics WHERE name = $1`, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// NextOffset returns one past the highest offset recorded for a partition,
// used to seed the in-memory offset counter on broker startup so restarts
// don't reuse offsets already durably indexed in Postgres.
func (s *Store) NextOffset(ctx context.Context, topicID string, partition int) (int64, error) {
	var max *int64
	err := s.pool.QueryRow(ctx, `
		SELECT MAX("offset") FROM message_index WHERE topic_id = $1 AND partition = $2
	`, topicID, partition).Scan(&max)
	if err != nil {
		return 0, err
	}
	if max == nil {
		return 0, nil
	}
	return *max + 1, nil
}

func (s *Store) InsertMessageIndex(ctx context.Context, topicID string, partition int, offset, segmentBase, bytePosition int64, byteLength int32, key string, headers map[string]string, timestamp int64) error {
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO message_index (topic_id, partition, "offset", segment_base_offset, byte_position, byte_length, key, headers, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, topicID, partition, offset, segmentBase, bytePosition, byteLength, key, headersJSON, timestamp)
	return err
}

func (s *Store) GetMessageIndexRange(ctx context.Context, topicID string, partition int, fromOffset int64, limit int) ([]MessageIndexRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT "offset", segment_base_offset, byte_position, byte_length, COALESCE(key, ''), headers, timestamp
		FROM message_index
		WHERE topic_id = $1 AND partition = $2 AND "offset" >= $3
		ORDER BY "offset" ASC
		LIMIT $4
	`, topicID, partition, fromOffset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []MessageIndexRow
	for rows.Next() {
		var m MessageIndexRow
		var headersJSON []byte
		if err := rows.Scan(&m.Offset, &m.SegmentBaseOffset, &m.BytePosition, &m.ByteLength, &m.Key, &headersJSON, &m.Timestamp); err != nil {
			return nil, err
		}
		if len(headersJSON) > 0 {
			_ = json.Unmarshal(headersJSON, &m.Headers)
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func (s *Store) DeleteMessageIndexForSegments(ctx context.Context, topicID string, partition int, segmentBases []int64) error {
	if len(segmentBases) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		DELETE FROM message_index WHERE topic_id = $1 AND partition = $2 AND segment_base_offset = ANY($3)
	`, topicID, partition, segmentBases)
	return err
}

func (s *Store) GetOrCreateConsumerGroup(ctx context.Context, name, topicID string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO consumer_groups (name, topic_id) VALUES ($1, $2)
		ON CONFLICT (name, topic_id) DO UPDATE SET name = EXCLUDED.name
		RETURNING id
	`, name, topicID).Scan(&id)
	return id, err
}

func (s *Store) CommitOffset(ctx context.Context, groupID string, partition int, offset int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO consumer_offsets (group_id, partition, "offset")
		VALUES ($1, $2, $3)
		ON CONFLICT (group_id, partition) DO UPDATE SET "offset" = EXCLUDED."offset", committed_at = now()
	`, groupID, partition, offset)
	return err
}

func (s *Store) GetCommittedOffset(ctx context.Context, groupID string, partition int) (int64, bool, error) {
	var offset int64
	err := s.pool.QueryRow(ctx, `
		SELECT "offset" FROM consumer_offsets WHERE group_id = $1 AND partition = $2
	`, groupID, partition).Scan(&offset)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return offset, true, nil
}

func (s *Store) InsertDLQ(ctx context.Context, topic string, partition int, offset int64, reason string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO dlq (original_topic, original_partition, original_offset, reason)
		VALUES ($1, $2, $3, $4)
	`, topic, partition, offset, reason)
	return err
}

func (s *Store) ListDLQ(ctx context.Context, limit int) ([]DLQRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, original_topic, original_partition, original_offset, reason, failed_at
		FROM dlq ORDER BY failed_at DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []DLQRow
	for rows.Next() {
		var d DLQRow
		if err := rows.Scan(&d.ID, &d.OriginalTopic, &d.OriginalPartition, &d.OriginalOffset, &d.Reason, &d.FailedAt); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

// ConsumerLag returns, for every consumer group / partition pair, the
// committed offset vs. the latest produced offset (the difference is what
// the React dashboard renders as consumer lag).
func (s *Store) ConsumerLag(ctx context.Context) ([]ConsumerLagRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT cg.name, t.name, co.partition, co.offset,
		       COALESCE((SELECT MAX(mi."offset") + 1 FROM message_index mi
		                 WHERE mi.topic_id = t.id AND mi.partition = co.partition), 0)
		FROM consumer_offsets co
		JOIN consumer_groups cg ON cg.id = co.group_id
		JOIN topics t ON t.id = cg.topic_id
		ORDER BY cg.name, co.partition
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ConsumerLagRow
	for rows.Next() {
		var r ConsumerLagRow
		if err := rows.Scan(&r.GroupName, &r.TopicName, &r.Partition, &r.CommittedOffset, &r.LatestOffset); err != nil {
			return nil, err
		}
		r.Lag = r.LatestOffset - r.CommittedOffset
		if r.Lag < 0 {
			r.Lag = 0
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
