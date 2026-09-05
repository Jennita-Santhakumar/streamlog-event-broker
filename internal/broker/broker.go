package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/storage"
)

var (
	ErrTopicNotFound       = errors.New("topic not found")
	ErrTopicAlreadyExists  = errors.New("topic already exists")
	ErrPartitionOutOfRange = errors.New("partition out of range")
)

type Message struct {
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp int64             `json:"timestamp"`
}

type Broker struct {
	store           *storage.Store
	dataDir         string
	maxSegmentBytes int64
	brokerID        int

	mu     sync.RWMutex
	topics map[string]*topicRuntime
}

func NewBroker(store *storage.Store, dataDir string, maxSegmentBytes int64, brokerID int) *Broker {
	return &Broker{
		store:           store,
		dataDir:         dataDir,
		maxSegmentBytes: maxSegmentBytes,
		brokerID:        brokerID,
		topics:          make(map[string]*topicRuntime),
	}
}

// LoadExisting rebuilds in-memory partition logs and offset counters for
// every topic already recorded in Postgres. Called once at startup so a
// restarted broker picks up exactly where it left off instead of losing
// track of on-disk segments (the PRD's own in-memory reference broker loses
// everything on restart — this is the fix for that).
func (b *Broker) LoadExisting(ctx context.Context) error {
	topics, err := b.store.ListTopics(ctx)
	if err != nil {
		return err
	}
	for _, t := range topics {
		if err := b.attachTopic(ctx, t); err != nil {
			return fmt.Errorf("loading topic %s: %w", t.Name, err)
		}
	}
	return nil
}

func (b *Broker) attachTopic(ctx context.Context, meta storage.Topic) error {
	rt := &topicRuntime{
		meta:           meta,
		partitions:     make([]*Partition, meta.Partitions),
		partitionLocks: make([]sync.Mutex, meta.Partitions),
		nextOffsets:    make([]int64, meta.Partitions),
	}

	for i := 0; i < meta.Partitions; i++ {
		dir := partitionDir(b.dataDir, meta.ID, i)
		p, err := OpenPartition(dir, b.maxSegmentBytes)
		if err != nil {
			return err
		}
		rt.partitions[i] = p

		next, err := b.store.NextOffset(ctx, meta.ID, i)
		if err != nil {
			return err
		}
		rt.nextOffsets[i] = next
	}

	b.mu.Lock()
	b.topics[meta.Name] = rt
	b.mu.Unlock()
	return nil
}

func (b *Broker) CreateTopic(ctx context.Context, name string, partitions, replication int, retentionMs int64) (*storage.Topic, error) {
	t, err := b.store.CreateTopic(ctx, name, partitions, replication, retentionMs)
	if err != nil {
		if errors.Is(err, storage.ErrAlreadyExists) {
			return nil, ErrTopicAlreadyExists
		}
		return nil, err
	}
	if err := b.attachTopic(ctx, *t); err != nil {
		return nil, err
	}
	return t, nil
}

func (b *Broker) ListTopics(ctx context.Context) ([]storage.Topic, error) {
	return b.store.ListTopics(ctx)
}

func (b *Broker) DeleteTopic(ctx context.Context, name string) error {
	b.mu.Lock()
	rt, ok := b.topics[name]
	if ok {
		delete(b.topics, name)
	}
	b.mu.Unlock()

	if err := b.store.DeleteTopic(ctx, name); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return ErrTopicNotFound
		}
		return err
	}

	if ok {
		for _, p := range rt.partitions {
			p.Close()
		}
		_ = os.RemoveAll(filepath.Join(b.dataDir, rt.meta.ID))
	}
	return nil
}

func (b *Broker) getTopic(name string) (*topicRuntime, error) {
	b.mu.RLock()
	rt, ok := b.topics[name]
	b.mu.RUnlock()
	if !ok {
		return nil, ErrTopicNotFound
	}
	return rt, nil
}

// Publish assigns the next offset for the key's partition, appends the
// message bytes to that partition's on-disk segment log, and durably
// indexes the (offset -> segment, byte position) mapping in Postgres. The
// per-partition lock spans the whole sequence so a failed Postgres insert
// can safely roll back the in-memory offset counter without racing another
// publish to the same partition.
func (b *Broker) Publish(ctx context.Context, topicName, key string, value []byte, headers map[string]string) (partition int, offset int64, err error) {
	rt, err := b.getTopic(topicName)
	if err != nil {
		return 0, 0, err
	}

	partition = hashPartition(key, len(rt.partitions))

	rt.partitionLocks[partition].Lock()
	defer rt.partitionLocks[partition].Unlock()

	offset = rt.reserveOffset(partition)
	timestamp := time.Now().UnixMilli()

	segmentBase, position, err := rt.partitions[partition].Append(offset, value)
	if err != nil {
		rt.rollbackOffset(partition)
		return 0, 0, err
	}

	if err := b.store.InsertMessageIndex(ctx, rt.meta.ID, partition, offset, segmentBase, position, int32(len(value)), key, headers, timestamp); err != nil {
		rt.rollbackOffset(partition)
		return 0, 0, err
	}

	return partition, offset, nil
}

// Consume returns messages from partition starting at fromOffset, stopping
// once maxBytes worth of message values has been accumulated (matches the
// PRD's pull-based consumer API semantics).
func (b *Broker) Consume(ctx context.Context, topicName string, partition int, fromOffset int64, maxBytes int) ([]Message, error) {
	rt, err := b.getTopic(topicName)
	if err != nil {
		return nil, err
	}
	if partition < 0 || partition >= len(rt.partitions) {
		return nil, ErrPartitionOutOfRange
	}

	rows, err := b.store.GetMessageIndexRange(ctx, rt.meta.ID, partition, fromOffset, 1000)
	if err != nil {
		return nil, err
	}

	var (
		messages []Message
		size     int
	)
	for _, row := range rows {
		if size > 0 && size+int(row.ByteLength) > maxBytes {
			break
		}
		value, err := rt.partitions[partition].ReadRecord(row.SegmentBaseOffset, row.BytePosition, row.ByteLength)
		if err != nil {
			return nil, err
		}
		messages = append(messages, Message{
			Offset:    row.Offset,
			Key:       row.Key,
			Value:     value,
			Headers:   row.Headers,
			Timestamp: row.Timestamp,
		})
		size += int(row.ByteLength)
	}
	return messages, nil
}

func (b *Broker) LatestOffset(topicName string, partition int) (int64, error) {
	rt, err := b.getTopic(topicName)
	if err != nil {
		return 0, err
	}
	if partition < 0 || partition >= len(rt.partitions) {
		return 0, ErrPartitionOutOfRange
	}
	rt.partitionLocks[partition].Lock()
	defer rt.partitionLocks[partition].Unlock()
	return rt.peekNextOffset(partition), nil
}

func (b *Broker) PartitionCount(topicName string) (int, error) {
	rt, err := b.getTopic(topicName)
	if err != nil {
		return 0, err
	}
	return len(rt.partitions), nil
}

// CommitOffset records that groupName has fully processed messages up to
// and including lastProcessedOffset on partition. Internally this stores
// lastProcessedOffset+1 (the next offset to read), which is exactly what
// CommittedOffset returns — so a caller never has to remember to add one
// themselves at the call site.
func (b *Broker) CommitOffset(ctx context.Context, groupName, topicName string, partition int, lastProcessedOffset int64) error {
	rt, err := b.getTopic(topicName)
	if err != nil {
		return err
	}
	groupID, err := b.store.GetOrCreateConsumerGroup(ctx, groupName, rt.meta.ID)
	if err != nil {
		return err
	}
	return b.store.CommitOffset(ctx, groupID, partition, lastProcessedOffset+1)
}

func (b *Broker) CommittedOffset(ctx context.Context, groupName, topicName string, partition int) (int64, bool, error) {
	rt, err := b.getTopic(topicName)
	if err != nil {
		return 0, false, err
	}
	groupID, err := b.store.GetOrCreateConsumerGroup(ctx, groupName, rt.meta.ID)
	if err != nil {
		return 0, false, err
	}
	return b.store.GetCommittedOffset(ctx, groupID, partition)
}

func (b *Broker) ReportDLQ(ctx context.Context, topicName string, partition int, offset int64, reason string) error {
	if _, err := b.getTopic(topicName); err != nil {
		return err
	}
	return b.store.InsertDLQ(ctx, topicName, partition, offset, reason)
}

func (b *Broker) ListDLQ(ctx context.Context, limit int) ([]storage.DLQRow, error) {
	return b.store.ListDLQ(ctx, limit)
}

func (b *Broker) ConsumerLag(ctx context.Context) ([]storage.ConsumerLagRow, error) {
	return b.store.ConsumerLag(ctx)
}

// RunRetentionLoop periodically deletes segments (and their Postgres index
// rows) that have aged past each topic's retention_ms, matching the PRD's
// "message retention with time-based cleanup" feature.
func (b *Broker) RunRetentionLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.sweepRetention(ctx)
		}
	}
}

func (b *Broker) sweepRetention(ctx context.Context) {
	b.mu.RLock()
	topics := make([]*topicRuntime, 0, len(b.topics))
	for _, rt := range b.topics {
		topics = append(topics, rt)
	}
	b.mu.RUnlock()

	now := time.Now().UnixMilli()
	for _, rt := range topics {
		cutoff := now - rt.meta.RetentionMs
		for i, p := range rt.partitions {
			removed, err := p.SweepExpiredSegments(cutoff)
			if err != nil || len(removed) == 0 {
				continue
			}
			_ = b.store.DeleteMessageIndexForSegments(ctx, rt.meta.ID, i, removed)
		}
	}
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, rt := range b.topics {
		for _, p := range rt.partitions {
			p.Close()
		}
	}
}
