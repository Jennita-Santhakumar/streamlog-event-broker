package broker

import (
	"hash/fnv"
	"sync"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/storage"
)

// topicRuntime holds everything needed to serve reads/writes for one topic
// without hitting Postgres on every call: open partition logs and an
// in-memory next-offset counter per partition (seeded from Postgres at
// startup via Store.NextOffset, kept authoritative in memory afterwards).
type topicRuntime struct {
	meta       storage.Topic
	partitions []*Partition

	// One lock per partition serializes the "assign offset -> append to
	// segment -> index in Postgres" sequence for that partition, which is
	// what keeps offsets gapless and crash-safe without needing a
	// distributed transaction across the filesystem and Postgres.
	partitionLocks []sync.Mutex
	nextOffsets    []int64
}

// reserveOffset, rollbackOffset and peekNextOffset all assume the caller
// already holds partitionLocks[partition] for the duration of the
// reserve -> append -> index sequence — that lock, not atomics, is what
// keeps offsets gapless when a Postgres index insert fails after the offset
// was reserved.
func (t *topicRuntime) reserveOffset(partition int) int64 {
	offset := t.nextOffsets[partition]
	t.nextOffsets[partition]++
	return offset
}

func (t *topicRuntime) rollbackOffset(partition int) {
	t.nextOffsets[partition]--
}

func (t *topicRuntime) peekNextOffset(partition int) int64 {
	return t.nextOffsets[partition]
}

// hashPartition picks a partition for a message the same way Kafka's default
// partitioner does: hash the key and mod by partition count so all messages
// for the same key land on the same partition (needed for per-key ordering).
// A message published with no key round-robins via FNV of a random-ish
// nonce-free fallback (offset-independent, so we hash on a counter owned by
// the caller instead) — see events handler for the empty-key case.
func hashPartition(key string, numPartitions int) int {
	if numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(numPartitions))
}
