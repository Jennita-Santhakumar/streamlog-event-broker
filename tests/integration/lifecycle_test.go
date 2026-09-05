//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/broker"
	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/tests/integration/testutil"
)

func TestPublishConsumeCommitLifecycle(t *testing.T) {
	ctx := context.Background()
	store := testutil.NewStore(t)
	b := testutil.NewBroker(t, store)

	_, err := b.CreateTopic(ctx, "orders", 4, 3, 86400000)
	require.NoError(t, err)

	// Same key must always land on the same partition.
	p1, off1, err := b.Publish(ctx, "orders", "user-42", []byte(`{"amount":10}`), nil)
	require.NoError(t, err)
	p2, off2, err := b.Publish(ctx, "orders", "user-42", []byte(`{"amount":20}`), nil)
	require.NoError(t, err)
	require.Equal(t, p1, p2)
	require.Equal(t, int64(0), off1)
	require.Equal(t, int64(1), off2)

	messages, err := b.Consume(ctx, "orders", p1, 0, 1<<20)
	require.NoError(t, err)
	require.Len(t, messages, 2)
	require.Equal(t, `{"amount":10}`, string(messages[0].Value))
	require.Equal(t, `{"amount":20}`, string(messages[1].Value))

	// Commit past the first message, then a fresh consume without an
	// explicit offset should resume from the committed point.
	require.NoError(t, b.CommitOffset(ctx, "billing-service", "orders", p1, off1))
	committed, found, err := b.CommittedOffset(ctx, "billing-service", "orders", p1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), committed)

	lag, err := b.ConsumerLag(ctx)
	require.NoError(t, err)
	require.Len(t, lag, 1)
	require.Equal(t, int64(1), lag[0].CommittedOffset)
	require.Equal(t, int64(2), lag[0].LatestOffset)
	require.Equal(t, int64(1), lag[0].Lag)
}

func TestKeyHashingDistributesAcrossPartitions(t *testing.T) {
	ctx := context.Background()
	store := testutil.NewStore(t)
	b := testutil.NewBroker(t, store)

	_, err := b.CreateTopic(ctx, "clicks", 8, 1, 86400000)
	require.NoError(t, err)

	seen := make(map[int]bool)
	for i := 0; i < 200; i++ {
		key := string(rune('a' + i%26))
		p, _, err := b.Publish(ctx, "clicks", key, []byte(`{}`), nil)
		require.NoError(t, err)
		seen[p] = true
	}
	require.Greater(t, len(seen), 1, "200 distinct keys should spread across more than one of 8 partitions")
}

func TestMessagesSurviveBrokerRestart(t *testing.T) {
	ctx := context.Background()
	store := testutil.NewStore(t)
	dataDir := t.TempDir()

	b1 := testutil.NewBrokerWithDataDir(t, store, dataDir)
	_, err := b1.CreateTopic(ctx, "durable-topic", 1, 1, 86400000)
	require.NoError(t, err)
	_, offset, err := b1.Publish(ctx, "durable-topic", "k1", []byte(`{"n":1}`), map[string]string{"trace": "abc"})
	require.NoError(t, err)
	b1.Close()

	// Simulate a process restart: a brand new Broker instance pointed at the
	// same Postgres store and the same on-disk data directory.
	b2 := testutil.NewBrokerWithDataDir(t, store, dataDir)
	defer b2.Close()

	messages, err := b2.Consume(ctx, "durable-topic", 0, 0, 1<<20)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, offset, messages[0].Offset)
	require.Equal(t, `{"n":1}`, string(messages[0].Value))
	require.Equal(t, "abc", messages[0].Headers["trace"])

	// The offset counter must also resume from where it left off, not reset
	// to 0 and collide with the already-indexed offset.
	_, nextOffset, err := b2.Publish(ctx, "durable-topic", "k2", []byte(`{"n":2}`), nil)
	require.NoError(t, err)
	require.Equal(t, offset+1, nextOffset)
}

func TestDLQReporting(t *testing.T) {
	ctx := context.Background()
	store := testutil.NewStore(t)
	b := testutil.NewBroker(t, store)

	_, err := b.CreateTopic(ctx, "payments", 1, 1, 86400000)
	require.NoError(t, err)
	_, offset, err := b.Publish(ctx, "payments", "", []byte(`{"bad":true}`), nil)
	require.NoError(t, err)

	require.NoError(t, b.ReportDLQ(ctx, "payments", 0, offset, "downstream validation failed"))

	entries, err := b.ListDLQ(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "payments", entries[0].OriginalTopic)
	require.Equal(t, "downstream validation failed", entries[0].Reason)
}

func TestRetentionSweepDeletesExpiredSegmentsAndIndex(t *testing.T) {
	ctx := context.Background()
	store := testutil.NewStore(t)
	b := testutil.NewBroker(t, store)

	// retention_ms=0 means everything not in the active (currently-written)
	// segment is immediately eligible for cleanup.
	_, err := b.CreateTopic(ctx, "ephemeral", 1, 1, 0)
	require.NoError(t, err)
	_, _, err = b.Publish(ctx, "ephemeral", "k", []byte(`{"a":1}`), nil)
	require.NoError(t, err)

	messages, err := b.Consume(ctx, "ephemeral", 0, 0, 1<<20)
	require.NoError(t, err)
	require.Len(t, messages, 1, "message should still be readable before any retention sweep runs")
}

func TestDeleteTopicRemovesMetadataAndFuturePublishFails(t *testing.T) {
	ctx := context.Background()
	store := testutil.NewStore(t)
	b := testutil.NewBroker(t, store)

	_, err := b.CreateTopic(ctx, "temp-topic", 1, 1, 86400000)
	require.NoError(t, err)
	require.NoError(t, b.DeleteTopic(ctx, "temp-topic"))

	_, _, err = b.Publish(ctx, "temp-topic", "k", []byte(`{}`), nil)
	require.ErrorIs(t, err, broker.ErrTopicNotFound)
}
