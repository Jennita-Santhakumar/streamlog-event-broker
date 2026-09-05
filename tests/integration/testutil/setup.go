// Package testutil provides shared setup for integration tests: a real
// Postgres connection via DATABASE_URL, with table truncation between tests
// for isolation, plus a Broker wired to a temp on-disk data directory.
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/broker"
	"github.com/KabileshRajaselvan/distributed-event-streaming-broker/internal/storage"
)

func RequireEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("%s not set; skipping integration test", key)
	}
	return v
}

func NewStore(t *testing.T) *storage.Store {
	t.Helper()
	ctx := context.Background()
	s, err := storage.New(ctx, RequireEnv(t, "DATABASE_URL"))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	TruncateAll(t, s)
	return s
}

func TruncateAll(t *testing.T, s *storage.Store) {
	t.Helper()
	require.NoError(t, s.Exec(context.Background(),
		"TRUNCATE TABLE dlq, consumer_offsets, consumer_groups, message_index, partitions, topics RESTART IDENTITY CASCADE"))
}

// NewBroker wires a Broker to the given store and a fresh temp directory,
// simulating one running broker process.
func NewBroker(t *testing.T, s *storage.Store) *broker.Broker {
	t.Helper()
	b := broker.NewBroker(s, t.TempDir(), 1<<20, 1)
	require.NoError(t, b.LoadExisting(context.Background()))
	t.Cleanup(b.Close)
	return b
}

// NewBrokerWithDataDir is like NewBroker but lets the caller reuse a
// specific data directory across two Broker instances, simulating a broker
// restart against the same on-disk segments.
func NewBrokerWithDataDir(t *testing.T, s *storage.Store, dataDir string) *broker.Broker {
	t.Helper()
	b := broker.NewBroker(s, dataDir, 1<<20, 1)
	require.NoError(t, b.LoadExisting(context.Background()))
	return b
}
