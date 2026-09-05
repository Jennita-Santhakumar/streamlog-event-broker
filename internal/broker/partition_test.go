package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPartitionAppendAndReadRecord(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, 1<<20)
	require.NoError(t, err)
	defer p.Close()

	base1, pos1, err := p.Append(0, []byte("msg-0"))
	require.NoError(t, err)
	base2, pos2, err := p.Append(1, []byte("msg-1-longer"))
	require.NoError(t, err)
	require.Equal(t, base1, base2) // same segment, no rollover yet

	got1, err := p.ReadRecord(base1, pos1, int32(len("msg-0")))
	require.NoError(t, err)
	require.Equal(t, "msg-0", string(got1))

	got2, err := p.ReadRecord(base2, pos2, int32(len("msg-1-longer")))
	require.NoError(t, err)
	require.Equal(t, "msg-1-longer", string(got2))
}

func TestPartitionRollsOverAtMaxSegmentBytes(t *testing.T) {
	dir := t.TempDir()
	// Tiny segment size so a couple of small appends force a rollover.
	p, err := OpenPartition(dir, 10)
	require.NoError(t, err)
	defer p.Close()

	base1, _, err := p.Append(0, []byte("12345678")) // 4 header + 8 = 12 bytes, over the 10-byte cap
	require.NoError(t, err)
	base2, _, err := p.Append(1, []byte("x"))
	require.NoError(t, err)

	require.NotEqual(t, base1, base2, "second append should have rolled over to a new segment")
	require.Equal(t, int64(1), base2, "new segment should be named after the offset that started it")
}

func TestPartitionReopenRebuildsSegmentIndex(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, 1<<20)
	require.NoError(t, err)
	base, pos, err := p.Append(0, []byte("persisted"))
	require.NoError(t, err)
	require.NoError(t, p.Close())

	reopened, err := OpenPartition(dir, 1<<20)
	require.NoError(t, err)
	defer reopened.Close()

	got, err := reopened.ReadRecord(base, pos, int32(len("persisted")))
	require.NoError(t, err)
	require.Equal(t, "persisted", string(got))
}

func TestSweepExpiredSegmentsSkipsActiveSegment(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, 10)
	require.NoError(t, err)
	defer p.Close()

	_, _, err = p.Append(0, []byte("12345678")) // rolls over on next append
	require.NoError(t, err)
	_, _, err = p.Append(1, []byte("x")) // now active segment is base=1
	require.NoError(t, err)

	// Everything modified before "now" is technically expired, but the
	// active segment must survive a sweep since it's still being written to.
	removed, err := p.SweepExpiredSegments(time.Now().UnixMilli())
	require.NoError(t, err)
	require.Equal(t, []int64{0}, removed)
}

func TestHashPartitionIsStableAndDistributes(t *testing.T) {
	require.Equal(t, 0, hashPartition("any-key", 1))

	p1 := hashPartition("user-123", 8)
	p2 := hashPartition("user-123", 8)
	require.Equal(t, p1, p2, "same key must always map to the same partition")

	seen := make(map[int]bool)
	for i := 0; i < 200; i++ {
		key := string(rune('a' + i%26))
		seen[hashPartition(key, 8)] = true
	}
	require.Greater(t, len(seen), 1, "distinct keys should spread across more than one partition")
}
