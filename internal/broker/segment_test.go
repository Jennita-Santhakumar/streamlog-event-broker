package broker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	seg, err := openSegment(dir, 0)
	require.NoError(t, err)
	defer seg.Close()

	pos1, err := seg.Append([]byte("hello"))
	require.NoError(t, err)
	require.Equal(t, int64(0), pos1)

	pos2, err := seg.Append([]byte("world!"))
	require.NoError(t, err)
	require.Equal(t, int64(4+5), pos2)

	got1, err := seg.ReadAt(pos1, 5)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got1))

	got2, err := seg.ReadAt(pos2, 6)
	require.NoError(t, err)
	require.Equal(t, "world!", string(got2))
}

func TestSegmentReopenPreservesData(t *testing.T) {
	dir := t.TempDir()
	seg, err := openSegment(dir, 0)
	require.NoError(t, err)
	pos, err := seg.Append([]byte("durable"))
	require.NoError(t, err)
	require.NoError(t, seg.Close())

	reopened, err := openSegment(dir, 0)
	require.NoError(t, err)
	defer reopened.Close()

	got, err := reopened.ReadAt(pos, 7)
	require.NoError(t, err)
	require.Equal(t, "durable", string(got))
	require.Equal(t, int64(4+7), reopened.Size())
}
