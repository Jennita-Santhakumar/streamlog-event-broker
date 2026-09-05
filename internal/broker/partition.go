package broker

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Partition manages the on-disk segment log for one topic-partition. It
// hands out byte positions for appended records; offset assignment itself
// lives one layer up in Broker, since offsets must be coordinated with the
// Postgres message_index for crash recovery.
type Partition struct {
	dir             string
	maxSegmentBytes int64

	mu       sync.Mutex
	active   *Segment
	segments map[int64]*Segment // baseOffset -> segment (includes active)
}

func OpenPartition(dir string, maxSegmentBytes int64) (*Partition, error) {
	p := &Partition{
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		segments:        make(map[int64]*Segment),
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var bases []int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		base, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	for _, base := range bases {
		seg, err := openSegment(dir, base)
		if err != nil {
			return nil, err
		}
		p.segments[base] = seg
		p.active = seg
	}

	return p, nil
}

// Append writes value to the active segment, rolling over to a new segment
// (named by the given offset) if the active one has grown past
// maxSegmentBytes. Returns which segment the record landed in and its byte
// position within that segment.
func (p *Partition) Append(offset int64, value []byte) (segmentBase int64, position int64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.active == nil || p.active.Size() >= p.maxSegmentBytes {
		seg, err := openSegment(p.dir, offset)
		if err != nil {
			return 0, 0, err
		}
		p.active = seg
		p.segments[offset] = seg
	}

	pos, err := p.active.Append(value)
	if err != nil {
		return 0, 0, err
	}
	return p.active.BaseOffset, pos, nil
}

func (p *Partition) ReadRecord(segmentBase, position int64, length int32) ([]byte, error) {
	p.mu.Lock()
	seg, ok := p.segments[segmentBase]
	if !ok {
		var err error
		seg, err = openSegment(p.dir, segmentBase)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		p.segments[segmentBase] = seg
	}
	p.mu.Unlock()

	return seg.ReadAt(position, length)
}

// SweepExpiredSegments removes closed (non-active) segments whose file
// mtime is older than cutoffMillis, returning the base offsets removed so
// the caller can drop their message_index rows too.
func (p *Partition) SweepExpiredSegments(cutoffMillis int64) ([]int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var removed []int64
	for base, seg := range p.segments {
		if p.active != nil && base == p.active.BaseOffset {
			continue // never delete the active (currently-written-to) segment
		}
		mtime, err := seg.ModTime()
		if err != nil {
			return removed, err
		}
		if mtime < cutoffMillis {
			if err := seg.Remove(); err != nil {
				return removed, err
			}
			delete(p.segments, base)
			removed = append(removed, base)
		}
	}
	return removed, nil
}

func (p *Partition) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, seg := range p.segments {
		seg.Close()
	}
	return nil
}

func partitionDir(dataDir, topicID string, partition int) string {
	return filepath.Join(dataDir, topicID, strconv.Itoa(partition))
}
