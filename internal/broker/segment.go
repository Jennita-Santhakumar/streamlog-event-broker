package broker

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Segment is a single append-only log file, matching Kafka's on-disk segment
// model: a topic-partition's log is split across one or more segment files
// named by the offset of the first record they contain. Each record is
// length-prefixed (4-byte big-endian) so a reader can seek to any byte
// position and read exactly one record without scanning from the start.
type Segment struct {
	BaseOffset int64
	path       string
	file       *os.File
	size       int64
}

func segmentPath(dir string, baseOffset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
}

func openSegment(dir string, baseOffset int64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := segmentPath(dir, baseOffset)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{BaseOffset: baseOffset, path: path, file: f, size: info.Size()}, nil
}

// Append writes one record's raw value bytes to the segment and returns the
// byte position the record starts at (used later for random-access reads).
func (s *Segment) Append(value []byte) (position int64, err error) {
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(value)))

	position = s.size

	if _, err := s.file.Write(header); err != nil {
		return 0, err
	}
	if _, err := s.file.Write(value); err != nil {
		return 0, err
	}
	if err := s.file.Sync(); err != nil {
		return 0, err
	}

	s.size += int64(len(header) + len(value))
	return position, nil
}

// ReadAt reads exactly one length-prefixed record starting at the given byte
// position.
func (s *Segment) ReadAt(position int64, length int32) ([]byte, error) {
	buf := make([]byte, length)
	n, err := s.file.ReadAt(buf, position+4)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if int32(n) != length {
		return nil, fmt.Errorf("short read: expected %d bytes, got %d", length, n)
	}
	return buf, nil
}

func (s *Segment) Size() int64 {
	return s.size
}

func (s *Segment) Close() error {
	return s.file.Close()
}

func (s *Segment) Remove() error {
	s.file.Close()
	return os.Remove(s.path)
}

// ModTime reports the segment file's last-write time, used by the retention
// sweep to decide whether a closed segment is older than a topic's
// retention_ms window.
func (s *Segment) ModTime() (int64, error) {
	info, err := s.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.ModTime().UnixMilli(), nil
}
