package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Om-Jadon/raftkv/internal/raft"
)

const (
	hardStateName = "hardstate"
	logName       = "log"
	snapName      = "snapshot"
)

// FileStorage is a durable raft.Storage backed by a directory.
type FileStorage struct {
	mu   sync.Mutex
	dir  string
	logF *os.File
}

// Open creates or opens a FileStorage in dir. The directory is created if needed.
func Open(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir}
	if err := s.openLog(); err != nil {
		return nil, err
	}
	return s, nil
}

// Dir returns the data directory.
func (s *FileStorage) Dir() string { return s.dir }

// Close releases the log file handle.
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logF == nil {
		return nil
	}
	err := s.logF.Close()
	s.logF = nil
	return err
}

func (s *FileStorage) openLog() error {
	path := filepath.Join(s.dir, logName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}
	s.logF = f
	return nil
}

// SaveHardState implements raft.Storage.
func (s *FileStorage) SaveHardState(term uint64, votedFor *raft.NodeID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]byte, 17)
	binary.LittleEndian.PutUint64(buf[0:8], term)
	if votedFor != nil {
		buf[8] = 1
		binary.LittleEndian.PutUint64(buf[9:17], uint64(*votedFor))
	}
	return atomicWrite(s.dir, hardStateName, buf)
}

// AppendEntries implements raft.Storage. One fsync for the whole batch.
func (s *FileStorage) AppendEntries(entries []raft.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	for _, e := range entries {
		if e.Index == 0 {
			continue // never persist the sentinel
		}
		rec := encodeRecord(e)
		if _, err := s.logF.Write(rec); err != nil {
			return err
		}
	}
	return s.logF.Sync()
}

// TruncateFrom implements raft.Storage by rewriting the log without entries >= index.
func (s *FileStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := s.readLogLocked()
	if err != nil {
		return err
	}
	kept := make([]raft.LogEntry, 0, len(entries))
	for _, e := range entries {
		if e.Index >= index {
			break
		}
		if e.Index == 0 {
			continue
		}
		kept = append(kept, e)
	}
	var buf []byte
	for _, e := range kept {
		buf = append(buf, encodeRecord(e)...)
	}
	if err := atomicWrite(s.dir, logName, buf); err != nil {
		return err
	}
	_ = s.logF.Close()
	return s.openLog()
}

// Load implements raft.Storage.
func (s *FileStorage) Load() (raft.HardState, []raft.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hs, err := s.loadHardStateLocked()
	if err != nil {
		return raft.HardState{}, nil, err
	}
	entries, err := s.readLogLocked()
	if err != nil {
		return raft.HardState{}, nil, err
	}
	return hs, entries, nil
}

func (s *FileStorage) loadHardStateLocked() (raft.HardState, error) {
	path := filepath.Join(s.dir, hardStateName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.HardState{}, nil
	}
	if err != nil {
		return raft.HardState{}, err
	}
	if len(data) < 9 {
		return raft.HardState{}, nil
	}
	hs := raft.HardState{CurrentTerm: binary.LittleEndian.Uint64(data[0:8])}
	if data[8] == 1 && len(data) >= 17 {
		id := raft.NodeID(binary.LittleEndian.Uint64(data[9:17]))
		hs.VotedFor = &id
	}
	return hs, nil
}

func (s *FileStorage) readLogLocked() ([]raft.LogEntry, error) {
	path := filepath.Join(s.dir, logName)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return []raft.LogEntry{{Term: 0, Index: 0}}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries := []raft.LogEntry{{Term: 0, Index: 0}}
	for {
		e, err := decodeNext(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Torn or corrupt tail: keep what we have.
			break
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// SaveSnapshot persists the snapshot, then drops log entries at or below
// LastIncludedIndex. The snapshot must be durable before the log is truncated.
func (s *FileStorage) SaveSnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := encodeSnapshot(snap)
	if err := atomicWrite(s.dir, snapName, buf); err != nil {
		return err
	}
	return s.compactLogLocked(snap.LastIncludedIndex)
}

func (s *FileStorage) compactLogLocked(lastIncluded uint64) error {
	if lastIncluded == 0 {
		return nil
	}
	entries, err := s.readLogLocked()
	if err != nil {
		return err
	}
	var buf []byte
	for _, e := range entries {
		if e.Index <= lastIncluded || e.Index == 0 {
			continue
		}
		buf = append(buf, encodeRecord(e)...)
	}
	if err := atomicWrite(s.dir, logName, buf); err != nil {
		return err
	}
	_ = s.logF.Close()
	return s.openLog()
}

// LoadSnapshot implements raft.Storage.
func (s *FileStorage) LoadSnapshot() (raft.Snapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, snapName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return raft.Snapshot{}, false, nil
	}
	if err != nil {
		return raft.Snapshot{}, false, err
	}
	snap, ok := decodeSnapshot(data)
	return snap, ok, nil
}

func encodeRecord(e raft.LogEntry) []byte {
	cmdLen := len(e.Command)
	body := make([]byte, 8+8+4+cmdLen)
	binary.LittleEndian.PutUint64(body[0:8], e.Term)
	binary.LittleEndian.PutUint64(body[8:16], e.Index)
	binary.LittleEndian.PutUint32(body[16:20], uint32(cmdLen))
	copy(body[20:], e.Command)
	crc := crc32.ChecksumIEEE(body)

	rec := make([]byte, 4+len(body)+4)
	binary.LittleEndian.PutUint32(rec[0:4], uint32(len(body)))
	copy(rec[4:], body)
	binary.LittleEndian.PutUint32(rec[4+len(body):], crc)
	return rec
}

func decodeNext(r io.Reader) (raft.LogEntry, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return raft.LogEntry{}, err
	}
	n := binary.LittleEndian.Uint32(lenBuf[:])
	if n == 0 || n > 64<<20 {
		return raft.LogEntry{}, errors.New("invalid record length")
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return raft.LogEntry{}, err
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return raft.LogEntry{}, err
	}
	want := binary.LittleEndian.Uint32(crcBuf[:])
	got := crc32.ChecksumIEEE(body)
	if want != got {
		return raft.LogEntry{}, errors.New("crc mismatch")
	}
	if len(body) < 20 {
		return raft.LogEntry{}, errors.New("short body")
	}
	term := binary.LittleEndian.Uint64(body[0:8])
	index := binary.LittleEndian.Uint64(body[8:16])
	cmdLen := binary.LittleEndian.Uint32(body[16:20])
	if uint32(len(body)-20) < cmdLen {
		return raft.LogEntry{}, errors.New("short command")
	}
	cmd := append([]byte(nil), body[20:20+cmdLen]...)
	return raft.LogEntry{Term: term, Index: index, Command: cmd}, nil
}

func encodeSnapshot(snap raft.Snapshot) []byte {
	dataLen := len(snap.Data)
	buf := make([]byte, 8+8+4+dataLen)
	binary.LittleEndian.PutUint64(buf[0:8], snap.LastIncludedIndex)
	binary.LittleEndian.PutUint64(buf[8:16], snap.LastIncludedTerm)
	binary.LittleEndian.PutUint32(buf[16:20], uint32(dataLen))
	copy(buf[20:], snap.Data)
	return buf
}

func decodeSnapshot(data []byte) (raft.Snapshot, bool) {
	if len(data) < 20 {
		return raft.Snapshot{}, false
	}
	idx := binary.LittleEndian.Uint64(data[0:8])
	term := binary.LittleEndian.Uint64(data[8:16])
	n := binary.LittleEndian.Uint32(data[16:20])
	if uint32(len(data)-20) < n {
		return raft.Snapshot{}, false
	}
	return raft.Snapshot{
		LastIncludedIndex: idx,
		LastIncludedTerm:  term,
		Data:              append([]byte(nil), data[20:20+n]...),
	}, true
}

func atomicWrite(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	final := filepath.Join(dir, name)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
