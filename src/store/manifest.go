package store

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
)

const manifestFilename = "MANIFEST"

type manifestRecord struct {
	Op   string `json:"op"`   // "add" or "del"
	Path string `json:"path"` // SSTable file path
}

type manifest struct {
	file *os.File
}

func openManifest(dir string) (*manifest, []string, error) {
	path := filepath.Join(dir, manifestFilename)

	// Replay existing manifest to find the live SSTable list.
	live, err := replayManifest(path)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}

	return &manifest{file: f}, live, nil
}

func replayManifest(path string) ([]string, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil // fresh store, no manifest yet
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Use a slice to preserve insertion order (newest-first is enforced at load time).
	var ordered []string
	alive := make(map[string]bool)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var rec manifestRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue // skip malformed tail lines from a crash
		}
		switch rec.Op {
		case "add":
			if !alive[rec.Path] {
				ordered = append(ordered, rec.Path)
				alive[rec.Path] = true
			}
		case "del":
			alive[rec.Path] = false
		}
	}

	// Return only live entries in their original order (oldest→newest).
	// The store will reverse this to newest-first when loading readers.
	var live []string
	for _, p := range ordered {
		if alive[p] {
			live = append(live, p)
		}
	}
	return live, scanner.Err()
}

func (m *manifest) recordAdd(path string) error {
	return m.append(manifestRecord{Op: "add", Path: path})
}

func (m *manifest) recordDel(path string) error {
	return m.append(manifestRecord{Op: "del", Path: path})
}

func (m *manifest) append(rec manifestRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := m.file.Write(data); err != nil {
		return err
	}
	return m.file.Sync()
}

func (m *manifest) close() error {
	return m.file.Close()
}
