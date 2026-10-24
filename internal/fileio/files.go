// Package fileio handles local source hashing and no-clobber publication.
package fileio

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rushikeshg25/ripple/internal/wire"
)

type Source struct {
	File     *os.File
	Metadata wire.Metadata
}

func OpenSource(ctx context.Context, path string, maxSize uint64) (*Source, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("source must be a regular file")
	}
	if maxSize > wire.MaxSize || uint64(info.Size()) > maxSize {
		return nil, fmt.Errorf("source exceeds max size")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(contextReader{ctx: ctx, r: f}, int64(maxSize)+1))
	if err != nil {
		return nil, fmt.Errorf("hash source: %w", err)
	}
	if n != info.Size() {
		return nil, fmt.Errorf("source size changed while hashing")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	m := wire.Metadata{Size: uint64(n)}
	copy(m.SHA256[:], h.Sum(nil))
	success = true
	return &Source{File: f, Metadata: m}, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type Destination struct {
	file         *os.File
	temp, target string
	published    bool
}

func NewDestination(path string) (*Destination, error) {
	target, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if _, err = os.Lstat(target); err == nil {
		return nil, fmt.Errorf("destination already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(target), ".ripple-*.part")
	if err != nil {
		return nil, err
	}
	return &Destination{file: f, temp: f.Name(), target: target}, nil
}
func (d *Destination) Write(p []byte) (int, error) {
	if d.file == nil {
		return 0, os.ErrClosed
	}
	return d.file.Write(p)
}

// Publish uses a same-filesystem hard link for atomic no-replace semantics.
// File contents are synced; directory crash durability is not promised.
func (d *Destination) Publish() error {
	if d.published {
		return nil
	}
	if d.file == nil {
		return os.ErrClosed
	}
	if err := d.file.Sync(); err != nil {
		return err
	}
	err := d.file.Close()
	d.file = nil
	if err != nil {
		return err
	}
	if err = os.Link(d.temp, d.target); err != nil {
		return err
	}
	d.published = true
	if err = os.Remove(d.temp); err != nil {
		return fmt.Errorf("file published but temporary link cleanup failed: %w", err)
	}
	return nil
}

// Close removes only the temporary path, even if publication already succeeded.
func (d *Destination) Close() error {
	var closeErr error
	if d.file != nil {
		closeErr = d.file.Close()
		d.file = nil
	}
	removeErr := os.Remove(d.temp)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	return errors.Join(closeErr, removeErr)
}
