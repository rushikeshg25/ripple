package fileio

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSourceHashAndRewind(t *testing.T) {
	p := filepath.Join(t.TempDir(), "source")
	data := []byte("hello ripple")
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenSource(context.Background(), p, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer s.File.Close()
	if s.Metadata.SHA256 != sha256.Sum256(data) || s.Metadata.Size != uint64(len(data)) {
		t.Fatal(s.Metadata)
	}
	b := make([]byte, len(data))
	if n, err := s.File.Read(b); err != nil || n != len(data) || string(b) != string(data) {
		t.Fatal(n, err)
	}
	if _, err := OpenSource(context.Background(), p, 1); err == nil {
		t.Fatal("size cap ignored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := OpenSource(ctx, p, 1024); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := OpenSource(context.Background(), filepath.Dir(p), 1024); err == nil {
		t.Fatal("accepted directory")
	}
}
func TestPublicationAndAbort(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "result")
	d, err := NewDestination(target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Write([]byte("verified")); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial output visible")
	}
	if err = d.Publish(); err != nil {
		t.Fatal(err)
	}
	if err = d.Publish(); err != nil {
		t.Fatal("publish not idempotent", err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "verified" {
		t.Fatal(string(got), err)
	}
	if _, err = NewDestination(target); err == nil {
		t.Fatal("overwrite allowed")
	}
	aborted, err := NewDestination(filepath.Join(dir, "aborted"))
	if err != nil {
		t.Fatal(err)
	}
	if err = aborted.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 {
		t.Fatal("temporary file leaked", files, err)
	}
}
func TestPublishDoesNotClobberConcurrentCreator(t *testing.T) {
	target := filepath.Join(t.TempDir(), "result")
	d, err := NewDestination(target)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err = d.Write([]byte("incoming")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(target, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = d.Publish(); err == nil {
		t.Fatal("clobbered destination")
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "existing" {
		t.Fatal(string(got), err)
	}
}
func TestDestinationRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "link")
	if err := os.Symlink(filepath.Join(dir, "missing"), target); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDestination(target); err == nil {
		t.Fatal("accepted symlink")
	}
}
