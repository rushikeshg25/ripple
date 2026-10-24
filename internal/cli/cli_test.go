package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHelpAndInvalidArguments(t *testing.T) {
	var out bytes.Buffer
	if err := Run(context.Background(), nil, &out, io.Discard); err != nil || !strings.Contains(out.String(), "Ripple") {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"missing"}, {"send"}, {"receive"}, {"send", "-to", ":0", "-file", "x"}, {"receive", "-out", "x", "-window", "65"}, {"send", "-to", "127.0.0.1:1", "-file", "x", "extra"}, {"receive", "-out", "x", "-idle-timeout", "1s"}} {
		if err := Run(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Fatal("accepted", args)
		}
	}
}
func TestCancelledReceiveCreatesNoOutput(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, []string{"receive", "-listen", "127.0.0.1:0", "-out", filepath.Join(dir, "out")}, io.Discard, io.Discard); err == nil {
		t.Fatal("cancel ignored")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatal(files, err)
	}
}
func TestReceiverRefusesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), []string{"receive", "-out", path}, io.Discard, io.Discard); err == nil {
		t.Fatal("overwrite allowed")
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "keep" {
		t.Fatal(string(b), err)
	}
}
