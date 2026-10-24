package cli

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestRelayRejectsInvalidFlags(t *testing.T) {
	for _, args := range [][]string{{"relay"}, {"relay", "-to", "127.0.0.1:9", "-loss", "NaN"}, {"relay", "-to", "127.0.0.1:9", "-delay", "-1s"}, {"relay", "-to", "127.0.0.1:9", "-queue", "0"}, {"relay", "-to", "127.0.0.1:9", "-listen", "[::1]:0"}} {
		if err := Run(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Fatal("accepted", args)
		}
	}
}
func TestRelayHelp(t *testing.T) {
	var out strings.Builder
	if err := Run(context.Background(), []string{"relay", "-h"}, io.Discard, &out); err != nil || !strings.Contains(out.String(), "-loss") {
		t.Fatal(err, out.String())
	}
}
