package logging

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestPackageLoggersFollowLaterConfiguration(t *testing.T) {
	early := Logger("bezisync.early")
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(NewHandler(&buf, true, slog.LevelWarn)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	early.Info("dropped")
	early.Warn("kept", "k", 1)
	out := buf.String()
	if strings.Contains(out, "dropped") || !strings.Contains(out, `"logger": "bezisync.early"`) || !strings.Contains(out, `"msg": "kept"`) {
		t.Fatalf("got %q", out)
	}
}
