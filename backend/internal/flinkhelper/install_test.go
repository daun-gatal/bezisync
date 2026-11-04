package flinkhelper

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestInstallSelfCopiesThisBinary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "opt")
	if err := InstallSelf(dir); err != nil {
		t.Fatal(err)
	}
	self, _ := os.Executable()
	want, _ := os.ReadFile(self) //nolint:gosec // the test binary
	got, err := os.ReadFile(filepath.Join(dir, "bezisync"))
	info, _ := os.Stat(filepath.Join(dir, "bezisync"))
	if err != nil || !bytes.Equal(got, want) || info.Mode().Perm() != 0o755 {
		t.Fatalf("copied %d of %d bytes, mode %v, %v", len(got), len(want), info.Mode(), err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".bezisync.tmp")); !os.IsNotExist(err) {
		t.Fatal("the temporary copy was left behind")
	}
	if err := InstallSelf(dir); err != nil { // a restarted init container installs again
		t.Fatal(err)
	}
}
