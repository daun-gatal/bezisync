package release

import (
	"os"
	"path/filepath"
	"testing"
)

// Setting the current version rewrites every file byte-for-byte unchanged.
func TestSetVersionIsIdentityAtTheCurrentVersion(t *testing.T) {
	root := t.TempDir()
	for _, rel := range VersionFiles {
		copyFile(t, filepath.Join(repo, rel), filepath.Join(root, rel))
	}
	v, err := ReadVersion(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetVersion(root, v); err != nil {
		t.Fatal(err)
	}
	for _, rel := range VersionFiles {
		a, _ := os.ReadFile(filepath.Join(repo, rel))
		b, _ := os.ReadFile(filepath.Join(root, rel))
		if string(a) != string(b) {
			t.Errorf("%s changed", rel)
		}
	}
}
