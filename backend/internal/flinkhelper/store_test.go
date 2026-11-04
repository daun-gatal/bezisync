package flinkhelper

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

func newKey(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func TestMain(m *testing.M) { os.Exit(testdb.Main(m, nil)) }

// openTestStore opens a store with its own table (short id) on the test database.
func openTestStore(t *testing.T, key string) (*Store, string) {
	t.Helper()
	url := testdb.URL()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	id := make([]byte, 6)
	_, _ = rand.Read(id)
	short := hex.EncodeToString(id)
	s, err := OpenStore(context.Background(), cfg, short, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	testShort[s] = short
	return s, url
}

var testShort = map[*Store]string{}

func writeState(t *testing.T, dir, rel string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		p := filepath.Join(dir, rel, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStoreRoundTrip(t *testing.T) {
	key := newKey(t)
	s, url := openTestStore(t, key)
	ctx := context.Background()
	src := t.TempDir()
	writeState(t, src, "cp/abc/chk-3", map[string]string{"_metadata": "inline-state-3"})
	writeState(t, src, "cp/abc/chk-4", map[string]string{"_metadata": "inline-state-4", "extra/part": "spilled"})
	if latest, err := s.LatestAfter(ctx, 0); err != nil || latest != nil {
		t.Fatalf("an empty store has a latest state: %v %v", latest, err)
	}
	a, err := s.Save(ctx, "checkpoint", "periodic", 3, src, "cp/abc/chk-3")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Save(ctx, "checkpoint", "periodic", 4, src, "cp/abc/chk-4")
	if err != nil {
		t.Fatal(err)
	}
	if b.ID <= a.ID || b.Size == 0 || len(b.SHA256) != 64 {
		t.Fatalf("saved %+v %+v", a, b)
	}
	latest, err := s.LatestAfter(ctx, 0)
	if err != nil || latest.ID != b.ID || latest.FlinkID != 4 || latest.Path != "cp/abc/chk-4" {
		t.Fatalf("latest = %+v, %v", latest, err)
	}
	if after, err := s.LatestAfter(ctx, b.ID); err != nil || after != nil {
		t.Fatalf("a position at or below the floor was returned: %+v %v", after, err)
	}
	// A new container restores it at the path it was taken from.
	dst := t.TempDir()
	dir, err := s.Extract(ctx, b.ID, dst)
	if err != nil {
		t.Fatal(err)
	}
	if dir != filepath.Join(dst, "cp/abc/chk-4") {
		t.Fatal(dir)
	}
	for name, want := range map[string]string{"_metadata": "inline-state-4", "extra/part": "spilled"} {
		if got, _ := os.ReadFile(filepath.Join(dir, name)); string(got) != want {
			t.Errorf("%s = %q", name, got)
		}
	}
	// The payload is encrypted: the plain state never reaches the database.
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var payload []byte
	table := "debezium_state.flink_state_" + testShort[s]
	if err := pool.QueryRow(ctx, `SELECT payload FROM `+table+` WHERE id = $1`, b.ID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("inline-state-4")) {
		t.Fatal("state stored in clear")
	}
	if gz, err := gzip.NewReader(bytes.NewReader(payload)); err == nil {
		_ = gz.Close()
		t.Fatal("payload is a plain gzip stream")
	}
	// Another key can't read it; a tampered payload is refused.
	other, err := OpenStore(ctx, mustCfg(t, url), testShort[s], newKey(t))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Extract(ctx, b.ID, t.TempDir()); err == nil || !strings.Contains(err.Error(), "decrypted") {
		t.Fatalf("other key: %v", err)
	}
	payload[len(payload)-1] ^= 0xff
	if _, err := pool.Exec(ctx, `UPDATE `+table+` SET payload = $1 WHERE id = $2`, payload, b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Extract(ctx, b.ID, t.TempDir()); err == nil {
		t.Fatal("a tampered payload was restored")
	}
	if _, err := s.Extract(ctx, 999999, t.TempDir()); err == nil || !strings.Contains(err.Error(), "no longer exists") {
		t.Fatalf("missing id: %v", err)
	}
}

func mustCfg(t *testing.T, url string) *pgxpool.Config {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestStorePruneKeepsSavepoints(t *testing.T) {
	s, _ := openTestStore(t, newKey(t))
	ctx := context.Background()
	src := t.TempDir()
	writeState(t, src, "sp/savepoint-1", map[string]string{"_metadata": "sp"})
	if _, err := s.Save(ctx, "savepoint", "stop", 0, src, "sp/savepoint-1"); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 7; i++ {
		rel := filepath.Join("cp/job", "chk-"+string(rune('0'+i)))
		writeState(t, src, rel, map[string]string{"_metadata": rel})
		if _, err := s.Save(ctx, "checkpoint", "periodic", int64(i), src, rel); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Prune(ctx, 3); err != nil {
		t.Fatal(err)
	}
	l, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoints []int64
	savepoints := 0
	for _, v := range l {
		if v.Kind == "checkpoint" {
			checkpoints = append(checkpoints, v.FlinkID)
		} else {
			savepoints++
		}
	}
	if len(checkpoints) != 3 || checkpoints[0] != 7 || checkpoints[2] != 5 || savepoints != 1 {
		t.Fatalf("after prune: checkpoints %v savepoints %d", checkpoints, savepoints)
	}
}

func TestStoreRefusesBadPathsAndKeys(t *testing.T) {
	ctx := context.Background()
	if _, err := OpenStore(ctx, mustCfg(t, "postgres://u:p@127.0.0.1:1/x"), "0123456789ab", "short"); err == nil {
		t.Fatal("accepted a short key")
	}
	s, _ := openTestStore(t, newKey(t))
	for _, rel := range []string{"", ".", "..", "../x", "/abs"} {
		if _, err := s.Save(ctx, "checkpoint", "periodic", 1, t.TempDir(), rel); err == nil {
			t.Errorf("saved %q", rel)
		}
	}
	// An archive that tries to leave the state dir is refused.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	_ = tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg})
	_, _ = tw.Write([]byte("x"))
	_ = tw.Close()
	_ = gz.Close()
	dst := t.TempDir()
	if err := unarchive(buf.Bytes(), dst); err == nil {
		t.Fatal("extracted a path outside the state dir")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), "escape")); err == nil {
		t.Fatal("wrote outside the state dir")
	}
}
