package flinkhelper

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StateSchema is the state database schema BeziSync's engines keep positions in (Debezium's
// offsets live there too).
const StateSchema = "debezium_state"

// MaxStateBytes caps one stored checkpoint or savepoint (compressed). CDC state is small (source
// positions and copy progress); a pipeline whose state outgrows this needs object storage.
const MaxStateBytes = 64 << 20

// ErrStateTooLarge is returned when a checkpoint or savepoint exceeds MaxStateBytes.
var ErrStateTooLarge = errors.New("saved state exceeds the size BeziSync stores")

// Saved is one stored checkpoint or savepoint.
type Saved struct {
	ID      int64     `json:"id"`
	Kind    string    `json:"kind"`   // checkpoint | savepoint
	Reason  string    `json:"reason"` // periodic, stop, …
	FlinkID int64     `json:"flink_id"`
	Path    string    `json:"path"` // directory relative to the state dir
	TakenAt time.Time `json:"taken_at"`
	Size    int64     `json:"size"`
	SHA256  string    `json:"sha256"`
	// Generation is the column generation of the job that saved it (ADR 0013).
	Generation int `json:"generation"`
}

// Store keeps a pipeline's saved positions in the state database, encrypted with the pipeline's
// key (AES-256-GCM).
type Store struct {
	pool  *pgxpool.Pool
	table string
	aead  cipher.AEAD
	// Generation is recorded with every state this job saves (Settings.Generation).
	Generation int
}

// TableName is the state table of a pipeline.
func TableName(shortID string) string { return "flink_state_" + shortID }

// OpenStore connects to the state database and creates the pipeline's table when missing. key is
// the base64 encoding of 32 random bytes.
func OpenStore(ctx context.Context, cfg *pgxpool.Config, shortID, key string) (*Store, error) {
	raw, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(raw) != 32 {
		return nil, errors.New("state key must be 32 bytes, base64-encoded")
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool, table: pgx.Identifier{StateSchema, TableName(shortID)}.Sanitize(), aead: aead}
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{StateSchema}.Sanitize()); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+s.table+` (
		id bigserial PRIMARY KEY,
		kind text NOT NULL,
		reason text NOT NULL,
		flink_id bigint NOT NULL,
		path text NOT NULL,
		taken_at timestamptz NOT NULL DEFAULT now(),
		size bigint NOT NULL,
		sha256 text NOT NULL,
		kept boolean NOT NULL DEFAULT false,
		payload bytea NOT NULL)`); err != nil {
		pool.Close()
		return nil, err
	}
	// ADR 0013: the column generation each state was saved under (0 before it was recorded).
	if _, err := pool.Exec(ctx, `ALTER TABLE `+s.table+` ADD COLUMN IF NOT EXISTS generation integer NOT NULL DEFAULT 0`); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the connections.
func (s *Store) Close() { s.pool.Close() }

// Save archives stateDir/rel (a checkpoint's chk-N or a savepoint directory), encrypts it and
// stores it.
func (s *Store) Save(ctx context.Context, kind, reason string, flinkID int64, stateDir, rel string) (*Saved, error) {
	plain, err := archive(stateDir, rel)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(plain)
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	sealed := s.aead.Seal(nonce, nonce, plain, []byte(rel))
	if len(sealed) > MaxStateBytes {
		return nil, fmt.Errorf("%w (%d MB)", ErrStateTooLarge, len(sealed)>>20)
	}
	out := &Saved{Kind: kind, Reason: reason, FlinkID: flinkID, Path: rel, Size: int64(len(sealed)), SHA256: hex.EncodeToString(sum[:]), Generation: s.Generation}
	err = s.pool.QueryRow(ctx, `INSERT INTO `+s.table+` (kind, reason, flink_id, path, size, sha256, payload, generation)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, taken_at`, kind, reason, flinkID, rel, out.Size, out.SHA256, sealed, s.Generation).Scan(&out.ID, &out.TakenAt)
	return out, err
}

// List returns the stored states, newest first, without payloads.
func (s *Store) List(ctx context.Context) ([]Saved, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, kind, reason, flink_id, path, taken_at, size, sha256, generation FROM `+s.table+` ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Saved
	for rows.Next() {
		var v Saved
		if err := rows.Scan(&v.ID, &v.Kind, &v.Reason, &v.FlinkID, &v.Path, &v.TakenAt, &v.Size, &v.SHA256, &v.Generation); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// LatestAfter is the newest stored state with an id above after (nil when there is none).
func (s *Store) LatestAfter(ctx context.Context, after int64) (*Saved, error) {
	l, err := s.List(ctx)
	if err != nil || len(l) == 0 || l[0].ID <= after {
		return nil, err
	}
	return &l[0], nil
}

// MaxFlinkID is the highest Flink checkpoint id among every stored state, set aside or not (0
// when none recorded one).
func (s *Store) MaxFlinkID(ctx context.Context) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `SELECT coalesce(max(flink_id), 0) FROM `+s.table).Scan(&id)
	return id, err
}

// Extract restores a stored state into stateDir at the path it was taken from and returns the
// directory to restore from.
func (s *Store) Extract(ctx context.Context, id int64, stateDir string) (string, error) {
	var rel, want string
	var sealed []byte
	if err := s.pool.QueryRow(ctx, `SELECT path, sha256, payload FROM `+s.table+` WHERE id = $1`, id).Scan(&rel, &want, &sealed); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("saved position %d no longer exists", id)
		}
		return "", err
	}
	n := s.aead.NonceSize()
	if len(sealed) < n {
		return "", errors.New("saved position is corrupt")
	}
	plain, err := s.aead.Open(nil, sealed[:n], sealed[n:], []byte(rel))
	if err != nil {
		return "", errors.New("saved position can't be decrypted (was the master key changed?)")
	}
	if sum := sha256.Sum256(plain); hex.EncodeToString(sum[:]) != want {
		return "", errors.New("saved position is corrupt")
	}
	if err := unarchive(plain, stateDir); err != nil {
		return "", err
	}
	return filepath.Join(stateDir, rel), nil
}

// Prune keeps the newest keep checkpoints; savepoints are kept (BeziSync expires them).
func (s *Store) Prune(ctx context.Context, keep int) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM `+s.table+` WHERE kind = 'checkpoint' AND NOT kept AND id NOT IN
		(SELECT id FROM `+s.table+` WHERE kind = 'checkpoint' ORDER BY id DESC LIMIT $1)`, keep)
	return err
}

// archive tars and gzips stateDir/rel with paths relative to stateDir.
func archive(stateDir, rel string) ([]byte, error) {
	if !validRel(rel) {
		return nil, fmt.Errorf("invalid state path %q", rel)
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	root := filepath.Join(stateDir, rel)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name, err := filepath.Rel(stateDir, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !d.IsDir() && !info.Mode().IsRegular() {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(name)
		if d.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		f, err := os.Open(path) //nolint:gosec // a file under the state dir being archived
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// unarchive extracts into stateDir, refusing any path that leaves it.
func unarchive(data []byte, stateDir string) error {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(hdr.Name, "/")
		if !validRel(name) {
			return fmt.Errorf("saved position holds an invalid path %q", hdr.Name)
		}
		target := filepath.Join(stateDir, filepath.FromSlash(name))
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o750); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // validRel keeps target under the state dir
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, io.LimitReader(tr, MaxStateBytes*4)); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("saved position holds an unsupported entry %q", hdr.Name)
		}
	}
}

func validRel(rel string) bool {
	clean := filepath.Clean(filepath.FromSlash(rel))
	return rel != "" && !filepath.IsAbs(clean) && clean != "." && !strings.HasPrefix(clean, ".."+string(filepath.Separator)) && clean != ".."
}
