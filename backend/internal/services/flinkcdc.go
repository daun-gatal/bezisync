package services

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"software.sslmate.com/src/go-pkcs12"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

// Flink CDC pipelines: their own secrets and their saved positions.
//
// Saved positions live in the state database (debezium_state.flink_state_<id>), written by the
// helper in the pipeline's JobManager. The helper starts from the newest one after the floor in
// artifacts[ArtifactRestoreAfter]: Start over raises the floor (earlier positions stay listed until
// they expire) and Restore copies a chosen position above it.

// Pipeline artifacts of Flink CDC pipelines.
const (
	// ArtifactRestoreAfter: saved positions with an id up to this one are not restored.
	ArtifactRestoreAfter = "flink_restore_after"
	// ArtifactStartup: how a pipeline with no saved position after the floor starts
	// ({"mode": "initial"|"latest-offset"|"timestamp", "timestamp_ms": n}); set by Start over
	// until the next deploy.
	ArtifactStartup = "flink_startup"
)

// FlinkSecrets are a Flink CDC pipeline's own secrets.
type FlinkSecrets struct {
	// StateKey encrypts its saved positions (32 bytes, base64).
	StateKey string `json:"state_key"`
	// Keystore (PKCS#12, base64) and its password secure traffic between its JobManager and
	// TaskManagers (Flink internal TLS with mutual authentication).
	Keystore         string `json:"keystore"`
	KeystorePassword string `json:"keystore_password"`
}

// FlinkSecretsOf returns a pipeline's secrets, generating and sealing them the first time.
func FlinkSecretsOf(ctx context.Context, q db.Querier, p *models.Pipeline) (*FlinkSecrets, error) {
	if p.EngineSecretsEnc != nil && *p.EngineSecretsEnc != "" {
		var s FlinkSecrets
		if err := security.DecryptInto(*p.EngineSecretsEnc, &s); err != nil {
			return nil, err
		}
		return &s, nil
	}
	s, err := newFlinkSecrets(p.ShortID())
	if err != nil {
		return nil, err
	}
	sealed, err := security.EncryptSecret(map[string]any{"state_key": s.StateKey, "keystore": s.Keystore, "keystore_password": s.KeystorePassword})
	if err != nil {
		return nil, err
	}
	if _, err := q.Exec(ctx, `UPDATE pipelines SET engine_secrets_enc = $1 WHERE id = $2`, sealed, p.ID); err != nil {
		return nil, err
	}
	p.EngineSecretsEnc = &sealed
	return s, nil
}

func randomString(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// newFlinkSecrets generates a state key and a self-signed keystore (ECDSA P-256) shared by the
// pipeline's Flink processes: each one presents it and trusts only it.
func newFlinkSecrets(shortID string) (*FlinkSecrets, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return nil, err
	}
	now := time.Now().Add(-time.Hour)
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "bezisync-flink-" + shortID, Organization: []string{"BeziSync"}},
		NotBefore: now, NotAfter: now.AddDate(20, 0, 0),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	password, err := randomString(24)
	if err != nil {
		return nil, err
	}
	ks, err := pkcs12.Modern.Encode(priv, cert, nil, password)
	if err != nil {
		return nil, err
	}
	return &FlinkSecrets{StateKey: base64.StdEncoding.EncodeToString(key), Keystore: base64.StdEncoding.EncodeToString(ks), KeystorePassword: password}, nil
}

// SavedPosition is one stored checkpoint or savepoint of a Flink CDC pipeline.
type SavedPosition struct {
	ID      int64     `json:"id"`
	Kind    string    `json:"kind"`
	Reason  string    `json:"reason"`
	FlinkID int64     `json:"flink_id"`
	TakenAt time.Time `json:"taken_at"`
	Size    int64     `json:"size"`
	// Current: the pipeline would restore from it (it is after the Start over floor).
	Current bool `json:"current"`
}

func stateTable(shortID string) string {
	return pgx.Identifier{flinkhelper.StateSchema, flinkhelper.TableName(shortID)}.Sanitize()
}

// tableMissing: the pipeline never stored a position (no table or schema yet).
func tableMissing(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == "42P01" || pe.Code == "3F000")
}

// SavedPositions lists a pipeline's saved positions, newest first (none before its first start).
func SavedPositions(ctx context.Context, p *models.Pipeline) ([]SavedPosition, error) {
	conn, err := stateConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, `SELECT id, kind, reason, flink_id, taken_at, size FROM `+stateTable(p.ShortID())+` ORDER BY id DESC`)
	if err != nil {
		if tableMissing(err) {
			return []SavedPosition{}, nil
		}
		return nil, err
	}
	defer rows.Close()
	floor := restoreFloor(p)
	out := []SavedPosition{}
	for rows.Next() {
		var s SavedPosition
		if err := rows.Scan(&s.ID, &s.Kind, &s.Reason, &s.FlinkID, &s.TakenAt, &s.Size); err != nil {
			return nil, err
		}
		s.TakenAt = s.TakenAt.UTC()
		s.Current = s.ID > floor
		out = append(out, s)
	}
	if err := rows.Err(); err != nil && !tableMissing(err) {
		return nil, err
	}
	return out, nil
}

// restoreFloor is artifacts[ArtifactRestoreAfter] (0 when unset).
func restoreFloor(p *models.Pipeline) int64 {
	return int64(toFloat(p.Artifacts[ArtifactRestoreAfter]))
}

// latestSavedID is the newest saved position's id (0 when there is none).
func latestSavedID(ctx context.Context, conn *pgx.Conn, shortID string) (int64, error) {
	var id *int64
	err := conn.QueryRow(ctx, `SELECT max(id) FROM `+stateTable(shortID)).Scan(&id)
	if tableMissing(err) {
		return 0, nil
	}
	if err != nil || id == nil {
		return 0, err
	}
	return *id, nil
}

// DropSavedPositions removes every saved position of a pipeline (it is deleted).
func DropSavedPositions(ctx context.Context, shortID string) error {
	conn, err := stateConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, `DROP TABLE IF EXISTS `+stateTable(shortID))
	if tableMissing(err) {
		return nil
	}
	return err
}

func cloneArtifacts(p *models.Pipeline) db.Map {
	out := db.Map{}
	for k, v := range p.Artifacts {
		out[k] = v
	}
	return out
}

// requireStoppedFlink refuses position changes while the pipeline's cluster still runs.
func requireStoppedFlink(ctx context.Context, q db.Querier, p *models.Pipeline) error {
	if engine.Of(p.Engine) != engine.FlinkCDC {
		return problem.Conflict("engine_unsupported", "Only Flink CDC pipelines keep saved positions.", nil)
	}
	if p.DeployedVersionID == nil {
		return problem.Conflict("not_deployed", "The pipeline has never run; there is no position to change.", nil)
	}
	if p.DesiredState != "paused" {
		return problem.Conflict("not_paused", "Pause the pipeline first.", nil)
	}
	_, rt, err := RequireComplete(ctx, q, p)
	if err != nil {
		return err
	}
	d, err := DriverFor(ctx, q, rt)
	if err != nil {
		return err
	}
	obs, err := d.Observe(ctx, p.ShortID())
	if err != nil {
		return err
	}
	if obs.Phase != "missing" && obs.Phase != "stopped" {
		return problem.Conflict("still_running", "The pipeline is still stopping (saving its position). Try again in a few seconds.", nil)
	}
	return nil
}

// RestoreSavedPosition makes a paused Flink CDC pipeline resume from one of its saved positions:
// the position is copied as the newest one and the floor set just below it.
func RestoreSavedPosition(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, id int64) (*SavedPosition, error) {
	if err := requireStoppedFlink(ctx, q, p); err != nil {
		return nil, err
	}
	if err := restoreBelowFloor(p, id); err != nil {
		return nil, err
	}
	if err := restoreBeforeLayout(p, id); err != nil {
		return nil, err
	}
	conn, err := stateConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()
	table := stateTable(p.ShortID())
	var s SavedPosition
	err = conn.QueryRow(ctx, `INSERT INTO `+table+` (kind, reason, flink_id, path, size, sha256, payload, taken_at)
		SELECT kind, 'restored from #' || id, flink_id, path, size, sha256, payload, taken_at FROM `+table+` WHERE id = $1
		RETURNING id, kind, reason, flink_id, taken_at, size`, id).Scan(&s.ID, &s.Kind, &s.Reason, &s.FlinkID, &s.TakenAt, &s.Size)
	if errors.Is(err, pgx.ErrNoRows) || tableMissing(err) {
		return nil, problem.NotFound("Saved position")
	}
	if err != nil {
		return nil, err
	}
	s.Current, s.TakenAt = true, s.TakenAt.UTC()
	copyDone := CopyDoneSave(p)
	artifacts := cloneArtifacts(p)
	artifacts[ArtifactRestoreAfter] = s.ID - 1
	delete(artifacts, ArtifactStartup)
	p.Artifacts = artifacts
	// A position saved during the first copy goes on with the copy: the copy shape first (R28).
	grew := copyDone > 0 && id < copyDone && GrowBack(p) // id: the position restored (s is its copy)
	if err := SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	_, err = Record(ctx, q, AuditEvent{Kind: "pipeline.position_restored", Summary: fmt.Sprintf("Restored %s to saved position #%d (%s)", p.Name, id, s.TakenAt.Format(time.RFC3339)),
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID, Details: map[string]any{"saved_position": id, "grew_back": grew}})
	return &s, err
}

// resetFlink starts a paused Flink CDC pipeline over: every saved position is set aside (kept in
// History until it expires) and the next start uses startup mode: "resnapshot" copies every table
// again, "skip_to_now" reads changes from now, "from_time" from fromMS (MySQL, SQL Server). A
// PostgreSQL replication slot is moved to the current position.
func resetFlink(ctx context.Context, q db.Querier, a *auth.ProjectAccess, p *models.Pipeline, mode string, fromMS int64) (map[string]any, error) {
	source, err := GetConnection(ctx, q, p.SourceID)
	if err != nil {
		return nil, err
	}
	caps := engine.CapsFor(engine.FlinkCDC, source.Type)
	startup := map[string]any{}
	switch mode {
	case "resnapshot":
		startup["mode"] = "initial"
	case "skip_to_now":
		startup["mode"] = "latest-offset"
	case "from_time":
		if !caps.StartFromTime {
			return nil, problem.New(422, "engine_unsupported", caps.Why["start_from_time"])
		}
		if fromMS <= 0 || time.UnixMilli(fromMS).After(time.Now().Add(time.Minute)) {
			return nil, problem.New(422, "invalid_time", "Choose a time in the past.")
		}
		startup["mode"], startup["timestamp_ms"] = "timestamp", fromMS
	default:
		return nil, problem.New(422, "invalid_mode", "Unknown mode '"+mode+"'.")
	}
	conn, err := stateConn(ctx)
	if err != nil {
		return nil, err
	}
	latest, err := latestSavedID(ctx, conn, p.ShortID())
	_ = conn.Close(ctx)
	if err != nil {
		return nil, err
	}
	results := []any{map[string]any{"kind": "saved_positions", "name": flinkhelper.TableName(p.ShortID()), "outcome": "set aside"}}
	if source.Type == "postgresql" {
		spec, err := SpecOf(ctx, q, p.DeployedVersionID)
		if err != nil {
			return nil, err
		}
		src, err := connectors.GetSource(source.Type)
		if err != nil {
			return nil, err
		}
		resolved, err := Resolve(ctx, q, source)
		if err != nil {
			return nil, err
		}
		cctx := connectors.CompileContext{PipelineID: p.ID.String(), ShortID: p.ShortID(), Name: p.Name, Spec: *spec}
		forgot, err := src.ForgetPosition(ctx, resolved, cctx)
		if err != nil {
			return nil, err
		}
		for _, r := range forgot {
			m, _ := ToMap(r)
			results = append(results, m)
		}
	}
	artifacts := cloneArtifacts(p)
	artifacts[ArtifactRestoreAfter] = latest
	artifacts[ArtifactStartup] = startup
	p.Artifacts = artifacts
	// Copying every table again needs the copy shape (R27).
	grew := mode == "resnapshot" && GrowBack(p)
	if err := SavePipeline(ctx, q, p); err != nil {
		return nil, err
	}
	labels := map[string]string{"resnapshot": "copy every table again", "skip_to_now": "changes from now", "from_time": "changes since " + time.UnixMilli(fromMS).UTC().Format(time.RFC3339)}
	if _, err := Record(ctx, q, AuditEvent{Kind: "pipeline.offsets_reset", Summary: fmt.Sprintf("Started %s over (%s)", p.Name, labels[mode]),
		ProjectID: &a.Project.ID, Principal: a.Principal, ResourceType: "pipeline", ResourceID: &p.ID,
		Details: map[string]any{"mode": mode, "results": results, "set_aside_through": latest, "grew_back": grew}}); err != nil {
		return nil, err
	}
	return map[string]any{"mode": mode, "results": results}, nil
}

// Saved positions set aside by Start over or Restore (at or below the floor), and savepoints
// beyond the newest keptSavepoints, expire after SavedPositionRetention unless someone kept them.
// Checkpoints are pruned by the helper as it saves (newest 5).
const (
	SavedPositionRetention = 7 * 24 * time.Hour
	keptSavepoints         = 10
)

// ExpireSavedPositions removes expired saved positions of every Flink CDC pipeline.
func ExpireSavedPositions(ctx context.Context, q db.Querier, now time.Time) (int64, error) {
	pipes, err := db.Select[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE engine = $1 AND deleted_at IS NULL`, string(engine.FlinkCDC))
	if err != nil || len(pipes) == 0 {
		return 0, err
	}
	conn, err := stateConn(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()
	var removed int64
	for i := range pipes {
		p := &pipes[i]
		t := stateTable(p.ShortID())
		tag, err := conn.Exec(ctx, `DELETE FROM `+t+` WHERE NOT kept AND taken_at < $1 AND (id <= $2 OR (kind = 'savepoint' AND id NOT IN (
			SELECT id FROM `+t+` WHERE kind = 'savepoint' AND id > $2 ORDER BY id DESC LIMIT `+fmt.Sprint(keptSavepoints)+`)))`,
			now.Add(-SavedPositionRetention), restoreFloor(p))
		if tableMissing(err) {
			continue
		}
		if err != nil {
			return removed, err
		}
		removed += tag.RowsAffected()
	}
	return removed, nil
}
