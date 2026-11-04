package services

import (
	"context"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// The image catalog: which images of each engine Admins offer, and each engine's effective
// default. The install default of an engine (BEZISYNC_DEBEZIUM_DEFAULT_IMAGE,
// BEZISYNC_FLINK_CDC_DEFAULT_IMAGE) is offered when set; nothing is seeded. Flink CDC has no
// install default unless an Admin sets one: until a Flink CDC image is registered, the engine is
// unavailable (EngineAvailable).

// InstallDefaultImage is an engine's image from configuration.
func InstallDefaultImage(id engine.ID) string {
	if id == engine.FlinkCDC {
		return config.Get().FlinkCDCDefaultImage
	}
	return config.Get().DebeziumDefaultImage
}

// DefaultImage is the Debezium catalog default or the install default.
func DefaultImage(ctx context.Context, q db.Querier) (string, error) {
	return DefaultImageFor(ctx, q, engine.Debezium)
}

// DefaultImageFor is an engine's catalog default or its install default.
func DefaultImageFor(ctx context.Context, q db.Querier, id engine.ID) (string, error) {
	img, err := db.Find[struct {
		Image string `db:"image"`
	}](ctx, q, `SELECT image FROM debezium_images WHERE is_default IS TRUE AND engine = $1 LIMIT 1`, string(id))
	if err != nil {
		return "", err
	}
	if img != nil && img.Image != "" {
		return img.Image, nil
	}
	if install := InstallDefaultImage(id); install != "" {
		return install, nil
	}
	// No default chosen: the oldest offered image, if any ("" when the engine has none).
	first, err := db.Find[struct {
		Image string `db:"image"`
	}](ctx, q, `SELECT image FROM debezium_images WHERE engine = $1 AND deprecated IS FALSE ORDER BY created_at LIMIT 1`, string(id))
	if err != nil || first == nil {
		return "", err
	}
	return first.Image, nil
}

// Availability says whether an engine can run pipelines on this install, and why not.
type Availability struct {
	Available bool
	Reason    string
}

// EngineAvailable reports an engine's availability: Debezium always; Flink CDC once an Admin has
// registered a Flink CDC image (the install default or a catalog image that is not deprecated).
func EngineAvailable(ctx context.Context, q db.Querier, id engine.ID) (Availability, error) {
	if id != engine.FlinkCDC {
		return Availability{Available: true}, nil
	}
	image, err := DefaultImageFor(ctx, q, id)
	if err != nil {
		return Availability{}, err
	}
	if image == "" {
		return Availability{Reason: "Flink CDC isn't turned on: an Admin registers a Flink CDC image under Settings → Engine versions."}, nil
	}
	return Availability{Available: true}, nil
}

// CatalogEntry is one offered image.
type CatalogEntry struct {
	Image, Label, Notes   string
	IsDefault, Deprecated bool
	// Builtin is the install default from configuration (cannot be edited or removed).
	Builtin bool
	Row     *models.DebeziumImage
}

// CatalogEntries lists the offered Debezium images (the install default first when not a row).
func CatalogEntries(ctx context.Context, q db.Querier) ([]CatalogEntry, error) {
	return CatalogEntriesFor(ctx, q, engine.Debezium)
}

// CatalogEntriesFor lists an engine's offered images (the install default first when not a row).
func CatalogEntriesFor(ctx context.Context, q db.Querier, id engine.ID) ([]CatalogEntry, error) {
	rows, err := db.Select[models.DebeziumImage](ctx, q, `SELECT * FROM debezium_images WHERE engine = $1 ORDER BY created_at`, string(id))
	if err != nil {
		return nil, err
	}
	install := InstallDefaultImage(id)
	hasDefault, listed := false, install == ""
	for _, r := range rows {
		hasDefault = hasDefault || r.IsDefault
		listed = listed || r.Image == install
	}
	var out []CatalogEntry
	if !listed {
		out = append(out, CatalogEntry{Image: install, Label: "Install default", IsDefault: !hasDefault, Builtin: true})
	}
	for i := range rows {
		r := rows[i]
		out = append(out, CatalogEntry{Image: r.Image, Label: r.Label, Notes: r.Notes, IsDefault: r.IsDefault, Deprecated: r.Deprecated, Row: &rows[i]})
	}
	return out, nil
}

// Standing is how the Debezium catalog regards an image: default, offered, deprecated, unlisted.
func Standing(ctx context.Context, q db.Querier, image string) (string, error) {
	return StandingFor(ctx, q, engine.Debezium, image)
}

// StandingFor is how an engine's catalog regards an image.
func StandingFor(ctx context.Context, q db.Querier, id engine.ID, image string) (string, error) {
	entries, err := CatalogEntriesFor(ctx, q, id)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.Image == image {
			switch {
			case e.IsDefault:
				return "default", nil
			case e.Deprecated:
				return "deprecated", nil
			}
			return "offered", nil
		}
	}
	return "unlisted", nil
}
