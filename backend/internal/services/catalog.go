package services

import (
	"context"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// The Debezium image catalog: which images Admins offer, and the effective default. The
// install default (BEZISYNC_DEBEZIUM_DEFAULT_IMAGE) is always offered; nothing is seeded.

// DefaultImage is the catalog default or the install default.
func DefaultImage(ctx context.Context, q db.Querier) (string, error) {
	img, err := db.Find[struct {
		Image string `db:"image"`
	}](ctx, q, `SELECT image FROM debezium_images WHERE is_default IS TRUE LIMIT 1`)
	if err != nil {
		return "", err
	}
	if img != nil && img.Image != "" {
		return img.Image, nil
	}
	return config.Get().DebeziumDefaultImage, nil
}

// CatalogEntry is one offered image.
type CatalogEntry struct {
	Image, Label, Notes   string
	IsDefault, Deprecated bool
	// Builtin is the install default from configuration (cannot be edited or removed).
	Builtin bool
	Row     *models.DebeziumImage
}

// CatalogEntries lists the offered images (the install default first when not a row).
func CatalogEntries(ctx context.Context, q db.Querier) ([]CatalogEntry, error) {
	rows, err := db.Select[models.DebeziumImage](ctx, q, `SELECT * FROM debezium_images ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	install := config.Get().DebeziumDefaultImage
	hasDefault, listed := false, false
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

// Standing is how the catalog regards an image: default, offered, deprecated, unlisted.
func Standing(ctx context.Context, q db.Querier, image string) (string, error) {
	entries, err := CatalogEntries(ctx, q)
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
