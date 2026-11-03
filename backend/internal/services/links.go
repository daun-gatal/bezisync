package services

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Linked connections: a config field (descriptor "ui.connection") that names another connection
// of the project, e.g. the Kafka a Spanner source coordinates through. The field stores the
// connection's id, so renaming it keeps the link.

// NormalizeLinks replaces each linked-connection value (an id or a name) in a validated config
// with the id of a connection of the right kind and type in the project.
func NormalizeLinks(ctx context.Context, q db.Querier, projectID uuid.UUID, kind, typ string, config map[string]any) error {
	d, err := connectors.Describe(kind, typ)
	if err != nil {
		return err
	}
	for _, field := range sortedLinkFields(d) {
		value := strings.TrimSpace(connectors.Str(config, field))
		if value == "" {
			continue
		}
		target, err := findLinked(ctx, q, projectID, value)
		if err != nil {
			return err
		}
		wantKind, wantType, _ := strings.Cut(d.Links[field], "/")
		if target == nil || target.Kind != wantKind || target.Type != wantType {
			title := wantType
			if td, err := connectors.Describe(wantKind, wantType); err == nil {
				title = td.Title
			}
			return problem.New(422, "linked_connection_invalid", fmt.Sprintf("Choose a %s connection of this project.", title)).
				WithErrors([]map[string]any{{"loc": []string{"config", field}, "message": fmt.Sprintf("no %s connection '%s' in this project", title, value)}})
		}
		config[field] = target.ID.String()
	}
	return nil
}

func findLinked(ctx context.Context, q db.Querier, projectID uuid.UUID, value string) (*models.Connection, error) {
	if id, err := uuid.Parse(value); err == nil {
		return db.Find[models.Connection](ctx, q, `SELECT * FROM connections WHERE project_id = $1 AND id = $2`, projectID, id)
	}
	return db.Find[models.Connection](ctx, q, `SELECT * FROM connections WHERE project_id = $1 AND name = $2`, projectID, value)
}

func sortedLinkFields(d *connectors.Descriptor) []string {
	fields := make([]string, 0, len(d.Links))
	for f := range d.Links {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	return fields
}

// ResolveLinks attaches the linked connections of a resolved connection. A link whose
// connection is gone is left out: the connector reports it as a failed check.
func ResolveLinks(ctx context.Context, q db.Querier, projectID uuid.UUID, kind string, r *connectors.Resolved) error {
	d, err := connectors.Describe(kind, r.Type)
	if err != nil {
		return err
	}
	for _, field := range sortedLinkFields(d) {
		id, err := uuid.Parse(connectors.Str(r.Config, field))
		if err != nil {
			continue
		}
		target, err := db.Find[models.Connection](ctx, q, `SELECT * FROM connections WHERE project_id = $1 AND id = $2`, projectID, id)
		if err != nil {
			return err
		}
		wantKind, wantType, _ := strings.Cut(d.Links[field], "/")
		if target == nil || target.Kind != wantKind || target.Type != wantType {
			continue
		}
		linked, err := resolveOwn(ctx, q, target)
		if err != nil {
			return err
		}
		if r.Linked == nil {
			r.Linked = map[string]connectors.Resolved{}
		}
		r.Linked[field] = linked
	}
	return nil
}

// LinkedBy names the connections of the project that link to this one.
func LinkedBy(ctx context.Context, q db.Querier, c *models.Connection) ([]string, error) {
	var names []string
	for _, d := range connectors.Descriptors() {
		for _, field := range sortedLinkFields(d) {
			if d.Links[field] != c.Kind+"/"+c.Type {
				continue
			}
			rows, err := db.Select[struct {
				Name string `db:"name"`
			}](ctx, q, `SELECT name FROM connections WHERE project_id = $1 AND kind = $2 AND type = $3 AND config->>$4 = $5 ORDER BY name`,
				c.ProjectID, d.Kind, d.Type, field, c.ID.String())
			if err != nil {
				return nil, err
			}
			for _, r := range rows {
				names = append(names, r.Name)
			}
		}
	}
	sort.Strings(names)
	return names, nil
}

// SourceCapture is how the pipeline's source captures, with the source's title.
func SourceCapture(ctx context.Context, q db.Querier, p *models.Pipeline) (connectors.CaptureCaps, string, error) {
	source, err := GetConnection(ctx, q, p.SourceID)
	if err != nil || source == nil {
		return connectors.CaptureCaps{}, "", err
	}
	d, err := connectors.Describe("source", source.Type)
	if err != nil {
		return connectors.CaptureCaps{}, "", err
	}
	return d.Capture, d.Title, nil
}
