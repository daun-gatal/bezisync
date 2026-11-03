package services

import (
	"fmt"
	"sort"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// RequiredSecrets lists a secret schema's required fields.
func RequiredSecrets(schema *om.Map) []string {
	if schema == nil {
		return nil
	}
	raw, ok := schema.Get("required")
	if !ok {
		return nil
	}
	list, _ := raw.([]any)
	out := make([]string, 0, len(list))
	for _, v := range list {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// MissingSecrets lists the required secret fields that have no value (a reference counts as one).
func MissingSecrets(schema *om.Map, values map[string]any) []string {
	out := []string{}
	for _, f := range RequiredSecrets(schema) {
		if v, ok := values[f]; !ok || v == nil || v == "" {
			out = append(out, f)
		}
	}
	return out
}

// WithoutRequired is a secret schema whose fields are all optional: copies to another project
// may leave secrets for the people there to enter.
func WithoutRequired(schema *om.Map) *om.Map {
	if schema == nil {
		return nil
	}
	out := om.New()
	for _, k := range schema.Keys() {
		if k == "required" {
			continue
		}
		v, _ := schema.Get(k)
		out.Set(k, v)
	}
	return out
}

// ConnectionMissingSecrets lists a connection's required secrets that nobody has entered yet
// (a connection copied from another project without them).
func ConnectionMissingSecrets(c *models.Connection) ([]string, error) {
	d, err := connectors.Describe(c.Kind, c.Type)
	if err != nil {
		return nil, err
	}
	stored, err := StoredSecrets(c)
	if err != nil {
		return nil, err
	}
	return MissingSecrets(d.SecretSchema, stored), nil
}

// MissingSecret means a connection lacks a required secret (422, a blocking check in preflight).
func MissingSecret(c *models.Connection, fields []string) *problem.Problem {
	return problem.New(422, "secret_missing", fmt.Sprintf("Connection '%s' has no %s yet: enter it on the connection before using it.", c.Name, strings.Join(fields, ", ")))
}
