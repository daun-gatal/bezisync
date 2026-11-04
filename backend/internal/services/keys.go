package services

import (
	"context"
	"fmt"

	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/security"
)

// RotateKeys re-encrypts rows not yet under the current master key; counts per table.
func RotateKeys(ctx context.Context, q db.Querier) (map[string]int, []string, error) {
	current := security.CurrentKeyID()
	tables := []struct{ table, column string }{
		{"connections", "secrets_enc"}, {"runtimes", "secrets_enc"}, {"secret_stores", "secrets_enc"},
		{"alert_channels", "secrets_enc"}, {"agent_workloads", "spec_enc"}, {"sso_providers", "client_secret_enc"},
		{"pipelines", "engine_secrets_enc"},
	}
	counts := map[string]int{}
	order := []string{}
	for _, t := range tables {
		rows, err := db.Select[struct {
			ID    string  `db:"id"`
			Value *string `db:"value"`
		}](ctx, q, fmt.Sprintf(`SELECT id::text AS id, %s AS value FROM %s`, t.column, t.table))
		if err != nil {
			return nil, nil, err
		}
		changed := 0
		for _, r := range rows {
			if r.Value == nil || *r.Value == "" || security.SecretKeyID(*r.Value) == current {
				continue
			}
			plain, err := security.DecryptSecret(*r.Value)
			if err != nil {
				return nil, nil, err
			}
			enc, err := security.EncryptSecret(plain)
			if err != nil {
				return nil, nil, err
			}
			if _, err := q.Exec(ctx, fmt.Sprintf(`UPDATE %s SET %s = $1 WHERE id = $2::uuid`, t.table, t.column), enc, r.ID); err != nil {
				return nil, nil, err
			}
			changed++
		}
		counts[t.table] = changed
		order = append(order, t.table)
	}
	n, err := rotateSAMLKeys(ctx, q)
	if err != nil {
		return nil, nil, err
	}
	counts["instance_state"] = n
	order = append(order, "instance_state")
	return counts, order, nil
}
