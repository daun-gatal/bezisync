package domain

import (
	"fmt"
	"slices"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// Personal data is protected per pipeline: each captured column a source reports as looking
// like personal data is masked (salted hash), removed, or kept as it is on purpose
// (spec.source.columns). These checks run in preflight's Configuration section.

// DataColumn is what preflight knows about one column of a captured table.
type DataColumn struct {
	Name, Type string
	PII        bool
	// Maskable: the source can hash this column (character types only).
	Maskable bool
}

const personalTitle = "Personal data"

func personalFix(text string) *checks.Fix {
	return &checks.Fix{Kind: "text", Who: "you", Code: text}
}

// PersonalDataChecks checks the pipeline's column rules against the captured tables' columns
// (table → columns; nil when the source could not be read).
func PersonalDataChecks(spec PipelineSpec, tables map[string][]DataColumn, sourceTitle string, sourceCanMask bool, discoveryErr error) []checks.Check {
	if discoveryErr != nil {
		return []checks.Check{{Code: "pii_unknown", Title: personalTitle, Category: checks.CatTables, Status: checks.Warn,
			Message:   "Could not read the tables' columns, so BeziSync could not check which ones hold personal data.",
			Technical: Truncate(discoveryErr.Error(), 200)}}
	}
	var unmaskable, missing, unprotected []string
	masked, removed := 0, 0
	for _, table := range spec.Source.Tables.Include {
		cols, ok := tables[table]
		if !ok {
			continue // a missing table is the source section's concern
		}
		byName := make(map[string]DataColumn, len(cols))
		for _, c := range cols {
			byName[c.Name] = c
		}
		rules := spec.Source.Columns[table]
		handled := map[string]bool{}
		for _, list := range [][]string{rules.Exclude, rules.Mask, rules.Keep} {
			for _, name := range list {
				if _, ok := byName[name]; !ok && !handled[name] {
					missing = append(missing, table+"."+name)
				}
				handled[name] = true
			}
		}
		for _, name := range rules.Exclude {
			if _, ok := byName[name]; ok {
				removed++
			}
		}
		for _, name := range rules.Mask {
			c, ok := byName[name]
			if !ok || slices.Contains(rules.Exclude, name) {
				continue // removing wins over masking
			}
			if !c.Maskable {
				unmaskable = append(unmaskable, fmt.Sprintf("%s.%s (%s)", table, name, c.Type))
				continue
			}
			masked++
		}
		for _, c := range cols {
			if c.PII && !handled[c.Name] {
				unprotected = append(unprotected, table+"."+c.Name)
			}
		}
	}
	var out []checks.Check
	if len(unmaskable) > 0 {
		why := "Debezium hashes text columns only and sends any other column as it is."
		if !sourceCanMask {
			why = fmt.Sprintf("Debezium cannot hash values read from %s, so a masked column would be sent as it is.", sourceTitle)
		}
		out = append(out, checks.Check{Code: "pii_mask_unsupported", Title: personalTitle, Category: checks.CatTables, Status: checks.Fail, Blocking: true,
			Message: "These columns can't be masked, so they would reach the destination as they are: " + strings.Join(unmaskable, ", ") + ".",
			Why:     why,
			Fix:     personalFix("In Tables, choose Remove (or Keep as is) for each listed column.")})
	}
	if len(unprotected) > 0 {
		out = append(out, checks.Check{Code: "pii_unprotected", Title: personalTitle, Category: checks.CatTables, Status: checks.Warn,
			Message: "These columns look like personal data and are sent as they are: " + strings.Join(unprotected, ", ") + ".",
			Why:     "Their names suggest personal data (email, phone, address, birth date, card or ID numbers), and nobody chose what to do with them yet.",
			Fix:     personalFix("In Tables, choose Mask or Remove for each listed column, or Keep as is when it may be sent.")})
	}
	if len(missing) > 0 {
		out = append(out, checks.Check{Code: "pii_rule_column_missing", Title: personalTitle, Category: checks.CatTables, Status: checks.Warn,
			Message: "These columns have a masking rule but are not in their table, so the rule does nothing: " + strings.Join(missing, ", ") + ".",
			Why:     "The column was renamed or dropped after the rule was made. If it was renamed, the new name is sent as it is.",
			Fix:     personalFix("In Tables, check the table's columns and choose again.")})
	}
	if len(out) == 0 {
		msg := "No captured column looks like personal data."
		if masked+removed > 0 {
			msg = fmt.Sprintf("%d masked and %d removed column(s); nothing that looks like personal data is sent as it is.", masked, removed)
		}
		out = append(out, checks.Check{Code: "pii_protected", Title: personalTitle, Category: checks.CatTables, Status: checks.Pass, Message: msg})
	}
	return out
}
