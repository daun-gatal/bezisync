// Package checks is the preflight check model (Check, Fix, CheckReport) shared by
// connectors, deploy preflight, secret stores and policies, plus the check reference.
package checks

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Status values.
const (
	Pass = "pass"
	Warn = "warn"
	Fail = "fail"
	Skip = "skip"
	// Unknown is a check BeziSync can't verify (grey, never healthy, not blocking).
	Unknown = "unknown"
)

// Categories.
const (
	CatNetwork     = "network"
	CatAuth        = "auth"
	CatCDC         = "cdc_setup"
	CatPermissions = "permissions"
	CatCapacity    = "capacity"
	CatTables      = "tables"
	CatResilience  = "resilience"
	CatDestination = "destination"
	CatPolicy      = "policy"
)

// Fix is a copyable remediation; Kind tells the UI how to render Code.
type Fix struct {
	Kind            string `json:"kind"`
	Code            string `json:"code"`
	RestartRequired bool   `json:"restart_required"`
	Who             string `json:"who"`
}

// NewFix has Python's defaults (kind "text", who "DBA").
func NewFix(code string) *Fix {
	return &Fix{Kind: "text", Code: code, Who: "DBA"}
}

// Check is one preflight result.
type Check struct {
	Code      string `json:"code"`
	Title     string `json:"title"`
	Status    string `json:"status"`
	Category  string `json:"category"`
	Message   string `json:"message"`
	Technical string `json:"technical"`
	Why       string `json:"why"`
	Fix       *Fix   `json:"fix"`
	Blocking  bool   `json:"blocking"`
	Doc       string `json:"doc"`
}

// Failed reports status fail.
func (c *Check) Failed() bool { return c.Status == Fail }

// Report is a CheckReport.
type Report struct {
	OK         bool    `json:"ok"`
	Summary    string  `json:"summary"`
	Checks     []Check `json:"checks"`
	DurationMS int     `json:"duration_ms"`
	CheckedAt  string  `json:"checked_at"`
}

// Summarize builds a report from checks (adds doc links, counts failures and warnings).
func Summarize(list []Check, started time.Time) Report {
	if list == nil {
		list = []Check{}
	}
	fails, warns, unknowns := 0, 0, 0
	for i := range list {
		if list[i].Doc == "" {
			list[i].Doc = DocURL(list[i].Code)
		}
		switch list[i].Status {
		case Fail:
			fails++
		case Warn:
			warns++
		case Unknown:
			unknowns++
		}
	}
	var summary string
	switch {
	case fails > 0:
		s, verb := "s", "block"
		if fails == 1 {
			s, verb = "", "blocks"
		}
		summary = fmt.Sprintf("%d issue%s %s deploy", fails, s, verb)
	case warns > 0:
		s := "s"
		if warns == 1 {
			s = ""
		}
		summary = fmt.Sprintf("Ready · %d warning%s", warns, s)
	case unknowns > 0:
		summary = fmt.Sprintf("Ready · %d not verified", unknowns)
	default:
		summary = "Ready"
	}
	return Report{
		OK:         fails == 0,
		Summary:    summary,
		Checks:     list,
		DurationMS: int(time.Since(started).Milliseconds()),
		CheckedAt:  pytime.ISOFormat(time.Now()),
	}
}

// Run collects checks; helpers keep connector code declarative.
type Run struct {
	Checks []Check
}

// Add appends a check.
func (r *Run) Add(c Check) { r.Checks = append(r.Checks, c) }

// Passed records a passing check.
func (r *Run) Passed(code, title, category, message, technical string) {
	r.Add(Check{Code: code, Title: title, Status: Pass, Category: category, Message: message, Technical: technical})
}

// Blocked reports whether any check failed.
func (r *Run) Blocked() bool {
	for _, c := range r.Checks {
		if c.Status == Fail {
			return true
		}
	}
	return false
}

// DocsURL is where the generated check reference lives.
const DocsURL = "https://github.com/daun-gatal/bezisync/blob/main/docs/reference/checks.md"

// DocURL links a check code to its reference entry.
func DocURL(code string) string {
	return DocsURL + "#" + strings.ReplaceAll(code, "_", "-")
}

// Entry documents one check code in the reference.
type Entry struct {
	Code    string   `json:"code"`
	Section string   `json:"section"`
	Titles  []string `json:"titles"`
	Whys    []string `json:"whys"`
	Fixes   []string `json:"fixes"`
	// Also lists the other sections the check runs for: a Flink CDC check that reads a source
	// is linked from that source's section too.
	Also []string `json:"also,omitempty"`
}

// Connector names the connection type a reference section is about.
type Connector struct {
	Kind string `json:"kind"` // source or sink
	Type string `json:"type"`
}

// Catalog is the check reference (internal/checks/catalog.json). Every check code the
// product can emit is listed here; a test scans the Go source to keep it complete.
type Catalog struct {
	Sections []string `json:"sections"`
	Entries  []Entry  `json:"entries"`
	// Connectors maps the sections about one source or destination to its connection type, so
	// each says which engines read or write it.
	Connectors map[string]Connector `json:"connectors"`
}

// EnginesLine says which engines read or write a connection type (from engine.Supports).
func EnginesLine(c Connector) string {
	if c.Kind == "source" {
		if engine.Supports(engine.FlinkCDC, c.Type, "") {
			return "Read by Debezium and by Flink CDC."
		}
		return "Read by Debezium only."
	}
	switch {
	case engine.FlinkOnlySink(c.Type):
		return "Written by Flink CDC only."
	case engine.Supports(engine.FlinkCDC, "postgresql", c.Type):
		return "Written by Debezium (the default) and by Flink CDC."
	default:
		return "Written by Debezium only."
	}
}

//go:embed catalog.json
var catalogJSON []byte

// LoadCatalog parses the embedded reference.
func LoadCatalog() Catalog {
	var c Catalog
	if err := json.Unmarshal(catalogJSON, &c); err != nil {
		panic(fmt.Sprintf("checks: invalid catalog.json: %v", err))
	}
	return c
}

// Render produces docs/reference/checks.md.
func Render(c Catalog) string {
	byCode := map[string]Entry{}
	for _, e := range c.Entries {
		byCode[e.Code] = e
	}
	lines := []string{
		"# Preflight check reference",
		"",
		"Every check BeziSync runs on connections, runtimes and deployments. Generated from the",
		"source by `bezisync check-docs` — do not edit by hand. In the UI each check links here.",
		"",
	}
	for _, section := range c.Sections {
		var codes []string
		for code, e := range byCode {
			if e.Section == section {
				codes = append(codes, code)
			}
		}
		if len(codes) == 0 {
			continue
		}
		sort.Strings(codes)
		lines = append(lines, "## "+section, "")
		if conn, ok := c.Connectors[section]; ok {
			lines = append(lines, "*"+EnginesLine(conn)+"*", "")
		}
		var also []string
		for code, e := range byCode {
			if slices.Contains(e.Also, section) {
				also = append(also, code)
			}
		}
		if len(also) > 0 {
			sort.Strings(also)
			lines = append(lines, "On Flink CDC, these checks run for it too:", "")
			for _, code := range also {
				e := byCode[code]
				title := strings.Join(e.Titles, " / ")
				lines = append(lines, "- ["+title+"](#"+strings.ReplaceAll(code, "_", "-")+") (`"+code+"`, under "+e.Section+")")
			}
			lines = append(lines, "")
		}
		for _, code := range codes {
			e := byCode[code]
			anchor := strings.ReplaceAll(code, "_", "-")
			lines = append(lines, `<a id="`+anchor+`"></a>`, "### `"+code+"`", "")
			title := strings.Join(e.Titles, " / ")
			if title == "" {
				title = code
			}
			lines = append(lines, "**"+title+"**", "")
			if len(e.Whys) > 0 {
				for _, why := range e.Whys {
					lines = append(lines, "- Why: "+why)
				}
				lines = append(lines, "")
			}
			for _, fix := range e.Fixes {
				lines = append(lines, "Fix:", "", "```", strings.TrimSpace(fix), "```", "")
			}
		}
	}
	return strings.TrimRight(strings.Join(lines, "\n"), " \t\n\r") + "\n"
}
