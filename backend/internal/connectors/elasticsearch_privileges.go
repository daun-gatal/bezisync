package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// Elasticsearch index rights, per pipeline and without creating anything (ADR 0016 D5): one
// HEAD per index the pipeline writes, then _has_privileges for those names. Elasticsearch
// answers for names that don't exist yet, so nothing is created to find out.

// esPrivilegesPath is the _has_privileges endpoint of a server version; "" below 6.4 (none).
func esPrivilegesPath(major, minor int) string {
	switch {
	case major > 6:
		return "/_security/user/_has_privileges"
	case major == 6 && minor >= 4:
		return "/_xpack/security/user/_has_privileges"
	}
	return ""
}

// esMajorMinor reads "8.15.3".
func esMajorMinor(number string) (major, minor int, ok bool) {
	parts := strings.SplitN(number, ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	return major, minor, err1 == nil && err2 == nil
}

// esPrivileges are the privileges asked: writing and deleting documents, creating a missing index
// (create_index, or auto_configure from 7.9), and delete_index for "drop destinations".
func esPrivileges(major, minor int) []string {
	p := []string{"index", "delete", "create_index", "delete_index"}
	if major > 7 || (major == 7 && minor >= 9) {
		p = append(p, "auto_configure")
	}
	return p
}

// esRightsVerdict sorts the indexes by what their user lacks, from _has_privileges' answer.
// exists says which indexes are already there (they need no create right).
type esRightsVerdict struct {
	CannotWrite, CannotCreate, CannotDrop []string
}

func esJudgeRights(names []string, exists map[string]bool, answer map[string]map[string]bool) esRightsVerdict {
	var v esRightsVerdict
	for _, n := range names {
		has := answer[n]
		if !has["index"] || !has["delete"] {
			v.CannotWrite = append(v.CannotWrite, n)
		}
		if !exists[n] && !has["create_index"] && !has["auto_configure"] {
			v.CannotCreate = append(v.CannotCreate, n)
		}
		if !has["delete_index"] {
			v.CannotDrop = append(v.CannotDrop, n)
		}
	}
	return v
}

// esList shows up to five names.
func esList(names []string) string {
	if len(names) > 5 {
		return strings.Join(names[:5], ", ") + fmt.Sprintf(" and %d more", len(names)-5)
	}
	return strings.Join(names, ", ")
}

func esRightsFix(names []string) *checks.Fix {
	return &checks.Fix{Kind: "text", Who: "Elasticsearch admin", Code: "Give the user the index, delete and create_index (or, from 7.9, auto_configure) privileges on " +
		esList(names) + ", and delete_index for \"drop destinations\"; allow automatic index creation (action.auto_create_index) for them."}
}

// CheckPipeline checks the user's rights on the pipeline's indexes.
func (s *elasticsearchSink) CheckPipeline(ctx context.Context, conn Resolved, c CompileContext, facts PipelineFacts) []checks.Check {
	cfg, err := esCfg(conn)
	if err != nil {
		return nil // the connection check reports the settings
	}
	seen := map[string]bool{}
	var names []string
	for _, routed := range facts.Destinations {
		if n := s.FlinkDestination(conn, c, routed); !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil
	}
	check := func(status, msg, tech string) []checks.Check {
		out := checks.Check{Code: "elasticsearch_create_index", Title: "Index rights", Status: status, Category: checks.CatPermissions,
			Message: msg, Technical: firstRunes(tech, 300)}
		if status != checks.Pass {
			out.Fix = esRightsFix(names)
		}
		out.Blocking = status == checks.Fail
		return []checks.Check{out}
	}
	unknown := func(msg, tech string) []checks.Check {
		return check(checks.Unknown, msg+" Index rights aren't checked before deploy; the first change written shows a missing one.", tech)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	code, body, err := esCall(ctx, cfg, http.MethodGet, "/", nil, 10*time.Second)
	if err != nil {
		return unknown("BeziSync couldn't reach Elasticsearch to ask which index rights the user has.", err.Error())
	}
	if code != http.StatusOK {
		return unknown(fmt.Sprintf("Elasticsearch answered HTTP %d when asked for its version.", code), string(body))
	}
	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	_ = json.Unmarshal(body, &info)
	major, minor, ok := esMajorMinor(info.Version.Number)
	path := esPrivilegesPath(major, minor)
	if !ok || path == "" {
		return unknown("Elasticsearch "+info.Version.Number+" can't say which rights a user has (it answers that from 6.4).", "")
	}
	exists := map[string]bool{}
	for _, n := range names {
		code, body, err := esCall(ctx, cfg, http.MethodHead, "/"+url.PathEscape(n), nil, 10*time.Second)
		switch {
		case err != nil:
			return unknown("BeziSync couldn't look up the index "+n+".", err.Error())
		case code == http.StatusOK:
			exists[n] = true
		case code == http.StatusNotFound:
		default:
			return unknown(fmt.Sprintf("Elasticsearch answered HTTP %d when looking up the index %s.", code, n), string(body))
		}
	}
	req := map[string]any{"index": []any{map[string]any{"names": names, "privileges": esPrivileges(major, minor)}}}
	code, body, err = esCall(ctx, cfg, http.MethodPost, path, req, 10*time.Second)
	if err != nil {
		return unknown("BeziSync couldn't ask Elasticsearch which index rights the user has.", err.Error())
	}
	if code != http.StatusOK {
		// Security disabled answers like a refusal (security_exception), so no answer here passes.
		return unknown(fmt.Sprintf("Elasticsearch didn't say which index rights the user has (HTTP %d; it can't when security is disabled).", code), string(body))
	}
	var answer struct {
		Index map[string]map[string]bool `json:"index"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return unknown("Elasticsearch's answer about index rights couldn't be read.", string(body))
	}
	for _, n := range names {
		if _, ok := answer.Index[n]; !ok {
			return unknown("Elasticsearch's answer about index rights left out "+n+".", string(body))
		}
	}
	v := esJudgeRights(names, exists, answer.Index)
	var problems []string
	if len(v.CannotWrite) > 0 {
		problems = append(problems, "can't write or delete documents in "+esList(v.CannotWrite))
	}
	if len(v.CannotCreate) > 0 {
		problems = append(problems, "can't create "+esList(v.CannotCreate)+" (not there yet; the sink creates it with the first document)")
	}
	switch {
	case len(problems) > 0:
		return check(checks.Fail, "The user "+strings.Join(problems, ", and ")+".", "")
	case len(v.CannotDrop) > 0:
		return check(checks.Warn, "The user may write these indexes but not delete them ("+esList(v.CannotDrop)+
			"): deleting the pipeline with \"drop destinations\" would fail.", "")
	}
	return check(checks.Pass, "The user may write, delete documents in and create "+esList(names)+".", "")
}
