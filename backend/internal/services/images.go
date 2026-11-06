package services

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Debezium Server images: what each pipeline runs, and moving pipelines between versions.

// OfficialRepository is Debezium's own image repository.
const OfficialRepository = "quay.io/debezium/server"

var imageVersionRe = regexp.MustCompile(`^(\d+)\.(\d+)\.\d+\.\w+$`)

// SplitImage turns repo:tag into (repo, tag); digests and untagged images keep "".
func SplitImage(image string) (string, string) {
	if i := strings.Index(image, "@"); i >= 0 {
		return image[:i], ""
	}
	i := strings.LastIndex(image, ":")
	if i < 0 || strings.Contains(image[i+1:], "/") {
		return image, ""
	}
	return image[:i], image[i+1:]
}

var debeziumReleaseRe = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)\.(\w+)$`)

// DebeziumRelease reads an official image's release (3.7.0 and whether it is Final); ok is
// false for other repositories and tags, whose release BeziSync cannot know.
func DebeziumRelease(image string) (release [3]int, final, ok bool) {
	repo, tag := SplitImage(image)
	m := debeziumReleaseRe.FindStringSubmatch(tag)
	if (repo != OfficialRepository && repo != "debezium/server" && repo != "docker.io/debezium/server") || m == nil {
		return release, false, false
	}
	for i := range release {
		release[i], _ = strconv.Atoi(m[i+1])
	}
	return release, m[4] == "Final", true
}

// MinimumDebeziumCheck tells whether the image is new enough for the source (nil when it is,
// or when the source has no minimum).
func MinimumDebeziumCheck(image string, d *connectors.Descriptor) *checks.Check {
	if d == nil || d.Capture.MinDebezium == "" {
		return nil
	}
	var want [3]int
	for i, part := range strings.SplitN(d.Capture.MinDebezium, ".", 3) {
		want[i], _ = strconv.Atoi(part)
	}
	got, final, ok := DebeziumRelease(image)
	if !ok {
		return &checks.Check{Code: "debezium_source_version", Title: "Debezium version", Category: checks.CatDestination, Status: checks.Warn,
			Message: fmt.Sprintf("BeziSync can't tell which Debezium release %s is: %s needs %s or newer.", image, d.Title, d.Capture.MinDebezium)}
	}
	older := got[0] < want[0] || (got[0] == want[0] && (got[1] < want[1] || (got[1] == want[1] && got[2] < want[2])))
	// A pre-release of the minimum release may not have it yet.
	if older || (got == want && !final) {
		return &checks.Check{Code: "debezium_source_version", Title: "Debezium version", Category: checks.CatDestination, Status: checks.Fail, Blocking: true,
			Message: fmt.Sprintf("%s needs Debezium %s or newer; this pipeline runs %s.", d.Title, d.Capture.MinDebezium, image),
			Fix:     &checks.Fix{Kind: "text", Who: "Editor", Code: fmt.Sprintf("Move the pipeline to Debezium %s or newer (Change → Debezium version).", d.Capture.MinDebezium)}}
	}
	return nil
}

// ReleaseNotesURL links official images to their release notes.
func ReleaseNotesURL(image string) *string {
	repo, tag := SplitImage(image)
	m := imageVersionRe.FindStringSubmatch(tag)
	if repo != OfficialRepository || m == nil {
		return nil
	}
	s := fmt.Sprintf("https://debezium.io/releases/%s.%s/release-notes", m[1], m[2])
	return &s
}

// PipelineImage is one pipeline in an image group.
type PipelineImage struct {
	ID           uuid.UUID `json:"id"`
	Name         string    `json:"name"`
	Lifecycle    string    `json:"lifecycle"`
	Health       string    `json:"health"`
	Runtime      *string   `json:"runtime"`
	Pinned       bool      `json:"pinned"`
	PendingImage *string   `json:"pending_image"`
	Source       string    `json:"source"`
}

// ImageUsage groups deployed pipelines by image.
type ImageUsage struct {
	Image           string  `json:"image"`
	Repository      string  `json:"repository"`
	Tag             string  `json:"tag"`
	Official        bool    `json:"official"`
	IsDefault       bool    `json:"is_default"`
	ReleaseNotesURL *string `json:"release_notes_url"`
	Standing        string  `json:"standing"`
	// Sink names the destination whose own Debezium Server build this is (Iceberg), else nil.
	Sink      *string         `json:"sink"`
	Pipelines []PipelineImage `json:"pipelines"`
}

// Usage lists images in use by the project's deployed pipelines.
func Usage(ctx context.Context, q db.Querier, projectID uuid.UUID) ([]ImageUsage, error) {
	def, err := DefaultImage(ctx, q)
	if err != nil {
		return nil, err
	}
	groups := map[string]*ImageUsage{}
	pipelines, err := db.Select[models.Pipeline](ctx, q, `SELECT * FROM pipelines WHERE project_id = $1 AND deleted_at IS NULL AND deployed_version_id IS NOT NULL ORDER BY name`, projectID)
	if err != nil {
		return nil, err
	}
	for i := range pipelines {
		p := &pipelines[i]
		if EngineOf(p) != engine.Debezium {
			continue // Debezium Server images only; Flink CDC images are listed by their engine
		}
		var rt *models.Runtime
		if p.RuntimeID != nil {
			if rt, err = GetRuntime(ctx, q, *p.RuntimeID); err != nil {
				return nil, err
			}
		}
		deployed, err := SpecOf(ctx, q, p.DeployedVersionID)
		if err != nil {
			return nil, err
		}
		if deployed == nil {
			continue
		}
		sinkType, err := SinkTypeOf(ctx, q, p)
		if err != nil {
			return nil, err
		}
		image := ""
		if p.DeployedImage != nil && *p.DeployedImage != "" {
			image = *p.DeployedImage
		} else if image, err = ResolveImage(ctx, q, *deployed, rt, sinkType); err != nil {
			return nil, err
		}
		current, err := SpecOf(ctx, q, p.CurrentVersionID)
		if err != nil {
			return nil, err
		}
		pending := image
		if current != nil {
			if pending, err = ResolveImage(ctx, q, *current, rt, sinkType); err != nil {
				return nil, err
			}
		}
		g, ok := groups[image]
		if !ok {
			repo, tag := SplitImage(image)
			standing, err := Standing(ctx, q, image)
			if err != nil {
				return nil, err
			}
			g = &ImageUsage{Image: image, Repository: repo, Tag: tag, Official: repo == OfficialRepository, IsDefault: image == def,
				ReleaseNotesURL: ReleaseNotesURL(image), Standing: standing, Pipelines: []PipelineImage{}}
			if connectors.InSinkImageFamily(sinkType, image) {
				d, _ := connectors.Describe("sink", sinkType)
				g.Sink, g.Standing = &d.Title, "offered"
			}
			groups[image] = g
		}
		var rtName *string
		if rt != nil {
			rtName = &rt.Name
		}
		var pend *string
		if pending != image {
			pend = &pending
		}
		source := "default"
		switch {
		case deployed.DebeziumImage != nil && *deployed.DebeziumImage != "":
			source = "pinned"
		case rt != nil && rt.DefaultImage != nil && *rt.DefaultImage == image:
			source = "runtime"
		}
		g.Pipelines = append(g.Pipelines, PipelineImage{ID: p.ID, Name: p.Name, Lifecycle: p.Lifecycle, Health: p.Health, Runtime: rtName,
			Pinned: deployed.DebeziumImage != nil, PendingImage: pend, Source: source})
	}
	out := make([]ImageUsage, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDefault != out[j].IsDefault {
			return out[i].IsDefault
		}
		return out[i].Image < out[j].Image
	})
	return out, nil
}

// UpgradeResult is the outcome of moving one pipeline.
type UpgradeResult struct {
	PipelineID uuid.UUID `json:"pipeline_id"`
	Name       string    `json:"name"`
	OK         bool      `json:"ok"`
	Message    string    `json:"message"`
	Version    *int      `json:"version"`
	// Effects are what the release does to the pipeline's settings (omitted: nothing).
	Effects []SettingEffect `json:"effects,omitempty"`
}

func problemDetail(err error) (string, bool) {
	var p *problem.Problem
	if errors.As(err, &p) {
		return p.Detail, true
	}
	return "", false
}

// ChangeImage pins image (or follows the runtime default when nil) on each pipeline; each
// succeeds or fails on its own (savepoints).
func ChangeImage(ctx context.Context, q db.Querier, a *auth.ProjectAccess, ids []uuid.UUID, image *string, deploy bool) ([]UpgradeResult, error) {
	var target *string
	if image != nil && *image != "" {
		target = image
	}
	results := []UpgradeResult{}
	for _, pid := range ids {
		p, err := GetPipeline(ctx, q, a, pid, false)
		if err != nil {
			detail, ok := problemDetail(err)
			if !ok {
				return nil, err
			}
			results = append(results, UpgradeResult{PipelineID: pid, Name: "?", Message: detail})
			continue
		}
		if EngineOf(p) != engine.Debezium {
			results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, Message: "It runs on Flink CDC, which has its own image."})
			continue
		}
		current, err := SpecOf(ctx, q, p.CurrentVersionID)
		if err != nil {
			return nil, err
		}
		if current == nil {
			results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, Message: "The pipeline has no saved version."})
			continue
		}
		live := p.DeployedVersionID != nil
		sameVersion := p.CurrentVersionID != nil && p.DeployedVersionID != nil && *p.CurrentVersionID == *p.DeployedVersionID
		if deploy && live && !sameVersion {
			results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name,
				Message: "It has undeployed changes. Deploy or discard them first, so the upgrade ships alone."})
			continue
		}
		if equalPtr(current.DebeziumImage, target) {
			var rt *models.Runtime
			if p.RuntimeID != nil {
				if rt, err = GetRuntime(ctx, q, *p.RuntimeID); err != nil {
					return nil, err
				}
			}
			sinkType, err := SinkTypeOf(ctx, q, p)
			if err != nil {
				return nil, err
			}
			resolved, err := ResolveImage(ctx, q, *current, rt, sinkType)
			if err != nil {
				return nil, err
			}
			if !(deploy && live && p.DeployedImage != nil && *p.DeployedImage != resolved) {
				results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, OK: true, Message: "Already on this image."})
				continue
			}
			// Following the default, which changed since it was deployed: redeploy the same version.
			var message string
			err = db.Savepoint(ctx, q, func(sq db.Querier) error {
				required, err := ApprovalRequired(ctx, sq, a.Project.ID)
				if err != nil {
					return err
				}
				if required {
					if _, err := RequestApproval(ctx, sq, a, p, nil, nil, "Debezium image change"); err != nil {
						return err
					}
					message = "The redeploy on the default image waits for approval."
					return nil
				}
				if _, _, err := Deploy(ctx, sq, a, p, nil, nil); err != nil {
					return err
				}
				message = "Redeploying on " + resolved + "."
				return nil
			})
			if err != nil {
				detail, ok := problemDetail(err)
				if !ok {
					return nil, err
				}
				results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, Message: detail})
				continue
			}
			results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, OK: true, Message: message})
			continue
		}
		// What the release does to its settings: a setting it lacks blocks this pipeline's change.
		image, err := targetImage(ctx, q, p, *current, target)
		if err != nil {
			return nil, err
		}
		effects := VersionEffects(*current, image)
		var blocked []string
		for _, e := range effects {
			if e.Blocking {
				blocked = append(blocked, e.Key+" ("+e.Message+")")
			}
		}
		if len(blocked) > 0 {
			results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, Effects: effects,
				Message: "Not changed: " + image + " doesn't have " + strings.Join(blocked, "; ")})
			continue
		}
		spec := current.Clone()
		spec.DebeziumImage = target
		applyRenames(&spec, effects)
		note := "Debezium image → runtime default"
		if target != nil {
			note = "Debezium image → " + *target
		}
		var number *int
		waiting := false
		err = db.Savepoint(ctx, q, func(sq db.Querier) error {
			created, err := UpdatePipeline(ctx, sq, a, p, PipelineEdit{ExpectedVersion: p.Version, Spec: &spec, Note: note, Separate: true})
			if err != nil {
				return err
			}
			if created != nil {
				n := created.Number
				number = &n
			}
			if deploy && live {
				required, err := ApprovalRequired(ctx, sq, a.Project.ID)
				if err != nil {
					return err
				}
				if required {
					if _, err := RequestApproval(ctx, sq, a, p, nil, nil, "Debezium image change"); err != nil {
						return err
					}
					waiting = true
				} else if _, _, err := Deploy(ctx, sq, a, p, nil, nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			detail, ok := problemDetail(err)
			if !ok {
				return nil, err
			}
			results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, Message: detail})
			continue
		}
		message := "Saved; deploy it when ready."
		switch {
		case waiting:
			message = "Saved; the deploy waits for approval."
		case deploy && live:
			message = "Deploying the new version."
		}
		results = append(results, UpgradeResult{PipelineID: pid, Name: p.Name, OK: true, Message: message, Version: number, Effects: effects})
	}
	return results, nil
}

func equalPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

var _ = domain.DefaultSpec
