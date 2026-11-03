package api_test

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

const customImage = "registry.internal/debezium-server:3.7.0-acme"

func (e *pipelineEnv) workloadImage() string {
	for _, w := range e.driver.Workloads {
		return w.Image
	}
	return ""
}

func TestUpgradeCanaryThenRollback(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid
	u := base + "/pipelines/" + pipe.Str("id")
	if l := admin.get(base + "/debezium-images").expect(200).List("images"); len(l) != 0 {
		t.Fatal(l)
	}
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	def := config.Get().DebeziumDefaultImage
	listing := admin.get(base + "/debezium-images").expect(200)
	if listing.Str("default_image") != def || len(listing.List("images")) != 1 {
		t.Fatal(string(listing.Raw))
	}
	if listing.Str("images", 0, "image") != def || listing.At("images", 0, "is_default") != true || listing.At("images", 0, "official") != true ||
		listing.Str("images", 0, "pipelines", 0, "name") != "orders-cdc" || listing.At("images", 0, "pipelines", 0, "pinned") != false {
		t.Fatal(string(listing.Raw))
	}
	res := admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": customImage}).expect(200)
	if res.At(0, "ok") != true || res.At(0, "version") != float64(2) {
		t.Fatal(string(res.Raw))
	}
	e.tick()
	if e.workloadImage() != customImage {
		t.Fatal(e.workloadImage())
	}
	g := admin.get(base + "/debezium-images")
	if g.Str("images", 0, "image") != customImage || g.At("images", 0, "official") != false || g.At("images", 0, "pipelines", 0, "pinned") != true {
		t.Fatal(string(g.Raw))
	}
	noted := false
	for _, v := range admin.get(u + "/versions").List() {
		n, _ := at(t, v, "note").(string)
		noted = noted || strings.Contains(n, "Debezium image")
	}
	if !noted {
		t.Fatal("no version note")
	}
	again := admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": customImage}).expect(200)
	if again.At(0, "ok") != true || again.At(0, "version") != nil {
		t.Fatal(string(again.Raw))
	}
	admin.post(u+"/deploy", map[string]any{"version": 1}).expect(200)
	e.tick()
	if e.workloadImage() != def {
		t.Fatal(e.workloadImage())
	}
}

func TestUndeployedEditsBlockAnUpgradeAndViewersCannotUpgrade(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid
	u := base + "/pipelines/" + pipe.Str("id")
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	admin.patch(u, map[string]any{"spec": map[string]any{"source": map[string]any{"tables": map[string]any{"include": []string{"shop.orders", "shop.refunds"}}}}},
		ifMatch(admin.get(u).At("version"))...).expect(200)
	blocked := admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": customImage}).expect(200)
	if blocked.At(0, "ok") != false || !strings.Contains(blocked.Str(0, "message"), "undeployed changes") {
		t.Fatal(string(blocked.Raw))
	}
	saved := admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": customImage, "deploy": false}).expect(200)
	if saved.At(0, "ok") != true || saved.At(0, "version") != float64(3) {
		t.Fatal(string(saved.Raw))
	}
	if p := admin.get(base+"/debezium-images").Str("images", 0, "pipelines", 0, "pending_image"); p != customImage {
		t.Fatal(p)
	}
	viewer := member(t, e.srv, admin, e.pid, "viewer")
	viewer.get(base + "/debezium-images").expect(200)
	viewer.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": customImage}).expect(403)
	admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": "bad image; rm"}).expect(422)
	admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{}, "image": customImage}).expect(422)
}

const nextImage = "quay.io/debezium/server:3.8.0.Final"
const catalogURL = "/api/v1/debezium-catalog"

func TestTheInstallDefaultIsAlwaysOfferedUntilAnAdminPicksAnother(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	install := config.Get().DebeziumDefaultImage
	listing := admin.get(catalogURL).expect(200)
	if listing.Str("default_image") != install || len(listing.List("entries")) != 1 || listing.At("entries", 0, "builtin") != true ||
		listing.At("entries", 0, "is_default") != true || listing.At("entries", 0, "id") != nil {
		t.Fatal(string(listing.Raw))
	}
	added := admin.post(catalogURL, map[string]any{"image": nextImage, "label": "3.8", "is_default": true}).expect(201)
	if added.Str("default_image") != nextImage {
		t.Fatal(string(added.Raw))
	}
	by := map[any]any{}
	for _, e := range added.List("entries") {
		by[at(t, e, "image")] = e
	}
	if at(t, by[nextImage], "is_default") != true || at(t, by[install], "is_default") != false ||
		at(t, by[nextImage], "release_notes_url") != "https://debezium.io/releases/3.8/release-notes" {
		t.Fatal(string(added.Raw))
	}
	if c := admin.post(catalogURL, map[string]any{"image": nextImage}).Code(); c != "image_exists" {
		t.Fatal(c)
	}
	admin.post(catalogURL, map[string]any{"image": "bad image; rm -rf"}).expect(422)
	row := at(t, by[nextImage], "id").(string)
	if c := admin.patch(catalogURL+"/"+row, map[string]any{"deprecated": true}).Code(); c != "catalog_default" {
		t.Fatal(c)
	}
	if c := admin.del(catalogURL + "/" + row).Code(); c != "catalog_default" {
		t.Fatal(c)
	}
	if d := admin.patch(catalogURL+"/"+row, map[string]any{"is_default": false}).expect(200).Str("default_image"); d != install {
		t.Fatal(d)
	}
	dep := admin.patch(catalogURL+"/"+row, map[string]any{"deprecated": true}).expect(200)
	for _, e := range dep.List("entries") {
		if at(t, e, "image") == nextImage && at(t, e, "deprecated") != true {
			t.Fatal(e)
		}
	}
	rest := admin.del(catalogURL + "/" + row).expect(200).List("entries")
	if len(rest) != 1 || at(t, rest[0], "image") != install {
		t.Fatal(rest)
	}
	admin.del(catalogURL + "/" + row).expect(404)
}

func TestEveryoneReadsOnlyAdminsChange(t *testing.T) {
	srv := server(t)
	admin := adminClient(t, srv)
	pid := defaultProject(t, admin)
	viewer := member(t, srv, admin, pid, "viewer")
	editor := member(t, srv, admin, pid, "editor")
	entry := viewer.get(catalogURL).expect(200).At("entries", 0).(map[string]any)
	if entry["pipelines"] != nil || entry["projects"] != nil {
		t.Fatal(entry)
	}
	editor.post(catalogURL, map[string]any{"image": nextImage}).expect(403)
	if p := admin.get(catalogURL).At("entries", 0, "pipelines"); p != float64(0) {
		t.Fatal(p)
	}
}

func TestANewDefaultReachesPipelinesAtTheirNextDeployOnly(t *testing.T) {
	e := newPipelineEnv(t)
	admin := e.admin
	pipe := createPipeline(t, admin, e.pid, e.ids, "shop.orders")
	base := "/api/v1/projects/" + e.pid
	u := base + "/pipelines/" + pipe.Str("id")
	admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.tick()
	install := config.Get().DebeziumDefaultImage
	if e.workloadImage() != install {
		t.Fatal(e.workloadImage())
	}
	admin.post(catalogURL, map[string]any{"image": nextImage, "is_default": true}).expect(201)
	e.tick()
	if e.workloadImage() != install {
		t.Fatal("silent restart")
	}
	g := admin.get(base + "/debezium-images")
	if g.Str("images", 0, "image") != install || g.Str("images", 0, "pipelines", 0, "pending_image") != nextImage ||
		g.Str("images", 0, "standing") != "offered" || g.Str("images", 0, "pipelines", 0, "source") != "default" {
		t.Fatal(string(g.Raw))
	}
	res := admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": nil}).expect(200)
	if res.At(0, "ok") != true {
		t.Fatal(string(res.Raw))
	}
	e.tick()
	if e.workloadImage() != nextImage {
		t.Fatal(e.workloadImage())
	}
	g = admin.get(base + "/debezium-images")
	if g.Str("images", 0, "image") != nextImage || g.Str("images", 0, "standing") != "default" {
		t.Fatal(string(g.Raw))
	}
	entries := admin.get(catalogURL).List("entries")
	last := entries[len(entries)-1]
	projects := at(t, last, "projects").([]any)
	if at(t, last, "pipelines") != float64(1) || len(projects) != 1 || at(t, projects[0], "slug") != "default" || at(t, projects[0], "pipelines") != float64(1) {
		t.Fatal(last)
	}
	admin.post(base+"/debezium-images:apply", map[string]any{"pipeline_ids": []string{pipe.Str("id")}, "image": "registry.internal/dbz:custom", "deploy": false}).expect(200)
	var check any
	for _, s := range admin.post(u+"/preflight", nil).List("sections") {
		for _, c := range at(t, s, "report", "checks").([]any) {
			if at(t, c, "code") == "debezium_image_unlisted" {
				check = c
			}
		}
	}
	if check == nil || at(t, check, "status") != "warn" || at(t, check, "blocking") != false {
		t.Fatal(check)
	}
}
