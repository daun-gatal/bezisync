package licenses

import (
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
)

// Every download of the Flink CDC image is a reviewed artifact under an allowed licence, and every
// reviewed artifact is still downloaded (the table can't go stale).
func TestFlinkCDCImageArtifactsReviewed(t *testing.T) {
	raw, err := os.ReadFile("../../" + FlinkCDCImage)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ReplaceAll(string(raw), "\\\n", " ")
	args := map[string]string{}
	for _, m := range regexp.MustCompile(`(?m)^ARG (\w+)=(\S+)`).FindAllStringSubmatch(text, -1) {
		args[m[1]] = m[2]
	}
	expand := func(s string) string {
		return regexp.MustCompile(`\$\{(\w+)\}`).ReplaceAllStringFunc(s, func(v string) string { return args[v[2:len(v)-1]] })
	}
	text = expand(text)
	reviewed := map[string]string{}
	for _, a := range FlinkCDCArtifacts {
		reviewed[a.File] = a.SPDX
	}
	seen := map[string]bool{}
	adds := regexp.MustCompile(`(?m)^ADD (?:--\S+ +)*(https?://\S+)`).FindAllStringSubmatch(text, -1)
	if len(adds) == 0 {
		t.Fatal("no downloads found")
	}
	for _, m := range adds {
		file := path.Base(m[1])
		spdx, ok := reviewed[file]
		if !ok {
			t.Errorf("%s downloads %s, which is not in FlinkCDCArtifacts (review its licence first)", FlinkCDCImage, file)
			continue
		}
		if !Allowed[spdx] {
			t.Errorf("%s: %s is not an allowed licence", file, spdx)
		}
		if !strings.Contains(m[0], "--checksum=sha256:") {
			t.Errorf("%s is downloaded without a pinned checksum", file)
		}
		seen[file] = true
	}
	for f := range reviewed {
		if !seen[f] {
			t.Errorf("FlinkCDCArtifacts lists %s, which the image no longer downloads", f)
		}
	}
	if !strings.Contains(text, `ARG MYSQL_DRIVER=""`) {
		t.Error("MySQL Connector/J (GPL) must stay opt-in: ARG MYSQL_DRIVER defaults to empty")
	}
}
