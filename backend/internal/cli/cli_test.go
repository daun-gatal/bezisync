package cli

import (
	"os"
	"strings"
	"testing"
)

// frontend/openapi.json is what `bezisync openapi` renders (make openapi); the typed client
// is generated from it.
func TestOpenAPIDocumentMatchesTheFrontend(t *testing.T) {
	front, err := os.ReadFile("../../../frontend/openapi.json")
	if err != nil {
		t.Skip("frontend not checked out next to the backend")
	}
	doc, err := OpenAPIDocument()
	if err != nil {
		t.Fatal(err)
	}
	if doc != string(front) {
		t.Fatal("frontend/openapi.json is stale: run make openapi")
	}
}

// `bezisync licenses` prints BeziSync's own licence and notice first, then the modules it links.
func TestLicensesPrintsOwnAndThirdPartyLicences(t *testing.T) {
	var out, errOut strings.Builder
	if code := Execute([]string{"licenses"}, strings.NewReader(""), &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	text := out.String()
	for _, want := range []string{"BeziSync: NOTICE", "BeziSync: LICENSE", "Apache License", "Third-party software", "github.com/spf13/cobra", "Go standard library and runtime"} {
		if !strings.Contains(text, want) {
			t.Errorf("licenses output lacks %q", want)
		}
	}
	if strings.Index(text, "BeziSync: LICENSE") > strings.Index(text, "Third-party software") {
		t.Error("BeziSync's own licence must come before the third-party software")
	}
}
