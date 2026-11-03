package cli

import (
	"os"
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
