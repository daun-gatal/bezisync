package api

import "testing"

// A redirect after sign-in never leaves the site, whatever the browser does to the URL.
func TestSafeNextStaysOnSite(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"/p/default/alerts?tab=rules", "/p/default/alerts?tab=rules"},
		{"/p/default/pipelines/a%20b#top", "/p/default/pipelines/a%20b#top"},
		{"/", "/"},
		{"", "/"},
		{"p/default", "/"},
		{"https://evil.example", "/"},
		{"//evil.example", "/"},
		{`/\evil.example`, "/"},
		{`/p\..\..\evil`, "/"},
		// Browsers strip tabs and newlines, turning these into "//evil.example".
		{"/\t/evil.example", "/"},
		{"/\n/evil.example", "/"},
		{"/\r/evil.example", "/"},
		{"/\t\\evil.example", "/"},
		{"/ /evil.example", "/"},
		{"/\x00/evil.example", "/"},
		{"/ /evil.example", "/"},
		{"/ /evil.example", "/"},
	} {
		if got := safeNext(tc.in); got != tc.want {
			t.Errorf("safeNext(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
