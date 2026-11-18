package api_test

import (
	"net/http"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

// Signing out clears the cookies with the attributes they were set with (HttpOnly except the
// SPA's CSRF cookie, Secure when configured, SameSite=Lax), so the browser replaces them.
func TestLogoutClearsCookiesWithTheirAttributes(t *testing.T) {
	srv := server(t)
	withConfig(t, func(s *config.Settings) { s.CookieSecure = true })
	admin := adminClient(t, srv)
	res := admin.post("/api/v1/auth/logout", nil).expect(204)
	seen := map[string]bool{}
	for _, line := range res.Header.Values("Set-Cookie") {
		c, err := http.ParseSetCookie(line)
		if err != nil {
			t.Fatal(err)
		}
		seen[c.Name] = true
		if c.MaxAge >= 0 || c.Value != "" || !c.Secure || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || c.HttpOnly != (c.Name == "bz_session") {
			t.Errorf("deleting %s: %q", c.Name, line)
		}
	}
	if !seen["bz_session"] || !seen["bz_csrf"] {
		t.Fatalf("cookies cleared: %v", seen)
	}
}
