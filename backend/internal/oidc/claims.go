package oidc

import (
	"net/http"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/version"
)

// Reading identity fields from ID tokens and userinfo answers, the same way for every provider.

// Lookup finds a claim by its exact name first (Auth0's namespaced claims such as
// "https://acme.com/groups" contain dots), then as a dotted path into nested objects
// (Keycloak's "realm_access.roles").
func Lookup(claims map[string]any, name string) any {
	if name == "" {
		return nil
	}
	if v, ok := claims[name]; ok {
		return v
	}
	var cur any = claims
	for _, part := range strings.Split(name, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		if cur, ok = m[part]; !ok {
			return nil
		}
	}
	return cur
}

// ErrGroupsOverage: the provider sent a reference to the person's groups instead of the groups
// (Microsoft Entra ID does past 200 groups in a token). Signing in with no groups would silently
// remove every role the groups grant, so the sign-in is refused instead.
var ErrGroupsOverage = &Error{"Your identity provider sent a link to your groups instead of the groups (you belong to too many). " +
	"Ask an Admin to send only the groups BeziSync uses (for Entra ID: \"Groups assigned to the application\", or app roles in the roles claim)."}

// groupsFrom reads the groups claim; ok is false when the provider signalled that the groups
// were left out (Entra's _claim_names overage marker).
func groupsFrom(claims map[string]any, name string) ([]string, bool) {
	v := Lookup(claims, name)
	if v == nil {
		if names, _ := claims["_claim_names"].(map[string]any); names != nil {
			if _, over := names[name]; over {
				return nil, false
			}
			if _, over := names["groups"]; over && name == "groups" {
				return nil, false
			}
		}
		return []string{}, true
	}
	return claimStrings(v), true
}

// userAgent identifies BeziSync to providers (GitHub's API rejects requests without one).
type userAgent struct{ base http.RoundTripper }

func (u userAgent) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("User-Agent") == "" {
		r = r.Clone(r.Context())
		r.Header.Set("User-Agent", "BeziSync/"+version.Version)
	}
	return u.base.RoundTrip(r)
}
