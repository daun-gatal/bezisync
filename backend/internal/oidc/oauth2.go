package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Plain OAuth2 sign-in (GitHub, Bitbucket, Gitea, …): no ID token, so the person is read from the
// provider's userinfo endpoint with the access token the code is exchanged for. Nothing a
// provider returns is trusted for more than it says: the subject must be present, the email must
// be a valid address, and it counts as verified only when the provider says so (GitHub: its
// primary verified address from /user/emails).

// maxBody bounds every answer read from a provider.
const maxBody = 1 << 20

// githubPages caps how many pages of orgs or teams are read (100 each).
const githubPages = 10

var linkNext = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

func readJSON(resp *http.Response, v any) error {
	dec := json.NewDecoder(io.LimitReader(resp.Body, maxBody))
	dec.UseNumber()
	return dec.Decode(v)
}

func finishOAuth2(ctx context.Context, p *Provider, verifier, code string) (*Identity, error) {
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", RedirectURI())
	form.Set("code_verifier", verifier)
	form.Set("client_id", p.ClientID)
	if p.SecretInBody {
		form.Set("client_secret", p.ClientSecret)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, &Error{"The identity provider's token URL is not valid."}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json") // GitHub answers form-encoded otherwise
	if !p.SecretInBody {
		req.SetBasicAuth(url.QueryEscape(p.ClientID), url.QueryEscape(p.ClientSecret)) // RFC 6749 §2.3.1
	}
	resp, err := client().Do(req)
	if err != nil {
		return nil, &Error{"The identity provider did not answer."}
	}
	defer func() { _ = resp.Body.Close() }()
	var tokens map[string]any
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &Error{fmt.Sprintf("The identity provider refused the sign-in (HTTP %d).", resp.StatusCode)}
	}
	if err := readJSON(resp, &tokens); err != nil {
		return nil, &Error{"The identity provider's token answer is not JSON."}
	}
	// Some providers (GitHub) answer 200 with an error body.
	if e, _ := tokens["error"].(string); e != "" {
		return nil, &Error{fmt.Sprintf("The identity provider refused the sign-in (%s).", e)}
	}
	access, _ := tokens["access_token"].(string)
	if access == "" {
		return nil, &Error{"The identity provider returned no access token."}
	}
	if tt, _ := tokens["token_type"].(string); tt != "" && !strings.EqualFold(tt, "bearer") {
		return nil, &Error{fmt.Sprintf("The identity provider returned a %s token; BeziSync needs a bearer token.", tt)}
	}
	info := map[string]any{}
	if err := getJSON(ctx, p.UserinfoURL, access, p.GitHub, &info); err != nil {
		return nil, err
	}
	sub := claimString(Lookup(info, p.Claims.Subject))
	if sub == "" {
		return nil, &Error{fmt.Sprintf("The identity provider sent no %q (the user's id).", p.Claims.Subject)}
	}
	// A field the Admin names says whether the address is verified; without one it never is.
	email := strings.ToLower(claimString(Lookup(info, p.Claims.Email)))
	verified := p.Claims.EmailVerified != "" && claimBool(Lookup(info, p.Claims.EmailVerified))
	var groups []string
	if p.GitHub {
		// /user's email is the public profile address, never proof of ownership.
		if email, verified, err = githubEmail(ctx, p, access); err != nil {
			return nil, err
		}
		if groups, err = githubGroups(ctx, p, access); err != nil {
			return nil, err
		}
	} else {
		var ok bool
		if groups, ok = groupsFrom(info, p.GroupsClaim); !ok {
			return nil, ErrGroupsOverage
		}
	}
	if email == "" {
		return nil, &Error{"The identity provider did not share an email address (check the scopes)."}
	}
	if _, msg := val.ValidateEmail(email); msg != "" {
		return nil, &Error{"The identity provider sent an email address that is not valid."}
	}
	if !verified && !p.AllowUnverifiedEmail {
		return nil, &Error{"Your email address is not verified at the identity provider."}
	}
	name := nameFrom(info, p.Claims.Name)
	if name == "" {
		name = claimString(info["login"])
	}
	if groups == nil {
		groups = []string{}
	}
	return &Identity{Subject: "oauth2:" + p.Issuer + "|" + sub, Email: email, Name: name, Groups: groups, EmailVerified: verified}, nil
}

// getJSON reads one userinfo-style document with the access token.
func getJSON(ctx context.Context, u, access string, github bool, v any) error {
	_, err := getPage(ctx, u, access, github, v)
	return err
}

// ErrGitHubPartial: GitHub left out organizations whose SAML single sign-on this sign-in is not
// authorized for. Their teams would be missing, which would silently remove the roles they
// grant, so the sign-in is refused.
var ErrGitHubPartial = &Error{"GitHub hid some of your organizations because they use SAML single sign-on that this sign-in isn't authorized for. " +
	"Sign in to those organizations on GitHub (github.com/orgs/<org>/sso), then sign in to BeziSync again."}

func getPage(ctx context.Context, u, access string, github bool, v any) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", &Error{"The identity provider's user URL is not valid."}
	}
	req.Header.Set("Authorization", "Bearer "+access)
	req.Header.Set("Accept", "application/json")
	if github {
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	}
	resp, err := client().Do(req)
	if err != nil {
		return "", &Error{"The identity provider did not answer."}
	}
	defer func() { _ = resp.Body.Close() }()
	if github && strings.HasPrefix(resp.Header.Get("X-GitHub-SSO"), "partial-results") {
		return "", ErrGitHubPartial
	}
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		if github && strings.HasPrefix(access, "ghu_") {
			return "", &Error{"This is a GitHub App, which needs the \"Email addresses\" and \"Members\" read permissions; or use an OAuth app (GitHub → Settings → Developer settings → OAuth Apps)."}
		}
		return "", &Error{fmt.Sprintf("The identity provider did not let BeziSync read your profile (HTTP %d): check the scopes.", resp.StatusCode)}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", &Error{fmt.Sprintf("The identity provider could not share your profile (HTTP %d).", resp.StatusCode)}
	}
	if err := readJSON(resp, v); err != nil {
		return "", &Error{"The identity provider's profile answer is not JSON."}
	}
	next := ""
	if m := linkNext.FindStringSubmatch(resp.Header.Get("Link")); m != nil {
		next = m[1]
	}
	return next, nil
}

// githubAPI is the API base of a GitHub user URL (https://api.github.com/user → https://api.github.com,
// GitHub Enterprise https://ghe.example.com/api/v3/user → https://ghe.example.com/api/v3).
func githubAPI(userinfo string) string {
	return strings.TrimSuffix(strings.TrimRight(userinfo, "/"), "/user")
}

// sameOrigin keeps paging on the API the Admin configured (a Link header is provider input).
func sameOrigin(a, b string) bool {
	ua, errA := url.Parse(a)
	ub, errB := url.Parse(b)
	return errA == nil && errB == nil && ua.Scheme == ub.Scheme && ua.Host == ub.Host
}

func githubEmail(ctx context.Context, p *Provider, access string) (string, bool, error) {
	var emails []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}
	if err := getJSON(ctx, githubAPI(p.UserinfoURL)+"/user/emails", access, true, &emails); err != nil {
		var e *Error
		if errors.As(err, &e) && strings.Contains(e.Msg, "check the scopes") {
			return "", false, &Error{"GitHub did not share your email addresses: add the user:email scope."}
		}
		return "", false, err
	}
	for _, pass := range []bool{true, false} { // the primary verified address first
		for _, e := range emails {
			if e.Verified && (e.Primary || !pass) {
				return strings.ToLower(e.Email), true, nil
			}
		}
	}
	for _, e := range emails {
		if e.Primary {
			return strings.ToLower(e.Email), false, nil
		}
	}
	return "", false, nil
}

// githubGroups are the person's organizations ("acme") and teams ("acme/data-platform").
func githubGroups(ctx context.Context, p *Provider, access string) ([]string, error) {
	base := githubAPI(p.UserinfoURL)
	groups := []string{}
	seen := map[string]bool{}
	add := func(g string) {
		if g != "" && !seen[g] {
			seen[g] = true
			groups = append(groups, g)
		}
	}
	read := func(start string, each func(json.RawMessage)) error {
		u := start
		for i := 0; u != "" && i < githubPages; i++ {
			var page []json.RawMessage
			next, err := getPage(ctx, u, access, true, &page)
			if err != nil {
				var e *Error
				if errors.As(err, &e) && strings.Contains(e.Msg, "check the scopes") {
					return &Error{"GitHub did not share your organizations: add the read:org scope."}
				}
				return err
			}
			for _, item := range page {
				each(item)
			}
			if next != "" && !sameOrigin(next, start) {
				break
			}
			u = next
		}
		return nil
	}
	if err := read(base+"/user/orgs?per_page=100", func(raw json.RawMessage) {
		var o struct {
			Login string `json:"login"`
		}
		if json.Unmarshal(raw, &o) == nil {
			add(strings.ToLower(o.Login))
		}
	}); err != nil {
		return nil, err
	}
	if err := read(base+"/user/teams?per_page=100", func(raw json.RawMessage) {
		var t struct {
			Slug string `json:"slug"`
			Org  struct {
				Login string `json:"login"`
			} `json:"organization"`
		}
		if json.Unmarshal(raw, &t) == nil && t.Slug != "" && t.Org.Login != "" {
			add(strings.ToLower(t.Org.Login))
			add(strings.ToLower(t.Org.Login + "/" + t.Slug))
		}
	}); err != nil {
		return nil, err
	}
	return groups, nil
}

// claimString reads a string or an integer claim (GitHub's numeric ids) as text.
func claimString(v any) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return x.String()
		}
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
	}
	return ""
}

func claimBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return strings.EqualFold(x, "true")
	}
	return false
}

func claimStrings(v any) []string {
	switch x := v.(type) {
	case []any:
		out := []string{}
		for _, item := range x {
			if s := claimString(item); s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if x = strings.TrimSpace(x); x != "" {
			return []string{x}
		}
	}
	return []string{}
}
