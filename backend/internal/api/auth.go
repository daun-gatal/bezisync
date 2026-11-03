package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/httpapi"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/oidc"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/ratelimit"
	"github.com/daun-gatal/bezisync/backend/internal/samlsp"
	"github.com/daun-gatal/bezisync/backend/internal/security"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// UserRead is the signed-in user.
type UserRead struct {
	ID                 uuid.UUID `json:"id"`
	Email              string    `json:"email"`
	Name               string    `json:"name"`
	IsSuperuser        bool      `json:"is_superuser"`
	UIMode             string    `json:"ui_mode"`
	MutedNotifications []string  `json:"muted_notifications"`
}

// ProjectRef is a project the user can open.
type ProjectRef struct {
	ID          uuid.UUID `json:"id"`
	Slug        string    `json:"slug"`
	Name        string    `json:"name"`
	Role        *string   `json:"role"`
	Permissions []string  `json:"permissions"`
	IsSandbox   bool      `json:"is_sandbox"`
}

// MeRead is the current user with their projects.
type MeRead struct {
	User     UserRead     `json:"user"`
	Projects []ProjectRef `json:"projects"`
}

func userRead(u *models.User) UserRead {
	return UserRead{ID: u.ID, Email: u.Email, Name: u.Name, IsSuperuser: u.IsSuperuser, UIMode: u.UIMode, MutedNotifications: u.Muted()}
}

func me(ctx context.Context, q db.Querier, p *auth.Principal) (MeRead, error) {
	u := p.User
	type pair struct {
		project models.Project
		m       *models.Membership
	}
	var pairs []pair
	if u.IsSuperuser {
		projects, err := db.Select[models.Project](ctx, q, `SELECT * FROM projects ORDER BY name`)
		if err != nil {
			return MeRead{}, err
		}
		members, err := db.Select[models.Membership](ctx, q, `SELECT * FROM memberships WHERE user_id = $1`, u.ID)
		if err != nil {
			return MeRead{}, err
		}
		byProject := map[uuid.UUID]*models.Membership{}
		for i := range members {
			byProject[members[i].ProjectID] = &members[i]
		}
		for _, pr := range projects {
			pairs = append(pairs, pair{pr, byProject[pr.ID]})
		}
	} else {
		rows, err := db.Select[struct {
			models.Project
			MID         uuid.UUID `db:"m_id"`
			MRole       string    `db:"m_role"`
			MCanApprove bool      `db:"m_can_approve"`
		}](ctx, q, `SELECT p.*, m.id AS m_id, m.role AS m_role, m.can_approve AS m_can_approve
			FROM memberships m JOIN projects p ON p.id = m.project_id WHERE m.user_id = $1 ORDER BY p.name`, u.ID)
		if err != nil {
			return MeRead{}, err
		}
		for _, r := range rows {
			pairs = append(pairs, pair{r.Project, &models.Membership{ID: r.MID, Role: r.MRole, CanApprove: r.MCanApprove}})
		}
	}
	out := MeRead{User: userRead(u), Projects: []ProjectRef{}}
	tok := p.APIToken
	for _, pr := range pairs {
		if tok != nil && tok.ProjectID != nil && *tok.ProjectID != pr.project.ID {
			continue
		}
		var role *string
		if pr.m != nil {
			r := pr.m.Role
			role = &r
		}
		out.Projects = append(out.Projects, ProjectRef{ID: pr.project.ID, Slug: pr.project.Slug, Name: pr.project.Name, Role: role,
			Permissions: auth.PermissionsFor(pr.m, u.IsSuperuser).Sorted(), IsSandbox: pr.project.IsSandbox})
	}
	return out, nil
}

func authRoutes(r chi.Router) {
	r.Post("/auth/login", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		type login struct{ Email, Password string }
		b, err := body(r, func(v *val.V, o *val.Object) login {
			return login{
				Email:    str(v, o, "email", "", val.StrRules{Min: 3, Max: 320}, true),
				Password: str(v, o, "password", "", val.StrRules{Min: 1, Max: 1024}, true),
			}
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		if _, on, err := services.PasswordLogin(ctx, pool(ctx)); err != nil {
			return err
		} else if !on {
			return problem.New(403, "password_login_disabled", "Sign-in with email and password is turned off. Use your organization's sign-in instead.")
		}
		key := httpapi.ClientHost(r) + ":" + strings.ToLower(b.Email)
		if err := ratelimit.Login.Check(key); err != nil {
			return err
		}
		var out MeRead
		err = tx(ctx, func(q db.Querier) error {
			u, err := db.Find[models.User](ctx, q, `SELECT * FROM users WHERE lower(email) = $1`, strings.ToLower(b.Email))
			if err != nil {
				return err
			}
			if u == nil || !u.IsActive || u.PasswordHash == nil || *u.PasswordHash == "" || !security.VerifyPassword(*u.PasswordHash, b.Password) {
				return problem.New(401, "invalid_credentials", "Email or password is incorrect.")
			}
			ratelimit.Login.Reset(key)
			if _, err := auth.StartSession(ctx, q, w, u, r.Header.Get("user-agent")); err != nil {
				return err
			}
			now := db.Now()
			u.LastLoginAt, u.UpdatedAt = &now, now
			if err := db.Update(ctx, q, "users", u); err != nil {
				return err
			}
			p := &auth.Principal{User: u}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "auth.login", Summary: u.Email + " signed in", Principal: p}); err != nil {
				return err
			}
			out, err = me(ctx, q, p)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/auth/logout", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		if err := auth.EndSession(r.Context(), pool(r.Context()), w, p); err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
	r.Get("/auth/me", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		out, err := me(r.Context(), pool(r.Context()), p)
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Patch("/auth/me", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		type update struct {
			Name, UIMode *string
			Muted        []string
			MutedSet     bool
		}
		b, err := body(r, func(v *val.V, o *val.Object) update {
			var u update
			u.Name = optStr(v, o, "name", val.StrRules{Max: 200})
			if f := o.Get("ui_mode"); f.Present && !f.IsNull() {
				if s, ok := v.Literal(f.Loc, f.Raw, "guided", "expert"); ok {
					u.UIMode = &s
				}
			}
			if f := o.Get("muted_notifications"); f.Present && !f.IsNull() {
				if items, ok := v.List(f.Loc, f.Raw); ok {
					u.MutedSet = true
					for i, it := range items {
						if s, ok := v.Literal(f.Loc.Add(i), it, "alert_fired", "alert_resolved", "approval_requested", "approval_decided", "feedback_received"); ok {
							u.Muted = append(u.Muted, s)
						}
					}
				}
			}
			return u
		})
		if err != nil {
			return err
		}
		ctx := r.Context()
		var out MeRead
		err = tx(ctx, func(q db.Querier) error {
			u := p.User
			if b.Name != nil {
				u.Name = *b.Name
			}
			if b.UIMode != nil {
				u.UIMode = *b.UIMode
			}
			if b.MutedSet {
				set := map[string]bool{}
				for _, m := range b.Muted {
					set[m] = true
				}
				list := make([]string, 0, len(set))
				for m := range set {
					list = append(list, m)
				}
				sort.Strings(list)
				l := db.List{}
				for _, m := range list {
					l = append(l, m)
				}
				u.MutedNotifications = l
			}
			u.UpdatedAt = db.Now()
			if err := db.Update(ctx, q, "users", u); err != nil {
				return err
			}
			var err error
			out, err = me(ctx, q, p)
			return err
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
}

// TokenRead is an API token (the secret only in the create response).
type TokenRead struct {
	ID         uuid.UUID    `json:"id"`
	Name       string       `json:"name"`
	Prefix     string       `json:"prefix"`
	ProjectID  *uuid.UUID   `json:"project_id"`
	ReadOnly   bool         `json:"read_only"`
	ExpiresAt  *pytime.Time `json:"expires_at"`
	LastUsedAt *pytime.Time `json:"last_used_at"`
	CreatedAt  pytime.Time  `json:"created_at"`
	Token      *string      `json:"token"`
}

func tokenRead(t *models.APIToken, raw *string) TokenRead {
	return TokenRead{ID: t.ID, Name: t.Name, Prefix: t.Prefix, ProjectID: t.ProjectID, ReadOnly: t.ReadOnly, ExpiresAt: pytime.P(t.ExpiresAt),
		LastUsedAt: pytime.P(t.LastUsedAt), CreatedAt: pytime.T(t.CreatedAt), Token: raw}
}

func interactive(p *auth.Principal) error {
	if p.APIToken != nil {
		return problem.New(403, "session_required", "Manage API tokens from a signed-in session, not with a token.")
	}
	return nil
}

func tokenRoutes(r chi.Router) {
	r.Get("/auth/tokens", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		rows, err := db.Select[models.APIToken](r.Context(), pool(r.Context()), `SELECT * FROM api_tokens WHERE user_id = $1 ORDER BY created_at DESC`, p.User.ID)
		if err != nil {
			return err
		}
		out := make([]TokenRead, len(rows))
		for i := range rows {
			out[i] = tokenRead(&rows[i], nil)
		}
		httpapi.JSON(w, http.StatusOK, out)
		return nil
	}))
	r.Post("/auth/tokens", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		p, err := principal(r)
		if err != nil {
			return err
		}
		type create struct {
			Name      string
			ProjectID *uuid.UUID
			ReadOnly  bool
			Expires   *int
		}
		b, err := body(r, func(v *val.V, o *val.Object) create {
			c := create{Name: str(v, o, "name", "", val.StrRules{Min: 1, Max: 200}, true), ProjectID: optUUID(v, o, "project_id"),
				ReadOnly: boolean(v, o, "read_only", false)}
			days := 90
			c.Expires = &days
			if f := o.Get("expires_in_days"); f.Present {
				if f.IsNull() {
					c.Expires = nil
				} else if n, ok := v.IntIn(f.Loc, f.Raw, val.Num{GE: val.F(1), LE: val.F(3650)}); ok {
					c.Expires = &n
				}
			}
			return c
		})
		if err != nil {
			return err
		}
		if err := interactive(p); err != nil {
			return err
		}
		ctx := r.Context()
		var out TokenRead
		err = tx(ctx, func(q db.Querier) error {
			if b.ProjectID != nil {
				if _, err := auth.Project(ctx, q, p, *b.ProjectID); err != nil { // 404 unless the user can see it
					return err
				}
			}
			raw := security.NewToken("bzt")
			now := db.Now()
			t := &models.APIToken{ID: db.NewID(), UserID: p.User.ID, Name: b.Name, TokenHash: security.TokenHash(raw), Prefix: raw[:10],
				ProjectID: b.ProjectID, ReadOnly: b.ReadOnly, CreatedAt: now, UpdatedAt: now}
			if b.Expires != nil && *b.Expires != 0 {
				exp := now.Add(time.Duration(*b.Expires) * 24 * time.Hour)
				t.ExpiresAt = &exp
			}
			if err := db.Insert(ctx, q, "api_tokens", t); err != nil {
				return err
			}
			summary := "Created API token " + b.Name
			if b.ReadOnly {
				summary += " (read-only)"
			}
			if _, err := services.Record(ctx, q, services.AuditEvent{Kind: "token.created", Summary: summary, ProjectID: b.ProjectID, Principal: p,
				ResourceType: "api_token", ResourceID: &t.ID}); err != nil {
				return err
			}
			out = tokenRead(t, &raw)
			return nil
		})
		if err != nil {
			return err
		}
		httpapi.JSON(w, http.StatusCreated, out)
		return nil
	}))
	r.Delete("/auth/tokens/{token_id}", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		id, err := pathID(r, "token_id")
		if err != nil {
			return err
		}
		p, err := principal(r)
		if err != nil {
			return err
		}
		if err := interactive(p); err != nil {
			return err
		}
		ctx := r.Context()
		err = tx(ctx, func(q db.Querier) error {
			t, err := db.Find[models.APIToken](ctx, q, `SELECT * FROM api_tokens WHERE id = $1`, id)
			if err != nil {
				return err
			}
			if t == nil || t.UserID != p.User.ID {
				return problem.NotFound("API token")
			}
			if err := db.Delete(ctx, q, "api_tokens", t.ID); err != nil {
				return err
			}
			_, err = services.Record(ctx, q, services.AuditEvent{Kind: "token.revoked", Summary: "Revoked API token " + t.Name, ProjectID: t.ProjectID,
				Principal: p, ResourceType: "api_token", ResourceID: &t.ID})
			return err
		})
		if err != nil {
			return err
		}
		httpapi.NoContent(w)
		return nil
	}))
}

func safeNext(v string) string {
	if strings.HasPrefix(v, "/") && !strings.HasPrefix(v, "//") && !strings.Contains(v, `\`) {
		return v
	}
	return "/"
}

func ssoRoutes(r chi.Router) {
	r.Get("/auth/providers", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		q := pool(ctx)
		_, password, err := services.PasswordLogin(ctx, q)
		if err != nil {
			return err
		}
		rows, err := services.SSOProviders(ctx, q, true)
		if err != nil {
			return err
		}
		providers := make([]any, len(rows))
		for i := range rows {
			providers[i] = obj("id", rows[i].ID, "label", rows[i].Label)
		}
		httpapi.JSON(w, http.StatusOK, obj("password", password, "providers", providers, "lab", config.Get().Lab))
		return nil
	}))
	clearCookie := func(w http.ResponseWriter) {
		http.SetCookie(w, &http.Cookie{Name: oidc.Cookie, Value: "", Path: oidc.CookiePath, MaxAge: -1, Expires: time.Unix(0, 0)}) //nolint:gosec // deleting the cookie
	}
	r.Get("/auth/oidc/login", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		q := httpapi.Q(r)
		next := "/"
		if v := q.OptStr("next"); v != nil {
			vv := &val.V{}
			if s, ok := vv.Constrained(val.Loc{"query", "next"}, *v, val.StrRules{Max: 1000}); ok {
				next = s
			} else {
				return vv.Problem()
			}
		}
		providerID := ""
		if v := q.OptStr("provider"); v != nil {
			providerID = *v
		}
		ctx := r.Context()
		store := pool(ctx)
		if providerID == "" { // a single provider needs no choice
			rows, err := services.SSOProviders(ctx, store, true)
			if err != nil {
				return err
			}
			if len(rows) == 1 {
				providerID = rows[0].ID.String()
			}
		}
		m, err := services.EnabledSSOProvider(ctx, store, providerID)
		if err != nil {
			return err
		}
		if err := services.RequireSSO(m); err != nil {
			return err
		}
		if m.Kind == oidc.KindSAML {
			var to, relay string
			err := tx(ctx, func(q db.Querier) error {
				var err error
				to, relay, err = services.StartSAML(ctx, q, m, safeNext(next))
				return err
			})
			if err != nil {
				if se := (*samlsp.Error)(nil); errors.As(err, &se) {
					redirect(w, "/login?error="+quote(se.Msg))
					return nil
				}
				return err
			}
			http.SetCookie(w, samlRelayCookie(relay, int(oidc.TTL.Seconds())))
			redirect(w, to)
			return nil
		}
		p, err := services.CodeProvider(m)
		if err != nil {
			return err
		}
		url, cookie, err := oidc.Begin(ctx, p, safeNext(next))
		if err != nil {
			if oe := (*oidc.Error)(nil); errors.As(err, &oe) {
				redirect(w, "/login?error="+quote(oe.Msg))
				return nil
			}
			return err
		}
		//nolint:gosec // Secure follows BEZISYNC_COOKIE_SECURE
		http.SetCookie(w, &http.Cookie{Name: oidc.Cookie, Value: cookie, MaxAge: int(oidc.TTL.Seconds()), HttpOnly: true, Secure: config.Get().CookieSecure,
			SameSite: http.SameSiteLaxMode, Path: oidc.CookiePath, Expires: time.Now().Add(oidc.TTL)})
		redirect(w, url)
		return nil
	}))
	r.Get("/auth/oidc/callback", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		vv := &val.V{}
		read := func(name string, max int) string {
			v := r.URL.Query()[name]
			if len(v) == 0 {
				return ""
			}
			s, _ := vv.Constrained(val.Loc{"query", name}, v[len(v)-1], val.StrRules{Max: max})
			return s
		}
		code, state, idpErr, iss := read("code", 4000), read("state", 200), read("error", 200), read("iss", 500)
		if p := vv.Problem(); p != nil {
			return p
		}
		ctx := r.Context()
		fail := func(msg string) error {
			clearCookie(w)
			redirect(w, "/login?error="+quote(msg))
			return nil
		}
		if idpErr != "" || code == "" || state == "" {
			reason := idpErr
			if reason == "" {
				reason = "no code"
			}
			return fail(fmt.Sprintf("The identity provider did not sign you in (%s).", reason))
		}
		cookie := ""
		if c, err := r.Cookie(oidc.Cookie); err == nil {
			cookie = c.Value
		}
		var provider *models.SSOProvider
		find := func(id string) (*oidc.Provider, error) {
			m, err := services.EnabledSSOProvider(ctx, pool(ctx), id)
			if err != nil || m == nil || m.Kind == oidc.KindSAML {
				return nil, err
			}
			provider = m
			return services.CodeProvider(m)
		}
		identity, next, err := oidc.Finish(ctx, find, cookie, code, state, iss)
		if err != nil {
			if oe := (*oidc.Error)(nil); errors.As(err, &oe) {
				return fail(oe.Msg)
			}
			return err
		}
		var oerr *oidc.Error
		err = tx(ctx, func(q db.Querier) error {
			user, err := services.SyncSSOUser(ctx, q, identity, provider)
			if err != nil {
				return err
			}
			_, err = auth.StartSession(ctx, q, w, user, r.Header.Get("user-agent"))
			return err
		})
		if err != nil {
			if e := (*oidc.Error)(nil); errors.As(err, &e) {
				oerr = e
			} else {
				return err
			}
		}
		if oerr != nil {
			return fail(oerr.Msg)
		}
		clearCookie(w)
		redirect(w, safeNext(next))
		return nil
	}))
}

// samlRelayCookie ties a SAML sign-in to the browser that started it. The identity provider posts
// back cross-site, so the cookie must be SameSite=None, which browsers allow on https only;
// over plain http it is Lax and does not come back (the relay token in the database still
// makes each request single-use).
func samlRelayCookie(value string, maxAge int) *http.Cookie {
	secure := config.Get().CookieSecure
	same := http.SameSiteLaxMode
	if secure {
		same = http.SameSiteNoneMode
	}
	//nolint:gosec // Secure follows BEZISYNC_COOKIE_SECURE
	return &http.Cookie{Name: "bz_saml", Value: value, MaxAge: maxAge, Path: samlsp.ACSPath, HttpOnly: true, Secure: secure, SameSite: same,
		Expires: time.Now().Add(time.Duration(maxAge) * time.Second)}
}

func samlRoutes(r chi.Router) {
	r.Get("/auth/saml/metadata", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		var md []byte
		err := tx(ctx, func(q db.Querier) error {
			keys, _, err := services.SAMLKeys(ctx, q)
			if err != nil {
				return err
			}
			md, err = samlsp.SPMetadata(keys)
			return err
		})
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/samlmetadata+xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(md)
		return nil
	}))
	r.Post("/auth/saml/acs", httpapi.H(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		fail := func(msg string) error {
			http.SetCookie(w, samlRelayCookie("", -1))
			redirect(w, "/login?error="+quote(msg))
			return nil
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4*samlsp.MaxMetadata+4096)
		if err := r.ParseForm(); err != nil {
			return fail("The identity provider's answer could not be read.")
		}
		cookie := ""
		if c, err := r.Cookie("bz_saml"); err == nil {
			cookie = c.Value
		}
		var next string
		var serr *samlsp.Error
		var oerr *oidc.Error
		// Verified (and the request and assertion marked used) in its own transaction, so a
		// later failure never makes the response usable again.
		var identity *oidc.Identity
		var provider *models.SSOProvider
		err := tx(ctx, func(q db.Querier) error {
			var err error
			identity, provider, next, err = services.FinishSAML(ctx, q, r.PostForm.Get("SAMLResponse"), r.PostForm.Get("RelayState"), cookie, config.Get().CookieSecure)
			return err
		})
		if err == nil {
			err = tx(ctx, func(q db.Querier) error {
				user, err := services.SyncSSOUser(ctx, q, identity, provider)
				if err != nil {
					return err
				}
				_, err = auth.StartSession(ctx, q, w, user, r.Header.Get("user-agent"))
				return err
			})
		}
		switch {
		case errors.As(err, &serr):
			return fail(serr.Msg)
		case errors.As(err, &oerr):
			return fail(oerr.Msg)
		case err != nil:
			return err
		}
		http.SetCookie(w, samlRelayCookie("", -1))
		// 303: the browser follows with a GET, which carries the new (Lax) session cookie.
		w.Header().Set("Location", safeNext(next))
		w.WriteHeader(http.StatusSeeOther)
		return nil
	}))
}
