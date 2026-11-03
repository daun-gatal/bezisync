// Package apiclient is a small signed-in BeziSync API session (cookies plus the CSRF header)
// for the repository's tools.
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// Error is an HTTP error answer.
type Error struct {
	Status int
	Code   string
	Detail string
}

func (e *Error) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Detail) }

// Client talks to {base}/api/v1.
type Client struct {
	base string
	http *http.Client
}

// New checks the URL and creates a client.
func New(base string) (*Client, error) {
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		return nil, fmt.Errorf("the URL must be http(s), not %q", base)
	}
	jar, _ := cookiejar.New(nil)
	return &Client{base: strings.TrimRight(base, "/") + "/api/v1", http: &http.Client{Jar: jar, Timeout: 120 * time.Second}}, nil
}

func (c *Client) csrf() string {
	u, _ := url.Parse(c.base)
	for _, ck := range c.http.Jar.Cookies(u) {
		if ck.Name == "bz_csrf" {
			return ck.Value
		}
	}
	return ""
}

// Call sends a request and decodes the JSON answer into out (when not nil).
func (c *Client) Call(ctx context.Context, method, path string, body, out any, headers ...string) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if tok := c.csrf(); tok != "" && method != http.MethodGet {
		req.Header.Set("X-CSRF-Token", tok)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 400 {
		e := &Error{Status: res.StatusCode, Detail: string(raw)}
		var problem struct{ Code, Detail, Title string }
		if json.Unmarshal(raw, &problem) == nil {
			e.Code = problem.Code
			if e.Detail = problem.Detail; e.Detail == "" {
				e.Detail = problem.Title
			}
		} else if len(e.Detail) > 500 {
			e.Detail = e.Detail[:500]
		}
		return e
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

// Login signs in.
func (c *Client) Login(ctx context.Context, email, password string) error {
	return c.Call(ctx, http.MethodPost, "/auth/login", map[string]string{"email": email, "password": password}, nil)
}

// Logout signs out, ignoring failures.
func (c *Client) Logout(ctx context.Context) {
	_ = c.Call(ctx, http.MethodPost, "/auth/logout", nil, nil)
}
