// Package samlsp is BeziSync as a SAML 2.0 service provider (Settings → Sign-in, kind "saml").
//
// One service provider serves every SAML identity provider: its entity ID is its metadata URL
// and its assertion consumer service (ACS) receives HTTP-POST responses. AuthnRequests go out
// with the HTTP-Redirect binding. Signatures, issuer, destination, recipient and status are
// checked by gosaml2/goxmldsig; this package adds what they leave to the caller: exactly one
// assertion, a mandatory audience restriction naming this SP, the validity window (with a small
// clock skew), InResponseTo matching the request BeziSync sent, persistent subjects, and the
// assertion ID for replay protection.
package samlsp

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"regexp"
	"strings"
	"time"

	saml2 "github.com/russellhaering/gosaml2"
	"github.com/russellhaering/gosaml2/types"
	dsig "github.com/russellhaering/goxmldsig"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// Paths of the service provider.
const (
	MetadataPath = "/api/v1/auth/saml/metadata"
	ACSPath      = "/api/v1/auth/saml/acs"
)

// Skew is how far the identity provider's clock may be from BeziSync's.
const Skew = 2 * time.Minute

// MaxMetadata bounds metadata documents (pasted or fetched).
const MaxMetadata = 1 << 20

// Error is a sign-in or configuration failure that is safe to show.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

func fail(format string, args ...any) error { return &Error{fmt.Sprintf(format, args...)} }

// EntityID is the service provider's entity ID (its metadata URL).
func EntityID() string { return strings.TrimRight(config.Get().PublicURL, "/") + MetadataPath }

// ACSURL is where identity providers post their responses.
func ACSURL() string { return strings.TrimRight(config.Get().PublicURL, "/") + ACSPath }

// IdP is one SAML identity provider as an Admin configured it.
type IdP struct {
	EntityID string
	SSOURL   string
	// Certificates are the IdP's signing certificates (PEM).
	Certificates []string
	// SignRequests signs AuthnRequests with BeziSync's key (some IdPs require it).
	SignRequests bool
	// AllowIdPInitiated accepts responses BeziSync did not ask for (off: login CSRF risk).
	AllowIdPInitiated bool
	// Attribute names; empty uses the common names.
	EmailAttr, NameAttr, GroupsAttr string
}

// Keys are the service provider's own key and certificate (signing requests, decrypting
// encrypted assertions, published in its metadata).
type Keys struct {
	Signer  crypto.Signer
	CertDER []byte
}

// NewKeys makes a key pair and a self-signed certificate (valid 10 years).
func NewKeys() (keyPEM, certPEM string, err error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return "", "", err
	}
	now := time.Now()
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "BeziSync SAML service provider"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(10, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	return keyPEM, certPEM, nil
}

// LoadKeys parses a stored key pair.
func LoadKeys(keyPEM, certPEM string) (*Keys, error) {
	kb, _ := pem.Decode([]byte(keyPEM))
	cb, _ := pem.Decode([]byte(certPEM))
	if kb == nil || cb == nil {
		return nil, errors.New("samlsp: malformed key pair")
	}
	key, err := x509.ParsePKCS1PrivateKey(kb.Bytes)
	if err != nil {
		return nil, err
	}
	return &Keys{Signer: key, CertDER: cb.Bytes}, nil
}

// ParseCertificate reads one PEM certificate, or the base64 body of one (as metadata carries it).
func ParseCertificate(s string) (*x509.Certificate, error) {
	s = strings.TrimSpace(s)
	var der []byte
	if b, _ := pem.Decode([]byte(s)); b != nil {
		if b.Type != "CERTIFICATE" {
			return nil, fail("Paste a certificate (BEGIN CERTIFICATE), not a %s.", strings.ToLower(b.Type))
		}
		der = b.Bytes
	} else {
		raw, err := base64.StdEncoding.DecodeString(strings.Join(strings.Fields(s), ""))
		if err != nil {
			return nil, fail("The certificate is neither PEM nor base64.")
		}
		der = raw
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fail("The certificate can't be read (%v).", err)
	}
	return cert, nil
}

// PEM renders a certificate as PEM.
func PEM(c *x509.Certificate) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw}))
}

// Metadata is an IdP's metadata reduced to what BeziSync needs.
type Metadata struct {
	EntityID     string
	SSOURL       string
	Certificates []string
	WantsSigned  bool
}

var entityIDRe = regexp.MustCompile(`^\S{1,500}$`)

// ParseMetadata reads IdP metadata (an EntityDescriptor, or an EntitiesDescriptor holding one
// identity provider). encoding/xml resolves no external entities.
func ParseMetadata(raw []byte) (*Metadata, error) {
	if len(raw) > MaxMetadata {
		return nil, fail("The metadata is larger than 1 MB.")
	}
	var one types.EntityDescriptor
	var candidates []types.EntityDescriptor
	if err := xml.Unmarshal(raw, &one); err == nil && one.IDPSSODescriptor != nil {
		candidates = []types.EntityDescriptor{one}
	} else {
		var many struct {
			XMLName xml.Name                 `xml:"urn:oasis:names:tc:SAML:2.0:metadata EntitiesDescriptor"`
			List    []types.EntityDescriptor `xml:"EntityDescriptor"`
		}
		if err := xml.Unmarshal(raw, &many); err != nil {
			return nil, fail("This is not SAML metadata (an EntityDescriptor with an IDPSSODescriptor).")
		}
		for _, e := range many.List {
			if e.IDPSSODescriptor != nil {
				candidates = append(candidates, e)
			}
		}
	}
	if len(candidates) != 1 {
		return nil, fail("The metadata must describe exactly one identity provider (it describes %d).", len(candidates))
	}
	e := candidates[0]
	if !entityIDRe.MatchString(e.EntityID) {
		return nil, fail("The metadata has no usable entityID.")
	}
	m := &Metadata{EntityID: e.EntityID, WantsSigned: e.IDPSSODescriptor.WantAuthnRequestsSigned}
	for _, s := range e.IDPSSODescriptor.SingleSignOnServices {
		if s.Binding == saml2.BindingHttpRedirect {
			m.SSOURL = s.Location
		}
	}
	if m.SSOURL == "" {
		return nil, fail("The identity provider offers no HTTP-Redirect sign-in endpoint, which BeziSync uses.")
	}
	for _, kd := range e.IDPSSODescriptor.KeyDescriptors {
		if kd.Use != "" && kd.Use != "signing" {
			continue
		}
		for _, c := range kd.KeyInfo.X509Data.X509Certificates {
			cert, err := ParseCertificate(c.Data)
			if err != nil {
				return nil, err
			}
			m.Certificates = append(m.Certificates, PEM(cert))
		}
	}
	if len(m.Certificates) == 0 {
		return nil, fail("The metadata has no signing certificate.")
	}
	return m, nil
}

// ValidSSOURL reports what is wrong with an IdP sign-in URL ("" when acceptable).
func ValidSSOURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "must be the identity provider's https sign-in URL"
	}
	if s := config.Get(); u.Scheme == "http" && s.Environment == "production" && !s.AllowPlainHTTP {
		return "must use https"
	}
	return ""
}

func serviceProvider(idp *IdP, keys *Keys, now time.Time) (*saml2.SAMLServiceProvider, error) {
	roots := make([]*x509.Certificate, 0, len(idp.Certificates))
	for _, c := range idp.Certificates {
		cert, err := ParseCertificate(c)
		if err != nil {
			return nil, err
		}
		roots = append(roots, cert)
	}
	if len(roots) == 0 {
		return nil, fail("The identity provider has no signing certificate.")
	}
	sp := &saml2.SAMLServiceProvider{
		IdentityProviderSSOURL:      idp.SSOURL,
		IdentityProviderSSOBinding:  saml2.BindingHttpRedirect,
		IdentityProviderIssuer:      idp.EntityID,
		AssertionConsumerServiceURL: ACSURL(),
		ServiceProviderIssuer:       EntityID(),
		AudienceURI:                 EntityID(),
		IDPCertificateStore:         &dsig.MemoryX509CertificateStore{Roots: roots},
		SignAuthnRequests:           idp.SignRequests,
		AllowMissingAttributes:      true,
		Clock:                       dsig.NewFakeClockAt(now),
		MaximumDecompressedBodySize: MaxMetadata,
	}
	if keys != nil {
		ks := &saml2.KeyStore{Signer: keys.Signer, Cert: keys.CertDER}
		if err := sp.SetSPKeyStore(ks); err != nil {
			return nil, err
		}
		if err := sp.SetSPSigningKeyStore(ks); err != nil {
			return nil, err
		}
	}
	return sp, nil
}

// SPMetadata is the service provider's metadata document for Admins to give their IdP.
func SPMetadata(keys *Keys) ([]byte, error) {
	sp, err := serviceProvider(&IdP{Certificates: []string{PEM(mustParse(keys.CertDER))}}, keys, time.Now())
	if err != nil {
		return nil, err
	}
	md, err := sp.Metadata()
	if err != nil {
		return nil, err
	}
	md.SPSSODescriptor.AuthnRequestsSigned = false // per provider; the IdP decides whether it wants them
	md.SPSSODescriptor.WantAssertionsSigned = true
	var b bytes.Buffer
	b.WriteString(xml.Header)
	enc := xml.NewEncoder(&b)
	enc.Indent("", "  ")
	if err := enc.Encode(md); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func mustParse(der []byte) *x509.Certificate {
	c, err := x509.ParseCertificate(der)
	if err != nil {
		panic(err)
	}
	return c
}

// BuildRequest makes an AuthnRequest and the URL to send the browser to.
func BuildRequest(idp *IdP, keys *Keys, relayState string) (requestID, redirectURL string, err error) {
	sp, err := serviceProvider(idp, keys, time.Now())
	if err != nil {
		return "", "", err
	}
	doc, err := sp.BuildAuthRequestDocument()
	if err != nil {
		return "", "", err
	}
	requestID = doc.Root().SelectAttrValue("ID", "")
	if requestID == "" {
		return "", "", errors.New("samlsp: AuthnRequest without an ID")
	}
	redirectURL, err = sp.BuildAuthURLRedirect(relayState, doc)
	return requestID, redirectURL, err
}

// Result is a verified sign-in.
type Result struct {
	// AssertionID and Expires feed the replay guard.
	AssertionID string
	Expires     time.Time
	// InResponseTo is the request the response answers ("" for IdP-initiated sign-in).
	InResponseTo string
	NameID       string
	Email, Name  string
	Groups       []string
}

const transientFormat = "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"
const emailFormat = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"

var (
	emailAttrs  = []string{"email", "mail", "emailaddress", "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/emailaddress", "urn:oid:0.9.2342.19200300.100.1.3"}
	nameAttrs   = []string{"name", "displayname", "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name", "http://schemas.microsoft.com/identity/claims/displayname", "urn:oid:2.16.840.1.113730.3.1.241", "cn"}
	groupsAttrs = []string{"groups", "memberof", "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups", "http://schemas.xmlsoap.org/claims/group", "role", "roles"}
)

// attr returns the values of the first name (the configured one only, or the common names in
// order) that any attribute carries, matching names and friendly names case-insensitively.
// Values of every attribute with that name are merged: some identity providers send each group
// as its own Attribute element.
func attr(attrs []types.Attribute, configured string, common []string) []string {
	names := common
	if configured != "" {
		names = []string{configured}
	}
	for _, want := range names {
		var out []string
		seen := map[string]bool{}
		for _, a := range attrs {
			if !strings.EqualFold(a.Name, want) && (a.FriendlyName == "" || !strings.EqualFold(a.FriendlyName, want)) {
				continue
			}
			for _, v := range a.Values {
				if s := strings.TrimSpace(v.Value); s != "" && !seen[s] {
					seen[s] = true
					out = append(out, s)
				}
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

// groupsLink is what Microsoft Entra ID sends instead of the groups past 150 of them.
const groupsLink = "http://schemas.microsoft.com/claims/groups.link"

func parseTime(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

// ParseResponse verifies a posted SAMLResponse. expected is the ID of the AuthnRequest BeziSync
// sent for this browser, or "" when IdP-initiated sign-in is accepted.
func ParseResponse(idp *IdP, keys *Keys, encoded, expected string, now time.Time) (*Result, error) {
	if encoded == "" {
		return nil, fail("The identity provider sent no SAML response.")
	}
	if len(encoded) > 4*MaxMetadata {
		return nil, fail("The SAML response is too large.")
	}
	sp, err := serviceProvider(idp, keys, now)
	if err != nil {
		return nil, err
	}
	resp, err := sp.ValidateEncodedResponse(encoded)
	if err != nil {
		return nil, fail("The SAML response is not valid (%s).", describe(err))
	}
	if len(resp.Assertions) != 1 {
		return nil, fail("The SAML response must carry exactly one signed assertion (it carries %d).", len(resp.Assertions))
	}
	a := resp.Assertions[0]
	scd := a.Subject.SubjectConfirmation.SubjectConfirmationData
	if expected != "" {
		if resp.InResponseTo != expected || scd.InResponseTo != expected {
			return nil, fail("The SAML response does not answer the sign-in BeziSync started. Start again.")
		}
	} else if resp.InResponseTo != "" || scd.InResponseTo != "" {
		return nil, fail("The SAML response answers a sign-in this browser did not start. Start again.")
	}
	c := a.Conditions
	if c == nil || c.NotBefore == "" || c.NotOnOrAfter == "" {
		return nil, fail("The SAML assertion has no validity window (Conditions NotBefore / NotOnOrAfter).")
	}
	notBefore, ok1 := parseTime(c.NotBefore)
	notOnOrAfter, ok2 := parseTime(c.NotOnOrAfter)
	if !ok1 || !ok2 {
		return nil, fail("The SAML assertion's validity window can't be read.")
	}
	if now.Add(Skew).Before(notBefore) {
		return nil, fail("The SAML assertion is not valid yet: check the identity provider's clock.")
	}
	if !now.Add(-Skew).Before(notOnOrAfter) {
		return nil, fail("The SAML assertion has expired. Sign in again.")
	}
	if len(c.AudienceRestrictions) == 0 {
		return nil, fail("The SAML assertion names no audience; configure the identity provider with BeziSync's entity ID (%s).", EntityID())
	}
	for _, r := range c.AudienceRestrictions {
		found := false
		for _, aud := range r.Audiences {
			found = found || aud.Value == EntityID()
		}
		if !found {
			return nil, fail("The SAML assertion is for another service; set the audience to BeziSync's entity ID (%s).", EntityID())
		}
	}
	if a.ID == "" {
		return nil, fail("The SAML assertion has no ID.")
	}
	if a.Subject.NameID == nil || strings.TrimSpace(a.Subject.NameID.Value) == "" {
		return nil, fail("The SAML assertion names no user (NameID).")
	}
	nameID := strings.TrimSpace(a.Subject.NameID.Value)
	if a.Subject.NameID.Format == transientFormat {
		return nil, fail("The identity provider sends a transient NameID, which changes at every sign-in; configure a persistent or email NameID.")
	}
	var values []types.Attribute
	if a.AttributeStatement != nil {
		values = a.AttributeStatement.Attributes
	}
	res := &Result{AssertionID: a.ID, Expires: notOnOrAfter, InResponseTo: resp.InResponseTo, NameID: nameID}
	if exp, ok := parseTime(scd.NotOnOrAfter); ok && exp.After(res.Expires) {
		res.Expires = exp
	}
	if v := attr(values, idp.EmailAttr, emailAttrs); len(v) > 0 {
		res.Email = strings.ToLower(v[0])
	} else if a.Subject.NameID.Format == emailFormat || strings.Contains(nameID, "@") {
		res.Email = strings.ToLower(nameID)
	}
	if res.Email == "" {
		return nil, fail("The identity provider sent no email address (an email attribute, or an email NameID).")
	}
	if _, msg := val.ValidateEmail(res.Email); msg != "" {
		return nil, fail("The identity provider sent an email address that is not valid.")
	}
	if v := attr(values, idp.NameAttr, nameAttrs); len(v) > 0 {
		res.Name = v[0]
	} else {
		given := attr(values, "", []string{"givenname", "firstname", "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/givenname", "urn:oid:2.5.4.42"})
		sur := attr(values, "", []string{"surname", "lastname", "sn", "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/surname", "urn:oid:2.5.4.4"})
		res.Name = strings.TrimSpace(strings.Join(append(append([]string{}, first(given)...), first(sur)...), " "))
	}
	res.Groups = attr(values, idp.GroupsAttr, groupsAttrs)
	if res.Groups == nil {
		// Too many groups: Entra sends a link instead. Signing in with none would silently drop
		// every role they grant.
		if attr(values, "", []string{groupsLink}) != nil {
			return nil, fail("Your identity provider sent a link to your groups instead of the groups (you belong to too many). " +
				"Ask an Admin to send only the groups assigned to the BeziSync app.")
		}
		res.Groups = []string{}
	}
	return res, nil
}

func first(l []string) []string {
	if len(l) == 0 {
		return nil
	}
	return l[:1]
}

// describe turns library errors into short reasons (no XML echoed back).
func describe(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Signature could not be verified"), strings.Contains(msg, "Could not verify certificate against trusted certs"):
		return "the signature does not match the identity provider's certificate"
	case strings.Contains(msg, "Missing signature"), strings.Contains(msg, "missing signature"), errors.Is(err, dsig.ErrMissingSignature):
		return "it is not signed"
	case strings.Contains(msg, "Cert is not valid at this time"):
		return "the identity provider's certificate has expired or is not valid yet"
	case strings.Contains(msg, "Issuer"):
		return "it was issued by another identity provider"
	case strings.Contains(msg, "Destination"), strings.Contains(msg, "Recipient"):
		return "it was sent to another address than BeziSync's ACS URL"
	case strings.Contains(msg, "StatusCode"):
		return "the identity provider reported a failure"
	case strings.Contains(msg, "NotOnOrAfter"):
		return "it has expired"
	}
	if len(msg) > 160 {
		msg = msg[:160]
	}
	return msg
}
