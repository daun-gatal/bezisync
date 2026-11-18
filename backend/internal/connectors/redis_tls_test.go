package connectors

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

// The check verifies the certificate chain and the host name before sending the password.
func TestRedisTLSVerifiesTheHostName(t *testing.T) {
	r := redisClient(redisConfig{Host: "redis.example", Port: 6380, TLS: true}, time.Second)
	defer func() { _ = r.Close() }()
	tc := r.Options().TLSConfig
	if tc == nil || tc.InsecureSkipVerify || tc.ServerName != "redis.example" || tc.VerifyConnection != nil {
		t.Fatalf("TLS config %+v", tc)
	}
}

// A certificate BeziSync refuses is explained, through go-redis's error wrapping.
func TestRedisCertificateProblemsAreExplained(t *testing.T) {
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_SSRF_ALLOW_CIDRS" {
			return `["127.0.0.0/8"]`, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)

	cert := selfSigned(t, "redis.example")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	received := make(chan int, 8)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			buf := make([]byte, 64)
			n, _ := c.Read(buf) // the handshake fails on the client side: nothing arrives
			received <- n
			_ = c.Close()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	pw := "s3cret-password"
	r := redisClient(redisConfig{Host: "127.0.0.1", Port: port, TLS: true, Password: &pw}, 2*time.Second)
	defer func() { _ = r.Close() }()
	err = r.Ping(context.Background()).Err()
	if err == nil {
		t.Fatal("a self-signed certificate was accepted")
	}
	if msg := redisCertProblem(err, "127.0.0.1"); !strings.Contains(msg, "not signed by an authority") {
		t.Fatalf("%q from %v", msg, err)
	}
	if n := <-received; n != 0 {
		t.Fatalf("the server received %d bytes after the handshake", n)
	}

	leaf, _ := x509.ParseCertificate(cert.Certificate[0])
	hostErr := &tls.CertificateVerificationError{Err: x509.HostnameError{Certificate: leaf, Host: "10.0.0.5"}}
	if msg := redisCertProblem(hostErr, "10.0.0.5"); !strings.Contains(msg, "not for 10.0.0.5") {
		t.Fatal(msg)
	}
	if msg := redisCertProblem(context.DeadlineExceeded, "h"); msg != "" {
		t.Fatal(msg)
	}
}

func selfSigned(t *testing.T, host string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: host}, DNSNames: []string{host}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
