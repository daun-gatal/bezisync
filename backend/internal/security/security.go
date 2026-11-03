// Package security provides password hashing, opaque tokens and secret encryption.
//
// Secrets are encrypted with AES-256-GCM under the process master key. Each ciphertext
// carries a key id (first 8 hex chars of SHA-256 of the key) so keys can be rotated:
// previous master keys stay valid for decryption while `bezisync rotate-master-key`
// re-encrypts every row under the new key. The envelope format, password hashes and token
// hashes are byte-compatible with the Python implementation.
package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
)

const envelopeVersion = "v1"

// argon2-cffi defaults (PasswordHasher()): argon2id, t=3, m=64 MiB, p=4, 32-byte hash, 16-byte salt.
const (
	argonTime    = 3
	argonMemory  = 65536
	argonThreads = 4
	argonKeyLen  = 32
	argonSaltLen = 16
)

var b64 = base64.RawStdEncoding

// HashPassword returns a PHC string identical in format to argon2-cffi's.
func HashPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads, b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}

// VerifyPassword checks a password against a PHC hash (argon2id or argon2i). Malformed
// hashes simply fail.
func VerifyPassword(hash, password string) bool {
	parts := strings.Split(hash, "$")
	// "", "argon2id", "v=19", "m=..,t=..,p=..", salt, key
	if len(parts) != 6 || parts[0] != "" {
		return false
	}
	variant := parts[1]
	if variant != "argon2id" && variant != "argon2i" {
		return false
	}
	if parts[2] != "v="+strconv.Itoa(argon2.Version) {
		return false
	}
	var m, t uint32
	var p uint8
	for _, kv := range strings.Split(parts[3], ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return false
		}
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return false
		}
		switch k {
		case "m":
			m = uint32(n)
		case "t":
			t = uint32(n)
		case "p":
			if n > 255 {
				return false
			}
			p = uint8(n)
		default:
			return false
		}
	}
	if m == 0 || t == 0 || p == 0 {
		return false
	}
	salt, err := b64.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := b64.DecodeString(parts[5])
	if err != nil || len(want) == 0 {
		return false
	}
	var got []byte
	if variant == "argon2id" {
		got = argon2.IDKey([]byte(password), salt, t, m, p, uint32(len(want))) //nolint:gosec // len(want) is small
	} else {
		got = argon2.Key([]byte(password), salt, t, m, p, uint32(len(want))) //nolint:gosec // len(want) is small
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

// TokenURLSafe is Python's secrets.token_urlsafe(n): n random bytes, URL-safe base64, unpadded.
func TokenURLSafe(n int) string {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// NewToken returns an opaque bearer secret such as "bzs_…" (session) or "bzt_…" (API token).
func NewToken(prefix string) string {
	return prefix + "_" + TokenURLSafe(32)
}

func hmacHex(key []byte, token string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil))
}

// TokenHash is the keyed hash of a bearer secret under the current master key.
func TokenHash(token string) string {
	return hmacHex(currentKey(), token)
}

// TokenHashes are the hashes under the current and every previous master key, current
// first; lookups match any so sessions and tokens survive a key rotation.
func TokenHashes(token string) []string {
	keys := Keys()
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, hmacHex(k.Raw, token))
	}
	return out
}

// Key is one master key and its id.
type Key struct {
	ID  string
	Raw []byte
}

// KeyID is the first 8 hex chars of SHA-256 of the key.
func KeyID(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:8]
}

func decodeKey(encoded string) []byte {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		panic(fmt.Sprintf("invalid master key: %v", err))
	}
	return raw
}

func currentKey() []byte {
	return decodeKey(config.Get().MasterKey)
}

// Keys returns the current key followed by previous keys (deduplicated by id).
func Keys() []Key {
	s := config.Get()
	encoded := []string{s.MasterKey}
	for _, k := range strings.Split(s.PreviousMasterKeys, ",") {
		if strings.TrimSpace(k) != "" {
			encoded = append(encoded, k)
		}
	}
	seen := map[string]bool{}
	var out []Key
	for _, e := range encoded {
		raw := decodeKey(e)
		id := KeyID(raw)
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, Key{ID: id, Raw: raw})
	}
	return out
}

// CurrentKeyID is the id of the current master key.
func CurrentKeyID() string {
	return KeyID(currentKey())
}

// EncryptSecret seals a JSON object in a v1 envelope: "v1.<keyid>.<nonce>.<ciphertext>".
func EncryptSecret(values any) (string, error) {
	key := currentKey()
	payload, err := pyjson.DumpsCompact(values)
	if err != nil {
		return "", err
	}
	gcm, err := newGCM(key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := gcm.Seal(nil, nonce, []byte(payload), []byte(envelopeVersion))
	return strings.Join([]string{
		envelopeVersion, KeyID(key),
		base64.StdEncoding.EncodeToString(nonce), base64.StdEncoding.EncodeToString(ct),
	}, "."), nil
}

// MustEncrypt is EncryptSecret for values that always serialize.
func MustEncrypt(values any) string {
	s, err := EncryptSecret(values)
	if err != nil {
		panic(err)
	}
	return s
}

// ErrUnknownKey means the envelope was sealed under a key this process does not know.
var ErrUnknownKey = errors.New("secret was encrypted with an unknown master key")

// DecryptSecret opens an envelope into a JSON object.
func DecryptSecret(blob string) (map[string]any, error) {
	raw, err := DecryptRaw(blob)
	if err != nil {
		return nil, err
	}
	// Numbers stay json.Number so integers re-encrypt as integers (no float drift).
	generic, err := pyjson.DecodeGeneric(raw)
	if err != nil {
		return nil, err
	}
	out, ok := generic.(map[string]any)
	if !ok {
		return nil, errors.New("secret envelope does not hold an object")
	}
	return out, nil
}

// DecryptInto opens an envelope into v.
func DecryptInto(blob string, v any) error {
	raw, err := DecryptRaw(blob)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// DecryptRaw opens an envelope and returns the JSON bytes.
func DecryptRaw(blob string) ([]byte, error) {
	parts := strings.Split(blob, ".")
	if len(parts) != 4 {
		return nil, errors.New("malformed secret envelope")
	}
	if parts[0] != envelopeVersion {
		return nil, errors.New("unsupported secret envelope")
	}
	var key []byte
	for _, k := range Keys() {
		if k.ID == parts[1] {
			key = k.Raw
			break
		}
	}
	if key == nil {
		return nil, ErrUnknownKey
	}
	nonce, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, err
	}
	ct, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil {
		return nil, err
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("malformed secret envelope")
	}
	return gcm.Open(nil, nonce, ct, []byte(envelopeVersion))
}

// SecretKeyID returns the key id an envelope was sealed under.
func SecretKeyID(blob string) string {
	parts := strings.Split(blob, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// ConstantTimeEqual compares two secrets without leaking timing.
func ConstantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
