# security

## Overview
Package `security` holds the cryptographic primitives: Argon2 password hashing, opaque bearer
tokens and their keyed hashes, and AES-256-GCM encryption of stored secrets under the master
key. Formats are byte-compatible with the Python releases, so existing databases keep working.
[`auth`](../auth/README.md) uses the token hashes; services use the envelopes for connection
and secret-store credentials.

## Interface
- `[security.go:HashPassword]` / `[security.go:VerifyPassword]` — argon2id PHC strings with argon2-cffi defaults (t=3, m=64 MiB, p=4); verify also accepts argon2i.
- `[security.go:NewToken]` (`<prefix>_` + 32 random bytes), `[security.go:TokenURLSafe]` — opaque secrets.
- `[security.go:TokenHash]` (HMAC-SHA256 under the current master key) and `[security.go:TokenHashes]` (under every known key, current first) — what the database stores for sessions and tokens.
- `[security.go:EncryptSecret]` / `[security.go:MustEncrypt]` — seal a JSON value as `v1.<keyid>.<nonce>.<ciphertext>`.
- `[security.go:DecryptSecret]` (to an object, numbers as `json.Number`), `[security.go:DecryptInto]`, `[security.go:DecryptRaw]` — open an envelope; `[security.go:ErrUnknownKey]` when the key id is unknown.
- `[security.go:Keys]`, `[security.go:Key]`, `[security.go:KeyID]`, `[security.go:CurrentKeyID]`, `[security.go:SecretKeyID]` — master-key ring and key ids (first 8 hex chars of SHA-256).
- `[security.go:ConstantTimeEqual]` — timing-safe string comparison.

## Invariants
- Never store a raw token; store `TokenHash` and look up by any of `TokenHashes` so sessions and tokens survive a master-key rotation (`[security.go:TokenHashes]`).
- Encryption always uses the current key; decryption accepts the current and every `PreviousMasterKeys` entry, matched by key id (`[security.go:Keys]`, `[security.go:DecryptRaw]`).
- The envelope version string `v1` is the GCM additional data; changing it breaks every stored secret (`[security.go:envelopeVersion]`).
- Decrypted numbers stay `json.Number` so integers re-encrypt unchanged (`[security.go:DecryptSecret]`).
- Malformed password hashes fail verification instead of erroring; comparison is constant-time (`[security.go:VerifyPassword]`).
- An undecodable master key panics (configuration error) (`[security.go:decodeKey]`); a random-source failure panics in `TokenURLSafe`.

## Side Effects
- Reads `MasterKey` and `PreviousMasterKeys` from `config.Get()`. Uses `crypto/rand`. No I/O otherwise.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/security/"`
