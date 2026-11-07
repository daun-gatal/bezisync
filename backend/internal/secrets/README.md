# secrets

## Overview
Reads connection secrets kept in external stores (HashiCorp Vault KV v2, AWS Secrets Manager,
Google Secret Manager, Kubernetes Secrets). A secret field holds `ref+<store>://<path>#<key>`
instead of a value; only the reference is stored, and the value is read from the store when a
connector needs it. Stores are configured per project; services build a client from a store's
config and decrypted credentials.

## Interface
- `[secrets.go:Ref]` / `[secrets.go:ParseRef]` — a parsed reference; `ParseRef` returns nil for values that do not start with `ref+`.
- `[secrets.go:Client]` — `Read`, `Test` and `List(limit)` for one store.
- `[secrets.go:Build]` — creates a client for a kind in `[secrets.go:Kinds]` (`vault`, `aws`, `gcp`, `kubernetes`).
- `[secrets.go:Get]` — reads a path and optionally one JSON key of it; `[secrets.go:Keys]` — the sorted key names of a JSON-object secret.
- `[secrets.go:ListLimit]` — 1000, how many paths a picker is offered.
- `[secrets.go:Error]` — store failures, with messages that never contain secret values.
- `[secrets.go:DumpsUnsorted]` — `json.dumps(v)` in insertion order (the shape a whole-secret read returns).

## Invariants
- References must match `[secrets.go:refRe]` (store `[a-z0-9][a-z0-9-]{0,62}`, path without `#` or spaces, optional key); a malformed `ref+` value is an `Error`.
- Values never leave `Read`/`Get`: `[secrets.go:Keys]` returns names only, `List` never reads a value, and error messages name paths and keys but not contents.
- User-supplied addresses go through the SSRF guard, but not all of them at dial time:
  - Vault URLs are checked by `[secrets.go:guard]` (a clear early error), then every call goes through `ssrf.HTTPClient` (each dial checked, no environment proxy) without following redirects, and the answer is read up to `[secrets.go:maxStoreResponse]` (4 MiB) (`[secrets.go:vault.call]`). AWS Secrets Manager calls (a custom `endpoint` included) go through `ssrf.HTTPClient`, set after the credential chain loaded so instance and pod roles still work; Google Secret Manager reads at most 4 MiB.
  - AWS custom endpoints are checked once in `[secrets.go:newAWS]`; requests then go through the AWS SDK's own HTTP client.
  - GCP custom endpoints are checked at `[secrets.go:Build]` and every request dials through `ssrf.HTTPClient` (`[secrets.go:gcpStore.get]`); without a service-account key such an endpoint is treated as an emulator and gets no sign-in.
  - Pasted kubeconfigs dial through `ssrf.DialContext` with environment proxies disabled (`[secrets.go:kubeconfigREST]`).
- Pasted kubeconfigs that sign in with `exec` or an auth provider are refused (`[secrets.go:kubeconfigREST]`).
- Kubernetes listings skip service-account tokens, Helm releases and image pull secrets (`[secrets.go:skippedSecretTypes]`).
- `List` results are sorted and capped at the given limit, with `truncated` set when more exist (`[secrets.go:capped]`).
- Vault and GCP HTTP calls time out after 8 s (`[secrets.go:timeout]`); the Vault token (static, or from the first AppRole login) is cached on the client and never refreshed.
- `[secrets.go:Get]` returns a string key's value as is and any other JSON value re-encoded with `[secrets.go:DumpsUnsorted]`; Vault and Kubernetes reads return the whole secret as a JSON object (Kubernetes keys sorted).

## Side Effects
- Network calls to Vault (`/v1/auth/<mount>/login`, `/v1/<mount>/data|metadata/…`, `/v1/auth/token/lookup-self`), AWS Secrets Manager, `https://secretmanager.googleapis.com/v1` or a custom endpoint, and the Kubernetes API.
- DNS resolution through `ssrf.ResolveChecked`; the AWS client and Google default credentials read the process's AWS/GCP environment and credential files when no keys are given; the in-cluster Kubernetes client reads the pod's service account.

## Verification
- Run: `make verify-quick GO_PKGS="./internal/secrets/"`
