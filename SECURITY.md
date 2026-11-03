# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a security problem. Report it privately through
GitHub: **Security → Report a vulnerability** on this repository (private vulnerability
reporting). Include what you found, how to reproduce it, the version (`bezisync version`, or the
image tag) and what an attacker gains.

We acknowledge a report within **3 business days**, keep you updated while we fix it, and credit
you in the release notes if you want.

## Supported versions

Only the latest release receives security fixes. BeziSync is below 1.0: upgrade to the newest
`X.Y.Z` to get a fix.

## Handling a report (maintainers)

1. Reproduce it, and confirm whether it crosses a boundary in the
   [threat model](docs/security/threat-model.md).
2. Fix leaked secrets, crossing a project boundary, privilege escalation and code execution
   before anything else, in a private security advisory fork, and release the fix.
3. Publish the advisory, reply to the reporter and credit them if they want.
4. If the report shows a missing control, add it to the threat model in the same change.

## For operators

The [threat model](docs/security/threat-model.md) lists the controls, the trust boundaries and the
risks an operator must handle. Production hardening is in the [guide](docs/README.md). Instances
run with `BEZISYNC_LAB=true` show testers the in-app testing policy (`/testing-policy`) and take
their reports through **Feedback**; admins read them under **Settings → Feedback**.
