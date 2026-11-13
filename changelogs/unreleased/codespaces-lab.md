type: minor

### Added
- **Run your own lab in GitHub Codespaces** - the README's "Open in GitHub Codespaces" button starts a private BeziSync in your own codespace: the control plane, sample PostgreSQL and MySQL shops with live orders, a PostgreSQL that isn't ready for change data capture (so preflight shows its fixes), Kafka, a PostgreSQL warehouse and an HTTP endpoint, with a random admin password. Nothing is hosted for you; it runs on your free Codespaces quota (ADR 0018).
- **More in the Sandbox** - the demo stack's Sandbox project can now also create a MySQL source, a PostgreSQL source that isn't ready for CDC, a JDBC warehouse and an HTTP destination (each destination with a template), and teammates for the Editor, Operator and Viewer roles. Each is opt-in through new `BEZISYNC_SANDBOX_*` settings and created once, with the project; without them the Sandbox is unchanged.

### Fixed
- **Security updates** - built with Go 1.27.4 (GO-2026-6859 in `net/http`) and Apache Thrift 0.25.0 (CVE-2026-82458, CVE-2026-63772, CVE-2026-83663, CVE-2026-66055, CVE-2026-94637).
