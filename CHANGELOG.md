# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.2.0] - 2026-11-04

### Added
- **About BeziSync** - Help & tour → About BeziSync shows the server version and the license (Apache 2.0), links the source code, and opens the licenses and notices of the third-party software in the web UI and the server.
- **Licenses shipped with BeziSync** - The web image serves `/licenses.txt`; the server binary carries its own (`bezisync licenses`, `GET /api/v1/system/licenses`, and `LICENSES.txt` in each CLI archive). Each lists BeziSync's NOTICE and LICENSE, then every included dependency with its license text and notices.

### Changed
- **Lab testing policy** - The open lab's testing policy now says BeziSync is open source (Apache 2.0) and links its GitHub repository, offers GitHub private vulnerability reporting alongside in-app Feedback, and points load testing at your own copy.

## [v0.1.0] - 2026-11-03

### Added
- **BeziSync is open source** - the first public release under the Apache License 2.0: a self-service control plane for Debezium Server with preflight checks, source protection, one-click operations, evidence-based health, encrypted write-only secrets, RBAC and SSO, per-pipeline personal-data handling and AI agents over MCP
