type: minor

### Added
- **About BeziSync** - Help & tour → About BeziSync shows the server version and the license (Apache 2.0), links the source code, and opens the licenses and notices of the third-party software in the web UI and the server.
- **Licenses shipped with BeziSync** - The web image serves `/licenses.txt`; the server binary carries its own (`bezisync licenses`, `GET /api/v1/system/licenses`, and `LICENSES.txt` in each CLI archive). Each lists BeziSync's NOTICE and LICENSE, then every included dependency with its license text and notices.
