type: patch

### Fixed
- **Iceberg REST catalogs that can't vend credentials pass the write check** - BeziSync's own catalog client (checks, the "Write a test table" check, tails, column reads) asked the catalog for vended credentials on every request, even for a connection with its own object-storage keys (iceberg-go's default). A catalog that can't vend, such as Polaris over storage without STS, refused the request with HTTP 400 ("Credential vending was requested … but no credentials are available"), and the check blocked the deploy although the connector writes with the connection's keys. Only "Credentials vended by the catalog" asks for them now.
