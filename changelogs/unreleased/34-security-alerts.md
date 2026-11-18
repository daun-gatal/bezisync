type: patch

### Fixed
- **Sign-in redirect** - the address to continue to after signing in could leave BeziSync: a tab or newline after the first `/` passed the check, and browsers drop those characters (`/<tab>/evil.example` became `//evil.example`). A backslash, whitespace or control character anywhere now sends you to the home page, in the server and the web app.
- **Redis checks over TLS** - the check accepted any certificate a trusted authority issued, for any name, before sending the password. It now also requires the certificate to name the host you entered, and says why it refused a certificate instead of "Redis did not answer". Connect by a name the certificate lists, not an IP address (Debezium Server itself still checks only the chain).
- **Sign-out cookies** - signing out and finishing an SSO sign-in now clear the session, CSRF and SSO cookies with the same `HttpOnly`, `Secure` and `SameSite` attributes they were set with.
- **Integer settings** - a whole number past the platform's range (`1e19`) was turned into an arbitrary value instead of being refused; MySQL size estimates past 64 bits showed as negative numbers.
