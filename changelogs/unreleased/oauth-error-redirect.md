type: patch

### Changed
- **Agent sign-in errors** - when an AI agent's sign-in request is refused after BeziSync recognised the agent, the error goes back to the redirect address the agent registered, taken from that registration rather than from the request. Behaviour is unchanged; the rule no longer depends on an implicit convention in the code.
