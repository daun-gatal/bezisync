type: patch

### Fixed
- **Review stops re-saving a change the server refused** - when a save is refused (a property BeziSync doesn't allow, a setting only the server checks, someone else's edit, a network error), Review & deploy shows why and waits: it tries again when you change something, click Try again (for network and server errors) or leave the step, instead of saving every second. A refused advanced property is caught as you type, in Guided mode too, and one an older pipeline already has still saves.
