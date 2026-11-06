type: patch

### Fixed
- **Review stops re-saving a change the server refused** - when a save is refused (a property BeziSync doesn't allow, a setting only the server checks, someone else's edit, a network error), Review & deploy shows why and waits: it tries again when you change something, click Try again (for network and server errors) or leave the step, instead of saving every second. A refused advanced property is caught as you type, in Guided mode too, and one an older pipeline already has still saves.
- **Review says which field to fix** - "Not saved" now names the field (**Not saved: fix Memory**, or **Memory and 2 more**) and takes you to it, and each error sits under its own field: a custom memory over the project's limit is shown under Memory, not CPU, and a Flink CDC streaming size under the streaming field it is about. In Guided mode, an engine setting or advanced property that blocks saving is listed with Remove instead of blocking unseen.
