## Description

<!-- Provide a brief description of what this PR does. Pull requests target `preview`: merging releases nothing; a maintainer releases from preview. -->

## Type of Change

<!-- Check all that apply. -->

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Refactor
- [ ] CI/release/chore
- [ ] Other:

## Related Issue

<!-- Link to related issue(s), if any. -->
Closes #

## Changes Made

<!-- Describe the changes in detail. -->

## Testing

<!-- Describe how you tested your changes. See `.rules/testing.md` ("Who runs what"). -->

- [ ] `make verify` passes locally
- [ ] Helm charts: `make chart-test` when a chart changed
- [ ] Docker build or compose flow tested when relevant
- [ ] Security-sensitive paths reviewed when relevant

**End-to-end specs run** (`make e2e-quick SPECS="…"`): <!-- list them, or write "not run" -->

<!-- Maintainers run the full `make e2e` on the head commit before merging and post the result here. -->

### Test Steps

<!-- If applicable, provide steps to test the changes. -->

## Screenshots

<!-- If applicable, add screenshots or screen recordings. -->

## Changelog Fragment and chart version

<!-- A fragment is required exactly when the PR changes an image (backend/, frontend/ outside their .dockerignore); CI fails otherwise. The next release from preview ships it. -->
<!-- A change under deploy/helm/<chart>/ bumps that chart's version in its Chart.yaml instead (no fragment), and `make chart-docs` regenerates its README. -->
<!-- If the PR number is not known yet, use a descriptive temporary filename and rename later if practical. -->

- [ ] Added `changelogs/unreleased/<pr-number>-<slug>.md`
- [ ] Bumped the version of every changed chart and ran `make chart-docs`
- [ ] Not needed: no image or chart changes

<details>
<summary>Fragment format</summary>

```md
type: patch

### Fixed
- **Feature name** - description
```

Use `major` for breaking changes, `minor` for new features, and `patch` for bug fixes or internal improvements.
</details>

## Checklist

- [ ] I read `AGENTS.md` or `CLAUDE.md`
- [ ] I kept secrets out of logs, API responses, compiled previews, and test fixtures
- [ ] I added or updated tests for behavior changes
- [ ] I updated docs or guides for build/run/test/release changes
- [ ] I checked for active-pipeline state safety when changing mutations/deletes
- [ ] I performed a self-review

## Additional Notes

<!-- Anything reviewers should know. -->
