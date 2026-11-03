## Description

<!-- Provide a brief description of what this PR does. -->

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

<!-- Describe how you tested your changes. -->

- [ ] Backend tests/lint/types pass
- [ ] Frontend tests/types/build pass
- [ ] Docker build or compose flow tested when relevant
- [ ] Security-sensitive paths reviewed when relevant

### Test Steps

<!-- If applicable, provide steps to test the changes. -->

## Screenshots

<!-- If applicable, add screenshots or screen recordings. -->

## Changelog Fragment

<!-- Required exactly when the PR changes an image or chart (backend/, frontend/ outside their .dockerignore, deploy/helm/); CI fails otherwise. Merging it releases. -->
<!-- If the PR number is not known yet, use a descriptive temporary filename and rename later if practical. -->

- [ ] Added `changelogs/unreleased/<pr-number>-<slug>.md`
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
