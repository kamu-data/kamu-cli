## Description

<!-- Link issues that will be closed automatically when this PR is merged -->
Closes: #issue

<!-- Describe your changes in detail for the reviewers (e.g. include your changelog entries) -->

## Checklist before requesting a review

- [ ] Unit and integration tests added
  <!-- Replace with ❌ if the statement is false and include an explanation -->
- [ ] Compatibility:
    <!-- Old clients can communicate to the new version without upgrading -->
  - [ ] Network APIs: ✅
    <!-- New version will work with the old workspaces, repositories, and metadata -->
  - [ ] Workspace layout and metadata: ✅
    <!-- New version can read existing user configs -->
  - [ ] Configuration: ✅
    <!-- Change does not include new versions of any container images -->
  - [ ] Container images: ✅
- [ ] Observability:
    <!-- How will we know how the feature behaves in production, is it being used, is it working correctly? -->
  - [ ] Tracing / [metrics](https://github.com/kamu-data/kamu-standards/blob/master/metrics_design.md): ✅
    <!-- How will we find out that the feature breaks -->
  - [ ] Alerts: ✅
- [ ] Documentation:
    <!-- Document how your changes benefit or affect the *end user* -->
  - [ ] [Changelog](./CHANGELOG.md): ✅
    <!-- How will users find out about and learn how to use this feature?
         Leave checkmark if documentation is not required or generated via API schemas / CLI reference.
         Include a PR for `kamu-docs` repo or a ticket reference otherwise -->
  - [ ] [Public documentation](https://github.com/kamu-data/kamu-docs/): ✅
  <!-- If this change will require some updates in downstream services mark with ❌ -->
- [ ] Downstream effects:
    <!-- Will the node need to be updated, e.g. with new services or DI catalog configuration? -->
  - [ ] [kamu-node](https://github.com/kamu-data/kamu-node): ✅
    <!-- Will web-ui need to be updated, e.g. to new GQL / REST API schemas? -->
  - [ ] [kamu-web-ui](https://github.com/kamu-data/kamu-web-ui): ✅
    <!-- Non-trivial deployment instructions added to release queue -->
  - [ ] [release-queue](https://github.com/orgs/kamu-data/projects/4/views/1)