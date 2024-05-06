## Description

Closes: #issue

<!--- Describe your changes in detail and include your changelog entries -->

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
    <!-- Change does not includes new versions of any container images -->
  - [ ] Container images: ✅
- [ ] Documentation:
    <!-- Document how your changes benefit or affect the *end user* -->
  - [ ] [Changelog](./CHANGELOG.md): ✅
    <!-- How will users find out about and learn how to use this feature?
         Leave checkmark if documentation is not required or generated via API schemas / CLI reference.
         Include a PR for `kamu-docs` repo or a ticket reference otherwise -->
  - [ ] [Public documentation](https://github.com/kamu-data/kamu-docs/): ✅
  <!-- If this change will require some updates in downstream services mark with ❌ -->
- [ ] Downstream effects:
    <!-- Will node need to be updated with new services or DI catalog configuration? -->
  - [ ] [kamu-node](https://github.com/kamu-data/kamu-node): ✅