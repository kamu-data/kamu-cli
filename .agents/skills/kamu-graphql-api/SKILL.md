---
name: kamu-graphql-api
description: GraphQL API workflow for Kamu CLI. Use when adding or changing GraphQL queries, mutations, roots, resolvers, GraphQL models, code-first schema generation, GraphQL enum mappings, or schema regeneration in kamu-adapter-graphql.
---

# Kamu GraphQL API

GraphQL schema is code-first in `kamu-adapter-graphql`.

## Implementation Rules

### 1. File Organization

- Never edit `resources/schema.gql` by hand.
- Add new API surface through grouped root objects in `src/adapter/graphql/src/root.rs`.
- Follow the existing query and mutation module structure.
- Keep resolver and root files focused on entrypoints.
- Extract GraphQL value types and conversions into `models.rs` when the surface grows.

### 2. Resolver Implementation

- Implement resolver bodies fully for production code.
- For schema-only prototypes where the user explicitly states to defer implementation (e.g., "just add the schema without implementation" or "stub out the resolver"), resolver bodies may be left as `todo!()`.

## Enums

- For GraphQL enums that mirror domain enums, prefer `#[graphql(remote = ...)]` when names align.
- If names do not align, prefer unifying names unless compatibility requires otherwise.
- Avoid hand-written mapping impls when a remote enum mapping is sufficient.

## Schema Regeneration

Regenerate the schema with the existing test:

```sh
cargo nextest run -E 'test(update_graphql_schema)'
```

Review generated schema changes for intentional API surface only.

### Handling Breaking Changes

If a schema change introduces breaking changes (removes fields, changes types, renames operations):

- Document the breaking changes in the PR description.
- Update `CHANGELOG.md` under `### Changed` with the API change.
- Consider whether a deprecation period is needed before removal.
