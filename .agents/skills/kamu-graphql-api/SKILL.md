---
name: kamu-graphql-api
description: GraphQL API workflow for Kamu CLI. Use when adding or changing GraphQL queries, mutations, roots, resolvers, GraphQL models, code-first schema generation, GraphQL enum mappings, or schema regeneration in kamu-adapter-graphql.
---

# Kamu GraphQL API

GraphQL schema is code-first in `kamu-adapter-graphql`.

## Implementation Rules

- Never edit `resources/schema.gql` by hand.
- Add new API surface through grouped root objects in `src/adapter/graphql/src/root.rs`.
- Follow the existing query and mutation module structure.
- Keep resolver and root files focused on entrypoints.
- Extract GraphQL value types and conversions into `models.rs` when the surface grows.
- For schema-only prototypes explicitly requested by the user, resolver bodies may be left as `todo!()`.

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
