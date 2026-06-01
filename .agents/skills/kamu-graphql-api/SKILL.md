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

## Remote GraphQL Facade (Backend-to-Backend)

### Location

`src/domain/resources/facade/src/facade/graphql/cynic_api/`

### Mechanism

The facade uses [Cynic](https://cynic-rs.dev/) for typed, compile-time-checked GraphQL operations — no hand-written `.graphql` files. Key files:

| File | Purpose |
|------|---------|
| `mod.rs` | Declares `#[cynic::schema("kamu")] mod schema {}` and submodules |
| `operations/` | One file per operation (query or mutation) |
| `fragments.rs` | Reusable `#[derive(cynic::QueryFragment)]` types |
| `inputs.rs` | `#[derive(cynic::InputObject)]` types |
| `variables.rs` | `#[derive(cynic::QueryVariables)]` structs |
| `scalars.rs` | Custom scalar mappings via `cynic::impl_scalar!` |
| `conversions.rs` | `From`/`TryFrom` impls between Cynic types and domain types |

Schema registration happens in `build.rs`:
```rust
cynic_codegen::register_schema("kamu")
    .from_sdl_file("../../../../resources/schema.gql")
    .unwrap()
    .as_default()
    .unwrap();
```
Schema drift is a **compile-time error**.

### Adding or Modifying a Facade Operation

1. Create or edit a file in `operations/` (e.g., `operations/my_op.rs`).
2. Define the operation struct with Cynic derives:
   ```rust
   #[derive(cynic::QueryFragment, Debug, Clone)]
   #[cynic(graphql_type = "Query", variables = "MyOpVariables")]
   pub(crate) struct MyOpQuery { ... }

   #[derive(cynic::QueryVariables, Debug, Clone)]
   pub(crate) struct MyOpVariables { ... }

   pub(crate) fn build_operation(variables: MyOpVariables)
       -> cynic::Operation<MyOpQuery, MyOpVariables>
   {
       MyOpQuery::build(variables)
   }
   ```
3. Add conversion logic in `conversions.rs` (`TryFrom<MyOpQuery> for domain::MyType`).
4. Call the operation via `GraphqlHttpClient::execute_operation(build_operation(vars))` in the facade impl.
5. If `resources/schema.gql` was recently regenerated, recompile so the proc-macro sees the updated schema.

### Custom Scalars

`scalars.rs` maps custom GraphQL scalars to Rust types. Two patterns:

- Existing Rust type: `cynic::impl_scalar!(MyRustType, schema::MyScalar);`
- New wrapper: `#[derive(cynic::Scalar)] struct MyScalar(String);`

Add new scalar mappings here **only** when a facade operation references a scalar not yet registered.

### Type Visibility Rule

All Cynic-derived types live inside `cynic_api/` and must **not** appear on the `ResourceFacade` trait API surface. Expose only domain types through the trait.
