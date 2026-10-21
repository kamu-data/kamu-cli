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

### 3. Schema Documentation

Every type, field, operation and enum value that reaches the schema carries a description. The
schema is the API reference for its consumers; an undocumented item is unfinished.

- One line, usually 3–10 words: `/// Account name to display`. Match the punctuation of the
  neighbouring docs.
- Wrap CamelCase product names in backticks (`` `DataFusion` ``); Clippy's `doc_markdown`
  rejects them bare.
- State the contract for a consumer: what the item is or does, and when it is null if that is
  not obvious. Read the code to get it right; do not restate the identifier.
- Result and error types say which outcome they represent: `/// Dataset was created`.
- General comment rules (no history, tickets or tallies) are in `kamu-prose-and-comments`.

async-graphql takes descriptions from different places per item kind:

| Item | Description goes on |
|---|---|
| `#[derive(SimpleObject / InputObject / Enum / Union / Interface / OneofObject)]` type | `///` on the struct or enum |
| `#[Object]` type | `///` on the `impl` block — a doc on the struct never reaches the schema |
| Struct field, enum value, union member, oneof variant | `///` on the field or variant |
| Resolver field or operation | `///` on the `async fn` |
| Interface field declared in `#[graphql(field(...))]` | `desc = "..."` inside that `field(...)` |
| Scalar | `///` on the `#[Scalar]` impl; with `simple_scalar!` / `simple_string_scalar!`, `///` as the first macro argument |
| Resolver argument | `#[graphql(desc = "...")]` on the parameter, when its meaning is not obvious |

After regenerating the schema, check the diff for new items without a `"""` description.

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

A schema change is breaking only if it removes fields, changes types, or renames operations **that
were previously released**. Verify before treating it as one — an unreleased field has no consumers
to break:

```sh
git show <last-release-tag>:resources/schema.gql | grep -c '<FieldOrInputName>'
```

While the work is uncommitted on a feature branch, review such changes for correctness and
intentionality only. Do not require or suggest a `CHANGELOG.md` entry — see the Changelog Review
Context in `AGENTS.md`.

During PR or release finalization:

- Document the breaking changes in the PR description.
- Add the consolidated `CHANGELOG.md` entry under `### Changed`.
- Consider whether a deprecation period is needed before removal.

## Remote GraphQL Facade (Backend-to-Backend)

### Location

`src/adapter/resources-facade-graphql/src/cynic_api/` (crate `kamu-adapter-resources-facade-graphql`;
responses are mapped back to domain types in the sibling `outcome_mapper/`)

### Mechanism

The facade uses [Cynic](https://cynic-rs.dev/) for typed, compile-time-checked GraphQL operations — no hand-written `.graphql` files. Key files:

| File | Purpose |
|------|---------|
| `mod.rs` | Declares `#[cynic::schema("kamu")] mod schema {}` and submodules |
| `operations/` | One file per operation (query or mutation), with its `#[derive(cynic::QueryVariables)]` struct |
| `fragments.rs` | Reusable `#[derive(cynic::QueryFragment)]` types |
| `inputs.rs` | `#[derive(cynic::InputObject)]` types |
| `scalars.rs` | Custom scalar mappings via `cynic::impl_scalar!` |
| `conversions.rs` | `From`/`TryFrom` impls between Cynic types and domain types |

Schema registration happens in `build.rs`, which skips regeneration while the schema's content hash
is unchanged:
```rust
let schema_path = manifest_dir
    .join("../../../resources/schema.gql")
    .canonicalize()
    .unwrap();

cynic_codegen::register_schema("kamu")
    .from_sdl_file(schema_path)
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

## What lives elsewhere

- Domain errors the resolvers map from: `kamu-domain-design`.
- Renaming a GraphQL-visible name (a breaking change): `kamu-renaming-a-concept`.
- GraphQL e2e scenarios: `kamu-cli-e2e-tests`.
