# Resource Label Filtering

> Part of the [Resources Framework](resources-framework.md). Covers the whole label path end to
> end — wire shape, resolution, the repository predicate, and the projection table — because it
> spans the facade, services, GraphQL and CLI sections rather than sitting inside any one of them.
>
> CLI `--label`/`-l` grammar: [§12](resources-framework.md#12-cli).
> GraphQL transport: [§11](resources-framework.md#11-graphql-api).

**Label filtering on `list`, `get` and `delete`.** Filtering rides on the
selectors themselves: each `ResourceSelector` carries an optional
`labels: Option<LabelFilter>` (`ResourceLabelFilterInput` is a `kamu_resources`
type alias to `odf::metadata::resource::LabelFilter`, raw `String`-keyed
entries). There is **no** call-level `label_filter` — the search requests carry
only `selectors`, `account` and `pagination`. The batch ref calls take a bare
`Vec<ResourceRef>` and carry no filter at all, since `ResourceRef` has no
`labels` field.

<a id="uniform-filter-identity"></a>
A **uniform** filter is the special case where every selector carries the same
labels. Selectors are a logical OR and one selector's fields are a conjunction,
so `(A ∧ L) ∨ (B ∧ L)` is exactly `(A ∨ B) ∧ L`. That identity is why a
call-level filter would add no expressive power, and why a caller wanting one
just stamps the same labels onto each selector. What per-selector labels *add*
is the case a call-level filter cannot express: two selectors filtering by
**different** labels in one call.

The filter is resolved per selector — a typed selector against its own schema, a
type-less one against every registered schema — then flattened to
`Vec<ResourceLabelPair>` and carried into the scope row. A selector whose schema
cannot carry its labels is dropped; if it is the only candidate, the filter
itself is at fault and the error surfaces instead.

**A bad filter fails the whole call.** Non-string label values (objects, arrays,
numbers, bools) are rejected as `NonStringValue` — only top-level string-valued
labels are indexed in `resource_labels_projection`, so a complex-JSON predicate
is unsatisfiable by construction. A bad value on *one* selector must not degrade
into "that selector matched nothing", which would hide the authoring mistake
behind the other selectors' rows. Pinned by RF-177.

Filtering covers `get` and `delete` as well as `list` because the CLI resolves
those in **two phases**: it first expands name patterns and the `%` all-types
token into identifiers via `search_handles`, then operates on the
resulting `ResourceRef::ById` set. A label filter is a *narrowing of the candidate
identifier set* — structurally the same job as a name pattern — so it belongs to
that phase-1 expansion. The
phase-2 batch calls (`get`, `delete`, `render_manifests`) structurally
**cannot** carry a filter — they take a bare `Vec<ResourceRef>` — so the old
"pass `label_filter: None` by convention" precondition is now enforced by the type
system instead of by discipline.

One consequence of the ref/selector split is worth noting here: because a filtered
lookup cannot go through the ref API, the CLI routes exact names through
`get_handles` only when there is **no** label filter, and falls back to escaped
wildcard-free patterns via `search_handles` when there is. The first keeps an
N-name batch a single row; the second costs one `ILIKE` row per name, which is the
price of filtering.

`ResourceQuery` (`ExactNames(Vec<ResourceName>)` / `ExactIds(Vec<ResourceID>)` /
`NamePattern(String)`) replaces what used to be two independently-optional fields
(`exact_names`, `name_pattern`) that were never legitimately combined — every caller
set exactly one. The enum makes "exactly one selection mode" a type-level invariant
instead of a runtime convention.

It is shared by both listing methods: `search` and `search_handles` both
narrow through the same type, so the two paths cannot drift apart on pattern or ID
semantics. `ResourceScope` pairs each type with its own optional `ResourceQuery`,
which is what lets one call express `vs/a-% ss/b-%`.

**`ResourceQuery` is repository-facing only.** The facade and GraphQL speak ODF's
`ResourceSelector` — scalar, one `id` and one `name` *pattern* per selector, with
several selectors acting as a logical OR. `coalesce_selectors` folds those into the
repository's list-carrying `ResourceScope`, which is what keeps an N-id batch a
single row. Two consequences worth remembering:

- **Listing has no exact-name mode.** A selector's `name` is a `LIKE` pattern by ODF
  definition, so an exact name travels as a wildcard-free escaped pattern. The
  exact-vs-pattern distinction is the `ResourceRef`/`ResourceSelector` split, not a
  second field. Exact-name *lookups* therefore go through the ref API
  (`get_handles`), which preserves one batched `ExactNames` row instead of one
  `ILIKE` row per name.
- **`ResourceSelector::account` and `labels` are per-row.** `ResourceTypeQuery`
  carries an optional `account_id` and a `label_pairs: Vec<ResourceLabelPair>`;
  a `None` account means the call-level one, which the scoped reads still take
  as a scalar and use as the default, and empty pairs mean unfiltered. That is
  what lets one call span several accounts and several filters, and it is why
  the coalescer groups by `(schema, account, labels)` rather than by schema
  alone — two selectors differing only by account or by labels describe
  different resources and must not merge. Label pairs are canonically sorted
  before they enter the key, so authoring order does not decide whether two
  filters are "the same".

  Accounts are resolved in one batch, deduplicated by spelling, with the
  permission check applied per **distinct** account. **Any denial fails the whole
  call**: a partial result would narrow the caller's request without saying so.
  Pinned by RF-105, and by `test_search_resource_handles_per_row_account` in the
  shared repository suite — the only safety net for SQLite, whose scope predicate
  is not compile-time checked.

  `ResourceScope::AnyType` carries no per-row account, so a type-less selector
  naming one is rejected (`UnrepresentableScopeError::AnyTypeWithAccount`)
  rather than silently scoped to the caller.

Resolution happens in the local facade, strictly before dispatch, through the same
`ResourceExtensionSchemaResolver` used by manifest apply (`ResourceExtensionKind::Label`).
A raw filter key that fails `TypeRef::from_str`, resolves to a non-label schema,
resolves to an inapplicable/unknown URI, carries a non-string value, or collides
with another key after canonicalization is rejected as
`ResourceInvalidLabelFilterError` before any repository access. Repositories
receive only the resolved predicate and never resolve aliases or touch the
extension-schema registry.

**Multi-type queries resolve the filter per schema and collapse.** One selector
may span several schemas — a type-less selector (what the CLI's `%` all-types
token produces, coalescing to `ResourceScope::AnyType`) resolves its labels
against every registered schema. Within that one selector the resolved trees are
compared: today they are always equal — the built-in `environment` label applies
to every resource type, unregistered short names resolve to free-form identity,
and the one *scoped* registered label
([`legacy-config-target-dataset`](resources-framework.md#legacy-dataset-association--the-legacy-config-target-dataset-label),
which applies to `VariableSet`/`SecretSet` only) drops the schemas it cannot
apply to from the candidate set rather than resolving differently on them — so
the uniform single-predicate path is taken. Per-*schema* divergence
remains *reserved, not implemented*: it is guarded by an assertion and a comment
rather than by OR-across-types SQL that no test could exercise. Note this is a
different axis from per-*selector* filtering, which **is** implemented: several
typed selectors (`kamu list vs/a-% ss/b-%`) each resolve their own filter and
each carry it into their own scope row. A schema whose `applications` list
excludes the filtered label is dropped from that selector's candidate set (no
resource of that type can match); only if **every** candidate fails to resolve is
the resolution error surfaced.

**Resolved shape is a tree; evaluation is AND-only.** The raw entries are parsed by
`ResourceLabelFilterExprParser::parse` (domain,
`values/resource_label_filter_parser.rs`, separate from the model types in
`values/resource_label_filter.rs`) into a `ResourceLabelFilterExpr` tree, and
resolution produces the mirrored `ResolvedResourceLabelFilter`:

```rust
pub enum ResolvedResourceLabelFilter {
    True,                                    // resolved form of "no filter"
    Eq { key: TypeRef, value: String },
    And(Vec<ResolvedResourceLabelFilter>),
    Not(Box<ResolvedResourceLabelFilter>),
    Or(Vec<ResolvedResourceLabelFilter>),
}
```

The tree shape is deliberate: `$not`/`$or` are fully *representable and resolvable*,
so adding support for them later is a backend-local change that touches no
signature. The **capability boundary lives in the repository layer**, not the
resolver: every backend calls the single shared helper `flatten_conjunction`, which
walks `True`/`Eq`/`And` into a flat key/value pair list and returns
`UnsupportedOperator` for `Not`/`Or`. That is mapped to
`ResourceLabelFilterProblemCode::UnsupportedExpression` at the facade edge, so
"what is supported" is defined in exactly one place. The duplicate-after-canonicalization
check applies **per conjunction level** — two `Or` branches may legitimately test
the same key.

<a id="resource_labels_projection-index"></a>
**`resource_labels_projection` index.** A normalized Postgres/SQLite table
(`resource_id, label_key, label_value`, `PK(resource_id, label_key)`, `FK →
resources ON DELETE CASCADE`, covering index on `(label_key, label_value,
resource_id)`) mirroring the top-level **string-valued** entries of
`resources.labels` — non-string values (numbers, objects, arrays, booleans, null)
are not indexed. The `_projection` suffix signals it carries no independent
state of its own — every row is derived from `resources.labels`.

It is maintained by a dedicated sibling trait, `ResourceLabelProjectionRepository`
(`domain/src/repo/resource_label_projection_repository.rs`:
`replace_entries(resource_id, &[(String,String)])` /
`find_entries(resource_id)`), **not** inline inside `ResourceRepository`'s
`create_resource`/`update_resource(s)`. It is invoked from
`ResourcePersistenceServiceHelper::sync_snapshots` — the single choke point behind
`create`/`save`/`delete(_many)` — immediately after
`resource_repository.update_resources(...)` succeeds, with both repositories
resolved from the same transactional DI scope so the snapshot write and the
projection write commit or roll back together. This keeps `ResourceRepository`
scoped to `resources`/`resource_events` and gives the projection's read side room
to grow into real filtered queries (Phase 9) without touching `ResourceRepository`'s
surface. Implemented per backend (`Postgres…`/`Sqlite…`/`InMemoryResourceLabelProjectionRepository`,
registered alongside `ResourceRepository` at every composition root and test
harness); Postgres's `replace_entries` insert uses a compile-time checked static
query via `UNNEST($2::text[], $3::text[])` over paired key/value arrays rather than
a dynamic `QueryBuilder`. Resources are only ever soft-deleted in practice, so
`ON DELETE CASCADE` exists for maintenance safety but has no dedicated test (same
as other cascade FKs in this codebase).

**Rebuilding projections — known gap.** If `resource_labels_projection` ever drifts
from `resources.labels` (manual DB surgery, a bug, a skipped migration step), there
is **no rebuild tooling** — no admin command, no maintenance job, nothing. In
principle a rebuild only needs a single-table scan (`SELECT resource_id, labels
FROM resources` → re-derive string entries → `replace_entries`), since
`resources.labels` is already the trusted materialized value; no `resource_events`
replay is required for *this* projection. This gap isn't unique to this table: no
projection anywhere in the resources framework has rebuild tooling today, including
`resources` itself relative to `resource_events` (which *would* need event replay,
a materially bigger problem). Building either is out of scope for this feature —
flagged here as a pre-existing operational gap for a future theme, not something
introduced by label filtering.
