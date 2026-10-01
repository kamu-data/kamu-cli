---
name: kamu-rust-style
description: Rust coding style for Kamu CLI — imports and paths, exhaustive matches without catch-all arms, numeric conversions, enum/string mappings with strum, formatting macros, module and file organization, visibility, re-exports, macros vs functions. Use before writing or editing any Rust code; the edit hook requires it for every .rs file.
---

# Kamu Rust Style

Follow the style of the surrounding code first; the rules below settle what surrounding code
leaves open. `rustfmt` (run by the post-edit hook and `cargo fmt`) owns layout, and `make clippy`
owns what lints can see. This skill owns the rest.

## Rules

### Imports and paths

Import items with `use`; never spell out crate paths inline:

```rust
// No
b.add::<wakeup_listener::WakeupListenerMetrics>();
let name = kamu_flow_system::FLOW_AGENT_NAME;

// Yes
use kamu_flow_system::FLOW_AGENT_NAME;
use wakeup_listener::WakeupListenerMetrics;
b.add::<WakeupListenerMetrics>();
```

Qualify a name only to resolve a clash. Two exceptions:

- **App wiring** (`src/app/cli/src/app.rs`, `database.rs`) integrates every crate, so it qualifies
  components explicitly to show where each comes from.
- **Open Data Fabric types are always written with the `odf::` prefix, never imported:**
  `odf::DatasetID`, `odf::metadata::MetadataEvent`, `odf::dataset::MetadataChain`. The prefix marks
  a protocol-level type at every use and keeps it apart from same-named Kamu types. Reach ODF
  through the `odf` facade crate, not the `odf-*` sub-crates (code inside `src/odf/` aside).
  Import from `odf` only what cannot be used qualified — extension traits needed for method
  syntax (`use odf::utils::data::DataFrameExt;`) — and test helpers such as
  `odf::metadata::testing::MetadataFactory`.

```rust
// No
use odf::{DatasetAlias, DatasetID};
fn resolve(id: &DatasetID) -> DatasetAlias { ... }

// Yes
fn resolve(id: &odf::DatasetID) -> odf::DatasetAlias { ... }
```

### Numeric conversions never use `as`

`as` silently truncates, wraps or loses precision, and reads the same as a lossless conversion.

| Conversion | Write |
|---|---|
| Lossless | `f64::from(x_u32)`, `i64::from(x_i32)`, `u64::from(x_u32)` |
| Narrowing | `usize::try_from(x).unwrap()` when overflow is a bug, or handle the error |
| Time span to float | `chrono::TimeDelta::as_seconds_f64()`, `std::time::Duration::as_secs_f64()` — not `as f64` on integer milliseconds |

### Enum ↔ string mappings come from `strum`

Derive `IntoStaticStr`, `Display`, `EnumString`, `EnumDiscriminants`, `EnumIter` instead of
hand-written `match` arms (e.g. `FlowOutcome` → `FlowOutcomeKind` via `EnumDiscriminants`). A
hand-written mapping drifts from the variants as they change.

- Keep a manual mapping only when the strings are irregular.
- Strings that are persisted or exported (DB values, metric labels, event type names) are data:
  pin them with a test **before** converting a manual mapping to `strum`, so the conversion
  cannot silently change them (see `kamu-renaming-a-concept`).

### Matches on our enums are exhaustive — no catch-all `_` arm

List every variant. A `_` (or a binding like `other =>`) arm turns adding a variant into a silent
behaviour change: the new case falls into whatever the catch-all does, and nobody is asked. An
exhaustive match turns it into a compile error at every site that must decide, which is exactly
where review should happen.

```rust
// No — a new FlowStatus variant silently counts as "not finished"
match status {
    FlowStatus::Finished => true,
    _ => false,
}

// Yes — a new variant fails to compile here until someone decides
match status {
    FlowStatus::Finished => true,
    FlowStatus::Waiting | FlowStatus::Running | FlowStatus::Retrying => false,
}
```

- Group variants that share an outcome with `|` rather than reaching for `_`.
- The same applies to predicates: `matches!(status, A | B)` over an enum we own is a hidden
  catch-all when the answer must be decided per variant — write the exhaustive `match`.
- Error conversions are the most common case (see `kamu-domain-design`, "Error Handling").

`_` stays legitimate where the compiler cannot enumerate cases or the set is not ours to extend:
integers, strings and other open domains; `#[non_exhaustive]` enums from other crates (list the
known variants, then a deliberate fallback); and `if let` / `let else` that intentionally handle
one shape.

### Formatting

Inline captured identifiers: `format!("value={value}")`, not `format!("value={}", value)`.

### Macros

Keep macros declarative and thin. Algorithmic logic goes into ordinary functions or services the
macro calls — macro bodies are hard to read, debug and test.

### Module and file organization

- Split conceptually distinct logic into its own module early, rather than growing a file until it
  has to be broken up.
- Order model files top-down: the highest-level result/union type first, then the structs and
  enums it references, each type's `impl` blocks immediately after the type.
- When a function repeats a logical section, extract a named helper for it if it represents a
  coherent concept.

### Visibility and re-exports

- Default to the tightest visibility: private, then `pub(crate)`. Use `pub` only at a real crate
  boundary.
- Do not publicly re-export internal helper modules unless external consumers or macro expansion
  truly require it.

## Rejected approaches

Do not re-propose these without new evidence.

| Approach | Why it was rejected |
|---|---|
| `x as f64` / `x as usize` "because it is shorter" | Truncation and wrapping are silent; `From`/`try_from` make the intent and the failure mode explicit. |
| Inline crate paths to avoid touching the `use` block | Hides the file's dependencies; the `use` block is where a reader looks for them. |
| Importing ODF types with `use odf::…` to shorten signatures | Loses the protocol-type marker and invites clashes with Kamu types of the same name. |
| Hand-written `match` for enum ↔ string | Drifts from the variants; `strum` derives stay in sync. |
| Catch-all `_` arm on an enum we own, "to keep the match short" | A new variant silently takes the fallback path instead of failing to compile where a decision is needed. |
| Silencing a lint with `#[allow]` / `#[expect]` | Hides the problem; fix the cause or ask first (AGENTS.md, "Validation"). |

## What lives elsewhere

- Comments, doc comments, dividing lines: `kamu-prose-and-comments`.
- Test structure and assertions: `kamu-test-harness`.
- DI components and scopes: `kamu-dill-di`.
- Error types and domain modelling: `kamu-domain-design`.
