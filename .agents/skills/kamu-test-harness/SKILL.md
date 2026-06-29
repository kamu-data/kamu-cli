---
name: kamu-test-harness
description: >
    Test organization, harness design, and DI wiring patterns for Kamu CLI tests.
    Use this skill whenever writing, editing, or reviewing any test code in the Kamu
    codebase — including unit tests, integration tests, or test helpers. Covers:
    harness struct design, dill catalog wiring, per-account service isolation,
    temporary directories, in-memory test doubles, cross-crate reusable harnesses,
    and keeping test bodies high-level and readable. Always consult this skill before
    writing a new test, adding a helper function, or structuring a new test module.
---

# Kamu Test Harness

See also: `kamu-dill-di` for general dill patterns.

---

## Core Philosophy

Tests in this codebase follow two non-negotiable rules:

**Rule 1 — Test bodies must be high-level.**
A test body reads like a specification: arrange, act, assert — with no
low-level boilerplate visible. Stream chains, `.unwrap()` ladders, catalog
resolution, and service construction never appear inside a `#[test]` or
`#[tokio::test]` function. Everything noisy goes into harness methods.

**Rule 2 — Helpers belong on the harness struct, not as free functions.**
There are no free `async fn` helpers, no `mod utils` full of functions that take
a service or catalog as an argument. All reusable operations are methods on the
harness struct. This keeps test code discoverable, namespaced, and self-contained.

### What a well-written test looks like

```rust
#[tokio::test]
async fn test_second_write_is_visible_across_accounts() {
    let harness = MyHarness::new();

    harness.save_events("alice", id, vec![created.clone()]).await;
    let prev_id = harness.get_last_event_id("alice", id).await;
    harness.save_events_after("bob", id, prev_id, vec![updated.clone()]).await;

    let events = harness.get_events_for("alice", id).await;
    assert_eq!(events.len(), 2);
}
```

No catalog construction, no service resolution, no stream collection — the test
describes the scenario, not the plumbing.

### Anti-patterns — never write these

```rust
// Bad: free helper function — put it on the harness instead
async fn save_and_get(svc: &FooService, id: ResourceID) -> Vec<Event> { ... }

// Bad: catalog / service resolution inside a test body
let catalog = CatalogBuilder::new_chained(&harness.base_catalog).build();
let svc = catalog.get_one::<FooService>().unwrap();

// Bad: stream boilerplate inside a test body
let events = harness.bridge()
    .get_events(&id, GetEventsOpts::default())
    .map(|r| r.unwrap().1)
    .collect::<Vec<_>>()
    .await;

// Bad: standalone utils module with free functions
mod utils {
    pub async fn create_dataset(catalog: &dill::Catalog, name: &str) { ... }
}
```

---

## Scope 1 — Local harness (single test suite)

A test file or small test module that does not need to share its harness with
other suites defines a local harness struct at the bottom of the file. It is not exported.

```rust
struct MyHarness {
    base_catalog: dill::Catalog,
    _tmp: tempfile::TempDir,   // must be a named field, NOT `_`
}                              // `_` drops immediately, deleting the dir

impl MyHarness {
    /// Workspace directory exists — `is_in_workspace()` returns true.
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let base_catalog = Self::build_base_catalog(WorkspaceLayout::new(tmp.path()));
        Self { base_catalog, _tmp: tmp }
    }

    /// No workspace directory — `is_in_workspace()` returns false.
    fn outside_workspace() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let base_catalog =
            Self::build_base_catalog(WorkspaceLayout::new(tmp.path().join("workspace")));
        Self { base_catalog, _tmp: tmp }
    }

    fn build_base_catalog(workspace_layout: WorkspaceLayout) -> dill::Catalog {
        let mut b = CatalogBuilder::new();
        b.add_value(workspace_layout);
        b.add::<InMemoryFooStore>();
        b.add_builder(WorkspaceService::builder().with_multi_tenant(false));
        b.build()
    }
}
```

### Per-account service isolation

When services are account-scoped, produce a chained child catalog per account.
The base catalog's singletons (e.g. the shared in-memory store) are inherited,
so state written through one account's service is visible from another's.

```rust
impl MyHarness {
    // Build a fresh chained catalog for this account. Cheap — call freely.
    fn catalog_for(&self, account_name: &str) -> dill::Catalog {
        CatalogBuilder::new_chained(&self.base_catalog)
            .add_value(CurrentAccountSubject::new_test_with(account_name))
            .add::<FooRegistryService>()
            .add::<FooStateService>()
            .build()
    }

    // Shortcut accessor — hides catalog resolution from test bodies
    fn state_svc(&self, account_name: &str) -> Arc<FooStateService> {
        self.catalog_for(account_name)
            .get_one::<FooStateService>()
            .unwrap()
    }
}
```

### Harness helper methods — hide all boilerplate

Every repetitive operation a test would otherwise inline belongs as a method.
Each method unwraps internally; tests never see `Result` or stream chains.

```rust
impl MyHarness {
    async fn get_events_for(&self, account: &str, id: ResourceID) -> Vec<TestEvent> {
        self.state_svc(account)
            .get_events(&id, GetEventsOpts::default())
            .map(|r| r.unwrap().1)
            .collect()
            .await
    }

    async fn get_last_event_id(&self, account: &str, id: ResourceID) -> EventID {
        self.state_svc(account)
            .get_events(&id, GetEventsOpts::default())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .last()
            .unwrap()
            .unwrap()
            .0
    }

    async fn save_events(
        &self,
        account: &str,
        id: ResourceID,
        events: Vec<TestEvent>,
    ) -> EventID {
        self.state_svc(account)
            .save_events(&id, None, events)
            .await
            .unwrap()
    }

    async fn save_events_after(
        &self,
        account: &str,
        id: ResourceID,
        prev_id: EventID,
        events: Vec<TestEvent>,
    ) -> EventID {
        self.state_svc(account)
            .save_events(&id, Some(prev_id), events)
            .await
            .unwrap()
    }
}
```

---

## Scope 2 — Crate-level reusable harness

When several test suites within the same crate share catalog wiring, extract a
shared harness into `src/testing/` gated behind a `testing` feature flag.
It is never compiled into production builds.

**`Cargo.toml`**:

```toml
[features]
testing = ["dep:kamu-foo-inmem", "dep:oop"]

[dependencies]
kamu-foo-inmem = { workspace = true, optional = true }
oop = { version = "0.0.2", optional = true }
```

**`src/lib.rs`**:

```rust
#[cfg(feature = "testing")]
pub mod testing;
```

**`src/testing/mod.rs`**:

```rust
mod base_foo_service_harness;
pub use base_foo_service_harness::*;
```

The harness itself looks identical to a local harness but is `pub`. Tests that
only need a harness local to one integration-test binary stay in `tests/` — they
do not need the feature-flag treatment.

---

## Scope 3 — Cross-crate composable harness hierarchy

When test infrastructure spans multiple crates (e.g. a domain-services crate
builds on top of a resource-infra crate), compose harnesses layer by layer with
`#[oop::extend]` instead of duplicating catalog wiring.

Each layer:

1. Holds its own `catalog` field, chained on the parent layer's `catalog()`.
2. Overrides `catalog()` to return its own (most-derived) catalog.
3. Adds harness helper methods for its own domain.

```rust
// Layer 1: in kamu-resource-services/src/testing/
pub struct BaseResourceServiceHarness {
    catalog: dill::Catalog,
}

impl BaseResourceServiceHarness {
    pub fn new() -> Self {
        let mut b = CatalogBuilder::new();
        // wire resource-layer infrastructure
        Self { catalog: b.build() }
    }
    pub fn catalog(&self) -> &dill::Catalog { &self.catalog }
    // resource-level helper methods...
}

// Layer 2: in kamu-configuration-services/src/testing/
#[oop::extend(BaseResourceServiceHarness, base)]
pub struct BaseConfigurationServiceHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,        // chained on base.catalog()
}

impl BaseConfigurationServiceHarness {
    pub fn new() -> Self {
        let base = BaseResourceServiceHarness::new();
        let mut b = CatalogBuilder::new_chained(base.catalog());
        // add configuration-specific repos and services
        let catalog = b.build();
        Self { base, catalog }
    }
    pub fn catalog(&self) -> &dill::Catalog { &self.catalog }
    // configuration-level helper methods...
}

// Layer 3: in an integration test binary (tests/)
#[oop::extend(BaseConfigurationServiceHarness, base)]
pub struct DatasetEnvVarServiceHarness {
    base: BaseConfigurationServiceHarness,
    catalog: dill::Catalog,
}

impl DatasetEnvVarServiceHarness {
    pub fn new() -> Self {
        let base = BaseConfigurationServiceHarness::new();
        let mut b = CatalogBuilder::new_chained(base.catalog());
        // add dataset-env-var-specific services
        let catalog = b.build();
        Self { base, catalog }
    }
    pub fn catalog(&self) -> &dill::Catalog { &self.catalog }
    // dataset-env-var helper methods...
}
```

### How catalog resolution works

- `catalog()` on any layer always returns **that layer's own catalog** — the
  most-derived one at that scope. It chains through all parent catalogs, so
  every service registered at every layer is resolvable.
- `#[oop::extend]` makes parent helper methods available directly on the child
  struct — no explicit delegation needed.
- Singletons from layer 1 are inherited by all layers. State written via a
  layer-3 service is visible from a layer-1 helper.

### Consuming a cross-crate harness

```toml
# In the consuming crate's Cargo.toml:
[dev-dependencies]
kamu-configuration-services = { workspace = true, features = ["testing"] }
```
