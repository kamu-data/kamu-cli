---
name: kamu-test-harness
description: Test harness and DI wiring patterns for Kamu CLI unit and integration tests. Use when writing tests that need a Catalog, per-account service isolation, temporary directories, or in-memory test doubles.
---

# Kamu Test Harness

Tests that exercise services requiring a `dill` catalog are organized around a **harness struct** that owns the base catalog and any long-lived resources such as temporary directories.

See also: `kamu-dill-di` for general dill patterns.

## Harness Structure

```rust
struct MyHarness {
    base_catalog: dill::Catalog,
    _tmp: tempfile::TempDir,  // keep alive for the harness lifetime
}

impl MyHarness {
    /// Workspace directory exists — `is_in_workspace()` returns true.
    /// Panics if tempfile::tempdir() fails (acceptable in test context).
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
        b.add::<InMemoryFooStore>();           // shared singleton store
        b.add_builder(WorkspaceService::builder().with_multi_tenant(false));
        b.build()
    }
}
```

`_tmp` must be a named field — assigning it to `_` drops it immediately, deleting the directory before any test runs.

## Per-Account Catalog

For services that are account-scoped, build a chained child catalog per account. The base catalog's singletons (e.g. the shared store) are inherited automatically.

```rust
impl MyHarness {
    fn catalog(&self, account_name: &str) -> dill::Catalog {
        CatalogBuilder::new_chained(&self.base_catalog)
            .add_value(CurrentAccountSubject::new_test_with(account_name))
            .add::<FooRegistryService>()
            .add::<FooStateService>()
            .build()
    }

    fn state_svc(&self, account_name: &str) -> Arc<FooStateService> {
        self.catalog(account_name)
            .get_one::<FooStateService>()
            .unwrap()
    }
}
```

A new chained catalog is built per call. This is cheap and keeps each shortcut method self-contained. Because the returned `Arc<FooStateService>` holds a reference to the shared store singleton, state written through one account's service instance is visible when a new service instance is created for the same or a different account.

## Composable Harness Hierarchy with `#[oop::extend]`

When multiple test crates need the same infrastructure layer, embed harnesses compositionally rather than duplicating catalog wiring. Use `#[oop::extend(BaseHarness, field)]` to inherit all `impl` methods of the base harness.

```rust
// In the base crate's src/testing/ module (under a `testing` feature):
pub struct BaseResourceServiceHarness {
    catalog: dill::Catalog,
}

impl BaseResourceServiceHarness {
    pub fn new() -> Self { /* wires resource infra only */ }
    pub fn catalog(&self) -> &dill::Catalog { &self.catalog }
}

// In a domain crate's src/testing/ module (under a `testing` feature):
#[oop::extend(BaseResourceServiceHarness, base)]
pub struct BaseConfigurationServiceHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,   // chained on top of base.catalog()
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
    // domain-specific accessor helpers...
}

// In an integration test binary (tests/):
#[oop::extend(BaseConfigurationServiceHarness, base)]
pub struct DatasetEnvVarServiceHarness {
    base: BaseConfigurationServiceHarness,
    catalog: dill::Catalog,
}
```

### How catalog resolution works

1. **Each harness level owns its catalog**: The base level creates the initial catalog with foundational dependencies. Each derived level chains a new catalog on top of its parent's catalog.
2. **Each level overrides `catalog()`**: The method returns a reference to its own catalog field, which is the most specific (most-derived) catalog at that level.
3. **Callers always resolve from the narrowest scope**: When you call `catalog()` on a `DatasetEnvVarServiceHarness`, you get the top-level catalog that chains through all parent catalogs, ensuring all services are available.
4. **Parent methods are inherited**: Thanks to `#[oop::extend]`, methods from parent harnesses are automatically available on derived harnesses, so you can call base-level helpers without explicit delegation.

### Exposing harnesses to other crates

Harnesses that higher-level crates will reuse belong in `src/testing/` of the service crate, gated behind a `testing` feature flag so they are never compiled into production builds:

**`Cargo.toml`:**

```toml
[features]
testing = ["dep:kamu-foo-inmem", "dep:oop"]

[dependencies]
kamu-foo-inmem = { workspace = true, optional = true }
oop = { version = "0.0.2", optional = true }
```

**`src/lib.rs`:**

```rust
#[cfg(feature = "testing")]
pub mod testing;
```

**`src/testing/mod.rs`:**

```rust
mod base_foo_service_harness;
pub use base_foo_service_harness::*;
```

Consuming crates activate it in their dev-dependencies:

```toml
[dev-dependencies]
kamu-foo-services = { workspace = true, features = ["testing"] }
```

Tests that only need integration-test-binary-local harnesses (not reused elsewhere) stay in `tests/` as before.
