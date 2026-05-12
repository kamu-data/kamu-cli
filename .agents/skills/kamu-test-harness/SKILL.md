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
