---
name: kamu-dill-di
description: Dill dependency-injection patterns for Kamu CLI. Use when defining components, registering interfaces or trait objects, configuring catalog scopes, chaining catalogs, or wiring DI in services and tests.
---

# Kamu Dill DI

`dill` is the lightweight Rust DI container used throughout this repo. Components are registered in a `Catalog` and resolved on demand.

## Overview

This skill covers five key aspects of dill usage:

1. **Defining Components**: How to mark structs or impl blocks for DI injection
2. **Scopes**: Controlling whether components are transient (new per request) or singletons (shared)
3. **Registration**: Adding components, values, and builders to a catalog
4. **Chaining Catalogs**: Creating child catalogs that inherit and override parent registrations
5. **Resolving Components**: Looking up components from the catalog at runtime

## Defining a Component

When putting `#[dill::component]` on the struct, Dill generates a constructor that injects declared parameters from the catalog.

```rust
#[dill::component]
pub struct MyService {
    store: Arc<dyn MyStore>,
    subject: Arc<CurrentAccountSubject>,
}

```

Sometimes this is undesirable, such as when the struct fields don't directly correspond to the constructor parameters, or when you want to perform additional setup in the constructor. In that case, put `#[dill::component]` on the **`impl` block**, not on the struct definition.

```rust
pub struct MyService {
    store: Arc<dyn MyStore>,
    subject: Arc<CurrentAccountSubject>,
}

#[dill::component(pub)]
impl MyService {
    pub fn new(
        store: Arc<dyn MyStore>,
        subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self { store, subject }
    }
}
```

## Scopes

By default, dill creates a **new instance** for every resolution request. To get a single shared instance, add `#[dill::scope(dill::Singleton)]`:

```rust
#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl MyCache {
    pub fn new() -> Self { Self::default() }
}
```

Singletons are scoped to the catalog in which the component is registered. Child (chained) catalogs that do not override the component will always resolve to the same instance from the parent catalog.

Singletons are expected when a global shared state is needed.

## Registration

```rust
let mut b = CatalogBuilder::new();

// DI-constructed component:
b.add::<MyService>();

// Pre-built value (acts as a singleton):
b.add_value(WorkspaceLayout::new(path));

// Component with a builder for custom configuration:
b.add_builder(WorkspaceService::builder().with_multi_tenant(false));
```

To call `.builder()` the `dill::Component` trait must be in scope:

```rust
use dill::Component as _;
```

## Chaining Catalogs

`CatalogBuilder::new_chained(&parent)` creates a child catalog that inherits all registrations from `parent`. Components added to the child override the parent; everything else is resolved from the parent.

```rust
let child = CatalogBuilder::new_chained(&base_catalog)
    .add_value(CurrentAccountSubject::new_test_with("alice"))
    .add::<SomePerRequestService>()
    .build();
```

The parent catalog's singletons remain alive as long as the parent `Catalog` object is kept alive. Dropping the parent while child catalogs are still in use will cause the child catalogs to panic when attempting to access inherited singletons.

## Resolving Components

```rust
// Concrete type:
let svc = catalog.get_one::<MyService>().unwrap();

// Trait object:
let store = catalog.get_one::<dyn MyStore>().unwrap();
```

`get_one` returns `Result<Arc<T>, CatalogError>`. If the component is not registered in the catalog (or any parent catalog in the chain), it returns an error. The resolved `Arc` keeps the component alive independently of the catalog that produced it.
