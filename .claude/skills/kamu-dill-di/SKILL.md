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
4. **Chaining Catalogs**: Creating child catalogs that inherit parent registrations and add their own
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

Singletons are scoped to the catalog in which the component is registered. Child (chained) catalogs always resolve to the same instance from the parent catalog.

Singletons are expected when a global shared state is needed.

### Singletons must not depend on repositories

**A `Singleton` component must never instantiate a repository — neither directly
nor indirectly through an intermediate component.** Repository components are
transaction-scoped: they hold a `TransactionRefT` and may only be instantiated
in transactional catalogs.

A singleton outlives every transaction, so capturing an `Arc<dyn SomeRepository>`
pins that transaction for the lifetime of the process and trips the runtime's
leak guard:

```
Attempting to extract inner transaction while more than one strong reference is
present. This may be an indication that transaction reference is leaked, i.e.
held by some component whose lifetime exceeds the intended span of the
transaction scope.
```

The crash surfaces far from its cause — typically as a panicking background
agent task rather than an error at startup — so it is worth avoiding by
construction.

Inject `dill::CatalogWeakRef` instead and resolve the repository per call:

```rust
pub struct InMemoryResourceRepository {
    state: Arc<Mutex<State>>,
    catalog: CatalogWeakRef,
}

#[component(pub)]
#[interface(dyn ResourceRepository)]
#[scope(Singleton)]
impl InMemoryResourceRepository {
    pub fn new(catalog: CatalogWeakRef) -> Self { /* ... */ }

    fn account_repository(&self) -> Option<Arc<dyn AccountRepository>> {
        self.catalog.upgrade().get_one::<dyn AccountRepository>().ok()
    }
}
```

`FlowAgentImpl` and `FlowSystemEventAgentImpl` use the same idiom.

Note that in-memory repositories are themselves singletons, so this applies to
them too. A violation may stay dormant for a long time: it only fires in a
configuration that pairs the singleton with a *real* transactional
implementation of its dependency, e.g. an in-memory repository wired next to a
database-backed one (see `configure_database_components` for the per-provider
wiring).

To audit, parse `#[component]` / `#[interface(dyn ...)]` / `#[scope(Singleton)]`,
treat any component whose body mentions `TransactionRef` as transaction-scoped,
and walk singleton dependencies transitively through the trait -> impl map.

## Grouping: one dependency scope per component

A component's methods should all run with the same dependencies available. When one method needs
something the others must not require — an open transaction, the acting `Principal`, a config
that is only sometimes registered — the component mixes responsibilities. Split it so that each
part injects exactly what it uses in its constructor.

The signal is a dependency fetched at call time to keep the constructor resolvable everywhere:
a `dill::Lazy` field, a held `Catalog` queried with `get_one` inside a method, or a missing
registration treated as "off". Regroup instead:

| Mix | Regrouping | Precedent |
|---|---|---|
| Work with no transaction plus a step that needs one | The transactional step moves to the component that opens the transaction, as a `#[transactional_methodN]` injecting its own dependencies | `DatasetUpdateSteps::commit_sync` (the `SyncService` transfer holds none) |
| An authorized operation plus a system path that skips authorization | The use case keeps only the authorized path; the shared work goes into a plain helper both callers build | `UpdateAccountUseCase` and `UpdateAccountUseCaseHelper` |
| Core work plus an optional feature enabled by registration | The feature becomes its own component taking `Option<Arc<T>>` dependencies | `IngestEnvVarsResolver` |
| A transaction-free component that needs one transactional write | That write becomes a small component with a `#[transactional_methodN]`, injected instead of a catalog | `PulledSeedStore` in the transfer protocols |
| Branches whose dependencies differ (create versus update) | One transactional method per branch, each injecting only its own | `commit_sync_into_existing`, `commit_sync_into_new` |

Holding a catalog is right to open transactions with `#[transactional_methodN]` (the method
declares what it injects), in request handlers and agents that work on a per-request or
background catalog by design, and in a singleton that resolves repositories per call
([above](#singletons-must-not-depend-on-repositories)).

### Helpers are never components

A helper — a type named `…Helper` holding shared logic for a few callers — is a plain struct:
each caller builds it in its own constructor from dependencies the caller already injects.
Registering it would make it a service without saying so, and every catalog that resolves its
callers would have to register it too.

```rust
pub struct UpdateAccountUseCaseImpl {
    helper: UpdateAccountUseCaseHelper,
    // ...
}

#[dill::component(pub)]
#[dill::interface(dyn UpdateAccountUseCase)]
impl UpdateAccountUseCaseImpl {
    pub fn new(
        account_service: Arc<dyn AccountService>,
        outbox: Arc<dyn Outbox>,
        time_source: Arc<dyn SystemTimeSource>,
        // ...
    ) -> Self {
        Self {
            helper: UpdateAccountUseCaseHelper::new(account_service, outbox, time_source),
            // ...
        }
    }
}
```

If the shared logic deserves to be injected on its own, it is a service: name it for what it does.

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

`CatalogBuilder::new_chained(&parent)` creates a child catalog that inherits all registrations from `parent`.

A child **cannot override** a registration of its parent. Resolution looks across the whole chain,
so registering a type the parent already provides makes `get_one` fail with `Ambiguous` (an
`AllOf` sees both). To vary a value, build a sibling catalog on a common parent that holds neither
(precedent: the background catalog in `app.rs`, [identity](../../../docs/internal/identity.md)).

```rust
let mut b = CatalogBuilder::new_chained(&base_catalog);
CurrentAccountSubject::new_test_with(&"alice").register(&mut b);
let child = b.add::<SomePerRequestService>().build();
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

## Rejected approaches

Do not re-propose these without new evidence.

| Approach | Why it was rejected |
|---|---|
| A `Singleton` holding `Arc<dyn SomeRepository>` (directly or via a dependency) | Pins one transaction for the process lifetime; trips the leak guard far from the cause. Use `CatalogWeakRef`. |
| Silencing the leak-guard panic | It is the only signal of a leaked transaction; the leak itself corrupts transaction boundaries. |
| `dill::Lazy` fields or `catalog.get_one` inside a method, so a component whose methods need different dependencies stays resolvable everywhere | Hides that the component mixes responsibilities and turns missing registrations into runtime errors; split it by the dependencies each part needs ([Grouping](#grouping-one-dependency-scope-per-component)). |
| A `…Helper` registered as a dill component | Makes shared logic a hidden service that every catalog must register; build helpers in the callers' constructors ([Helpers](#helpers-are-never-components)). |

## What lives elsewhere

- Wiring components in tests (harness catalogs, per-account isolation): `kamu-test-harness`.
- Which repositories and services exist per bounded context: `kamu-domain-design`.
- Full-app catalog wiring lives in `src/app/cli/src/app.rs` and `database.rs`.
