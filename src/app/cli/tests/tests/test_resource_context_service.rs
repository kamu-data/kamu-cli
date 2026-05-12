// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{CatalogBuilder, Component as _};
use kamu_accounts::CurrentAccountSubject;
use kamu_cli::resource_context::{
    CLIResourceContextStore,
    CurrentContextStateService,
    InMemoryResourceContextStore,
    ResolvedResourceContext,
    ResourceContextLastTestResult,
    ResourceContextLastTestStatus,
    ResourceContextRecord,
    ResourceContextRegistryService,
    ResourceContextResolver,
    ResourceContextStoreScope,
};
use kamu_cli::{WorkspaceLayout, WorkspaceService, resource_context};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CurrentContextStateService – account isolation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_workspace_scope_is_isolated_per_account() {
    let h = ResourceContextHarness::new();

    h.state_svc("alice")
        .set_current_context(ResourceContextStoreScope::Workspace, Some("prod".into()))
        .unwrap();
    h.state_svc("bob")
        .set_current_context(ResourceContextStoreScope::Workspace, Some("staging".into()))
        .unwrap();

    assert_eq!(
        h.state_svc("alice")
            .get_current_context_in_scope(ResourceContextStoreScope::Workspace),
        Some("prod".into()),
    );
    assert_eq!(
        h.state_svc("bob")
            .get_current_context_in_scope(ResourceContextStoreScope::Workspace),
        Some("staging".into()),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_user_scope_is_isolated_per_account() {
    let h = ResourceContextHarness::new();

    h.state_svc("alice")
        .set_current_context(ResourceContextStoreScope::User, Some("cloud".into()))
        .unwrap();
    // bob never sets a user-scope context

    assert_eq!(
        h.state_svc("alice")
            .get_current_context_in_scope(ResourceContextStoreScope::User),
        Some("cloud".into()),
    );
    assert_eq!(
        h.state_svc("bob")
            .get_current_context_in_scope(ResourceContextStoreScope::User),
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_current_context_prefers_workspace_over_user_scope() {
    let h = ResourceContextHarness::new();

    let alice = h.state_svc("alice");
    alice
        .set_current_context(ResourceContextStoreScope::User, Some("user-ctx".into()))
        .unwrap();
    alice
        .set_current_context(ResourceContextStoreScope::Workspace, Some("ws-ctx".into()))
        .unwrap();

    // Reload via a fresh catalog (simulates a new invocation)
    assert_eq!(
        h.state_svc("alice").get_current_context(),
        Some("ws-ctx".into()),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_current_context_falls_back_to_user_scope() {
    let h = ResourceContextHarness::new();

    h.state_svc("alice")
        .set_current_context(ResourceContextStoreScope::User, Some("user-ctx".into()))
        .unwrap();

    // No workspace-scope selection; reload via a fresh catalog
    assert_eq!(
        h.state_svc("alice").get_current_context(),
        Some("user-ctx".into()),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Shared registry – visible to all accounts
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_shared_registry_visible_to_all_accounts() {
    let h = ResourceContextHarness::new();
    h.add_remote_context(ResourceContextStoreScope::Workspace, "shared");

    assert!(h.registry_svc("alice").get_context("shared").is_some());
    assert!(h.registry_svc("bob").get_context("shared").is_some());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceContextResolver – precedence rules
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resolver_explicit_context_wins_over_current() {
    let h = ResourceContextHarness::new();
    h.add_remote_context(ResourceContextStoreScope::Workspace, "explicit");
    h.add_remote_context(ResourceContextStoreScope::Workspace, "current");

    h.state_svc("alice")
        .set_current_context(ResourceContextStoreScope::Workspace, Some("current".into()))
        .unwrap();

    let resolved = h.resolver("alice").resolve(Some("explicit")).unwrap();
    assert!(
        matches!(resolved, ResolvedResourceContext::RemoteWorkspace { ref name, .. } if name == "explicit")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resolver_uses_current_account_context() {
    let h = ResourceContextHarness::new();
    h.add_remote_context(ResourceContextStoreScope::Workspace, "alice-ctx");
    h.add_remote_context(ResourceContextStoreScope::Workspace, "bob-ctx");

    h.state_svc("alice")
        .set_current_context(
            ResourceContextStoreScope::Workspace,
            Some("alice-ctx".into()),
        )
        .unwrap();
    h.state_svc("bob")
        .set_current_context(ResourceContextStoreScope::Workspace, Some("bob-ctx".into()))
        .unwrap();

    let alice_resolved = h.resolver("alice").resolve(None).unwrap();
    let bob_resolved = h.resolver("bob").resolve(None).unwrap();

    assert!(
        matches!(alice_resolved, ResolvedResourceContext::RemoteWorkspace { ref name, .. } if name == "alice-ctx"),
        "Alice should resolve to alice-ctx"
    );
    assert!(
        matches!(bob_resolved, ResolvedResourceContext::RemoteWorkspace { ref name, .. } if name == "bob-ctx"),
        "Bob should resolve to bob-ctx"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resolver_falls_back_to_local_workspace() {
    let h = ResourceContextHarness::new();

    let resolved = h.resolver("alice").resolve(None).unwrap();
    assert_eq!(resolved, ResolvedResourceContext::LocalWorkspace);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resolver_error_when_no_context_and_no_workspace() {
    let h = ResourceContextHarness::outside_workspace();

    assert!(h.resolver("alice").resolve(None).is_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resolver_local_context_fails_outside_workspace() {
    let h = ResourceContextHarness::outside_workspace();

    assert!(h.resolver("alice").resolve(Some("local")).is_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resolver_unknown_context_returns_error() {
    let h = ResourceContextHarness::new();

    assert!(
        h.resolver("alice")
            .resolve(Some("no-such-context"))
            .is_err()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceContextResolver – "local" keyword account isolation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_local_context_selection_is_account_specific() {
    let h = ResourceContextHarness::new();
    h.add_remote_context(ResourceContextStoreScope::Workspace, "remote");

    // Alice explicitly selects "local"; Bob selects a remote context
    h.state_svc("alice")
        .set_current_context(ResourceContextStoreScope::Workspace, Some("local".into()))
        .unwrap();
    h.state_svc("bob")
        .set_current_context(ResourceContextStoreScope::Workspace, Some("remote".into()))
        .unwrap();

    // Alice resolves to local workspace via the explicit "local" name
    assert_eq!(
        h.resolver("alice").resolve(None).unwrap(),
        ResolvedResourceContext::LocalWorkspace,
    );

    // Bob resolves to his remote context; Alice's "local" selection did not affect
    // him
    assert!(
        matches!(
            h.resolver("bob").resolve(None).unwrap(),
            ResolvedResourceContext::RemoteWorkspace { ref name, .. } if name == "remote"
        ),
        "Bob should resolve to 'remote', not LocalWorkspace"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ResourceContextRegistryService – last-test results isolated per account
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_last_test_results_isolated_per_account() {
    let h = ResourceContextHarness::new();
    h.add_remote_context(ResourceContextStoreScope::Workspace, "prod");

    // Bob sees no cached result before any check is recorded
    let bob_contexts = h.registry_svc("bob").list_effective_contexts_with_scope();
    assert!(
        bob_contexts[0].last_test_result.is_none(),
        "Bob should see no cached result yet"
    );

    // Alice records a successful check result
    h.registry_svc("alice")
        .set_context_last_test_result(
            ResourceContextStoreScope::Workspace,
            "prod",
            ResourceContextLastTestResult {
                status: ResourceContextLastTestStatus::ReachableValid,
                checked_at: chrono::Utc::now(),
                detail: None,
            },
        )
        .unwrap();

    // Bob still sees no result — Alice's check did not pollute Bob's state
    let bob_contexts = h.registry_svc("bob").list_effective_contexts_with_scope();
    assert!(
        bob_contexts[0].last_test_result.is_none(),
        "Bob should still see no result after Alice's check"
    );

    // Bob records an unreachable result
    h.registry_svc("bob")
        .set_context_last_test_result(
            ResourceContextStoreScope::Workspace,
            "prod",
            ResourceContextLastTestResult {
                status: ResourceContextLastTestStatus::Unreachable,
                checked_at: chrono::Utc::now(),
                detail: None,
            },
        )
        .unwrap();

    // Alice still sees her own valid result — Bob's write did not overwrite it
    let alice_contexts = h.registry_svc("alice").list_effective_contexts_with_scope();
    assert_eq!(
        alice_contexts[0]
            .last_test_result
            .as_ref()
            .map(|r| &r.status),
        Some(&ResourceContextLastTestStatus::ReachableValid),
        "Alice's result should be unchanged after Bob's check"
    );

    // Bob sees his own unreachable result
    let bob_contexts = h.registry_svc("bob").list_effective_contexts_with_scope();
    assert_eq!(
        bob_contexts[0].last_test_result.as_ref().map(|r| &r.status),
        Some(&ResourceContextLastTestStatus::Unreachable),
        "Bob should see his own unreachable result"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CLIResourceContextStore – legacy runtime-state file (no accounts map) is
// tolerated
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_legacy_runtime_state_file_is_tolerated() {
    let tmp = tempfile::tempdir().unwrap();
    let workspace_dir = tmp.path().to_owned();

    // Write a pre-Phase1 runtime-state file: flat layout without the accounts map
    std::fs::write(
        workspace_dir.join(".kamucontexts.state"),
        r#"kind: KamuResourceContextsState
version: 1
content:
  currentContextName: legacy-ctx
  contexts:
    legacy-ctx:
      status: reachableValid
      checkedAt: "2024-01-01T00:00:00Z"
"#,
    )
    .unwrap();

    let base = {
        let mut b = CatalogBuilder::new();
        b.add_value(WorkspaceLayout::new(&workspace_dir));
        b.add::<CLIResourceContextStore>();
        b.build()
    };

    let make_state_svc = |account: &str| {
        CatalogBuilder::new_chained(&base)
            .add_value(CurrentAccountSubject::new_test_with(&account))
            .add::<resource_context::CurrentContextStateService>()
            .build()
            .get_one::<CurrentContextStateService>()
            .unwrap()
    };

    // The legacy file is readable without panic; unknown top-level fields are
    // ignored
    let alice_svc = make_state_svc("alice");
    assert_eq!(
        alice_svc.get_current_context_in_scope(ResourceContextStoreScope::Workspace),
        None,
        "Legacy file should be read as empty (unknown fields silently ignored)"
    );

    // Writing Alice's new-format state round-trips correctly
    alice_svc
        .set_current_context(ResourceContextStoreScope::Workspace, Some("prod".into()))
        .unwrap();
    assert_eq!(
        alice_svc.get_current_context_in_scope(ResourceContextStoreScope::Workspace),
        Some("prod".into()),
    );

    // A fresh service for Bob reads the rewritten file and sees no context for Bob
    let bob_svc = make_state_svc("bob");
    assert_eq!(
        bob_svc.get_current_context_in_scope(ResourceContextStoreScope::Workspace),
        None,
        "Bob should see no context after Alice's write"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
//
// Strategy: put the shared in-memory store and WorkspaceService into a base
// catalog, then build per-account chained catalogs on top.  Calling
// `catalog("alice")` and `catalog("bob")` on the same harness simulates two
// separate CLI invocations (`kamu --account X`) that share the same workspace.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceContextHarness {
    base_catalog: dill::Catalog,
    _tmp: tempfile::TempDir,
}

impl ResourceContextHarness {
    /// Harness where the workspace directory exists (`is_in_workspace()` →
    /// true).
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let base_catalog = Self::build_base_catalog(WorkspaceLayout::new(tmp.path()));
        Self {
            base_catalog,
            _tmp: tmp,
        }
    }

    /// Harness where no workspace directory exists (`is_in_workspace()` →
    /// false).
    fn outside_workspace() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let base_catalog =
            Self::build_base_catalog(WorkspaceLayout::new(tmp.path().join("workspace")));
        Self {
            base_catalog,
            _tmp: tmp,
        }
    }

    fn build_base_catalog(workspace_layout: WorkspaceLayout) -> dill::Catalog {
        let mut b = CatalogBuilder::new();
        b.add_value(workspace_layout);
        b.add::<InMemoryResourceContextStore>(); // shared across all account catalogs
        b.add_builder(WorkspaceService::builder().with_multi_tenant(false));
        b.build()
    }

    /// Build a catalog for the given account.  The store and `WorkspaceService`
    /// are inherited from the base; only the account identity is account-
    /// specific.
    fn catalog(&self, account_name: &str) -> dill::Catalog {
        CatalogBuilder::new_chained(&self.base_catalog)
            .add_value(CurrentAccountSubject::new_test_with(&account_name))
            .add::<resource_context::ResourceContextRegistryService>()
            .add::<resource_context::CurrentContextStateService>()
            .add::<resource_context::ResourceContextResolver>()
            .build()
    }

    fn resolver(&self, account_name: &str) -> Arc<ResourceContextResolver> {
        self.catalog(account_name)
            .get_one::<ResourceContextResolver>()
            .unwrap()
    }

    fn state_svc(&self, account_name: &str) -> Arc<CurrentContextStateService> {
        self.catalog(account_name)
            .get_one::<CurrentContextStateService>()
            .unwrap()
    }

    fn registry_svc(&self, account_name: &str) -> Arc<ResourceContextRegistryService> {
        self.catalog(account_name)
            .get_one::<ResourceContextRegistryService>()
            .unwrap()
    }

    /// Add a named remote context to the shared registry (registry entries are
    /// account-agnostic).
    fn add_remote_context(&self, scope: ResourceContextStoreScope, context_name: &str) {
        self.registry_svc("__setup__")
            .upsert_context(
                scope,
                ResourceContextRecord::new(
                    context_name,
                    Url::parse("https://api.example.com").unwrap(),
                ),
            )
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
