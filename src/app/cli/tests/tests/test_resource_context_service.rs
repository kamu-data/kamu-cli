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
    CurrentContextStateService,
    InMemoryResourceContextStore,
    ResolvedResourceContext,
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
