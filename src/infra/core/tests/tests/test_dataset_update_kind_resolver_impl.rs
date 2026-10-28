// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use kamu::testing::BaseRepoHarness;
use kamu::*;
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_root_without_pull_alias_is_ingested() {
    let harness = UpdateKindHarness::new();
    let root = harness.create_root("root").await;

    assert_eq!(
        harness.resolve(&root).await.unwrap(),
        DatasetUpdateKind::PollingIngest
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_root_with_pull_alias_is_synced() {
    let harness = UpdateKindHarness::new();
    let root = harness.create_root("root").await;
    harness
        .add_alias(&root, "kamu.dev/root", RemoteAliasKind::Pull)
        .await;

    assert_eq!(
        harness.resolve(&root).await.unwrap(),
        DatasetUpdateKind::SyncFrom {
            remote_ref: remote_ref("kamu.dev/root")
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_derivative_without_pull_alias_is_transformed() {
    let harness = UpdateKindHarness::new();
    harness.create_root("root").await;
    let derivative = harness.create_derivative("derivative", "root").await;

    assert_eq!(
        harness.resolve(&derivative).await.unwrap(),
        DatasetUpdateKind::Transform
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_derivative_with_pull_alias_is_synced() {
    let harness = UpdateKindHarness::new();
    harness.create_root("root").await;
    let derivative = harness.create_derivative("derivative", "root").await;
    harness
        .add_alias(&derivative, "kamu.dev/derivative", RemoteAliasKind::Pull)
        .await;

    assert_eq!(
        harness.resolve(&derivative).await.unwrap(),
        DatasetUpdateKind::SyncFrom {
            remote_ref: remote_ref("kamu.dev/derivative")
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_several_pull_aliases_are_ambiguous() {
    let harness = UpdateKindHarness::new();
    let root = harness.create_root("root").await;
    harness
        .add_alias(&root, "kamu.dev/root", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&root, "other.dev/root", RemoteAliasKind::Pull)
        .await;

    assert_matches!(
        harness.resolve(&root).await,
        Err(ResolveDatasetUpdateKindError::AmbiguousSource(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_push_aliases_are_ignored() {
    let harness = UpdateKindHarness::new();
    let root = harness.create_root("root").await;
    let derivative = harness.create_derivative("derivative", "root").await;
    harness
        .add_alias(&root, "kamu.dev/root", RemoteAliasKind::Push)
        .await;
    harness
        .add_alias(&derivative, "kamu.dev/derivative", RemoteAliasKind::Push)
        .await;

    assert_eq!(
        harness.resolve(&root).await.unwrap(),
        DatasetUpdateKind::PollingIngest
    );
    assert_eq!(
        harness.resolve(&derivative).await.unwrap(),
        DatasetUpdateKind::Transform
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn remote_ref(remote_ref: &str) -> odf::DatasetRefRemote {
    odf::DatasetRefRemote::try_from(remote_ref).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct UpdateKindHarness {
    base_repo_harness: BaseRepoHarness,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    update_kind_resolver: Arc<dyn DatasetUpdateKindResolver>,
}

impl UpdateKindHarness {
    fn new() -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add_value(RemoteRepositoryRegistryNull)
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryNull>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DatasetUpdateKindResolverImpl>()
            .add::<DatasetPullSourceResolverImpl>()
            .build();

        Self {
            base_repo_harness,
            remote_aliases_registry: catalog.get_one().unwrap(),
            update_kind_resolver: catalog.get_one().unwrap(),
        }
    }

    fn alias(name: &str) -> odf::DatasetAlias {
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(name))
    }

    async fn create_root(&self, name: &str) -> odf::DatasetHandle {
        self.base_repo_harness
            .create_root_dataset(&Self::alias(name))
            .await
            .dataset_handle
    }

    async fn create_derivative(&self, name: &str, input_name: &str) -> odf::DatasetHandle {
        self.base_repo_harness
            .create_derived_dataset(
                &Self::alias(name),
                vec![Self::alias(input_name).as_local_ref()],
            )
            .await
            .dataset_handle
    }

    async fn add_alias(&self, target: &odf::DatasetHandle, alias: &str, kind: RemoteAliasKind) {
        self.remote_aliases_registry
            .get_remote_aliases(target)
            .await
            .unwrap()
            .add(&remote_ref(alias), kind)
            .await
            .unwrap();
    }

    async fn resolve(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<DatasetUpdateKind, ResolveDatasetUpdateKindError> {
        self.update_kind_resolver.resolve_update_kind(target).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
