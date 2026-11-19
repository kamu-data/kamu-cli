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
async fn test_update_kind_follows_dataset_kind_unless_pulled_from_remote() {
    let cases = [
        (
            odf::DatasetKind::Root,
            None,
            DatasetUpdateKind::PollingIngest,
        ),
        (
            odf::DatasetKind::Root,
            Some("kamu.dev/root"),
            DatasetUpdateKind::SyncFrom {
                remote_ref: BaseRepoHarness::remote_ref("kamu.dev/root"),
            },
        ),
        (
            odf::DatasetKind::Derivative,
            None,
            DatasetUpdateKind::Transform,
        ),
        (
            odf::DatasetKind::Derivative,
            Some("kamu.dev/derivative"),
            DatasetUpdateKind::SyncFrom {
                remote_ref: BaseRepoHarness::remote_ref("kamu.dev/derivative"),
            },
        ),
    ];

    for (kind, pull_alias, expected) in cases {
        let harness = UpdateKindHarness::new();
        let root = harness.create_root("root").await;
        let target = match kind {
            odf::DatasetKind::Root => root,
            odf::DatasetKind::Derivative => harness.create_derivative("derivative", "root").await,
        };
        if let Some(pull_alias) = pull_alias {
            harness
                .add_remote_alias(&target, pull_alias, RemoteAliasKind::Pull)
                .await;
        }

        assert_eq!(
            harness.resolve(&target).await.unwrap(),
            expected,
            "{kind:?} with pull alias {pull_alias:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_several_pull_aliases_are_ambiguous() {
    let harness = UpdateKindHarness::new();
    let root = harness.create_root("root").await;
    harness
        .add_remote_alias(&root, "kamu.dev/root", RemoteAliasKind::Pull)
        .await;
    harness
        .add_remote_alias(&root, "other.dev/root", RemoteAliasKind::Pull)
        .await;

    assert_matches!(
        harness.resolve(&root).await,
        Err(ResolveDatasetUpdateKindError::AmbiguousSource(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct UpdateKindHarness {
    base_repo_harness: BaseRepoHarness,
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
            .add::<RemoteDatasetUrlResolverImpl>()
            .add_value(RemoteRepositoryRegistryNull)
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryNull>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DatasetUpdateKindResolverImpl>()
            .add::<DatasetPullSourceResolverImpl>()
            .build();

        Self {
            base_repo_harness,
            update_kind_resolver: catalog.get_one().unwrap(),
        }
    }

    async fn create_derivative(&self, name: &str, input_name: &str) -> odf::DatasetHandle {
        self.base_repo_harness
            .create_derived_dataset(
                &BaseRepoHarness::alias(name),
                vec![BaseRepoHarness::alias(input_name).as_local_ref()],
            )
            .await
            .dataset_handle
    }

    async fn resolve(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<DatasetUpdateKind, ResolveDatasetUpdateKindError> {
        self.update_kind_resolver.resolve_update_kind(target).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
