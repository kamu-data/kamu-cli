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
async fn test_new_dataset_has_no_aliases() {
    let harness = RemoteAliasesHarness::new();
    let foo = harness.create_root("foo").await;

    let aliases = harness.remote_aliases(&foo).await;

    for kind in [RemoteAliasKind::Pull, RemoteAliasKind::Push] {
        assert!(aliases.is_empty(kind));
        assert_eq!(aliases.get_by_kind(kind).count(), 0);
        assert_eq!(aliases.single_by_kind(kind).unwrap(), None);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_added_aliases_persist_in_insertion_order_without_duplicates() {
    let harness = RemoteAliasesHarness::new();
    let foo = harness.create_root("foo").await;

    assert!(
        harness
            .add_alias(&foo, "https://example.com/b", RemoteAliasKind::Pull)
            .await
    );
    assert!(
        harness
            .add_alias(&foo, "https://example.com/a", RemoteAliasKind::Pull)
            .await
    );
    assert!(
        !harness
            .add_alias(&foo, "https://example.com/b", RemoteAliasKind::Pull)
            .await
    );

    assert_eq!(
        harness.aliases_of(&foo, RemoteAliasKind::Pull).await,
        ["https://example.com/b", "https://example.com/a"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_alias_kinds_are_independent() {
    let harness = RemoteAliasesHarness::new();
    let foo = harness.create_root("foo").await;
    let bar = harness.create_root("bar").await;

    harness
        .add_alias(&foo, "https://example.com/pull", RemoteAliasKind::Pull)
        .await;
    assert!(
        harness
            .add_alias(&foo, "https://example.com/both", RemoteAliasKind::Pull)
            .await
    );
    assert!(
        harness
            .add_alias(&foo, "https://example.com/both", RemoteAliasKind::Push)
            .await
    );

    let aliases = harness.remote_aliases(&foo).await;
    assert!(aliases.contains(
        &RemoteAliasesHarness::remote_ref("https://example.com/pull"),
        RemoteAliasKind::Pull
    ));
    assert!(!aliases.contains(
        &RemoteAliasesHarness::remote_ref("https://example.com/pull"),
        RemoteAliasKind::Push
    ));
    assert!(aliases.contains(
        &RemoteAliasesHarness::remote_ref("https://example.com/both"),
        RemoteAliasKind::Pull
    ));
    assert!(aliases.contains(
        &RemoteAliasesHarness::remote_ref("https://example.com/both"),
        RemoteAliasKind::Push
    ));

    // Aliases belong to their dataset
    let bar_aliases = harness.remote_aliases(&bar).await;
    assert!(bar_aliases.is_empty(RemoteAliasKind::Pull));
    assert!(bar_aliases.is_empty(RemoteAliasKind::Push));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_delete_removes_one_alias_of_its_kind() {
    let harness = RemoteAliasesHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "https://example.com/a", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&foo, "https://example.com/b", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&foo, "https://example.com/a", RemoteAliasKind::Push)
        .await;

    assert!(
        harness
            .delete_alias(&foo, "https://example.com/a", RemoteAliasKind::Pull)
            .await
    );
    assert!(
        !harness
            .delete_alias(&foo, "https://example.com/a", RemoteAliasKind::Pull)
            .await
    );
    assert!(
        !harness
            .delete_alias(&foo, "https://example.com/c", RemoteAliasKind::Push)
            .await
    );

    assert_eq!(
        harness.aliases_of(&foo, RemoteAliasKind::Pull).await,
        ["https://example.com/b"]
    );
    assert_eq!(
        harness.aliases_of(&foo, RemoteAliasKind::Push).await,
        ["https://example.com/a"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_clear_removes_all_aliases_of_its_kind() {
    let harness = RemoteAliasesHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "https://example.com/a", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&foo, "https://example.com/b", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&foo, "https://example.com/push", RemoteAliasKind::Push)
        .await;

    assert_eq!(harness.clear_aliases(&foo, RemoteAliasKind::Pull).await, 2);
    assert_eq!(harness.clear_aliases(&foo, RemoteAliasKind::Pull).await, 0);

    assert!(
        harness
            .remote_aliases(&foo)
            .await
            .is_empty(RemoteAliasKind::Pull)
    );
    assert_eq!(
        harness.aliases_of(&foo, RemoteAliasKind::Push).await,
        ["https://example.com/push"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_single_alias_is_chosen_per_kind() {
    let harness = RemoteAliasesHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "https://example.com/pull", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&foo, "https://example.com/push", RemoteAliasKind::Push)
        .await;

    let aliases = harness.remote_aliases(&foo).await;

    assert_eq!(
        aliases.single_by_kind(RemoteAliasKind::Pull).unwrap(),
        Some(&RemoteAliasesHarness::remote_ref(
            "https://example.com/pull"
        ))
    );
    assert_eq!(
        aliases.single_by_kind(RemoteAliasKind::Push).unwrap(),
        Some(&RemoteAliasesHarness::remote_ref(
            "https://example.com/push"
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_several_aliases_of_a_kind_are_ambiguous_only_for_that_kind() {
    let harness = RemoteAliasesHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "https://example.com/a", RemoteAliasKind::Push)
        .await;
    harness
        .add_alias(&foo, "https://example.com/b", RemoteAliasKind::Push)
        .await;
    harness
        .add_alias(&foo, "https://example.com/pull", RemoteAliasKind::Pull)
        .await;

    let aliases = harness.remote_aliases(&foo).await;

    assert_matches!(
        aliases.single_by_kind(RemoteAliasKind::Push),
        Err(AmbiguousRemoteAliasError {
            kind: RemoteAliasKind::Push
        })
    );
    assert_eq!(
        aliases.single_by_kind(RemoteAliasKind::Pull).unwrap(),
        Some(&RemoteAliasesHarness::remote_ref(
            "https://example.com/pull"
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Every helper reads the aliases anew, so each check sees what was stored
#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct RemoteAliasesHarness {
    base_repo_harness: BaseRepoHarness,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
}

impl RemoteAliasesHarness {
    fn remote_ref(remote_ref: &str) -> odf::DatasetRefRemote {
        odf::DatasetRefRemote::try_from(remote_ref).unwrap()
    }

    fn new() -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RemoteAliasesRegistryImpl>()
            .build();

        Self {
            base_repo_harness,
            remote_aliases_registry: catalog.get_one().unwrap(),
        }
    }

    async fn create_root(&self, name: &str) -> odf::DatasetHandle {
        self.base_repo_harness
            .create_root_dataset(&odf::DatasetAlias::new(
                None,
                odf::DatasetName::new_unchecked(name),
            ))
            .await
            .dataset_handle
    }

    async fn remote_aliases(&self, target: &odf::DatasetHandle) -> Box<dyn RemoteAliases> {
        self.remote_aliases_registry
            .get_remote_aliases(target)
            .await
            .unwrap()
    }

    async fn aliases_of(&self, target: &odf::DatasetHandle, kind: RemoteAliasKind) -> Vec<String> {
        self.remote_aliases(target)
            .await
            .get_by_kind(kind)
            .map(ToString::to_string)
            .collect()
    }

    async fn add_alias(
        &self,
        target: &odf::DatasetHandle,
        alias: &str,
        kind: RemoteAliasKind,
    ) -> bool {
        self.remote_aliases(target)
            .await
            .add(&Self::remote_ref(alias), kind)
            .await
            .unwrap()
    }

    async fn delete_alias(
        &self,
        target: &odf::DatasetHandle,
        alias: &str,
        kind: RemoteAliasKind,
    ) -> bool {
        self.remote_aliases(target)
            .await
            .delete(&Self::remote_ref(alias), kind)
            .await
            .unwrap()
    }

    async fn clear_aliases(&self, target: &odf::DatasetHandle, kind: RemoteAliasKind) -> usize {
        self.remote_aliases(target).await.clear(kind).await.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
