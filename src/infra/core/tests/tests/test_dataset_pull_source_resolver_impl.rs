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
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_dataset_without_pull_alias_has_no_source() {
    let harness = PullSourceHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "repo/foo", RemoteAliasKind::Push)
        .await;

    assert_eq!(harness.find_pull_alias(&foo).await.unwrap(), None);
    assert_eq!(harness.resolve_pull_source(&foo).await.unwrap(), None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_url_pull_alias_is_the_source() {
    let harness = PullSourceHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "https://example.com/foo", RemoteAliasKind::Pull)
        .await;

    assert_eq!(
        harness.find_pull_alias(&foo).await.unwrap(),
        Some(remote_ref("https://example.com/foo"))
    );
    assert_eq!(
        harness.resolve_pull_source(&foo).await.unwrap(),
        Some(DatasetPullSource {
            remote_ref: remote_ref("https://example.com/foo"),
            url: Url::parse("https://example.com/foo/").unwrap(),
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_repository_pull_alias_resolves_to_the_repository_url() {
    let harness = PullSourceHarness::new();
    harness.add_repository("repo", "https://example.com/repo/");
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "repo/bar", RemoteAliasKind::Pull)
        .await;

    assert_eq!(
        harness.resolve_pull_source(&foo).await.unwrap(),
        Some(DatasetPullSource {
            remote_ref: remote_ref("repo/bar"),
            url: Url::parse("https://example.com/repo/bar/").unwrap(),
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pull_alias_in_unknown_repository_is_reported() {
    let harness = PullSourceHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "missing/foo", RemoteAliasKind::Pull)
        .await;

    assert_matches!(
        harness.resolve_pull_source(&foo).await,
        Err(ResolvePullSourceError::RepositoryNotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_several_pull_aliases_are_ambiguous() {
    let harness = PullSourceHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "https://example.com/a", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&foo, "https://example.com/b", RemoteAliasKind::Pull)
        .await;

    assert_matches!(
        harness.find_pull_alias(&foo).await,
        Err(FindPullAliasError::AmbiguousSource)
    );
    assert_matches!(
        harness.resolve_pull_source(&foo).await,
        Err(ResolvePullSourceError::AmbiguousSource)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_dataset_is_found_by_its_pull_alias() {
    let harness = PullSourceHarness::new();
    harness.create_root("foo").await;
    let bar = harness.create_root("bar").await;
    let baz = harness.create_root("baz").await;
    harness
        .add_alias(&bar, "repo/bar", RemoteAliasKind::Pull)
        .await;
    harness
        .add_alias(&baz, "repo/other", RemoteAliasKind::Pull)
        .await;

    assert_eq!(
        harness.find_dataset_by_pull_alias("repo/bar").await,
        Some(bar)
    );
    assert_eq!(
        harness.find_dataset_by_pull_alias("repo/other").await,
        Some(baz)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_no_dataset_is_found_by_unknown_or_push_alias() {
    let harness = PullSourceHarness::new();
    let foo = harness.create_root("foo").await;
    harness
        .add_alias(&foo, "repo/foo", RemoteAliasKind::Push)
        .await;

    assert_eq!(harness.find_dataset_by_pull_alias("repo/foo").await, None);
    assert_eq!(harness.find_dataset_by_pull_alias("repo/bar").await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_pull_source_endpoint_carries_the_url_without_credentials() {
    let source = DatasetPullSource {
        remote_ref: remote_ref("repo/bar"),
        url: Url::parse("odf+https://example.com/bar/").unwrap(),
    };

    assert_eq!(
        source.to_sync_endpoint(),
        odf::metadata::tasks::SyncEndpoint {
            url: "odf+https://example.com/bar/".to_owned(),
            auth: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn remote_ref(remote_ref: &str) -> odf::DatasetRefRemote {
    odf::DatasetRefRemote::try_from(remote_ref).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct PullSourceHarness {
    base_repo_harness: BaseRepoHarness,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    pull_source_resolver: Arc<dyn DatasetPullSourceResolver>,
}

impl PullSourceHarness {
    fn new() -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let repos_dir = base_repo_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DatasetPullSourceResolverImpl>()
            .build();

        Self {
            base_repo_harness,
            remote_repo_registry: catalog.get_one().unwrap(),
            remote_aliases_registry: catalog.get_one().unwrap(),
            pull_source_resolver: catalog.get_one().unwrap(),
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

    fn add_repository(&self, name: &str, url: &str) {
        self.remote_repo_registry
            .add_repository(
                &odf::RepoName::new_unchecked(name),
                Url::parse(url).unwrap(),
            )
            .unwrap();
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

    async fn find_pull_alias(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<odf::DatasetRefRemote>, FindPullAliasError> {
        self.pull_source_resolver.find_pull_alias(target).await
    }

    async fn resolve_pull_source(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<DatasetPullSource>, ResolvePullSourceError> {
        self.pull_source_resolver.resolve_pull_source(target).await
    }

    async fn find_dataset_by_pull_alias(&self, alias: &str) -> Option<odf::DatasetHandle> {
        self.pull_source_resolver
            .find_dataset_by_pull_alias(&remote_ref(alias))
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
