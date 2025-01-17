// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use kamu::testing::BaseRepoHarness;
use kamu::*;
use kamu_core::*;
use tempfile::TempDir;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_repo_target() {
    let harness = PushTestHarness::new(TenancyConfig::SingleTenant, true);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let remote_repo_data = harness.maybe_remote_repo_data.unwrap();

    let (items, errors) = harness
        .push_request_planner
        .collect_plan(
            &[foo.dataset_handle.clone()],
            Some(&odf::DatasetPushTarget::Repository(
                remote_repo_data.remote_repo_name.clone(),
            )),
        )
        .await;
    assert!(errors.is_empty());

    assert_eq!(items.len(), 1);
    assert_eq!(
        *items.first().unwrap(),
        PushItem {
            local_handle: foo.dataset_handle,
            remote_target: RemoteTarget {
                url: remote_repo_data
                    .remote_repo_url
                    .join(&alias_foo.dataset_name)
                    .unwrap(),
                repo_name: Some(remote_repo_data.remote_repo_name.clone()),
                dataset_name: None,
                account_name: None,
            },
            push_target: Some(odf::DatasetPushTarget::Repository(
                remote_repo_data.remote_repo_name
            ))
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_url_target() {
    let harness = PushTestHarness::new(TenancyConfig::SingleTenant, false);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let push_url = Url::parse("http://example.com/foo").unwrap();

    let (items, errors) = harness
        .push_request_planner
        .collect_plan(
            &[foo.dataset_handle.clone()],
            Some(&odf::DatasetPushTarget::Url(push_url.clone())),
        )
        .await;
    assert!(errors.is_empty());

    assert_eq!(items.len(), 1);
    assert_eq!(
        *items.first().unwrap(),
        PushItem {
            local_handle: foo.dataset_handle,
            remote_target: RemoteTarget {
                url: push_url.clone(),
                repo_name: None,
                dataset_name: None,
                account_name: None,
            },
            push_target: Some(odf::DatasetPushTarget::Url(push_url))
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_remote_alias_target() {
    let harness = PushTestHarness::new(TenancyConfig::SingleTenant, true);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let remote_repo_data = harness.maybe_remote_repo_data.unwrap();

    let remote_alias = odf::DatasetAliasRemote {
        repo_name: remote_repo_data.remote_repo_name.clone(),
        dataset_name: odf::DatasetName::new_unchecked("bar"),
        account_name: None,
    };

    let (items, errors) = harness
        .push_request_planner
        .collect_plan(
            &[foo.dataset_handle.clone()],
            Some(&odf::DatasetPushTarget::Alias(remote_alias.clone())),
        )
        .await;
    assert!(errors.is_empty());

    assert_eq!(items.len(), 1);
    assert_eq!(
        *items.first().unwrap(),
        PushItem {
            local_handle: foo.dataset_handle,
            remote_target: RemoteTarget {
                url: remote_repo_data.remote_repo_url.join("bar").unwrap(),
                repo_name: Some(remote_repo_data.remote_repo_name),
                dataset_name: Some(remote_alias.dataset_name.clone()),
                account_name: None,
            },
            push_target: Some(odf::DatasetPushTarget::Alias(remote_alias))
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_remote_no_target_presaved_push_alias() {
    let harness = PushTestHarness::new(TenancyConfig::SingleTenant, true);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let remote_repo_data = harness.maybe_remote_repo_data.unwrap();

    let mut aliases = harness
        .remote_aliases_registry
        .get_remote_aliases(&foo.dataset_handle)
        .await
        .unwrap();
    aliases
        .add(
            &odf::DatasetRefRemote::Url(Arc::new(remote_repo_data.remote_repo_url.clone())),
            RemoteAliasKind::Push,
        )
        .await
        .unwrap();

    let (items, errors) = harness
        .push_request_planner
        .collect_plan(&[foo.dataset_handle.clone()], None)
        .await;
    assert!(errors.is_empty());

    assert_eq!(items.len(), 1);
    assert_eq!(
        *items.first().unwrap(),
        PushItem {
            local_handle: foo.dataset_handle,
            remote_target: RemoteTarget {
                url: remote_repo_data.remote_repo_url,
                repo_name: None,
                dataset_name: None,
                account_name: None,
            },
            push_target: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_remote_no_target_no_alias() {
    let harness = PushTestHarness::new(TenancyConfig::SingleTenant, true);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let remote_repo_data = harness.maybe_remote_repo_data.unwrap();

    let (items, errors) = harness
        .push_request_planner
        .collect_plan(&[foo.dataset_handle.clone()], None)
        .await;
    assert!(errors.is_empty());

    assert_eq!(items.len(), 1);
    assert_eq!(
        *items.first().unwrap(),
        PushItem {
            local_handle: foo.dataset_handle,
            remote_target: RemoteTarget {
                url: remote_repo_data
                    .remote_repo_url
                    .join(&alias_foo.dataset_name)
                    .unwrap(),
                repo_name: Some(remote_repo_data.remote_repo_name),
                dataset_name: None,
                account_name: None,
            },
            push_target: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_remote_no_target_no_alias_multiple_repos_exist() {
    let harness = PushTestHarness::new(TenancyConfig::SingleTenant, true);

    let extra_remote_tmp_dir = tempfile::tempdir().unwrap();
    let extra_remote_repo_url = Url::from_directory_path(extra_remote_tmp_dir.path()).unwrap();
    let extra_remote_repo_name = odf::RepoName::new_unchecked("extra-remote");
    harness
        .remote_repo_registry
        .add_repository(&extra_remote_repo_name, extra_remote_repo_url)
        .unwrap();

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let (items, errors) = harness
        .push_request_planner
        .collect_plan(&[foo.dataset_handle.clone()], None)
        .await;
    assert!(items.is_empty());

    assert_eq!(1, errors.len());
    assert_matches!(
        errors.first().unwrap(),
        PushResponse {
            local_handle: Some(a_local_handle),
            target: None,
            result: Err(PushError::AmbiguousTarget),
        } if *a_local_handle == foo.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_remote_no_target_no_alias_no_repositories() {
    let harness = PushTestHarness::new(TenancyConfig::SingleTenant, false);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo = harness.create_root_dataset(&alias_foo).await;

    let (items, errors) = harness
        .push_request_planner
        .collect_plan(&[foo.dataset_handle.clone()], None)
        .await;
    assert!(items.is_empty());

    assert_eq!(1, errors.len());
    assert_matches!(
        errors.first().unwrap(),
        PushResponse {
            local_handle: Some(a_local_handle),
            target: None,
            result: Err(PushError::NoTarget),
        } if *a_local_handle == foo.dataset_handle
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct PushTestHarness {
    base_repo_harness: BaseRepoHarness,
    push_request_planner: Arc<dyn PushRequestPlanner>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    maybe_remote_repo_data: Option<RemoteRepoData>,
}

struct RemoteRepoData {
    remote_repo_name: odf::RepoName,
    remote_repo_url: Url,
    _remote_tmp_dir: TempDir,
}

impl PushTestHarness {
    fn new(tenancy_config: TenancyConfig, create_remote_repo: bool) -> Self {
        let base_repo_harness = BaseRepoHarness::new(tenancy_config, None);

        let repos_dir = base_repo_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let catalog = dill::CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add_value(RemoteRepositoryRegistryImpl::create(repos_dir).unwrap())
            .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<PushRequestPlannerImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .build();

        let maybe_remote_repo_data = if create_remote_repo {
            let remote_tmp_dir = tempfile::tempdir().unwrap();
            let remote_repo_url = Url::from_directory_path(remote_tmp_dir.path()).unwrap();

            let remote_repo_name = odf::RepoName::new_unchecked("remote");
            let remote_repo_registry = catalog.get_one::<dyn RemoteRepositoryRegistry>().unwrap();
            remote_repo_registry
                .add_repository(&remote_repo_name, remote_repo_url.clone())
                .unwrap();

            Some(RemoteRepoData {
                remote_repo_name,
                remote_repo_url,
                _remote_tmp_dir: remote_tmp_dir,
            })
        } else {
            None
        };

        Self {
            base_repo_harness,
            push_request_planner: catalog.get_one().unwrap(),
            remote_aliases_registry: catalog.get_one().unwrap(),
            remote_repo_registry: catalog.get_one().unwrap(),
            maybe_remote_repo_data,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
