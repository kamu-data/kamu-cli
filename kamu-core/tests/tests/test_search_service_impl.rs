use crate::utils::MinioServer;
use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::Arc;
use url::Url;

// Create repo/bar dataset in a repo and check it appears in searches
fn do_test_search(tmp_workspace_dir: &Path, repo_url: Url) {
    let dataset_id = DatasetID::new_unchecked("foo");
    let repo = RepositoryID::new_unchecked("repo");
    let remote_dataset_ref = DatasetRefBuf::new_unchecked("repo/bar");

    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_workspace_dir).unwrap());
    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let repository_factory = Arc::new(RepositoryFactory::new(logger.clone()));
    let sync_svc = SyncServiceImpl::new(
        workspace_layout.clone(),
        metadata_repo.clone(),
        repository_factory.clone(),
        logger.clone(),
    );

    let search_svc = SearchServiceImpl::new(
        metadata_repo.clone(),
        repository_factory.clone(),
        logger.clone(),
    );

    // Add repository
    metadata_repo.add_repository(repo, repo_url).unwrap();

    // Add and sync dataset
    metadata_repo
        .add_dataset(
            MetadataFactory::dataset_snapshot()
                .id(&dataset_id)
                .source(MetadataFactory::dataset_source_root().build())
                .build(),
        )
        .unwrap();

    sync_svc
        .sync_to(
            dataset_id,
            &remote_dataset_ref,
            SyncOptions::default(),
            None,
        )
        .unwrap();

    // Search!
    assert_matches!(
        search_svc.search(None, SearchOptions::default()),
        Ok(SearchResult { datasets }) if datasets == vec![remote_dataset_ref.clone()]
    );
    assert_matches!(
        search_svc.search(Some("bar"), SearchOptions::default()),
        Ok(SearchResult { datasets }) if datasets == vec![remote_dataset_ref.clone()]
    );
    assert_matches!(
        search_svc.search(Some("foo"), SearchOptions::default()),
        Ok(SearchResult { datasets }) if datasets.is_empty()
    );
}

#[test]
fn test_search_local_fs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    do_test_search(tmp_workspace_dir.path(), repo_url);
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_search_s3() {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);

    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let bucket = "test-bucket";
    std::fs::create_dir(tmp_repo_dir.path().join(bucket)).unwrap();

    let minio = MinioServer::new(tmp_repo_dir.path(), access_key, secret_key);

    use std::str::FromStr;
    let repo_url = Url::from_str(&format!(
        "s3+http://{}:{}/{}",
        minio.address, minio.host_port, bucket
    ))
    .unwrap();

    do_test_search(tmp_workspace_dir.path(), repo_url);
}
