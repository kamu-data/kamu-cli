use indoc::indoc;
use kamu::domain::*;
use kamu::infra::ingest::*;
use kamu::infra::serde::yaml::*;
use kamu_test::*;

use url::Url;

#[test]
fn test_fetch_file_via_url() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.csv");
    let checkpoint_path = tempdir.path().join("fetch.yaml");
    let target_path = tempdir.path().join("fetch.bin");

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: Url::from_file_path(&src_path).unwrap().as_str().to_owned(),
        event_time: None,
        cache: None,
    });

    let fetch_svc = FetchService::new();

    // No file to fetch
    assert_err!(fetch_svc.fetch(&fetch_step, &checkpoint_path, &target_path, None), FetchError::NotFound {..});
    assert!(!target_path.exists());

    std::fs::write(
        &src_path,
        indoc!(
            "
            city,population
            A,1000
            B,2000
            C,3000
            "
        ),
    )
    .unwrap();

    // Normal fetch
    assert_ok!(
        fetch_svc.fetch(&fetch_step, &checkpoint_path, &target_path, None),
        FetchResult::Updated(_)
    );
    assert!(target_path.exists());

    // No modifications
    assert_ok!(
        fetch_svc.fetch(&fetch_step, &checkpoint_path, &target_path, None),
        FetchResult::UpToDate(_)
    );

    // Fetches again if mtime changed
    filetime::set_file_mtime(&src_path, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    assert_ok!(
        fetch_svc.fetch(&fetch_step, &checkpoint_path, &target_path, None),
        FetchResult::Updated(_)
    );
}
