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
    let target_path = tempdir.path().join("fetched.bin");

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: Url::from_file_path(&src_path).unwrap().as_str().to_owned(),
        event_time: None,
        cache: None,
    });

    let fetch_svc = FetchService::new();

    // No file to fetch
    assert_err!(fetch_svc.fetch(&fetch_step, None, &target_path, None), IngestError::NotFound {..});
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
    let res = fetch_svc
        .fetch(&fetch_step, None, &target_path, None)
        .unwrap();
    assert_eq!(res.was_up_to_date, false);
    assert!(target_path.exists());

    // No modifications
    let res2 = fetch_svc
        .fetch(&fetch_step, Some(res.checkpoint), &target_path, None)
        .unwrap();
    assert_eq!(res2.was_up_to_date, true);

    // Fetches again if mtime changed
    filetime::set_file_mtime(&src_path, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res3 = fetch_svc
        .fetch(&fetch_step, Some(res2.checkpoint), &target_path, None)
        .unwrap();
    assert_eq!(res3.was_up_to_date, false);
}
