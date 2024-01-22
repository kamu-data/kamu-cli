// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::{Arc, Mutex};

use chrono::prelude::*;
use chrono::Utc;
use container_runtime::ContainerRuntime;
use indoc::indoc;
use kamu::domain::*;
use kamu::ingest::*;
use kamu::utils::docker_images::BUSYBOX;
use opendatafabric::*;
use url::Url;

///////////////////////////////////////////////////////////////////////////////
// URL: file
///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fetch_url_file() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path = tempdir.path().join("data.csv");
    let target_path = tempdir.path().join("fetched.bin");

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: Url::from_file_path(&src_path).unwrap().as_str().to_owned(),
        event_time: None,
        cache: None,
        headers: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );

    // No file to fetch
    assert_matches!(
        fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step,
                None,
                &target_path,
                &Utc::now(),
                None
            )
            .await,
        Err(PollingIngestError::NotFound { .. })
    );
    assert!(!target_path.exists());

    std::fs::write(&src_path, CSV_BATCH_OUTPUT).unwrap();

    // Normal fetch
    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    let FetchResult::Updated(update) = res else {
        panic!("Unexpected result: {res:#?}");
    };
    assert!(!target_path.exists()); // Uses zero-copy path
    assert_eq!(update.zero_copy_path.as_ref(), Some(&src_path));

    // No modifications
    let res2 = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(res2, FetchResult::UpToDate);

    // Fetches again if mtime changed
    filetime::set_file_mtime(&src_path, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res3 = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(res3, FetchResult::Updated(_));
}

///////////////////////////////////////////////////////////////////////////////
// URL: http
///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fetch_url_http_unreachable() {
    let tempdir = tempfile::tempdir().unwrap();
    let target_path = tempdir.path().join("fetched.bin");

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("http://localhost:{}/data.csv", 123),
        event_time: None,
        cache: None,
        headers: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );

    assert_matches!(
        fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step,
                None,
                &target_path,
                &Utc::now(),
                None
            )
            .await,
        Err(PollingIngestError::Unreachable { .. })
    );
    assert!(!target_path.exists());
}

#[test_group::group(containerized)]
#[tokio::test]
async fn test_fetch_url_http_not_found() {
    let tempdir = tempfile::tempdir().unwrap();
    let target_path = tempdir.path().join("fetched.bin");

    let http_server = crate::utils::HttpServer::new(&tempdir.path().join("srv")).await;

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("http://localhost:{}/data.csv", http_server.host_port),
        event_time: None,
        cache: None,
        headers: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );

    assert_matches!(
        fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step,
                None,
                &target_path,
                &Utc::now(),
                None
            )
            .await,
        Err(PollingIngestError::NotFound { .. })
    );
    assert!(!target_path.exists());
}

#[test_group::group(containerized)]
#[tokio::test]
async fn test_fetch_url_http_ok() {
    let tempdir = tempfile::tempdir().unwrap();
    let server_dir = tempdir.path().join("srv");
    std::fs::create_dir(&server_dir).unwrap();

    let src_path = server_dir.join("data.csv");
    let target_path = tempdir.path().join("fetched.bin");

    std::fs::write(&src_path, CSV_BATCH_OUTPUT).unwrap();

    let http_server = crate::utils::HttpServer::new(&server_dir).await;

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("http://localhost:{}/data.csv", http_server.host_port),
        event_time: None,
        cache: None,
        headers: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );
    let listener = Arc::new(TestListener::new());

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            Some(listener.clone()),
        )
        .await
        .unwrap();

    assert_matches!(res, FetchResult::Updated(_));
    let update = match res {
        FetchResult::Updated(upd) => upd,
        _ => unreachable!(),
    };
    assert_matches!(&update.source_state, Some(PollingSourceState::ETag(_)));
    assert!(!update.has_more);
    assert!(target_path.exists());
    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap(),
        CSV_BATCH_OUTPUT
    );
    assert_eq!(
        listener.get_last_progress(),
        Some(FetchProgress {
            fetched_bytes: 37,
            total_bytes: TotalBytes::Exact(37),
        })
    );

    let res_repeat = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(res_repeat, FetchResult::UpToDate);
    assert!(target_path.exists());

    filetime::set_file_mtime(&src_path, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res_touch = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(res_touch, FetchResult::Updated(_));
    assert!(target_path.exists());

    std::fs::remove_file(&src_path).unwrap();
    assert_matches!(
        fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step,
                update.source_state.as_ref(),
                &target_path,
                &Utc::now(),
                None
            )
            .await,
        Err(PollingIngestError::NotFound { .. })
    );

    assert!(target_path.exists());
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_fetch_url_http_env_interpolation() {
    let tempdir = tempfile::tempdir().unwrap();
    let server_dir = tempdir.path().join("srv");
    std::fs::create_dir(&server_dir).unwrap();

    let src_path = server_dir.join("data.csv");
    let target_path = tempdir.path().join("fetched.bin");

    std::fs::write(&src_path, CSV_BATCH_OUTPUT).unwrap();

    let http_server = crate::utils::HttpServer::new(&server_dir).await;

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!(
            "http://localhost:{}/${{{{ env.KAMU_TEST }}}}",
            http_server.host_port
        ),
        event_time: None,
        cache: None,
        headers: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );
    let listener = Arc::new(TestListener::new());

    assert_matches!(
        fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step,
                None,
                &target_path,
                &Utc::now(),
                Some(listener.clone())
            )
            .await,
        Err(_)
    );

    std::env::set_var("KAMU_TEST", "data.csv");

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            Some(listener.clone()),
        )
        .await
        .unwrap();

    assert_matches!(res, FetchResult::Updated(_));
    assert!(target_path.exists());
    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap(),
        CSV_BATCH_OUTPUT
    );
    assert_eq!(
        listener.get_last_progress(),
        Some(FetchProgress {
            fetched_bytes: 37,
            total_bytes: TotalBytes::Exact(37),
        })
    );
}

///////////////////////////////////////////////////////////////////////////////
// URL: ftp
///////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "ftp")]
#[test_group::group(containerized)]
#[tokio::test]
async fn test_fetch_url_ftp_ok() {
    let tempdir = tempfile::tempdir().unwrap();
    let server_dir = tempdir.path().join("srv");
    std::fs::create_dir(&server_dir).unwrap();

    let src_path = server_dir.join("data.csv");
    let target_path = tempdir.path().join("fetched.bin");

    std::fs::write(&src_path, CSV_BATCH_OUTPUT).unwrap();

    let ftp_server = crate::utils::FtpServer::new(&server_dir).await;

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("ftp://foo:bar@localhost:{}/data.csv", ftp_server.host_port),
        event_time: None,
        cache: None,
        headers: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );
    let listener = Arc::new(TestListener::new());

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            Some(listener.clone()),
        )
        .await
        .unwrap();

    assert_matches!(res, FetchResult::Updated(_));
    assert!(target_path.exists());
    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap(),
        CSV_BATCH_OUTPUT
    );
    assert_eq!(
        listener.get_last_progress(),
        Some(FetchProgress {
            fetched_bytes: 37,
            total_bytes: TotalBytes::Exact(37),
        })
    );
}

///////////////////////////////////////////////////////////////////////////////
// FilesGlob
///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fetch_files_glob() {
    let tempdir = tempfile::tempdir().unwrap();

    let src_path_1 = tempdir.path().join("data-2020-10-01.csv");
    let target_path = tempdir.path().join("fetched.bin");

    let fetch_step = FetchStep::FilesGlob(FetchStepFilesGlob {
        path: tempdir
            .path()
            .join("data-*.csv")
            .to_str()
            .unwrap()
            .to_owned(),
        event_time: Some(EventTimeSource::FromPath(EventTimeSourceFromPath {
            pattern: r"data-(\d+-\d+-\d+)\.csv".to_owned(),
            timestamp_format: Some("%Y-%m-%d".to_owned()),
        })),
        cache: None,
        order: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );

    // No file to fetch
    assert_matches!(
        fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step,
                None,
                &target_path,
                &Utc::now(),
                None
            )
            .await,
        Err(PollingIngestError::NotFound { .. })
    );
    assert!(!target_path.exists());

    std::fs::write(&src_path_1, CSV_BATCH_OUTPUT).unwrap();

    // Normal fetch
    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    let FetchResult::Updated(update) = res else {
        panic!("Unexpected result: {res:#?}");
    };
    assert!(!target_path.exists()); // Uses zero-copy path
    assert_eq!(update.zero_copy_path.as_ref(), Some(&src_path_1));
    assert_matches!(
        &update.source_state,
        Some(PollingSourceState::ETag(etag)) if etag == "data-2020-10-01.csv"
    );
    assert_eq!(
        update.source_event_time,
        Some(Utc.with_ymd_and_hms(2020, 10, 1, 0, 0, 0).unwrap())
    );

    // No modifications
    let res2 = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(res2, FetchResult::UpToDate);

    // Doesn't fetch again if mtime changed
    filetime::set_file_mtime(&src_path_1, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res3 = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(res3, FetchResult::UpToDate);

    // Doesn't consider files with names lexicographically "less" than last
    // fetched
    let src_path_0 = tempdir.path().join("data-2020-01-01.csv");
    std::fs::write(&src_path_0, CSV_BATCH_OUTPUT).unwrap();

    let res4 = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(res4, FetchResult::UpToDate);

    // Multiple available
    let src_path_2 = tempdir.path().join("data-2020-10-05.csv");
    std::fs::write(
        &src_path_2,
        indoc!(
            "
            city,population
            A,1010
            "
        ),
    )
    .unwrap();

    let src_path_3 = tempdir.path().join("data-2020-10-10.csv");
    std::fs::write(
        &src_path_3,
        indoc!(
            "
            city,population
            A,1020
            "
        ),
    )
    .unwrap();

    let res5 = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    let FetchResult::Updated(update5) = res5 else {
        panic!("Unexpected result: {res5:#?}");
    };
    assert!(!target_path.exists()); // Uses zero-copy path
    assert_eq!(update5.zero_copy_path.as_ref(), Some(&src_path_2));
    assert_matches!(
        &update5.source_state,
        Some(PollingSourceState::ETag(etag)) if etag == "data-2020-10-05.csv"
    );
    assert_eq!(
        update5.source_event_time,
        Some(Utc.with_ymd_and_hms(2020, 10, 5, 0, 0, 0).unwrap())
    );
    assert!(update5.has_more);

    let res6 = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            update5.source_state.as_ref(),
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();
    let FetchResult::Updated(update6) = res6 else {
        panic!("Unexpected result: {res6:#?}");
    };
    assert!(!target_path.exists()); // Uses zero-copy path
    assert_eq!(update6.zero_copy_path.as_ref(), Some(&src_path_3));
    assert_matches!(
        update6.source_state,
        Some(PollingSourceState::ETag(etag)) if etag == "data-2020-10-10.csv"
    );
    assert_eq!(
        update6.source_event_time,
        Some(Utc.with_ymd_and_hms(2020, 10, 10, 0, 0, 0).unwrap())
    );
    assert!(!update6.has_more);
}

///////////////////////////////////////////////////////////////////////////////
// Container
///////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[ignore]
#[test_log::test(tokio::test)]
async fn test_fetch_container_ok() {
    let tempdir = tempfile::tempdir().unwrap();
    let target_path = tempdir.path().join("fetched.bin");

    let fetch_step = FetchStep::Container(FetchStepContainer {
        image: BUSYBOX.to_owned(),
        command: Some(vec!["printf".to_owned()]),
        args: Some(vec![CSV_BATCH_OUTPUT.to_owned()]),
        env: None,
    });

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        tempdir.path().join("run"),
    );
    let listener = Arc::new(TestListener::new());

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            Some(listener.clone()),
        )
        .await
        .unwrap();

    assert_matches!(res, FetchResult::Updated(_));
    assert!(target_path.exists());
    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap(),
        CSV_BATCH_OUTPUT
    );
    assert_eq!(
        listener.get_last_progress(),
        Some(FetchProgress {
            fetched_bytes: 37,
            total_bytes: TotalBytes::Unknown,
        })
    );
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_fetch_container_batch_size_default() {
    let temp_dir = tempfile::tempdir().unwrap();
    let target_path = temp_dir.path().join("fetched.bin");

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        temp_dir.path().join("run"),
    );
    let fetch_step = FetchStep::Container(FetchStepContainer {
        image: BUSYBOX.to_owned(),
        command: Some(vec!["sh".to_owned()]),
        args: Some(vec![
            "-c".to_owned(),
            format!("env | grep -q {ODF_BATCH_SIZE}={ODF_BATCH_SIZE_DEFAULT}"),
        ]),
        env: None,
    });

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(res, FetchResult::UpToDate);
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_fetch_container_batch_size_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let target_path = temp_dir.path().join("fetched.bin");

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        temp_dir.path().join("run"),
    );
    let custom_batch_size = 40_000;
    let fetch_step = FetchStep::Container(FetchStepContainer {
        image: BUSYBOX.to_owned(),
        command: Some(vec!["sh".to_owned()]),
        args: Some(vec![
            "-c".to_owned(),
            format!("env | grep -q {ODF_BATCH_SIZE}={custom_batch_size}"),
        ]),
        env: Some(vec![EnvVar {
            name: ODF_BATCH_SIZE.to_owned(),
            value: Some(custom_batch_size.to_string()),
        }]),
    });

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(res, FetchResult::UpToDate);
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_fetch_container_batch_size_invalid_format() {
    let temp_dir = tempfile::tempdir().unwrap();
    let target_path = temp_dir.path().join("fetched.bin");

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        temp_dir.path().join("run"),
    );
    let invalid_format_batch_size = "-42";
    let fetch_step = FetchStep::Container(FetchStepContainer {
        image: BUSYBOX.to_owned(),
        command: Some(vec!["sh".to_owned()]),
        args: Some(vec![
            "-c".to_owned(),
            format!("env | grep -q {ODF_BATCH_SIZE}={invalid_format_batch_size}"),
        ]),
        env: Some(vec![EnvVar {
            name: ODF_BATCH_SIZE.to_owned(),
            value: Some(invalid_format_batch_size.to_string()),
        }]),
    });

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            None,
        )
        .await;

    assert_matches!(
        res,
        Err(
            PollingIngestError::InvalidParameterFormat(e)
        ) if e.name == ODF_BATCH_SIZE && e.value == invalid_format_batch_size
    );
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_fetch_container_has_more_no_data() {
    let temp_dir = tempfile::tempdir().unwrap();
    let target_path = temp_dir.path().join("fetched.bin");

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        temp_dir.path().join("run"),
    );
    let fetch_step = FetchStep::Container(FetchStepContainer {
        image: BUSYBOX.to_owned(),
        command: Some(vec!["true".to_owned()]),
        args: None,
        env: None,
    });

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(res, FetchResult::UpToDate);
    assert!(target_path.exists());
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), "");
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_fetch_container_has_more_data_is_less_than_a_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let target_path = temp_dir.path().join("fetched.bin");

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        temp_dir.path().join("run"),
    );
    let custom_batch_size = 150;
    let fetch_step = FetchStep::Container(FetchStepContainer {
        image: BUSYBOX.to_owned(),
        command: Some(vec!["sh".to_owned()]),
        args: Some(vec!["-c".to_owned(), HAS_MORE_TESTER_SCRIPT.to_owned()]),
        env: Some(vec![
            EnvVar {
                name: ODF_BATCH_SIZE.to_owned(),
                value: Some(custom_batch_size.to_string()),
            },
            EnvVar {
                name: "ROWS_COUNT".to_owned(),
                value: Some(100.to_string()),
            },
        ]),
    });

    let res = fetch_svc
        .fetch(
            &generate_unique_operation_id(),
            &fetch_step,
            None,
            &target_path,
            &Utc::now(),
            None,
        )
        .await
        .unwrap();

    assert_matches!(
        res,
        FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::ETag(expected_etag)),
            has_more: false,
            ..
        }) if expected_etag == "100"
    );
    assert!(target_path.exists());
    assert_eq!(
        std::fs::read_to_string(&target_path).unwrap(),
        CSV_BATCH_OUTPUT
    );
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_fetch_container_has_more_data_is_more_than_a_batch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let target_path = temp_dir.path().join("fetched.bin");

    let fetch_svc = FetchService::new(
        Arc::new(ContainerRuntime::default()),
        temp_dir.path().join("run"),
    );
    let custom_batch_size = 40;
    let env_100_rows_count = EnvVar {
        name: "ROWS_COUNT".to_owned(),
        value: Some(100.to_string()),
    };

    /* 1) Ingest the first 40 rows
                            +
             Read before :  0
                          ---
                           40 / 100
    */
    let prev_source_state = {
        let fetch_step_1 = FetchStep::Container(FetchStepContainer {
            image: BUSYBOX.to_owned(),
            command: Some(vec!["sh".to_owned()]),
            args: Some(vec!["-c".to_owned(), HAS_MORE_TESTER_SCRIPT.to_owned()]),
            env: Some(vec![
                EnvVar {
                    name: ODF_BATCH_SIZE.to_owned(),
                    value: Some(custom_batch_size.to_string()),
                },
                env_100_rows_count.clone(),
            ]),
        });

        let res_1 = fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step_1,
                None,
                &target_path,
                &Utc::now(),
                None,
            )
            .await
            .unwrap();

        assert!(target_path.exists());
        assert_matches!(
            &res_1,
            FetchResult::Updated(FetchResultUpdated {
                source_state: Some(PollingSourceState::ETag(expected_etag)),
                has_more: true,
                ..
            }) if expected_etag == "40"
        );
        assert!(target_path.exists());
        assert_eq!(
            std::fs::read_to_string(&target_path).unwrap(),
            CSV_BATCH_OUTPUT
        );

        match res_1 {
            FetchResult::Updated(x) => x.source_state,
            _ => unreachable!(),
        }
    };

    /* 2) Ingest the second 40 rows
                             +
             Read before :  40
                           ---
                            80 / 100
    */
    let prev_source_state = {
        let fetch_step_2 = FetchStep::Container(FetchStepContainer {
            image: BUSYBOX.to_owned(),
            command: Some(vec!["sh".to_owned()]),
            args: Some(vec!["-c".to_owned(), HAS_MORE_TESTER_SCRIPT.to_owned()]),
            env: Some(vec![
                EnvVar {
                    name: ODF_BATCH_SIZE.to_owned(),
                    value: Some(custom_batch_size.to_string()),
                },
                env_100_rows_count.clone(),
            ]),
        });

        let res_2 = fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step_2,
                prev_source_state.as_ref(),
                &target_path,
                &Utc::now(),
                None,
            )
            .await
            .unwrap();

        assert_matches!(
            &res_2,
            FetchResult::Updated(FetchResultUpdated {
                source_state: Some(PollingSourceState::ETag(expected_etag)),
                has_more: true,
                ..
            }) if expected_etag == "80"
        );
        assert!(target_path.exists());
        assert_eq!(
            std::fs::read_to_string(&target_path).unwrap(),
            CSV_BATCH_OUTPUT
        );

        match res_2 {
            FetchResult::Updated(x) => x.source_state,
            _ => unreachable!(),
        }
    };

    /* 3) Try to ingest the last 40 rows, but have only 20 ones
                                                         +
                                         Read before :  80
                                                       ---
                                                       100 / 100
    */
    let prev_source_state = {
        let fetch_step_3 = FetchStep::Container(FetchStepContainer {
            image: BUSYBOX.to_owned(),
            command: Some(vec!["sh".to_owned()]),
            args: Some(vec!["-c".to_owned(), HAS_MORE_TESTER_SCRIPT.to_owned()]),
            env: Some(vec![
                EnvVar {
                    name: ODF_BATCH_SIZE.to_owned(),
                    value: Some(custom_batch_size.to_string()),
                },
                env_100_rows_count.clone(),
            ]),
        });

        let res_3 = fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step_3,
                prev_source_state.as_ref(),
                &target_path,
                &Utc::now(),
                None,
            )
            .await
            .unwrap();

        assert_matches!(
            &res_3,
            FetchResult::Updated(FetchResultUpdated {
                source_state: Some(PollingSourceState::ETag(expected_etag)),
                has_more: false,
                ..
            }) if expected_etag == "100"
        );
        assert!(target_path.exists());
        assert_eq!(
            std::fs::read_to_string(&target_path).unwrap(),
            CSV_BATCH_OUTPUT
        );

        match res_3 {
            FetchResult::Updated(x) => x.source_state,
            _ => unreachable!(),
        }
    };

    // 4) Try to ingest the next 40 rows from the exhausted source, but have no new
    //    data
    {
        let fetch_step_4 = FetchStep::Container(FetchStepContainer {
            image: BUSYBOX.to_owned(),
            command: Some(vec!["sh".to_owned()]),
            args: Some(vec!["-c".to_owned(), HAS_MORE_TESTER_SCRIPT.to_owned()]),
            env: Some(vec![
                EnvVar {
                    name: ODF_BATCH_SIZE.to_owned(),
                    value: Some(custom_batch_size.to_string()),
                },
                env_100_rows_count,
            ]),
        });

        let res_4 = fetch_svc
            .fetch(
                &generate_unique_operation_id(),
                &fetch_step_4,
                prev_source_state.as_ref(),
                &target_path,
                &Utc::now(),
                None,
            )
            .await
            .unwrap();

        assert_matches!(res_4, FetchResult::UpToDate);
        assert!(target_path.exists());
        assert_eq!(std::fs::read_to_string(&target_path).unwrap(), "");
    }
}

///////////////////////////////////////////////////////////////////////////////
// Utils: constants
///////////////////////////////////////////////////////////////////////////////

const CSV_BATCH_OUTPUT: &str = indoc!(
    "
    city,population
    A,1000
    B,2000
    C,3000
    "
);
const HAS_MORE_TESTER_SCRIPT: &str = indoc! {r#"
    #!/usr/bin/env sh

    set -euo pipefail

    if [[ -n "${DEBUG:-}" ]]; then
      set -x
    fi

    BATCH_SIZE="${ODF_BATCH_SIZE}"
    ETAG="${ODF_ETAG:-0}"

    simulate_set_new_etag() {
      printf $1 > "${ODF_NEW_ETAG_PATH}"
    }

    simulate_data_output() {
      echo "city,population"
      echo "A,1000"
      echo "B,2000"
      echo "C,3000"
    }

    simulate_has_more_data() {
      touch "${ODF_NEW_HAS_MORE_DATA_PATH}"
    }

    NEW_ETAG=$((${ETAG} + ${BATCH_SIZE}))
    HAS_MORE_DATA=$((${NEW_ETAG} < ${ROWS_COUNT}))
    NEW_ETAG=$((${HAS_MORE_DATA} ? ${NEW_ETAG} : ${ROWS_COUNT}))

    if [[ "${ETAG}" -lt "${ROWS_COUNT}" ]]; then
      if [[ "${HAS_MORE_DATA}" == "1" ]]; then
        simulate_has_more_data
      fi

      simulate_data_output
    fi

    simulate_set_new_etag "${NEW_ETAG}"
"#};

///////////////////////////////////////////////////////////////////////////////
// Utils: helpers
///////////////////////////////////////////////////////////////////////////////

fn generate_unique_operation_id() -> String {
    nanoid::nanoid!()
}

///////////////////////////////////////////////////////////////////////////////
// Utils: Listener
///////////////////////////////////////////////////////////////////////////////

struct TestListener {
    last_progress: Mutex<Option<FetchProgress>>,
}

impl TestListener {
    fn new() -> Self {
        Self {
            last_progress: Mutex::new(None),
        }
    }

    fn get_last_progress(&self) -> Option<FetchProgress> {
        self.last_progress.lock().unwrap().clone()
    }
}

impl FetchProgressListener for TestListener {
    fn on_progress(&self, progress: &FetchProgress) {
        *self.last_progress.lock().unwrap() = Some(progress.clone());
    }
}
