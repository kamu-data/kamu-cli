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
use std::sync::Mutex;

use crate::utils::{FtpServer, HttpServer};

use chrono::prelude::*;
use chrono::Utc;
use indoc::indoc;
use kamu::domain::*;
use kamu::infra::ingest::*;
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
    });

    let fetch_svc = FetchService::new();

    // No file to fetch
    assert_matches!(
        fetch_svc.fetch(&fetch_step, None, &target_path, None).await,
        Err(IngestError::NotFound { .. })
    );
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
        .await
        .unwrap();
    assert_eq!(res.was_up_to_date, false);
    assert!(target_path.exists());

    // No modifications
    let res2 = fetch_svc
        .fetch(&fetch_step, Some(res.checkpoint), &target_path, None)
        .await
        .unwrap();
    assert_eq!(res2.was_up_to_date, true);

    // Fetches again if mtime changed
    filetime::set_file_mtime(&src_path, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res3 = fetch_svc
        .fetch(&fetch_step, Some(res2.checkpoint), &target_path, None)
        .await
        .unwrap();
    assert_eq!(res3.was_up_to_date, false);
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
    });

    let fetch_svc = FetchService::new();

    assert_matches!(
        fetch_svc.fetch(&fetch_step, None, &target_path, None).await,
        Err(IngestError::Unreachable { .. })
    );
    assert!(!target_path.exists());
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_fetch_url_http_not_found() {
    let tempdir = tempfile::tempdir().unwrap();
    let target_path = tempdir.path().join("fetched.bin");

    let http_server = HttpServer::new(&tempdir.path().join("srv"));

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("http://localhost:{}/data.csv", http_server.host_port),
        event_time: None,
        cache: None,
    });

    let fetch_svc = FetchService::new();

    assert_matches!(
        fetch_svc.fetch(&fetch_step, None, &target_path, None).await,
        Err(IngestError::NotFound { .. })
    );
    assert!(!target_path.exists());
}

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_fetch_url_http_ok() {
    let tempdir = tempfile::tempdir().unwrap();
    let server_dir = tempdir.path().join("srv");
    std::fs::create_dir(&server_dir).unwrap();

    let src_path = server_dir.join("data.csv");
    let target_path = tempdir.path().join("fetched.bin");

    let content = indoc!(
        "
        city,population
        A,1000
        B,2000
        C,3000
        "
    );
    std::fs::write(&src_path, content).unwrap();

    let http_server = HttpServer::new(&server_dir);

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("http://localhost:{}/data.csv", http_server.host_port),
        event_time: None,
        cache: None,
    });

    let fetch_svc = FetchService::new();
    let listener = Arc::new(TestListener::new());

    let res = fetch_svc
        .fetch(&fetch_step, None, &target_path, Some(listener.clone()))
        .await
        .unwrap();

    assert!(!res.was_up_to_date);
    assert!(target_path.exists());
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
    assert_eq!(
        listener.get_last_progress(),
        Some(FetchProgress {
            total_bytes: 37,
            fetched_bytes: 37
        })
    );

    let cp_last_mod = FetchCheckpoint {
        etag: None,
        ..res.checkpoint.clone()
    };
    let res_repeat_last_mod = fetch_svc
        .fetch(&fetch_step, Some(cp_last_mod), &target_path, None)
        .await
        .unwrap();

    assert!(res_repeat_last_mod.was_up_to_date);
    assert!(target_path.exists());

    let cp_etag = FetchCheckpoint {
        last_modified: None,
        ..res.checkpoint.clone()
    };
    let res_repeat_etag = fetch_svc
        .fetch(&fetch_step, Some(cp_etag), &target_path, None)
        .await
        .unwrap();

    assert!(res_repeat_etag.was_up_to_date);
    assert!(target_path.exists());

    filetime::set_file_mtime(&src_path, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res_touch = fetch_svc
        .fetch(&fetch_step, Some(res.checkpoint), &target_path, None)
        .await
        .unwrap();

    assert!(!res_touch.was_up_to_date);
    assert!(target_path.exists());

    std::fs::remove_file(&src_path).unwrap();
    assert_matches!(
        fetch_svc
            .fetch(&fetch_step, Some(res_touch.checkpoint), &target_path, None)
            .await,
        Err(IngestError::NotFound { .. })
    );

    assert!(target_path.exists());
}

///////////////////////////////////////////////////////////////////////////////
// URL: ftp
///////////////////////////////////////////////////////////////////////////////

#[tokio::test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_fetch_url_ftp_ok() {
    let tempdir = tempfile::tempdir().unwrap();
    let server_dir = tempdir.path().join("srv");
    std::fs::create_dir(&server_dir).unwrap();

    let src_path = server_dir.join("data.csv");
    let target_path = tempdir.path().join("fetched.bin");

    let content = indoc!(
        "
        city,population
        A,1000
        B,2000
        C,3000
        "
    );
    std::fs::write(&src_path, content).unwrap();

    let ftp_server = FtpServer::new(&server_dir);

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("ftp://foo:bar@localhost:{}/data.csv", ftp_server.host_port),
        event_time: None,
        cache: None,
    });

    let fetch_svc = FetchService::new();
    let listener = Arc::new(TestListener::new());

    let res = fetch_svc
        .fetch(&fetch_step, None, &target_path, Some(listener.clone()))
        .await
        .unwrap();

    assert!(!res.was_up_to_date);
    assert!(target_path.exists());
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
    assert_eq!(
        listener.get_last_progress(),
        Some(FetchProgress {
            total_bytes: 37,
            fetched_bytes: 37
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

    let fetch_svc = FetchService::new();

    // No file to fetch
    assert_matches!(
        fetch_svc.fetch(&fetch_step, None, &target_path, None).await,
        Err(IngestError::NotFound { .. })
    );
    assert!(!target_path.exists());

    std::fs::write(
        &src_path_1,
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
        .await
        .unwrap();
    assert_eq!(res.was_up_to_date, false);
    assert!(target_path.exists());
    assert_eq!(
        res.checkpoint.last_filename,
        Some("data-2020-10-01.csv".to_owned())
    );
    assert_eq!(
        res.checkpoint.source_event_time,
        Some(Utc.ymd(2020, 10, 1).and_hms(0, 0, 0))
    );

    // No modifications
    let res2 = fetch_svc
        .fetch(&fetch_step, Some(res.checkpoint), &target_path, None)
        .await
        .unwrap();
    assert_eq!(res2.was_up_to_date, true);

    // Doesn't fetch again if mtime changed
    filetime::set_file_mtime(&src_path_1, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res3 = fetch_svc
        .fetch(&fetch_step, Some(res2.checkpoint), &target_path, None)
        .await
        .unwrap();
    assert_eq!(res3.was_up_to_date, true);

    // Doesn't consider files with names lexicographically "smaller" than last fetched
    let src_path_0 = tempdir.path().join("data-2020-01-01.csv");
    std::fs::write(
        &src_path_0,
        indoc!(
            "
            city,population
            A,100
            B,200
            C,300
            "
        ),
    )
    .unwrap();

    let res4 = fetch_svc
        .fetch(&fetch_step, Some(res3.checkpoint), &target_path, None)
        .await
        .unwrap();
    assert_eq!(res4.was_up_to_date, true);
    assert_eq!(
        res4.checkpoint.last_filename,
        Some("data-2020-10-01.csv".to_owned())
    );

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
        .fetch(&fetch_step, Some(res4.checkpoint), &target_path, None)
        .await
        .unwrap();
    assert_eq!(res5.was_up_to_date, false);
    assert!(target_path.exists());
    assert_eq!(
        res5.checkpoint.last_filename,
        Some("data-2020-10-05.csv".to_owned())
    );
    assert_eq!(
        res5.checkpoint.source_event_time,
        Some(Utc.ymd(2020, 10, 5).and_hms(0, 0, 0))
    );

    let res6 = fetch_svc
        .fetch(&fetch_step, Some(res5.checkpoint), &target_path, None)
        .await
        .unwrap();
    assert_eq!(res6.was_up_to_date, false);
    assert!(target_path.exists());
    assert_eq!(
        res6.checkpoint.last_filename,
        Some("data-2020-10-10.csv".to_owned())
    );
    assert_eq!(
        res6.checkpoint.source_event_time,
        Some(Utc.ymd(2020, 10, 10).and_hms(0, 0, 0))
    );
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
