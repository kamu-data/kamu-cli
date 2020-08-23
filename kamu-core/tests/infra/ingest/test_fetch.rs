use indoc::indoc;
use kamu::domain::*;
use kamu::infra::ingest::*;
use kamu::infra::serde::yaml::*;
use kamu::infra::utils::docker_client::*;
use kamu::infra::utils::docker_images;
use kamu_test::*;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use url::Url;

///////////////////////////////////////////////////////////////////////////////
// URL: file
///////////////////////////////////////////////////////////////////////////////

#[test]
fn test_fetch_url_file() {
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

///////////////////////////////////////////////////////////////////////////////
// URL: http
///////////////////////////////////////////////////////////////////////////////

#[test]
fn test_fetch_url_http_unreachable() {
    let tempdir = tempfile::tempdir().unwrap();
    let target_path = tempdir.path().join("fetched.bin");

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("http://localhost:{}/data.csv", 123),
        event_time: None,
        cache: None,
    });

    let fetch_svc = FetchService::new();

    assert_err!(
        fetch_svc.fetch(&fetch_step, None, &target_path, None),
        IngestError::Unreachable {..}
    );
    assert!(!target_path.exists());
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_fetch_url_http_not_found() {
    let tempdir = tempfile::tempdir().unwrap();
    let target_path = tempdir.path().join("fetched.bin");

    let http_server = HttpServer::new(&tempdir.path().join("srv"));

    let fetch_step = FetchStep::Url(FetchStepUrl {
        url: format!("http://localhost:{}/data.csv", http_server.host_port),
        event_time: None,
        cache: None,
    });

    let fetch_svc = FetchService::new();

    assert_err!(
        fetch_svc.fetch(&fetch_step, None, &target_path, None),
        IngestError::NotFound {..}
    );
    assert!(!target_path.exists());
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_fetch_url_http_ok() {
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
    let mut listener = TestListener::new();

    let res = fetch_svc
        .fetch(&fetch_step, None, &target_path, Some(&mut listener))
        .unwrap();

    assert!(!res.was_up_to_date);
    assert!(target_path.exists());
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
    assert_eq!(
        listener.last_progress,
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
        .unwrap();

    assert!(res_repeat_last_mod.was_up_to_date);
    assert!(target_path.exists());

    let cp_etag = FetchCheckpoint {
        last_modified: None,
        ..res.checkpoint.clone()
    };
    let res_repeat_etag = fetch_svc
        .fetch(&fetch_step, Some(cp_etag), &target_path, None)
        .unwrap();

    assert!(res_repeat_etag.was_up_to_date);
    assert!(target_path.exists());

    filetime::set_file_mtime(&src_path, filetime::FileTime::from_unix_time(0, 0)).unwrap();
    let res_touch = fetch_svc
        .fetch(&fetch_step, Some(res.checkpoint), &target_path, None)
        .unwrap();

    assert!(!res_touch.was_up_to_date);
    assert!(target_path.exists());

    std::fs::remove_file(&src_path).unwrap();
    assert_err!(
        fetch_svc.fetch(&fetch_step, Some(res_touch.checkpoint), &target_path, None),
        IngestError::NotFound {..}
    );

    assert!(target_path.exists());
}

///////////////////////////////////////////////////////////////////////////////
// URL: ftp
///////////////////////////////////////////////////////////////////////////////

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_fetch_url_ftp_ok() {
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
    let mut listener = TestListener::new();

    let res = fetch_svc
        .fetch(&fetch_step, None, &target_path, Some(&mut listener))
        .unwrap();

    assert!(!res.was_up_to_date);
    assert!(target_path.exists());
    assert_eq!(std::fs::read_to_string(&target_path).unwrap(), content);
    assert_eq!(
        listener.last_progress,
        Some(FetchProgress {
            total_bytes: 37,
            fetched_bytes: 37
        })
    );
}

///////////////////////////////////////////////////////////////////////////////
// Utils: Listener
///////////////////////////////////////////////////////////////////////////////

struct TestListener {
    last_progress: Option<FetchProgress>,
}

impl TestListener {
    fn new() -> Self {
        Self {
            last_progress: None,
        }
    }
}

impl FetchProgressListener for TestListener {
    fn on_progress(&mut self, progress: &FetchProgress) {
        self.last_progress = Some(progress.clone());
    }
}

///////////////////////////////////////////////////////////////////////////////
// Utils: HttpServer
///////////////////////////////////////////////////////////////////////////////

// TODO: Consider replacing with in-process server for speed
struct HttpServer {
    container_name: String,
    process: std::process::Child,
    host_port: u16,
}

impl HttpServer {
    fn new(server_dir: &Path) -> Self {
        use rand::Rng;

        let docker = DockerClient::new();

        let mut server_name = "kamu-test-http-".to_owned();
        server_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(30),
        );
        let server_port = 80;

        if !server_dir.exists() {
            std::fs::create_dir(&server_dir).unwrap();
        }

        let image = docker_images::HTTPD;

        docker
            .pull_cmd(image)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        let process = docker
            .run_cmd(DockerRunArgs {
                image: image.to_owned(),
                container_name: Some(server_name.to_owned()),
                expose_ports: vec![server_port],
                volume_map: vec![(
                    server_dir.to_owned(),
                    PathBuf::from("/usr/local/apache2/htdocs"),
                )],
                ..DockerRunArgs::default()
            })
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let host_port = docker
            .wait_for_host_port(&server_name, server_port, Duration::from_secs(20))
            .unwrap();

        docker
            .wait_for_socket(host_port, Duration::from_secs(20))
            .unwrap();

        Self {
            container_name: server_name,
            process: process,
            host_port: host_port,
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        let docker = DockerClient::new();
        let _ = docker
            .kill_cmd(&self.container_name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.process.wait();
    }
}

///////////////////////////////////////////////////////////////////////////////
// Utils: FtpServer
///////////////////////////////////////////////////////////////////////////////

// TODO: Consider replacing with in-process server for speed
struct FtpServer {
    container_name: String,
    process: std::process::Child,
    host_port: u16,
}

impl FtpServer {
    fn new(server_dir: &Path) -> Self {
        use rand::Rng;

        let docker = DockerClient::new();

        let mut server_name = "kamu-test-ftp-".to_owned();
        server_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(30),
        );

        if !server_dir.exists() {
            std::fs::create_dir(&server_dir).unwrap();
        }

        let image = docker_images::FTP;

        docker
            .pull_cmd(image)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();

        // TODO: this is likely very brittle because of all the port mapping
        // FTP is a crazy protocol :(
        let process = docker
            .run_cmd(DockerRunArgs {
                image: image.to_owned(),
                container_name: Some(server_name.to_owned()),
                expose_ports: vec![21],
                expose_port_map_range: vec![((47400, 47470), (47400, 47470))],
                volume_map: vec![(server_dir.to_owned(), PathBuf::from("/home/vsftpd"))],
                environment_vars: vec![
                    ("FTP_USER".to_owned(), "foo".to_owned()),
                    ("FTP_PASS".to_owned(), "bar".to_owned()),
                    ("PASV_ADDRESS".to_owned(), "localhost".to_owned()),
                ],
                ..DockerRunArgs::default()
            })
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let host_port = docker
            .wait_for_host_port(&server_name, 21, Duration::from_secs(20))
            .unwrap();

        docker
            .wait_for_socket(host_port, Duration::from_secs(20))
            .unwrap();

        Self {
            container_name: server_name,
            process: process,
            host_port: host_port,
        }
    }
}

impl Drop for FtpServer {
    fn drop(&mut self) {
        let docker = DockerClient::new();
        let _ = docker
            .kill_cmd(&self.container_name)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = self.process.wait();
    }
}
