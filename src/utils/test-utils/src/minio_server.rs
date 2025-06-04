// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::time::Duration;

use container_runtime::{ContainerProcess, ContainerRuntime};
use url::Url;

use crate::test_docker_images;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const TEST_BUCKET_NAME: &str = "test-bucket";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MinioServer {
    pub container_name: String,
    pub address: String,
    pub host_port: u16,
    pub access_key: String,
    pub secret_key: String,
    #[allow(dead_code)]
    container: ContainerProcess,
}

impl MinioServer {
    pub const IMAGE: &'static str = test_docker_images::MINIO;

    pub async fn new(server_dir: &Path, access_key: &str, secret_key: &str) -> Self {
        use rand::Rng;

        let container_runtime = ContainerRuntime::default();

        container_runtime
            .ensure_image(Self::IMAGE, None)
            .await
            .unwrap();

        let mut container_name = "kamu-test-minio-".to_owned();
        container_name.extend(
            rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(30)
                .map(char::from),
        );
        let server_port = 9000;

        if !server_dir.exists() {
            std::fs::create_dir(server_dir).unwrap();
        }

        let container = container_runtime
            .run_attached(Self::IMAGE)
            .random_container_name_with_prefix("kamu-test-minio-")
            .args(["server", "/data"])
            .expose_port(server_port)
            .volume((server_dir, "/data"))
            .environment_vars([
                ("MINIO_ACCESS_KEY", access_key),
                ("MINIO_SECRET_KEY", secret_key),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let host_port = container
            .wait_for_host_socket(server_port, Duration::from_secs(20))
            .await
            .unwrap();

        let address = container_runtime.get_runtime_host_addr();

        Self {
            container_name: container.container_name().to_string(),
            container,
            address,
            host_port,
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LocalS3Server {
    pub tmp_dir: tempfile::TempDir,
    pub minio: MinioServer,
    pub bucket: String,
    pub url: Url,
}

impl LocalS3Server {
    pub async fn new() -> Self {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        // TODO: Reconsider this - we should not be modifying global env from tests
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
            std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);
        }

        let tmp_dir = tempfile::tempdir().unwrap();
        let bucket = TEST_BUCKET_NAME.to_string();
        std::fs::create_dir(tmp_dir.path().join(&bucket)).unwrap();

        let minio = MinioServer::new(tmp_dir.path(), access_key, secret_key).await;

        let url = Url::parse(&format!(
            "s3+http://{}:{}/{}/",
            minio.address, minio.host_port, bucket
        ))
        .unwrap();

        Self {
            tmp_dir,
            minio,
            bucket,
            url,
        }
    }
}
