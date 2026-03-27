// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use container_runtime::{ContainerProcess, ContainerRuntime};
use s3_utils::S3Context;
use url::Url;

use crate::test_docker_images;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LocalS3Server {
    pub ctx: S3Context,
    pub container: ContainerProcess,

    pub bucket: String,
    pub url: Url,

    pub access_key: String,
    pub secret_key: String,
}

impl LocalS3Server {
    pub const IMAGE: &'static str = test_docker_images::RUSTFS;
    pub const TEST_BUCKET_NAME: &str = "test-bucket";

    pub async fn new() -> Self {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let bucket = Self::TEST_BUCKET_NAME.to_string();

        // Start RustFS server container
        let container_runtime = ContainerRuntime::default();

        container_runtime
            .ensure_image(Self::IMAGE, None)
            .await
            .unwrap();

        let server_port = 9000;

        let container = container_runtime
            .run_attached(Self::IMAGE)
            .random_container_name_with_prefix("kamu-test-rustfs-")
            .args(["server", "/data"])
            .expose_port(server_port)
            .environment_vars([
                ("RUSTFS_ACCESS_KEY", access_key),
                ("RUSTFS_SECRET_KEY", secret_key),
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
        let endpoint_url = format!("http://{address}:{host_port}");
        let url = Url::parse(&format!("s3+{endpoint_url}/{bucket}/")).unwrap();

        // Create test bucket
        let ctx = S3Context::builder()
            .with_url(&url)
            .with_credentials_from_keys(access_key, secret_key)
            .build()
            .await;

        ctx.create_bucket(&bucket).await.unwrap();

        Self {
            ctx,
            container,
            bucket,
            url,
            access_key: access_key.to_owned(),
            secret_key: secret_key.to_owned(),
        }
    }

    // TODO: We properly propagate credentials when working with node's own S3
    // storage, but we don't have a good mechanism to propagate credentials to S3
    // clients that are used to work with remote repositories and have to rely on
    // env vars.
    pub fn set_credentials_env_vars(&self) {
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", &self.access_key);
            std::env::set_var("AWS_SECRET_ACCESS_KEY", &self.secret_key);
        }
    }
}
