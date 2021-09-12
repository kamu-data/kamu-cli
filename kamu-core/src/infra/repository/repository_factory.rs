// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;

use super::repository_local_fs::*;
use super::repository_s3::*;

use dill::*;
use slog::{info, o, Logger};
use std::backtrace::Backtrace;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use url::Url;

pub struct RepositoryFactory {
    logger: Logger,
}

#[component(pub)]
impl RepositoryFactory {
    pub fn new(logger: Logger) -> Self {
        Self { logger: logger }
    }

    pub fn get_repository_client(
        &self,
        repo: &Repository,
    ) -> Result<Arc<Mutex<dyn RepositoryClient>>, RepositoryFactoryError> {
        match repo.url.scheme() {
            "file" => Ok(Arc::new(Mutex::new(RepositoryLocalFS::new(
                repo.url.to_file_path().unwrap(),
            )))),
            "s3" => self.get_s3_client(&repo.url),
            "s3+http" => self.get_s3_client(&repo.url),
            "s3+https" => self.get_s3_client(&repo.url),
            s @ _ => Err(RepositoryFactoryError::unsupported_protocol(s)),
        }
    }

    fn get_s3_client(
        &self,
        url: &Url,
    ) -> Result<Arc<Mutex<dyn RepositoryClient>>, RepositoryFactoryError> {
        // TODO: Support virtual hosted style URLs once rusoto supports them
        // See: https://github.com/rusoto/rusoto/issues/1482
        let (endpoint, bucket): (Option<String>, String) =
            match (url.scheme(), url.host_str(), url.port(), url.path()) {
                ("s3", Some(host), None, "") => (None, host.to_owned()),
                ("s3+http", Some(host), None, path) => {
                    (Some(format!("http://{}", host)), path.to_owned())
                }
                ("s3+http", Some(host), Some(port), path) => {
                    (Some(format!("http://{}:{}", host, port)), path.to_owned())
                }
                ("s3+https", Some(host), None, path) => {
                    (Some(format!("https://{}", host)), path.to_owned())
                }
                ("s3+https", Some(host), Some(port), path) => {
                    (Some(format!("https://{}:{}", host, port)), path.to_owned())
                }
                _ => return Err(RepositoryFactoryError::invalid_url(url.as_str())),
            };

        let bucket = bucket.trim_start_matches("/").to_owned();
        info!(self.logger, "Creating S3 client"; "endpoint" => &endpoint, "bucket" => &bucket);
        Ok(Arc::new(Mutex::new(RepositoryS3::new(
            endpoint,
            bucket,
            self.logger.new(o!("repo" => "s3")),
        ))))
    }
}

#[derive(Debug, Error)]
pub enum RepositoryFactoryError {
    #[error("No suitable repository implementation found for scheme \"{scheme}\"")]
    UnsupportedProtocol {
        scheme: String,
        backtrace: Backtrace,
    },
    #[error("Invalid url \"{url}\"")]
    InvalidURL { url: String, backtrace: Backtrace },
}

impl RepositoryFactoryError {
    pub fn unsupported_protocol(scheme: &str) -> Self {
        RepositoryFactoryError::UnsupportedProtocol {
            scheme: scheme.to_owned(),
            backtrace: Backtrace::capture(),
        }
    }

    pub fn invalid_url(url: &str) -> Self {
        RepositoryFactoryError::InvalidURL {
            url: url.to_owned(),
            backtrace: Backtrace::capture(),
        }
    }
}
