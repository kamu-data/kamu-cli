use crate::domain::*;

use super::remote_local_fs::*;
use super::remote_s3::*;

use slog::{info, o, Logger};
use std::backtrace::Backtrace;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use url::Url;

pub struct RemoteFactory {
    logger: Logger,
}

impl RemoteFactory {
    pub fn new(logger: Logger) -> Self {
        Self { logger: logger }
    }

    pub fn get_remote_client(
        &mut self,
        remote: &Remote,
    ) -> Result<Arc<Mutex<dyn RemoteClient>>, RemoteFactoryError> {
        match remote.url.scheme() {
            "file" => Ok(Arc::new(Mutex::new(RemoteLocalFS::new(
                remote.url.to_file_path().unwrap(),
            )))),
            "s3" => self.get_s3_client(&remote.url),
            "s3+http" => self.get_s3_client(&remote.url),
            "s3+https" => self.get_s3_client(&remote.url),
            s @ _ => Err(RemoteFactoryError::unsupported_protocol(s)),
        }
    }

    fn get_s3_client(
        &mut self,
        url: &Url,
    ) -> Result<Arc<Mutex<dyn RemoteClient>>, RemoteFactoryError> {
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
                _ => return Err(RemoteFactoryError::invalid_url(url.as_str())),
            };

        let bucket = bucket.trim_start_matches("/").to_owned();
        info!(self.logger, "Creating S3 client"; "endpoint" => &endpoint, "bucket" => &bucket);
        Ok(Arc::new(Mutex::new(RemoteS3::new(
            endpoint,
            bucket,
            self.logger.new(o!("remote" => "s3")),
        ))))
    }
}

#[derive(Debug, Error)]
pub enum RemoteFactoryError {
    #[error("No suitable remote implementation found for scheme \"{scheme}\"")]
    UnsupportedProtocol {
        scheme: String,
        backtrace: Backtrace,
    },
    #[error("Invalid url \"{url}\"")]
    InvalidURL { url: String, backtrace: Backtrace },
}

impl RemoteFactoryError {
    pub fn unsupported_protocol(scheme: &str) -> Self {
        RemoteFactoryError::UnsupportedProtocol {
            scheme: scheme.to_owned(),
            backtrace: Backtrace::capture(),
        }
    }

    pub fn invalid_url(url: &str) -> Self {
        RemoteFactoryError::InvalidURL {
            url: url.to_owned(),
            backtrace: Backtrace::capture(),
        }
    }
}
