use crate::domain::*;

use super::remote_local_fs::*;

use slog::Logger;
use std::backtrace::Backtrace;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub struct RemoteFactory {
    _logger: Logger,
}

impl RemoteFactory {
    pub fn new(logger: Logger) -> Self {
        Self { _logger: logger }
    }

    pub fn get_remote_client(
        &mut self,
        remote: &Remote,
    ) -> Result<Arc<Mutex<dyn RemoteClient>>, RemoteFactoryError> {
        match remote.url.scheme() {
            "file" => Ok(Arc::new(Mutex::new(RemoteLocalFS::new(
                remote.url.to_file_path().unwrap(),
            )))),
            s @ _ => Err(RemoteFactoryError::unsupported(s)),
        }
    }
}

#[derive(Debug, Error)]
pub enum RemoteFactoryError {
    #[error("No suitable remote implementation found for scheme \"{scheme}\"")]
    Unsupported {
        scheme: String,
        backtrace: Backtrace,
    },
}

impl RemoteFactoryError {
    pub fn unsupported(scheme: &str) -> Self {
        RemoteFactoryError::Unsupported {
            scheme: scheme.to_owned(),
            backtrace: Backtrace::capture(),
        }
    }
}
