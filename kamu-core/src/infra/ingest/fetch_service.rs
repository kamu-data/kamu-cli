use super::*;
use crate::domain::*;
use crate::infra::serde::yaml::formats::{datetime_rfc3339, datetime_rfc3339_opt};
use crate::infra::serde::yaml::*;

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::path::Path;
use url::Url;

pub struct FetchService {}

impl FetchService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn fetch(
        &self,
        fetch_step: &FetchStep,
        old_checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        listener: Option<&mut dyn FetchProgressListener>,
    ) -> Result<ExecutionResult<FetchCheckpoint>, FetchError> {
        match fetch_step {
            FetchStep::Url(ref furl) => {
                let url = Url::parse(&furl.url).map_err(|e| FetchError::internal(e))?;
                match url.scheme() {
                    "file" => self.fetch_file(
                        &url.to_file_path().unwrap(),
                        old_checkpoint,
                        target,
                        listener,
                    ),
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }

    fn fetch_file(
        &self,
        path: &Path,
        old_checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        maybe_listener: Option<&mut dyn FetchProgressListener>,
    ) -> Result<ExecutionResult<FetchCheckpoint>, FetchError> {
        use fs_extra::file::*;

        let mut null_listener = NullFetchProgressListener {};
        let listener = maybe_listener.unwrap_or(&mut null_listener);

        let meta = std::fs::metadata(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => FetchError::not_found(path),
            _ => FetchError::internal(e),
        })?;

        let mod_time: DateTime<Utc> = meta
            .modified()
            .map(|t| -> DateTime<Utc> { t.into() })
            .expect("File modification time is not available on this platform")
            .round_subsecs(3);

        if let Some(cp) = old_checkpoint {
            if cp.last_modified == Some(mod_time) {
                return Ok(ExecutionResult {
                    was_up_to_date: true,
                    checkpoint: cp,
                });
            }
        }

        let new_checkpoint = FetchCheckpoint {
            last_fetched: Utc::now(),
            last_modified: Some(mod_time),
            e_tag: None,
        };

        let mut options = CopyOptions::new();
        options.overwrite = true;

        let handle = |pr: TransitProcess| {
            let progress = FetchProgress {
                total_bytes: pr.total_bytes,
                fetched_bytes: pr.copied_bytes,
            };

            listener.on_progress(&progress);
        };

        // TODO: Use symlinks
        // TODO: Support compression
        fs_extra::file::copy_with_progress(path, target, &options, handle)
            .map_err(|e| FetchError::InternalError(Box::new(e)))?;

        Ok(ExecutionResult {
            was_up_to_date: false,
            checkpoint: new_checkpoint,
        })
    }
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FetchCheckpoint {
    #[serde(with = "datetime_rfc3339")]
    pub last_fetched: DateTime<Utc>,
    #[serde(with = "datetime_rfc3339_opt")]
    pub last_modified: Option<DateTime<Utc>>,
    pub e_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchProgress {
    pub total_bytes: u64,
    pub fetched_bytes: u64,
}

pub trait FetchProgressListener {
    fn on_progress(&mut self, _progress: &FetchProgress) {}
}

struct NullFetchProgressListener;
impl FetchProgressListener for NullFetchProgressListener {}
