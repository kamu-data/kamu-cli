use crate::domain::*;
use crate::infra::serde::yaml::formats::{datetime_rfc3339, datetime_rfc3339_opt};
use crate::infra::serde::yaml::*;

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::path::Path;
use url::Url;

pub struct FetchService;

impl FetchService {
    pub fn new() -> Self {
        Self {}
    }

    pub fn fetch(
        &self,
        fetch_step: &FetchStep,
        checkpoint_path: &Path,
        target: &Path,
        listener: Option<&mut dyn FetchProgressListener>,
    ) -> Result<FetchResult, FetchError> {
        let checkpoint = self.read_checkpoint(checkpoint_path)?;

        let res = match fetch_step {
            FetchStep::Url(ref furl) => {
                let url = Url::parse(&furl.url).map_err(|e| FetchError::internal(e))?;
                match url.scheme() {
                    "file" => {
                        self.fetch_file(&url.to_file_path().unwrap(), checkpoint, target, listener)
                    }
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        };

        match res {
            Ok(FetchResult::Updated(ref checkpoint)) => {
                self.write_checkpoint(checkpoint_path, checkpoint)?;
            }
            _ => (),
        }

        res
    }

    fn fetch_file(
        &self,
        path: &Path,
        checkpoint: Option<FetchCheckpoint>,
        target: &Path,
        maybe_listener: Option<&mut dyn FetchProgressListener>,
    ) -> Result<FetchResult, FetchError> {
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

        if let Some(ref cp) = checkpoint {
            if cp.last_modified == Some(mod_time) {
                return Ok(FetchResult::UpToDate(checkpoint.unwrap()));
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

        Ok(FetchResult::Updated(new_checkpoint))
    }

    fn read_checkpoint(
        &self,
        checkpoint_path: &Path,
    ) -> Result<Option<FetchCheckpoint>, FetchError> {
        if !checkpoint_path.exists() {
            Ok(None)
        } else {
            let file =
                std::fs::File::open(&checkpoint_path).map_err(|e| FetchError::internal(e))?;
            let manifest: Manifest<FetchCheckpoint> =
                serde_yaml::from_reader(file).map_err(|e| FetchError::internal(e))?;
            assert_eq!(manifest.kind, "FetchCheckpoint");
            Ok(Some(manifest.content))
        }
    }

    fn write_checkpoint(
        &self,
        checkpoint_path: &Path,
        checkpoint: &FetchCheckpoint,
    ) -> Result<(), FetchError> {
        let manifest = Manifest {
            api_version: 1,
            kind: "FetchCheckpoint".to_owned(),
            content: checkpoint.clone(),
        };
        let file = std::fs::File::create(checkpoint_path).map_err(|e| FetchError::internal(e))?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| FetchError::internal(e))?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum FetchResult {
    UpToDate(FetchCheckpoint),
    Updated(FetchCheckpoint),
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
