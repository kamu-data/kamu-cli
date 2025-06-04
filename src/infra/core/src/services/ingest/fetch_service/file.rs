// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, SubsecRound, TimeZone as _, Utc};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;

use super::*;
use crate::PollingSourceState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FetchService {
    pub(super) fn fetch_files_glob(
        fglob: &odf::metadata::FetchStepFilesGlob,
        prev_source_state: Option<&PollingSourceState>,
        target_path: &Path,
        system_time: &DateTime<Utc>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        match &fglob.order {
            None | Some(odf::metadata::SourceOrdering::ByName) => (),
            Some(ord) => panic!("Files glob source can only be ordered by name, found: {ord:?}"),
        }

        let last_filename = match prev_source_state {
            Some(PollingSourceState::ETag(etag)) => Some(etag),
            _ => None,
        };

        let matched_paths = glob::glob(&fglob.path)
            .int_err()?
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        let mut matched_files: Vec<(String, PathBuf)> = matched_paths
            .into_iter()
            .filter(|p| p.is_file())
            .map(|p| (p.file_name().unwrap().to_str().unwrap().to_owned(), p))
            .filter(|(name, _)| {
                if let Some(lfn) = last_filename {
                    name > lfn
                } else {
                    true
                }
            })
            .collect();

        matched_files.sort_by(|a, b| b.0.cmp(&a.0));

        tracing::info!(
            pattern = fglob.path.as_str(),
            ?last_filename,
            matches = ?matched_files,
            "Matched the glob pattern"
        );

        if matched_files.is_empty() {
            return if prev_source_state.is_some() {
                Ok(FetchResult::UpToDate)
            } else {
                Err(PollingIngestError::not_found(&fglob.path, None))
            };
        }

        let (first_filename, first_path) = matched_files.pop().unwrap();

        let source_event_time = match &fglob.event_time {
            None | Some(odf::metadata::EventTimeSource::FromSystemTime(_)) => Some(*system_time),
            Some(odf::metadata::EventTimeSource::FromPath(src)) => {
                Some(Self::extract_event_time_from_path(&first_filename, src)?)
            }
            Some(odf::metadata::EventTimeSource::FromMetadata(_)) => {
                return Err(EventTimeSourceError::incompatible(
                    "Files glob source does not support extracting event time fromMetadata, you \
                     should use fromPath instead",
                )
                .into());
            }
        };

        let FetchResult::Updated(fetch_res) =
            Self::fetch_file(&first_path, None, None, target_path, system_time, listener)?
        else {
            panic!("Glob rule should be caching individual files")
        };

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::ETag(first_filename)),
            source_event_time,
            has_more: !matched_files.is_empty(),
            zero_copy_path: fetch_res.zero_copy_path,
        }))
    }

    // TODO: Validate event_time_source
    // TODO: Support event time from ctime/modtime
    // TODO: Resolve symlinks
    pub(crate) fn fetch_file(
        path: &Path,
        event_time_source: Option<&odf::metadata::EventTimeSource>,
        prev_source_state: Option<&PollingSourceState>,
        _target_path: &Path,
        system_time: &DateTime<Utc>,
        _listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        tracing::info!(?path, "Ingesting file");

        let meta = std::fs::metadata(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                PollingIngestError::not_found(path.as_os_str().to_string_lossy(), Some(e.into()))
            }
            _ => e.int_err().into(),
        })?;

        let mod_time: DateTime<Utc> = meta
            .modified()
            .map(|t| -> DateTime<Utc> { t.into() })
            .expect("File modification time is not available on this platform")
            .round_subsecs(3);

        if let Some(PollingSourceState::LastModified(last_modified)) = prev_source_state
            && *last_modified == mod_time
        {
            return Ok(FetchResult::UpToDate);
        }

        let source_event_time = match event_time_source {
            None | Some(odf::metadata::EventTimeSource::FromMetadata(_)) => Some(mod_time),
            Some(odf::metadata::EventTimeSource::FromSystemTime(_)) => Some(*system_time),
            Some(odf::metadata::EventTimeSource::FromPath(_)) => {
                return Err(EventTimeSourceError::incompatible(
                    "File source does not supports fromPath event time source",
                )
                .into());
            }
        };

        Ok(FetchResult::Updated(FetchResultUpdated {
            source_state: Some(PollingSourceState::LastModified(mod_time)),
            source_event_time,
            has_more: false,
            zero_copy_path: Some(path.to_path_buf()),
        }))
    }

    fn extract_event_time_from_path(
        filename: &str,
        src: &odf::metadata::EventTimeSourceFromPath,
    ) -> Result<DateTime<Utc>, PollingIngestError> {
        let time_re = regex::Regex::new(&src.pattern).int_err()?;

        let time_fmt = match src.timestamp_format {
            Some(ref fmt) => fmt,
            None => "%Y-%m-%d",
        };

        if let Some(capture) = time_re.captures(filename) {
            if let Some(group) = capture.get(1) {
                match DateTime::parse_from_str(group.as_str(), time_fmt) {
                    Ok(dt) => Ok(dt.into()),
                    Err(_) => {
                        let date = chrono::NaiveDate::parse_from_str(group.as_str(), time_fmt)
                            .map_err(|e| EventTimeSourceError::bad_pattern(time_fmt, e))?;
                        let time = date.and_hms_opt(0, 0, 0).unwrap();
                        Ok(Utc.from_local_datetime(&time).unwrap())
                    }
                }
            } else {
                Err(EventTimeSourceError::failed_extract(format!(
                    "Pattern {} doesn't have group 1",
                    src.pattern
                ))
                .into())
            }
        } else {
            Err(EventTimeSourceError::failed_extract(format!(
                "Failed to match pattern {} to {}",
                src.pattern, filename
            ))
            .into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
