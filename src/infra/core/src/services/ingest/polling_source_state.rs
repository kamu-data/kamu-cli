// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use odf::metadata::serde::yaml::{datetime_rfc3339, datetime_rfc3339_opt, SourceStateDef};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::skip_serializing_none;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PollingSourceState {
    ETag(String),
    LastModified(DateTime<Utc>),
}

impl PollingSourceState {
    pub fn from_source_state(
        source_state: &odf::metadata::SourceState,
    ) -> Result<Option<Self>, InternalError> {
        if source_state.kind == odf::metadata::SourceState::KIND_ETAG {
            Ok(Some(Self::ETag(source_state.value.clone())))
        } else if source_state.kind == odf::metadata::SourceState::KIND_LAST_MODIFIED {
            let dt = DateTime::parse_from_rfc3339(&source_state.value)
                .map(Into::into)
                .int_err()?;
            Ok(Some(Self::LastModified(dt)))
        } else {
            tracing::debug!(kind = %source_state.kind, "Ignoring unsupported source state kind");
            Ok(None)
        }
    }

    pub fn try_from_source_state(source_state: &odf::metadata::SourceState) -> Option<Self> {
        match Self::from_source_state(source_state) {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(
                    ?source_state,
                    %error,
                    "Could not parse source state - ignoring"
                );
                None
            }
        }
    }

    pub fn to_source_state(&self) -> odf::metadata::SourceState {
        let (kind, value) = match self {
            Self::ETag(etag) => (
                odf::metadata::SourceState::KIND_ETAG.to_owned(),
                etag.clone(),
            ),
            Self::LastModified(last_modified) => (
                odf::metadata::SourceState::KIND_LAST_MODIFIED.to_owned(),
                last_modified.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            ),
        };
        odf::metadata::SourceState {
            source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
            kind,
            value,
        }
    }

    pub fn serialize<S: Serializer>(
        option: &Option<PollingSourceState>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match option {
            None => serializer.serialize_none(),
            Some(pss) => {
                let ss = pss.to_source_state();
                SourceStateDef::serialize(&ss, serializer)
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<PollingSourceState>, D::Error> {
        SourceStateDef::deserialize(deserializer)
            .map(|ss| Some(PollingSourceState::from_source_state(&ss).unwrap().unwrap()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Used to cache the fetch results to resume without re-downloading data
/// in case of errors in further ingestion steps.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct FetchSavepoint {
    #[serde(with = "datetime_rfc3339")]
    pub created_at: DateTime<Utc>,
    #[serde(default, with = "PollingSourceState")]
    pub source_state: Option<PollingSourceState>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub source_event_time: Option<DateTime<Utc>>,
    pub data: SavepointData,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase", tag = "kind")]
pub(crate) enum SavepointData {
    Owned { cache_key: String },
    Ref { path: PathBuf },
}

impl SavepointData {
    pub fn path(&self, cache_dir: &Path) -> PathBuf {
        match self {
            SavepointData::Owned { cache_key } => cache_dir.join(cache_key),
            SavepointData::Ref { path } => path.clone(),
        }
    }

    pub fn remove_owned(&self, cache_dir: &Path) -> Result<(), std::io::Error> {
        match self {
            SavepointData::Owned { cache_key } => std::fs::remove_file(cache_dir.join(cache_key)),
            SavepointData::Ref { .. } => Ok(()),
        }
    }
}
