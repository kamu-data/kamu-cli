// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, SecondsFormat, Utc};
use kamu_core::{InternalError, ResultIntoInternal};
use opendatafabric::serde::yaml::{datetime_rfc3339_opt, SourceStateDef};
use opendatafabric::SourceState;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::skip_serializing_none;

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PollingSourceState {
    ETag(String),
    LastModified(DateTime<Utc>),
}

impl PollingSourceState {
    pub fn from_source_state(source_state: &SourceState) -> Result<Option<Self>, InternalError> {
        if source_state.source == SourceState::SOURCE_POLLING {
            if source_state.kind == SourceState::KIND_ETAG {
                Ok(Some(Self::ETag(source_state.value.clone())))
            } else if source_state.kind == SourceState::KIND_LAST_MODIFIED {
                let dt = DateTime::parse_from_rfc3339(&source_state.value)
                    .map(|dt| dt.into())
                    .int_err()?;
                Ok(Some(Self::LastModified(dt)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn to_source_state(&self) -> SourceState {
        let (kind, value) = match self {
            Self::ETag(etag) => (SourceState::KIND_ETAG.to_owned(), etag.clone()),
            Self::LastModified(last_modified) => (
                SourceState::KIND_LAST_MODIFIED.to_owned(),
                last_modified.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            ),
        };
        SourceState {
            kind,
            source: SourceState::SOURCE_POLLING.to_owned(),
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

////////////////////////////////////////////////////////////////////////////////

/// Used to cache the fetch results te resume without re-downloading data
/// in case of errors in further ingestion steps.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FetchSavepoint {
    #[serde(default, with = "PollingSourceState")]
    pub source_state: Option<PollingSourceState>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub source_event_time: Option<DateTime<Utc>>,
    pub data_cache_key: String,
    pub has_more: bool,
}
