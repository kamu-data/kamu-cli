// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CollectionEntry {
    /// Time when this version was created
    pub system_time: DateTime<Utc>,

    /// Time when this version was created
    pub event_time: DateTime<Utc>,

    /// File system-like path
    /// Rooted, separated by forward slashes, with elements URL-encoded
    /// (e.g. `/foo%20bar/baz`)
    pub path: String,

    /// DID of the linked dataset
    pub reference: odf::DatasetID,

    /// Extra data associated with this entry
    pub extra_data: serde_json::Map<String, serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CollectionEntry {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let mut event: CollectionEntryEvent = serde_json::from_value(record).int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();
        event.record.extra_data.remove(&vocab.offset_column);
        event.record.extra_data.remove(&vocab.operation_type_column);

        Ok(Self {
            system_time: event.system_time,
            event_time: event.event_time,
            path: event.record.path,
            reference: event.record.reference,
            extra_data: event.record.extra_data,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Used to serialize/deserialize entry from a dataset
#[derive(serde::Serialize, serde::Deserialize)]
struct CollectionEntryRecord {
    pub path: String,

    #[serde(rename = "ref")]
    pub reference: odf::DatasetID,

    #[serde(flatten)]
    pub extra_data: serde_json::Map<String, serde_json::Value>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct CollectionEntryEvent {
    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub system_time: DateTime<Utc>,

    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub event_time: DateTime<Utc>,

    #[serde(flatten)]
    pub record: CollectionEntryRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
