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

use crate::ExtraDataFields;

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
    pub path: CollectionPath,

    /// DID of the linked dataset
    pub reference: odf::DatasetID,

    /// Extra data associated with this entry
    pub extra_data: ExtraDataFields,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Validate correctness (not empty, valid URL-encodeding)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollectionPath(String);

impl CollectionPath {
    pub fn new(path: String) -> Self {
        Self(path)
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl TryFrom<&str> for CollectionPath {
    type Error = InternalError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self::new(value.to_string()))
    }
}

impl std::fmt::Display for CollectionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Deref for CollectionPath {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CollectionEntry {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let mut event: CollectionEntryEvent = serde_json::from_value(record).int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();
        event
            .record
            .extra_data
            .as_mut_map()
            .remove(&vocab.offset_column);
        event
            .record
            .extra_data
            .as_mut_map()
            .remove(&vocab.operation_type_column);

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
    pub path: CollectionPath,

    #[serde(rename = "ref")]
    pub reference: odf::DatasetID,

    #[serde(flatten)]
    pub extra_data: ExtraDataFields,
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
