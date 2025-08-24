// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::mutations::CollectionEntryInput;
use crate::prelude::*;
use crate::queries::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone)]
#[graphql(complex)]
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
    #[graphql(name = "ref")]
    pub reference: DatasetID<'static>,

    /// Extra data associated with this entry
    pub extra_data: ExtraData,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CollectionEntry {
    pub fn from_input(input: CollectionEntryInput) -> Self {
        let now = Utc::now();

        Self {
            system_time: now,
            event_time: now,
            path: input.path,
            reference: input.reference,
            extra_data: input.extra_data.unwrap_or_default(),
        }
    }

    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let mut event: CollectionEntryEvent = serde_json::from_value(record).int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();
        event.record.extra_data.remove(&vocab.offset_column);
        event.record.extra_data.remove(&vocab.operation_type_column);

        Ok(Self {
            system_time: event.system_time,
            event_time: event.event_time,
            path: event.record.path.into(),
            reference: event.record.reference.into(),
            extra_data: ExtraData::new(event.record.extra_data),
        })
    }

    pub fn into_record_data(self) -> serde_json::Value {
        serde_json::to_value(CollectionEntryRecord {
            path: self.path.into(),
            reference: self.reference.into(),
            extra_data: self.extra_data.into(),
        })
        .unwrap()
    }

    pub fn is_equivalent_record(&self, other: &Self) -> bool {
        self.path == other.path
            && self.reference == other.reference
            && self.extra_data == other.extra_data
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[ComplexObject]
impl CollectionEntry {
    /// Resolves the reference to linked dataset
    #[tracing::instrument(level = "info", name = CollectionEntry_as_dataset, skip_all)]
    pub async fn as_dataset(&self, ctx: &Context<'_>) -> Result<Option<Dataset>> {
        match Dataset::try_from_ref(ctx, &self.reference.as_local_ref()).await? {
            TransformInputDataset::Accessible(v) => Ok(Some(v.dataset)),
            TransformInputDataset::NotAccessible(_) => Ok(None),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    CollectionEntry,
    CollectionEntryConnection,
    CollectionEntryEdge
);

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
