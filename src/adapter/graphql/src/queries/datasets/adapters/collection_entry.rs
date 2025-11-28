// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::prelude::*;
use crate::queries::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct CollectionEntry {
    pub(crate) entity: kamu_datasets::CollectionEntry,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CollectionEntry {
    pub fn new(entity: kamu_datasets::CollectionEntry) -> Self {
        Self { entity }
    }

    // TODO: remove this when all use cases are migrated
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let mut event: CollectionEntryEvent = serde_json::from_value(record).int_err()?;

        let vocab = odf::metadata::DatasetVocabulary::default();
        event.record.extra_data.remove(&vocab.offset_column);
        event.record.extra_data.remove(&vocab.operation_type_column);

        Ok(Self {
            entity: kamu_datasets::CollectionEntry {
                system_time: event.system_time,
                event_time: event.event_time,
                path: kamu_datasets::CollectionPath::new(event.record.path),
                reference: event.record.reference,
                extra_data: kamu_datasets::ExtraDataFields::new(event.record.extra_data),
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl CollectionEntry {
    /// Time when this entry was created
    pub async fn system_time(&self) -> DateTime<Utc> {
        self.entity.system_time
    }

    /// Time when this entry was created
    pub async fn event_time(&self) -> DateTime<Utc> {
        self.entity.event_time
    }

    /// File system-like path
    /// Rooted, separated by forward slashes, with elements URL-encoded
    /// (e.g. `/foo%20bar/baz`)
    pub async fn path(&self) -> CollectionPath<'_> {
        CollectionPath::from(&self.entity.path)
    }

    /// DID of the linked dataset
    #[graphql(name = "ref")]
    pub async fn reference(&self) -> DatasetID<'_> {
        DatasetID::from(&self.entity.reference)
    }

    /// Extra data associated with this entry
    pub async fn extra_data(&self) -> ExtraData {
        // TODO: avoid clone
        ExtraData::new(self.entity.extra_data.as_map().clone())
    }

    /// Resolves the reference to linked dataset
    #[tracing::instrument(level = "info", name = CollectionEntry_as_dataset, skip_all)]
    pub async fn as_dataset(&self, ctx: &Context<'_>) -> Result<Option<Dataset>> {
        Dataset::try_from_ref(ctx, &self.entity.reference.as_local_ref()).await
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
