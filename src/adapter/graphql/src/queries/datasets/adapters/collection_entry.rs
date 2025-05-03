// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_core::ResolvedDataset;

use crate::mutations::CollectionEntryInput;
use crate::prelude::*;
use crate::queries::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct CollectionEntry {
    #[graphql(skip)]
    #[expect(dead_code)]
    dataset: ResolvedDataset,

    /// Time when this version was created
    pub system_time: DateTime<Utc>,

    /// Time when this version was created
    pub event_time: DateTime<Utc>,

    /// File system-like path
    /// Rooted, separated by forward slashes, with elements URL-encoded
    /// (e.g. `/foo%20bar/baz`)
    pub path: String,

    /// DID of the linked dataset
    #[graphql(name = "ref")]
    pub reference: DatasetID<'static>,

    /// Extra data associated with this entry
    pub extra_data: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl CollectionEntry {
    pub fn from_input(dataset: ResolvedDataset, input: CollectionEntryInput) -> Self {
        let extra_data = input
            .extra_data
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let now = Utc::now();

        Self {
            dataset,
            system_time: now,
            event_time: now,
            path: input.path,
            reference: input.reference,
            extra_data,
        }
    }

    pub fn from_json(dataset: ResolvedDataset, record: serde_json::Value) -> Self {
        let serde_json::Value::Object(mut record) = record else {
            unreachable!()
        };

        // Parse system columns
        let vocab = odf::metadata::DatasetVocabulary::default();
        record.remove(&vocab.offset_column);
        record.remove(&vocab.operation_type_column);
        let system_time = DateTime::parse_from_rfc3339(
            record
                .remove(&vocab.system_time_column)
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into();
        let event_time = DateTime::parse_from_rfc3339(
            record
                .remove(&vocab.event_time_column)
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap()
        .into();

        // Parse core columns
        let path = String::from(record.remove("path").unwrap().as_str().unwrap());
        let reference =
            odf::DatasetID::from_did_str(record.remove("ref").unwrap().as_str().unwrap()).unwrap();

        Self {
            dataset,
            system_time,
            event_time,
            path,
            reference: reference.into(),
            extra_data: record.into(),
        }
    }

    pub fn to_record_data(&self) -> serde_json::Value {
        let mut record = self.extra_data.clone();
        record["path"] = self.path.clone().into();
        record["ref"] = self.reference.to_string().into();
        record
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
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
