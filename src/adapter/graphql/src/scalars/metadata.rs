// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use kamu_core::DatasetRegistry;

use crate::prelude::*;
use crate::queries::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlockExtended
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, SimpleObject, PartialEq, Eq)]
pub struct MetadataBlockExtended {
    pub block_hash: Multihash<'static>,
    pub prev_block_hash: Option<Multihash<'static>>,
    pub system_time: DateTime<Utc>,
    pub author: Account,
    pub event: MetadataEvent,
    pub sequence_number: u64,
}

impl MetadataBlockExtended {
    pub async fn new(
        ctx: &Context<'_>,
        block_hash: impl Into<Multihash<'static>>,
        block: odf::metadata::MetadataBlock,
        author: Account,
    ) -> Result<Self, InternalError> {
        let b: MetadataBlock = MetadataBlock::with_extended_aliases(ctx, block)
            .await?
            .into();
        Ok(Self {
            block_hash: block_hash.into(),
            prev_block_hash: b.prev_block_hash,
            system_time: b.system_time,
            author,
            event: b.event,
            sequence_number: b.sequence_number,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataFormat
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataManifestFormat {
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataFormat serde errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct MetadataManifestMalformed {
    pub message: String,
}

#[ComplexObject]
impl MetadataManifestMalformed {
    pub async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct MetadataManifestUnsupportedVersion {
    pub manifest_version: i32,
    pub supported_version_from: i32,
    pub supported_version_to: i32,
}

#[Object]
impl MetadataManifestUnsupportedVersion {
    pub async fn is_success(&self) -> bool {
        false
    }
    pub async fn message(&self) -> String {
        format!(
            "Unsupported manifest version {}, supported range is [{}, {}]",
            self.manifest_version, self.supported_version_from, self.supported_version_to
        )
    }
}

impl From<odf::metadata::serde::UnsupportedVersionError> for MetadataManifestUnsupportedVersion {
    fn from(e: odf::metadata::serde::UnsupportedVersionError) -> Self {
        Self {
            manifest_version: e.manifest_version,
            supported_version_from: e.supported_version_range.0 as i32,
            supported_version_to: e.supported_version_range.1 as i32,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Need to delete all this alias update code
impl SetTransform {
    pub async fn with_extended_aliases(
        ctx: &Context<'_>,
        v: odf::metadata::SetTransform,
    ) -> Result<odf::metadata::SetTransform, InternalError> {
        let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);

        let original_transform = v.transform;
        let mut input_dataset_id_to_transform_input_mapping = v
            .inputs
            .into_iter()
            .map(|input| {
                // Safety: dataset_ref is already resolved to id by this point
                let dataset_id = input.dataset_ref.id().unwrap().clone();
                (dataset_id, input)
            })
            .collect::<HashMap<_, _>>();

        let input_dataset_ids = input_dataset_id_to_transform_input_mapping
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let dataset_handles_resolution = dataset_registry
            .resolve_multiple_dataset_handles_by_ids(input_dataset_ids)
            .await
            .int_err()?;

        for dataset_handle in dataset_handles_resolution.resolved_handles {
            // Safety: the mapping ensures that a value is present
            let transform_input = input_dataset_id_to_transform_input_mapping
                .get_mut(&dataset_handle.id)
                .unwrap();

            // Overwrite only if no query alias was previously specified in the transform
            // input
            if transform_input.alias.is_none() {
                transform_input.alias = Some(dataset_handle.alias.to_string());
            }
        }

        let updated_inputs = input_dataset_id_to_transform_input_mapping
            .into_values()
            .collect();

        Ok(odf::metadata::SetTransform {
            inputs: updated_inputs,
            transform: original_transform,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MetadataBlock {
    pub async fn with_extended_aliases(
        ctx: &Context<'_>,
        v: odf::metadata::MetadataBlock,
    ) -> Result<odf::metadata::MetadataBlock, InternalError> {
        Ok(odf::metadata::MetadataBlock {
            system_time: v.system_time,
            prev_block_hash: v.prev_block_hash,
            sequence_number: v.sequence_number,
            event: MetadataEvent::with_extended_aliases(ctx, v.event).await?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MetadataEvent {
    async fn with_extended_aliases(
        ctx: &Context<'_>,
        event: odf::metadata::MetadataEvent,
    ) -> Result<odf::metadata::MetadataEvent, InternalError> {
        Ok(match event {
            odf::metadata::MetadataEvent::SetTransform(v) => {
                SetTransform::with_extended_aliases(ctx, v).await?.into()
            }
            _ => event,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
