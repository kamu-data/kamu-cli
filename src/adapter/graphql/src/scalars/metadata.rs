// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use kamu_datasets::DatasetRegistry;

use crate::prelude::*;
use crate::queries::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlockExtended
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Metadata block with its hash and author
#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct MetadataBlockExtended {
    #[graphql(skip)]
    pub block: odf::metadata::MetadataBlock,
    /// Hash of this block
    pub block_hash: Multihash<'static>,
    /// Hash of the preceding block, null for the first block
    pub prev_block_hash: Option<Multihash<'static>>,
    /// System time when this block was created
    pub system_time: DateTime<Utc>,
    /// Author of the block, reported as the dataset owner
    pub author: Account,
    /// Metadata event recorded in this block
    pub event: MetadataEvent,
    /// Position of this block in the chain, starting from zero
    pub sequence_number: u64,
}

#[ComplexObject]
impl MetadataBlockExtended {
    #[graphql(skip)]
    pub async fn new(
        ctx: &Context<'_>,
        block_hash: impl Into<Multihash<'static>>,
        block: odf::metadata::MetadataBlock,
        author: Account,
    ) -> Result<Self, InternalError> {
        let odf_block = MetadataBlock::with_extended_aliases(ctx, block).await?;
        let original_block = odf_block.clone();
        let block: MetadataBlock = odf_block.into();

        Ok(Self {
            block_hash: block_hash.into(),
            prev_block_hash: block.prev_block_hash,
            system_time: block.system_time,
            author,
            event: block.event,
            sequence_number: block.sequence_number.into(),
            block: original_block,
        })
    }

    /// Block serialized in the requested format
    #[expect(
        clippy::unused_async,
        reason = "async-graphql resolvers are async by framework contract"
    )]
    async fn encoded(
        &self,
        encoding: MetadataManifestFormat,
    ) -> Result<Option<EncodedBlock>, InternalError> {
        Ok(Some(EncodedBlock::new(encoding, &self.block)?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// EncodedBlock
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Metadata block serialized in a manifest format
#[derive(Debug, SimpleObject)]
pub struct EncodedBlock {
    /// Format the block is serialized in
    pub encoding: MetadataManifestFormat,
    /// Serialized block
    pub content: String,
}

impl EncodedBlock {
    pub fn new(
        encoding: MetadataManifestFormat,
        block: &odf::metadata::MetadataBlock,
    ) -> Result<Self, InternalError> {
        let content = Self::encode_block(block, encoding)?;
        Ok(Self { encoding, content })
    }

    pub fn encode_block(
        block: &odf::metadata::MetadataBlock,
        format: MetadataManifestFormat,
    ) -> Result<String, InternalError> {
        use odf::metadata::serde::MetadataBlockSerializer;

        match format {
            MetadataManifestFormat::Yaml => {
                let ser = odf::metadata::serde::yaml::YamlMetadataBlockSerializer;

                let buffer = ser.write_manifest(block).int_err()?;
                let content = std::str::from_utf8(&buffer).int_err()?;
                Ok(content.to_string())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataFormat
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Serialization format of metadata manifests
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataManifestFormat {
    /// YAML manifest
    Yaml,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MetadataFormat serde errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Manifest could not be parsed
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct MetadataManifestMalformed {
    /// Description of the problem
    pub message: String,
}

#[ComplexObject]
impl MetadataManifestMalformed {
    /// Always false
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

/// Manifest version is not supported by this node
#[Object]
impl MetadataManifestUnsupportedVersion {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }

    /// Description of the problem with the supported version range
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

impl SetTransform {
    pub async fn with_extended_aliases(
        ctx: &Context<'_>,
        v: odf::metadata::SetTransform,
    ) -> Result<odf::metadata::SetTransform, InternalError> {
        let input_ids = v
            .inputs
            .iter()
            .map(|input| Cow::Borrowed(input.dataset_ref.id().unwrap()))
            .collect::<Vec<_>>();
        let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);
        let dataset_infos = dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&input_ids)
            .await
            .int_err()?;

        let mut accessible_inputs: Vec<_> = dataset_infos
            .resolved_handles
            .iter()
            .map(|dataset_handle| odf::metadata::TransformInput {
                dataset_ref: dataset_handle.id.as_local_ref(),
                alias: Some(dataset_handle.alias.clone().to_string()),
            })
            .collect();

        accessible_inputs.extend(dataset_infos.unresolved_datasets.into_iter().map(
            |(dataset_id, _)| {
                let original_input_info = v
                    .inputs
                    .iter()
                    .find(|input| {
                        input.dataset_ref.id().unwrap().as_local_ref() == dataset_id.as_local_ref()
                    })
                    .unwrap();
                odf::metadata::TransformInput {
                    dataset_ref: original_input_info.dataset_ref.clone(),
                    alias: original_input_info.alias.clone(),
                }
            },
        ));
        Ok(odf::metadata::SetTransform {
            inputs: accessible_inputs,
            transform: v.transform,
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
        Ok(
            if let odf::metadata::MetadataEvent::SetTransform(v) = event {
                SetTransform::with_extended_aliases(ctx, v).await?.into()
            } else {
                event
            },
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Type of a metadata event
#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
pub enum MetadataEventType {
    /// Event that establishes the identity of a dataset
    Seed,
    /// Event that defines a source polled for new data
    SetPollingSource,
    /// Event that defines column names with special meaning
    SetVocab,
    /// Event that sets attached files
    SetAttachments,
    /// Event that sets the description, keywords and similar info
    SetInfo,
    /// Event that sets the license
    SetLicense,
    /// Event that sets the schema of the data
    SetDataSchema,
    /// Event that defines how a derivative dataset is computed
    SetTransform,
    /// Event that adds a source data can be pushed into
    AddPushSource,
}
