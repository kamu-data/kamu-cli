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
use kamu_core::DatasetRegistry;
use odf::dataset::MetadataChainVisitor;

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
    pub encoded: Option<String>,
}

impl MetadataBlockExtended {
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

    pub async fn new(
        ctx: &Context<'_>,
        block_hash: impl Into<Multihash<'static>>,
        block: odf::metadata::MetadataBlock,
        author: Account,
        encode_format_maybe: Option<MetadataManifestFormat>,
    ) -> Result<Self, InternalError> {
        let odf_block = MetadataBlock::with_extended_aliases(ctx, block).await?;

        let encoded = if let Some(format) = encode_format_maybe {
            Some(Self::encode_block(&odf_block, format)?)
        } else {
            None
        };

        let block: MetadataBlock = odf_block.into();

        Ok(Self {
            block_hash: block_hash.into(),
            prev_block_hash: block.prev_block_hash,
            system_time: block.system_time,
            author,
            event: block.event,
            sequence_number: block.sequence_number,
            encoded,
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
        Ok(match event {
            odf::metadata::MetadataEvent::SetTransform(v) => {
                SetTransform::with_extended_aliases(ctx, v).await?.into()
            }
            _ => event,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
pub enum MetadataEventType {
    Seed,
    SetPollingSource,
    SetVocab,
    SetAttachments,
    SetInfo,
    SetLicense,
    SetDataSchema,
    SetTransform,
    AddPushSource,
}

pub enum MetadataVisitor {
    SetPollingSource(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetPollingSource>),
    SetAttachments(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetAttachments>),
    SetInfo(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetInfo>),
    SetLicense(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetLicense>),
    SetDataSchema(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetDataSchema>),
    SetVocab(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetVocab>),
    Seed(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::Seed>),
    SetTransform(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetTransform>),
    AddPushSources(odf::dataset::SearchAddPushSourcesVisitor),
}

impl MetadataVisitor {
    pub fn as_visitor(
        &mut self,
    ) -> &mut dyn MetadataChainVisitor<Error = odf::dataset::Infallible> {
        match self {
            Self::SetAttachments(v) => v,
            Self::SetInfo(v) => v,
            Self::SetLicense(v) => v,
            Self::SetDataSchema(v) => v,
            Self::Seed(v) => v,
            Self::SetVocab(v) => v,
            Self::SetPollingSource(v) => v,
            Self::SetTransform(v) => v,
            Self::AddPushSources(v) => v,
        }
    }

    pub fn into_boxed_extractor(self) -> Box<dyn odf::dataset::ExtractBlock> {
        match self {
            Self::SetAttachments(v) => Box::new(v),
            Self::SetInfo(v) => Box::new(v),
            Self::SetLicense(v) => Box::new(v),
            Self::SetDataSchema(v) => Box::new(v),
            Self::Seed(v) => Box::new(v),
            Self::SetVocab(v) => Box::new(v),
            Self::SetPollingSource(v) => Box::new(v),
            Self::SetTransform(v) => Box::new(v),
            Self::AddPushSources(v) => Box::new(v),
        }
    }
}

impl From<MetadataEventType> for MetadataVisitor {
    fn from(event_type: MetadataEventType) -> Self {
        match event_type {
            MetadataEventType::SetPollingSource => {
                MetadataVisitor::SetPollingSource(odf::dataset::SearchSetPollingSourceVisitor::new())
            }
            MetadataEventType::SetTransform => {
                MetadataVisitor::SetTransform(odf::dataset::SearchSetTransformVisitor::new())
            }
            MetadataEventType::SetAttachments => {
                MetadataVisitor::SetAttachments(odf::dataset::SearchSetAttachmentsVisitor::new())
            }
            MetadataEventType::SetInfo => {
                MetadataVisitor::SetInfo(odf::dataset::SearchSetInfoVisitor::new())
            }
            MetadataEventType::SetLicense => {
                MetadataVisitor::SetLicense(odf::dataset::SearchSetLicenseVisitor::new())
            }
            MetadataEventType::SetDataSchema => {
                MetadataVisitor::SetDataSchema(odf::dataset::SearchSetDataSchemaVisitor::new())
            }
            MetadataEventType::Seed => {
                MetadataVisitor::Seed(odf::dataset::SearchSeedVisitor::new())
            }
            MetadataEventType::SetVocab => {
                MetadataVisitor::SetVocab(odf::dataset::SearchSetVocabVisitor::new())
            }
            MetadataEventType::AddPushSource => {
                MetadataVisitor::AddPushSources(odf::dataset::SearchAddPushSourcesVisitor::new())
            }
        }
    }
}
