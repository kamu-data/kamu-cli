// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use kamu_core::{self as domain, MetadataChainExt};
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::Account;

////////////////////////////////////////////////////////////////////////////////////////
// MetadataRef
////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct BlockRef {
    name: String,
    block_hash: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////
// MetadataChain
////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChain {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl MetadataChain {
    const DEFAULT_BLOCKS_PER_PAGE: usize = 20;

    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = dataset_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;
        Ok(dataset)
    }

    /// Returns all named metadata block references
    #[tracing::instrument(level = "info", skip_all)]
    async fn refs(&self, ctx: &Context<'_>) -> Result<Vec<BlockRef>> {
        let dataset = self.get_dataset(ctx).await?;
        Ok(vec![BlockRef {
            name: "head".to_owned(),
            block_hash: dataset
                .as_metadata_chain()
                .get_ref(&domain::BlockRef::Head)
                .await
                .int_err()?
                .into(),
        }])
    }

    /// Returns a metadata block corresponding to the specified hash
    #[tracing::instrument(level = "info", skip_all)]
    async fn block_by_hash(
        &self,
        ctx: &Context<'_>,
        hash: Multihash,
    ) -> Result<Option<MetadataBlockExtended>> {
        let dataset = self.get_dataset(ctx).await?;
        let block = dataset.as_metadata_chain().try_get_block(&hash).await?;
        Ok(block.map(|b| MetadataBlockExtended::new(hash, b, Account::mock())))
    }

    /// Returns a metadata block corresponding to the specified hash and encoded
    /// in desired format
    #[tracing::instrument(level = "info", skip_all)]
    async fn block_by_hash_encoded(
        &self,
        ctx: &Context<'_>,
        hash: Multihash,
        format: MetadataManifestFormat,
    ) -> Result<Option<String>> {
        use odf::serde::MetadataBlockSerializer;

        let dataset = self.get_dataset(ctx).await?;
        match dataset.as_metadata_chain().try_get_block(&hash).await? {
            None => Ok(None),
            Some(block) => match format {
                MetadataManifestFormat::Yaml => {
                    let ser = odf::serde::yaml::YamlMetadataBlockSerializer;
                    let buffer = ser.write_manifest(&block).int_err()?;
                    let content = std::str::from_utf8(&buffer).int_err()?;
                    Ok(Some(content.to_string()))
                }
            },
        }
    }

    // TODO: Add ref parameter (defaulting to "head")
    // TODO: Support before/after style iteration
    // TODO: PERF: Avoid traversing entire chain
    /// Iterates all metadata blocks in the reverse chronological order
    #[tracing::instrument(level = "info", skip_all)]
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MetadataBlockConnection> {
        let dataset = self.get_dataset(ctx).await?;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_BLOCKS_PER_PAGE);

        let blocks: Vec<_> = dataset
            .as_metadata_chain()
            .iter_blocks()
            .try_collect()
            .await
            .int_err()?;

        let total_count = blocks.len();

        let nodes: Vec<_> = blocks
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|(hash, block)| MetadataBlockExtended::new(hash, block, Account::mock()))
            .collect();

        Ok(MetadataBlockConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }

    /// Commits new event to the metadata chain
    #[tracing::instrument(level = "info", skip_all)]
    async fn commit_event(
        &self,
        ctx: &Context<'_>,
        event: String,
        event_format: MetadataManifestFormat,
    ) -> Result<CommitResult> {
        let event = match event_format {
            MetadataManifestFormat::Yaml => {
                let de = odf::serde::yaml::YamlMetadataEventDeserializer;
                match de.read_manifest(event.as_bytes()) {
                    Ok(event) => event,
                    Err(e @ odf::serde::Error::SerdeError { .. }) => {
                        return Ok(CommitResult::Malformed(MetadataManifestMalformed {
                            message: e.to_string(),
                        }))
                    }
                    Err(odf::serde::Error::UnsupportedVersion(e)) => {
                        return Ok(CommitResult::UnsupportedVersion(e.into()))
                    }
                    Err(e @ odf::serde::Error::IoError { .. }) => return Err(e.int_err().into()),
                }
            }
        };

        let dataset = self.get_dataset(ctx).await?;

        let result = match dataset
            .commit_event(event, domain::CommitOpts::default())
            .await
        {
            Ok(result) => CommitResult::Success(CommitResultSuccess {
                old_head: result.old_head.map(Into::into),
                new_head: result.new_head.into(),
            }),
            Err(domain::CommitError::ObjectNotFound(e)) => {
                CommitResult::AppendError(CommitResultAppendError {
                    message: format!("Event is referencing a non-existent object {}", e.hash),
                })
            }
            Err(domain::CommitError::MetadataAppendError(e)) => {
                CommitResult::AppendError(CommitResultAppendError {
                    message: e.to_string(),
                })
            }
            Err(e @ domain::CommitError::Internal(_)) => return Err(e.int_err().into()),
        };

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CommitResult {
    Success(CommitResultSuccess),
    Malformed(MetadataManifestMalformed),
    UnsupportedVersion(MetadataManifestUnsupportedVersion),
    AppendError(CommitResultAppendError),
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct CommitResultSuccess {
    pub old_head: Option<Multihash>,
    pub new_head: Multihash,
}

#[ComplexObject]
impl CommitResultSuccess {
    async fn message(&self) -> String {
        format!("Success")
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct CommitResultAppendError {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    MetadataBlockExtended,
    MetadataBlockConnection,
    MetadataBlockEdge
);
