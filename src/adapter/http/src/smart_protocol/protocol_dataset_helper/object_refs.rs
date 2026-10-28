// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use thiserror::Error;

use crate::smart_protocol::messages::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CollectMissingObjectReferencesFromIntervalError {
    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        odf::dataset::InvalidIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<odf::IterBlocksError> for CollectMissingObjectReferencesFromIntervalError {
    fn from(v: odf::IterBlocksError) -> Self {
        match v {
            odf::IterBlocksError::InvalidInterval(e) => Self::InvalidInterval(e),
            odf::IterBlocksError::RefNotFound(_)
            | odf::IterBlocksError::BlockNotFound(_)
            | odf::IterBlocksError::BlockVersion(_)
            | odf::IterBlocksError::BlockMalformed(_)
            | odf::IterBlocksError::Access(_)
            | odf::IterBlocksError::Internal(_) => Self::Internal(v.int_err()),
        }
    }
}

pub async fn collect_object_references_from_interval(
    dataset: &dyn odf::Dataset,
    head: &odf::Multihash,
    tail: Option<&odf::Multihash>,
    ignore_missing_tail: bool,
    missing_files_only: bool,
) -> Result<Vec<ObjectFileReference>, CollectMissingObjectReferencesFromIntervalError> {
    let mut res_references: Vec<ObjectFileReference> = Vec::new();

    let mut block_stream = dataset.as_metadata_chain().iter_blocks_interval(
        head.into(),
        tail.map(Into::into),
        ignore_missing_tail,
    );
    while let Some((_, block)) = block_stream.try_next().await? {
        collect_object_references_from_block(
            dataset,
            &block,
            &mut res_references,
            missing_files_only,
        )
        .await?;
    }

    Ok(res_references)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn collect_object_references_from_metadata(
    dataset: &dyn odf::Dataset,
    blocks: &VecDeque<odf::dataset::HashedMetadataBlockBytesDecoded>,
    missing_files_only: bool,
) -> Result<Vec<ObjectFileReference>, InternalError> {
    let mut res_references: Vec<ObjectFileReference> = Vec::new();
    for (_, block, _) in blocks {
        collect_object_references_from_block(
            dataset,
            block,
            &mut res_references,
            missing_files_only,
        )
        .await?;
    }

    Ok(res_references)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn collect_object_references_from_block(
    dataset: &dyn odf::Dataset,
    block: &odf::MetadataBlock,
    target_references: &mut Vec<ObjectFileReference>,
    missing_files_only: bool,
) -> Result<(), InternalError> {
    let data_repo = dataset.as_data_repo();
    let checkpoint_repo = dataset.as_checkpoint_repo();

    match &block.event {
        odf::MetadataEvent::AddData(e) => {
            if let Some(new_data) = &e.new_data
                && (!missing_files_only
                    || !data_repo
                        .contains(&new_data.physical_hash)
                        .await
                        .int_err()?)
            {
                target_references.push(ObjectFileReference {
                    object_type: ObjectType::DataSlice,
                    physical_hash: new_data.physical_hash.clone(),
                    size: new_data.size,
                });
            }
            if let Some(new_checkpoint) = &e.new_checkpoint
                && (!missing_files_only
                    || !checkpoint_repo
                        .contains(&new_checkpoint.physical_hash)
                        .await
                        .int_err()?)
            {
                target_references.push(ObjectFileReference {
                    object_type: ObjectType::Checkpoint,
                    physical_hash: new_checkpoint.physical_hash.clone(),
                    size: new_checkpoint.size,
                });
            }
        }
        odf::MetadataEvent::ExecuteTransform(e) => {
            if let Some(new_data) = &e.new_data
                && (!missing_files_only
                    || !data_repo
                        .contains(&new_data.physical_hash)
                        .await
                        .int_err()?)
            {
                target_references.push(ObjectFileReference {
                    object_type: ObjectType::DataSlice,
                    physical_hash: new_data.physical_hash.clone(),
                    size: new_data.size,
                });
            }
            if let Some(new_checkpoint) = &e.new_checkpoint
                && (!missing_files_only
                    || !checkpoint_repo
                        .contains(&new_checkpoint.physical_hash)
                        .await
                        .int_err()?)
            {
                target_references.push(ObjectFileReference {
                    object_type: ObjectType::Checkpoint,
                    physical_hash: new_checkpoint.physical_hash.clone(),
                    size: new_checkpoint.size,
                });
            }
        }

        odf::MetadataEvent::Seed(_)
        | odf::MetadataEvent::SetDataSchema(_)
        | odf::MetadataEvent::SetPollingSource(_)
        | odf::MetadataEvent::DisablePollingSource(_)
        | odf::MetadataEvent::AddPushSource(_)
        | odf::MetadataEvent::DisablePushSource(_)
        | odf::MetadataEvent::SetTransform(_)
        | odf::MetadataEvent::SetVocab(_)
        | odf::MetadataEvent::SetAttachments(_)
        | odf::MetadataEvent::SetInfo(_)
        | odf::MetadataEvent::SetLicense(_) => (),
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
