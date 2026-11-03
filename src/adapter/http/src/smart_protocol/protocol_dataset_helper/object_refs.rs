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
use internal_error::{InternalError, ResultIntoInternal};

use crate::smart_protocol::messages::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn collect_object_references_from_interval(
    dataset: &dyn odf::Dataset,
    head: &odf::Multihash,
    tail: Option<&odf::Multihash>,
    ignore_missing_tail: bool,
    missing_files_only: bool,
) -> Result<Vec<ObjectFileReference>, InternalError> {
    let mut res_references: Vec<ObjectFileReference> = Vec::new();

    let mut block_stream = dataset.as_metadata_chain().iter_blocks_interval(
        head.into(),
        tail.map(Into::into),
        ignore_missing_tail,
    );
    while let Some((_, block)) = block_stream.try_next().await.int_err()? {
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

pub(crate) async fn collect_object_references_from_metadata(
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
    let (new_data, new_checkpoint) = match &block.event {
        odf::MetadataEvent::AddData(e) => (e.new_data.as_ref(), e.new_checkpoint.as_ref()),
        odf::MetadataEvent::ExecuteTransform(e) => (e.new_data.as_ref(), e.new_checkpoint.as_ref()),
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
        | odf::MetadataEvent::SetLicense(_) => return Ok(()),
    };

    if let Some(new_data) = new_data
        && (!missing_files_only
            || !dataset
                .as_data_repo()
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
    if let Some(new_checkpoint) = new_checkpoint
        && (!missing_files_only
            || !dataset
                .as_checkpoint_repo()
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

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
