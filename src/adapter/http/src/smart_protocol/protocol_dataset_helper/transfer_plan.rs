// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use thiserror::Error;

use crate::smart_protocol::messages::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PrepareDatasetTransferEstimateError {
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

impl From<odf::IterBlocksError> for PrepareDatasetTransferEstimateError {
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

pub async fn prepare_dataset_transfer_plan(
    metadata_chain: &dyn odf::MetadataChain,
    stop_at: &odf::Multihash,
    begin_after: Option<&odf::Multihash>,
    ignore_missing_tail: bool,
) -> Result<TransferPlan, PrepareDatasetTransferEstimateError> {
    let mut block_stream = metadata_chain.iter_blocks_interval(
        stop_at.into(),
        begin_after.map(Into::into),
        ignore_missing_tail,
    );

    let mut blocks_count: u32 = 0;
    let mut bytes_in_blocks: u64 = 0;

    let mut data_objects_count: u32 = 0;
    let mut checkpoint_objects_count: u32 = 0;
    let mut bytes_in_data_objects: u64 = 0;
    let mut bytes_in_checkpoint_objects: u64 = 0;

    let mut data_records_count: u64 = 0;

    while let Some((hash, block)) = block_stream.try_next().await? {
        blocks_count += 1;

        bytes_in_blocks += metadata_chain.get_block_size(&hash).await.int_err()?;

        match block.event {
            odf::MetadataEvent::AddData(add_data) => {
                if let Some(new_data) = &add_data.new_data {
                    data_objects_count += 1;
                    bytes_in_data_objects += new_data.size;
                    data_records_count += new_data.num_records();
                }
                if let Some(new_checkpoint) = &add_data.new_checkpoint {
                    checkpoint_objects_count += 1;
                    bytes_in_checkpoint_objects += new_checkpoint.size;
                }
            }
            odf::MetadataEvent::ExecuteTransform(execute_transform) => {
                if let Some(new_data) = &execute_transform.new_data {
                    data_objects_count += 1;
                    bytes_in_data_objects += new_data.size;
                    data_records_count += new_data.num_records();
                }
                if let Some(new_checkpoint) = &execute_transform.new_checkpoint {
                    checkpoint_objects_count += 1;
                    bytes_in_checkpoint_objects += new_checkpoint.size;
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
    }

    Ok(TransferPlan {
        num_blocks: blocks_count,
        num_objects: data_objects_count + checkpoint_objects_count,
        num_records: data_records_count,
        bytes_in_raw_blocks: bytes_in_blocks,
        bytes_in_raw_objects: bytes_in_data_objects + bytes_in_checkpoint_objects,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
