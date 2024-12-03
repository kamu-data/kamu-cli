// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{DatasetID, MetadataBlock, MetadataEvent, MetadataEventTypeFlags, Multihash};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetKeyBlocksRepository: Send + Sync {
    async fn save_key_dataset_block(
        &self,
        dataset_id: &DatasetID,
        key_block_row: DatasetKeyBlockRow,
    ) -> Result<(), InternalError>;

    async fn drop_key_dataset_blocks_after(
        &self,
        dataset_id: &DatasetID,
        sequence_number: u64,
    ) -> Result<(), InternalError>;

    async fn try_loading_key_dataset_blocks(
        &self,
        dataset_id: &DatasetID,
        flags: MetadataEventTypeFlags,
    ) -> Result<Vec<DatasetKeyBlockRow>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetKeyBlockRow {
    pub block_hash: Multihash,
    pub block: MetadataBlock,
}

impl DatasetKeyBlockRow {
    pub fn key_block_type(&self) -> DatasetKeyBlockType {
        match &self.block.event {
            MetadataEvent::Seed(_) => DatasetKeyBlockType::Seed,
            MetadataEvent::SetPollingSource(_) => DatasetKeyBlockType::SetPollingSource,
            MetadataEvent::SetTransform(_) => DatasetKeyBlockType::SetTransform,
            MetadataEvent::SetVocab(_) => DatasetKeyBlockType::SetVocab,
            MetadataEvent::SetAttachments(_) => DatasetKeyBlockType::SetAttachments,
            MetadataEvent::SetInfo(_) => DatasetKeyBlockType::SetInfo,
            MetadataEvent::SetLicense(_) => DatasetKeyBlockType::SetLicense,
            MetadataEvent::SetDataSchema(_) => DatasetKeyBlockType::SetDataSchema,
            MetadataEvent::AddPushSource(_) => DatasetKeyBlockType::AddPushSource,

            // These ODF events are not supposed to be treated as key events
            MetadataEvent::AddData(_)
            | MetadataEvent::ExecuteTransform(_)
            | MetadataEvent::DisablePollingSource(_)
            | MetadataEvent::DisablePushSource(_) => unreachable!(),
        }
    }

    pub fn key_block_extra_key(&self) -> Option<String> {
        if let MetadataEvent::AddPushSource(push_source) = &self.block.event {
            Some(push_source.source_name.clone())
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum DatasetKeyBlockType {
    Seed,
    SetPollingSource,
    SetTransform,
    SetVocab,
    SetAttachments,
    SetInfo,
    SetLicense,
    SetDataSchema,
    AddPushSource,
}

impl DatasetKeyBlockType {
    pub fn vec_from_flags(flags: MetadataEventTypeFlags) -> Vec<DatasetKeyBlockType> {
        let mut res = Vec::new();

        if flags.contains(MetadataEventTypeFlags::SEED) {
            res.push(DatasetKeyBlockType::Seed);
        }
        if flags.contains(MetadataEventTypeFlags::SET_POLLING_SOURCE) {
            res.push(DatasetKeyBlockType::SetPollingSource);
        }
        if flags.contains(MetadataEventTypeFlags::SET_TRANSFORM) {
            res.push(DatasetKeyBlockType::SetTransform);
        }
        if flags.contains(MetadataEventTypeFlags::SET_VOCAB) {
            res.push(DatasetKeyBlockType::SetVocab);
        }
        if flags.contains(MetadataEventTypeFlags::SET_ATTACHMENTS) {
            res.push(DatasetKeyBlockType::SetAttachments);
        }
        if flags.contains(MetadataEventTypeFlags::SET_INFO) {
            res.push(DatasetKeyBlockType::SetInfo);
        }
        if flags.contains(MetadataEventTypeFlags::SET_LICENSE) {
            res.push(DatasetKeyBlockType::SetLicense);
        }
        if flags.contains(MetadataEventTypeFlags::SET_DATA_SCHEMA) {
            res.push(DatasetKeyBlockType::SetDataSchema);
        }
        if flags.contains(MetadataEventTypeFlags::ADD_PUSH_SOURCE) {
            res.push(DatasetKeyBlockType::AddPushSource);
        }

        res
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
