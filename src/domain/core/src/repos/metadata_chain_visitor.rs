// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hash::Hash;

use bitflags::bitflags;
use opendatafabric::{MetadataBlock, MetadataEvent, Multihash};

use crate::HashedMetadataBlockRef;

///////////////////////////////////////////////////////////////////////////////

// TODO: Follow-up change: generate in the ODF land
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct MetadataBlockTypeFlags: u32 {
        const ADD_DATA = 1 << 0;
        const EXECUTE_TRANSFORM = 1 << 1;
        const SEED = 1 << 2;
        const SET_POLLING_SOURCE = 1 << 3;
        const SET_TRANSFORM = 1 << 4;
        const SET_VOCAB = 1 << 5;
        const SET_ATTACHMENTS = 1 << 6;
        const SET_INFO = 1 << 7;
        const SET_LICENSE = 1 << 8;
        const SET_DATA_SCHEMA = 1 << 9;
        const ADD_PUSH_SOURCE = 1 << 10;
        const DISABLE_PUSH_SOURCE = 1 << 11;
        const DISABLE_POLLING_SOURCE = 1 << 12;
        //
        const DATA_BLOCK = Self::ADD_DATA.bits() | Self::EXECUTE_TRANSFORM.bits();
    }
}

impl From<&MetadataBlock> for MetadataBlockTypeFlags {
    fn from(block: &MetadataBlock) -> Self {
        match block.event {
            MetadataEvent::AddData(_) => Self::ADD_DATA,
            MetadataEvent::ExecuteTransform(_) => Self::EXECUTE_TRANSFORM,
            MetadataEvent::Seed(_) => Self::SEED,
            MetadataEvent::SetPollingSource(_) => Self::SET_POLLING_SOURCE,
            MetadataEvent::SetTransform(_) => Self::SET_TRANSFORM,
            MetadataEvent::SetVocab(_) => Self::SET_VOCAB,
            MetadataEvent::SetAttachments(_) => Self::SET_ATTACHMENTS,
            MetadataEvent::SetInfo(_) => Self::SET_INFO,
            MetadataEvent::SetLicense(_) => Self::SET_LICENSE,
            MetadataEvent::SetDataSchema(_) => Self::SET_DATA_SCHEMA,
            MetadataEvent::AddPushSource(_) => Self::ADD_PUSH_SOURCE,
            MetadataEvent::DisablePushSource(_) => Self::DISABLE_PUSH_SOURCE,
            MetadataEvent::DisablePollingSource(_) => Self::DISABLE_POLLING_SOURCE,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataVisitorDecision {
    /// Stop Marker. A visitor who has reported the end of work will not be
    /// invited to visit again
    Stop,
    /// A request for the previous block
    Next,
    /// A request for a specific previous block, specifying its hash.
    /// Reserved for long jumps through the metadata chain
    NextWithHash(Multihash),
    /// A request for previous blocks, of specific types, using flags.
    ///
    /// # Examples
    /// ```
    /// // Request for SetVocab block
    /// return MetadataVisitorDecision::NextOfType(MetadataBlockTypeFlags::SET_VOCAB);
    ///
    /// // Request for data blocks (ADD_DATA || EXECUTE_TRANSFORM)
    /// return MetadataVisitorDecision::NextOfType(MetadataBlockTypeFlags::DATA_BLOCK);
    ///
    /// // Request for a list of blocks of different types
    /// return MetadataVisitorDecision::NextOfType(MetadataBlockTypeFlags::SET_ATTACHMENTS | MetadataBlockTypeFlags::SET_LICENSE);
    /// ```
    NextOfType(MetadataBlockTypeFlags),
}

///////////////////////////////////////////////////////////////////////////////

pub trait MetadataChainVisitor: Sync + Send {
    type Error: std::error::Error;

    fn visit(
        &mut self,
        hashed_block_ref: HashedMetadataBlockRef,
    ) -> Result<MetadataVisitorDecision, Self::Error>;
}

///////////////////////////////////////////////////////////////////////////////
