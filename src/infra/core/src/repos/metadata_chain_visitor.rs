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
use kamu_core::{AppendError, HashedMetadataBlockRef};
use opendatafabric::{MetadataBlock, Multihash};

///////////////////////////////////////////////////////////////////////////////

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Decision {
    Stop,
    NextWithHash(Multihash),
    NextOfType(MetadataBlockTypeFlags),
}

///////////////////////////////////////////////////////////////////////////////

pub trait MetadataChainVisitor: Sync + Send {
    fn visit(&mut self) -> Result<Decision, AppendError>;

    fn visit_with_block(
        &mut self,
        hashed_block: HashedMetadataBlockRef,
    ) -> Result<Decision, AppendError>;
}

///////////////////////////////////////////////////////////////////////////////

pub type BoxedVisitors<'a> = Vec<Box<dyn MetadataChainVisitor + 'a>>;

#[async_trait::async_trait]
pub trait MetadataChainVisitorHost {
    async fn accept<'a>(
        &'a self,
        append_block: &MetadataBlock,
        visitors: BoxedVisitors<'a>,
    ) -> Result<(), AppendError>;
}

///////////////////////////////////////////////////////////////////////////////
