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
use kamu_core::AppendError;
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
    Next,
    NextWithHash(Multihash),
    NextOfType(MetadataBlockTypeFlags),
}

///////////////////////////////////////////////////////////////////////////////

pub type MetadataBlockWithOptionalHashRef<'a> = (&'a MetadataBlock, Option<&'a Multihash>);

pub trait MetadataChainVisitor: Sync + Send {
    fn visit(
        &mut self,
        block_with_optional_hash: MetadataBlockWithOptionalHashRef,
    ) -> Result<Decision, AppendError>;
}

///////////////////////////////////////////////////////////////////////////////

pub type BoxedVisitors = Vec<Box<dyn MetadataChainVisitor>>;

#[async_trait::async_trait]
pub trait MetadataChainVisitorHost {
    async fn accept(
        &self,
        append_block: &MetadataBlock,
        visitors: BoxedVisitors,
    ) -> Result<(), AppendError>;
}

///////////////////////////////////////////////////////////////////////////////
