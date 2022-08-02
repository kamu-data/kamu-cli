// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::Account;
use crate::scalars::*;

use async_graphql::*;
use chrono::{DateTime, Utc};

/////////////////////////////////////////////////////////////////////////////////////////
// MetadataBlockExtended
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct MetadataBlockExtended {
    pub block_hash: Multihash,
    pub prev_block_hash: Option<Multihash>,
    pub system_time: DateTime<Utc>,
    pub author: Account,
    pub event: MetadataEvent,
    pub sequence_number: i32,
}

impl MetadataBlockExtended {
    pub fn new<H: Into<Multihash>, B: Into<MetadataBlock>>(
        block_hash: H,
        block: B,
        author: Account,
    ) -> Self {
        let b = block.into();
        Self {
            block_hash: block_hash.into(),
            prev_block_hash: b.prev_block_hash,
            system_time: b.system_time,
            author,
            event: b.event,
            sequence_number: b.sequence_number,
        }
    }
}
