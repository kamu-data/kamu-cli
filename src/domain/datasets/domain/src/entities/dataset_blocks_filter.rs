// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{BlockHashDigestRange, BlockHashPrefix};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Selects metadata blocks of a dataset. Conditions combine with AND.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatasetBlocksFilter {
    /// Only blocks whose event type is in this set; `None` matches all types
    pub event_types: Option<odf::MetadataEventTypeFlags>,

    /// Only blocks whose hash starts with this prefix
    pub block_hash_prefix: Option<BlockHashPrefix>,
}

impl DatasetBlocksFilter {
    pub fn matches(&self, block_hash: &odf::Multihash, event_type: odf::MetadataEventType) -> bool {
        if let Some(event_types) = self.event_types
            && !event_types.contains(event_type.into())
        {
            return false;
        }

        if let Some(prefix) = &self.block_hash_prefix
            && !prefix.matches(block_hash)
        {
            return false;
        }

        true
    }

    /// Digests the hash prefix condition selects
    pub fn block_hash_digest_range(&self) -> BlockHashDigestRange {
        self.block_hash_prefix
            .as_ref()
            .map_or(BlockHashDigestRange::Any, BlockHashPrefix::digest_range)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
