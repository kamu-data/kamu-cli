// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::MetadataEventType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetKeyBlock {
    pub event_kind: MetadataEventType,
    pub sequence_number: u64,
    pub block_hash: odf::Multihash,
    pub block_payload: bytes::Bytes,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
