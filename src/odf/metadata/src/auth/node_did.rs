// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use multiformats::{DeserializeError, DidKey, DidKeyBytes};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unique identifier of the node
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct NodeDid(DidKey);

impl NodeDid {
    pub fn as_bytes(&self) -> DidKeyBytes {
        self.0.as_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError<DidKey>> {
        Ok(Self(DidKey::from_bytes(bytes).map_err(|e| {
            DeserializeError::new_from(e.source.unwrap())
        })?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
