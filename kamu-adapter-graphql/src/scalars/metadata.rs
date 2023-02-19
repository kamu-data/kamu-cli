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
use opendatafabric as odf;

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

/////////////////////////////////////////////////////////////////////////////////////////
// MetadataFormat
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataManifestFormat {
    Yaml,
}

///////////////////////////////////////////////////////////////////////////////
// MetadataFormat serde errors
///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct MetadataManifestMalformed {
    pub message: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct MetadataManifestUnsupportedVersion {
    pub manifest_version: i32,
    pub supported_version_from: i32,
    pub supported_version_to: i32,
}

#[Object]
impl MetadataManifestUnsupportedVersion {
    pub async fn message(&self) -> String {
        format!(
            "Unsupported manifest version {}, supported range is [{}, {}]",
            self.manifest_version, self.supported_version_from, self.supported_version_to
        )
    }
}

impl From<odf::serde::UnsupportedVersionError> for MetadataManifestUnsupportedVersion {
    fn from(e: odf::serde::UnsupportedVersionError) -> Self {
        Self {
            manifest_version: e.manifest_version,
            supported_version_from: e.supported_version_range.0 as i32,
            supported_version_to: e.supported_version_range.1 as i32,
        }
    }
}
