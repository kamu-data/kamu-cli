// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::generated::*;
use crate::serde::flatbuffers::{
    FlatbuffersMetadataBlockDeserializer, FlatbuffersMetadataBlockSerializer,
};
pub use crate::serde::{
    Buffer, DatasetSnapshotDeserializer, DatasetSnapshotSerializer, Error,
    MetadataBlockDeserializer, MetadataBlockSerializer,
};
use crate::{DatasetSnapshot, MetadataBlock, Sha3_256};
use ::serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////
// ManifestDef
///////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ManifestDefMetadataBlock {
    pub api_version: i32,
    pub kind: String,
    #[serde(with = "MetadataBlockDef")]
    pub content: MetadataBlock,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ManifestDefDatasetSnapshot {
    pub api_version: i32,
    pub kind: String,
    #[serde(with = "DatasetSnapshotDef")]
    pub content: DatasetSnapshot,
}

///////////////////////////////////////////////////////////////////////////////
// YamlMetadataBlockSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataBlockSerializer;

///////////////////////////////////////////////////////////////////////////////

impl YamlMetadataBlockSerializer {}

///////////////////////////////////////////////////////////////////////////////

impl MetadataBlockSerializer for YamlMetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<(Sha3_256, Buffer<u8>), Error> {
        let (block_hash, _) = FlatbuffersMetadataBlockSerializer.write_manifest(block)?;
        let new_block = MetadataBlock {
            block_hash: block_hash,
            ..block.clone()
        };

        let manifest = ManifestDefMetadataBlock {
            api_version: 1,
            kind: "MetadataBlock".to_owned(),
            content: new_block,
        };

        let buf = serde_yaml::to_vec(&manifest).map_err(|e| Error::serde(e))?;
        Ok((block_hash, Buffer::new(0, buf.len(), buf)))
    }

    fn write_manifest_unchecked(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error> {
        let manifest = ManifestDefMetadataBlock {
            api_version: 1,
            kind: "MetadataBlock".to_owned(),
            content: block.clone(),
        };

        let buf = serde_yaml::to_vec(&manifest).map_err(|e| Error::serde(e))?;
        Ok(Buffer::new(0, buf.len(), buf))
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlMetadataBlockDeserializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataBlockDeserializer;

///////////////////////////////////////////////////////////////////////////////

impl YamlMetadataBlockDeserializer {}

///////////////////////////////////////////////////////////////////////////////

impl MetadataBlockDeserializer for YamlMetadataBlockDeserializer {
    fn validate_manifest(&self, data: &[u8]) -> Result<(), Error> {
        self.read_manifest(data)?;
        Ok(())
    }

    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        // TODO: Will not work if conversion took place
        let block = self.read_manifest_unchecked(data)?;
        let fbdata = FlatbuffersMetadataBlockSerializer.write_manifest_unchecked(&block)?;
        FlatbuffersMetadataBlockDeserializer.validate_manifest(&fbdata)?;
        Ok(block)
    }

    fn read_manifest_unchecked(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        let manifest: ManifestDefMetadataBlock =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        // TODO: Handle conversions?
        assert_eq!(manifest.api_version, 1);
        assert_eq!(manifest.kind, "MetadataBlock");

        Ok(manifest.content)
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlDatasetSnapshotSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlDatasetSnapshotSerializer;

impl DatasetSnapshotSerializer for YamlDatasetSnapshotSerializer {
    fn write_manifest(&self, snapshot: &DatasetSnapshot) -> Result<Buffer<u8>, Error> {
        let manifest = ManifestDefDatasetSnapshot {
            api_version: 1,
            kind: "DatasetSnapshot".to_owned(),
            content: snapshot.clone(),
        };

        let buf = serde_yaml::to_vec(&manifest).map_err(|e| Error::serde(e))?;
        Ok(Buffer::new(0, buf.len(), buf))
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlDatasetSnapshotDeserializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlDatasetSnapshotDeserializer;

impl DatasetSnapshotDeserializer for YamlDatasetSnapshotDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<DatasetSnapshot, Error> {
        let manifest: ManifestDefDatasetSnapshot =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        // TODO: Handle conversions?
        assert_eq!(manifest.api_version, 1);
        assert_eq!(manifest.kind, "DatasetSnapshot");

        Ok(manifest.content)
    }
}
