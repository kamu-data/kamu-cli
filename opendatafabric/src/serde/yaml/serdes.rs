// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::derivations_generated::*;
pub use crate::serde::{
    Buffer, DatasetSnapshotDeserializer, DatasetSnapshotSerializer, Error,
    MetadataBlockDeserializer, MetadataBlockSerializer,
};
use crate::{
    serde::{EngineProtocolDeserializer, EngineProtocolSerializer},
    ExecuteQueryRequest, ExecuteQueryResponse, Multicodec,
};
use crate::{DatasetSnapshot, MetadataBlock};
use ::serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////
// ManifestDef
///////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ManifestDefMetadataBlock {
    pub kind: Multicodec,
    pub version: i32,
    #[serde(with = "MetadataBlockDef")]
    pub content: MetadataBlock,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ManifestDefDatasetSnapshot {
    pub kind: Multicodec,
    pub version: i32,
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
    fn write_manifest(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error> {
        let manifest = ManifestDefMetadataBlock {
            version: 1,
            kind: Multicodec::ODFMetadataBlock,
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
    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        let manifest: ManifestDefMetadataBlock =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        // TODO: Handle conversions?
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.kind, Multicodec::ODFMetadataBlock);

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
            version: 1,
            kind: Multicodec::ODFDatasetSnapshot,
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
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.kind, Multicodec::ODFDatasetSnapshot);

        Ok(manifest.content)
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlEngineProtocol
///////////////////////////////////////////////////////////////////////////////

pub struct YamlEngineProtocol;

#[derive(Serialize, Deserialize)]
struct ExecuteQueryRequestWrapper(#[serde(with = "ExecuteQueryRequestDef")] ExecuteQueryRequest);

#[derive(Serialize, Deserialize)]
struct ExecuteQueryResponseWrapper(#[serde(with = "ExecuteQueryResponseDef")] ExecuteQueryResponse);

impl EngineProtocolSerializer for YamlEngineProtocol {
    fn write_execute_query_request(&self, inst: &ExecuteQueryRequest) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_vec(&ExecuteQueryRequestWrapper(inst.clone()))
            .map_err(|e| Error::serde(e))?;

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_execute_query_response(
        &self,
        inst: &ExecuteQueryResponse,
    ) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_vec(&ExecuteQueryResponseWrapper(inst.clone()))
            .map_err(|e| Error::serde(e))?;

        Ok(Buffer::new(0, buf.len(), buf))
    }
}

impl EngineProtocolDeserializer for YamlEngineProtocol {
    fn read_execute_query_request(&self, data: &[u8]) -> Result<ExecuteQueryRequest, Error> {
        let inst: ExecuteQueryRequestWrapper =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        Ok(inst.0)
    }

    fn read_execute_query_response(&self, data: &[u8]) -> Result<ExecuteQueryResponse, Error> {
        let inst: ExecuteQueryResponseWrapper =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        Ok(inst.0)
    }
}
