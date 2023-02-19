// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::derivations_generated::*;
use super::{ContentFreeManifest, Manifest};
use crate::dtos::*;
pub use crate::serde::*;
use ::serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////
// Wrapeprs
///////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
struct MetadataBlockWrapper(#[serde(with = "MetadataBlockDef")] MetadataBlock);

#[derive(Deserialize, Serialize)]
struct MetadataEventWrapper(#[serde(with = "MetadataEventDef")] MetadataEvent);

#[derive(Deserialize, Serialize)]
struct DatasetSnapshotWrapper(#[serde(with = "DatasetSnapshotDef")] DatasetSnapshot);

#[derive(Serialize, Deserialize)]
struct ExecuteQueryRequestWrapper(#[serde(with = "ExecuteQueryRequestDef")] ExecuteQueryRequest);

#[derive(Serialize, Deserialize)]
struct ExecuteQueryResponseWrapper(#[serde(with = "ExecuteQueryResponseDef")] ExecuteQueryResponse);

///////////////////////////////////////////////////////////////////////////////
// YamlMetadataBlockSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataBlockSerializer;

impl MetadataBlockSerializer for YamlMetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error> {
        let manifest = Manifest {
            version: METADATA_BLOCK_CURRENT_VERSION as i32,
            kind: "MetadataBlock".to_owned(),
            content: MetadataBlockWrapper(block.clone()),
        };

        let buf = serde_yaml::to_string(&manifest)
            .map_err(|e| Error::serde(e))?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlMetadataBlockDeserializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataBlockDeserializer;

impl MetadataBlockDeserializer for YamlMetadataBlockDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        // Read short manifest first, with kind and version only
        let manifest_no_content: ContentFreeManifest =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        assert_eq!(manifest_no_content.kind, "MetadataBlock");

        let version = MetadataBlockVersion::try_from(manifest_no_content.version)?;
        Self::check_version_compatibility(version)?;

        // TODO: Handle conversions for compatible versions

        // Re-read full manifest with content definition
        let manifest: Manifest<MetadataBlockWrapper> =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        Ok(manifest.content.0)
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlMetadataEventSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataEventSerializer;

impl YamlMetadataEventSerializer {
    pub fn write_manifest(&self, event: &MetadataEvent) -> Result<Buffer<u8>, Error> {
        let manifest = Manifest {
            version: 1,
            kind: "MetadataEvent".to_owned(),
            content: MetadataEventWrapper(event.clone()),
        };

        let buf = serde_yaml::to_string(&manifest)
            .map_err(|e| Error::serde(e))?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlMetadataEventDeserializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataEventDeserializer;

impl YamlMetadataEventDeserializer {
    pub fn read_manifest(&self, data: &[u8]) -> Result<MetadataEvent, Error> {
        let manifest: Manifest<MetadataEventWrapper> =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        // TODO: Handle conversions?
        assert_eq!(manifest.kind, "MetadataEvent");
        assert_eq!(manifest.version, 1);

        Ok(manifest.content.0)
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlDatasetSnapshotSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlDatasetSnapshotSerializer;

impl DatasetSnapshotSerializer for YamlDatasetSnapshotSerializer {
    fn write_manifest(&self, snapshot: &DatasetSnapshot) -> Result<Buffer<u8>, Error> {
        let manifest = Manifest {
            version: 1,
            kind: "DatasetSnapshot".to_owned(),
            content: DatasetSnapshotWrapper(snapshot.clone()),
        };

        let buf = serde_yaml::to_string(&manifest)
            .map_err(|e| Error::serde(e))?
            .into_bytes();
        Ok(Buffer::new(0, buf.len(), buf))
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlDatasetSnapshotDeserializer
///////////////////////////////////////////////////////////////////////////////

pub struct YamlDatasetSnapshotDeserializer;

impl DatasetSnapshotDeserializer for YamlDatasetSnapshotDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<DatasetSnapshot, Error> {
        let manifest: Manifest<DatasetSnapshotWrapper> =
            serde_yaml::from_slice(data).map_err(|e| Error::serde(e))?;

        // TODO: Handle conversions?
        assert_eq!(manifest.kind, "DatasetSnapshot");
        assert_eq!(manifest.version, 1);

        Ok(manifest.content.0)
    }
}

///////////////////////////////////////////////////////////////////////////////
// YamlEngineProtocol
///////////////////////////////////////////////////////////////////////////////

pub struct YamlEngineProtocol;

impl EngineProtocolSerializer for YamlEngineProtocol {
    fn write_execute_query_request(&self, inst: &ExecuteQueryRequest) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&ExecuteQueryRequestWrapper(inst.clone()))
            .map_err(|e| Error::serde(e))?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_execute_query_response(
        &self,
        inst: &ExecuteQueryResponse,
    ) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&ExecuteQueryResponseWrapper(inst.clone()))
            .map_err(|e| Error::serde(e))?
            .into_bytes();

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
