// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ::serde::{Deserialize, Serialize};

use super::derivations_generated::*;
use super::{ContentFreeManifest, Manifest};
use crate::dtos::*;
pub use crate::serde::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Wrappers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
struct MetadataBlockWrapper(#[serde(with = "MetadataBlockDef")] MetadataBlock);

#[derive(Deserialize, Serialize)]
struct MetadataEventWrapper(#[serde(with = "MetadataEventDef")] MetadataEvent);

#[derive(Deserialize, Serialize)]
struct DatasetSnapshotWrapper(#[serde(with = "DatasetSnapshotDef")] DatasetSnapshot);

#[derive(Serialize, Deserialize)]
struct RawQueryRequestWrapper(#[serde(with = "RawQueryRequestDef")] RawQueryRequest);

#[derive(Serialize, Deserialize)]
struct RawQueryResponseWrapper(#[serde(with = "RawQueryResponseDef")] RawQueryResponse);

#[derive(Serialize, Deserialize)]
struct TransformRequestWrapper(#[serde(with = "TransformRequestDef")] TransformRequest);

#[derive(Serialize, Deserialize)]
struct TransformResponseWrapper(#[serde(with = "TransformResponseDef")] TransformResponse);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlMetadataBlockSerializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataBlockSerializer;

impl MetadataBlockSerializer for YamlMetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error> {
        let manifest = Manifest {
            version: METADATA_BLOCK_CURRENT_VERSION as i32,
            kind: "MetadataBlock".to_owned(),
            content: MetadataBlockWrapper(block.clone()),
        };

        let buf = serde_yaml::to_string(&manifest)
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlMetadataBlockDeserializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataBlockDeserializer;

impl MetadataBlockDeserializer for YamlMetadataBlockDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        // Read short manifest first, with kind and version only
        let manifest_no_content: ContentFreeManifest =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        assert_eq!(manifest_no_content.kind, "MetadataBlock");

        let version = MetadataBlockVersion::try_from(manifest_no_content.version)?;
        Self::check_version_compatibility(version)?;

        // TODO: Handle conversions for compatible versions

        // Re-read full manifest with content definition
        let manifest: Manifest<MetadataBlockWrapper> =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        Ok(manifest.content.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlMetadataEventSerializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataEventSerializer;

impl YamlMetadataEventSerializer {
    pub fn write_manifest_str(&self, event: &MetadataEvent) -> Result<String, Error> {
        let manifest = Manifest {
            version: 1,
            kind: "MetadataEvent".to_owned(),
            content: MetadataEventWrapper(event.clone()),
        };

        serde_yaml::to_string(&manifest).map_err(Error::serde)
    }

    pub fn write_manifest(&self, event: &MetadataEvent) -> Result<Buffer<u8>, Error> {
        let buf = self.write_manifest_str(event)?.into_bytes();
        Ok(Buffer::new(0, buf.len(), buf))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlMetadataEventDeserializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataEventDeserializer;

impl YamlMetadataEventDeserializer {
    pub fn read_manifest(&self, data: &[u8]) -> Result<MetadataEvent, Error> {
        let manifest: Manifest<MetadataEventWrapper> =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        // TODO: Handle conversions?
        assert_eq!(manifest.kind, "MetadataEvent");
        assert_eq!(manifest.version, 1);

        Ok(manifest.content.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlDatasetSnapshotSerializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlDatasetSnapshotSerializer;

impl YamlDatasetSnapshotSerializer {
    pub fn write_manifest_str(&self, snapshot: &DatasetSnapshot) -> Result<String, Error> {
        let manifest = Manifest {
            version: 1,
            kind: "DatasetSnapshot".to_owned(),
            content: DatasetSnapshotWrapper(snapshot.clone()),
        };

        serde_yaml::to_string(&manifest).map_err(Error::serde)
    }
}

impl DatasetSnapshotSerializer for YamlDatasetSnapshotSerializer {
    fn write_manifest(&self, snapshot: &DatasetSnapshot) -> Result<Buffer<u8>, Error> {
        let buf = self.write_manifest_str(snapshot)?.into_bytes();
        Ok(Buffer::new(0, buf.len(), buf))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlDatasetSnapshotDeserializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlDatasetSnapshotDeserializer;

impl YamlDatasetSnapshotDeserializer {
    pub fn read_manifests(
        &self,
        reader: impl std::io::Read,
    ) -> Result<Vec<DatasetSnapshot>, Error> {
        let mut ret = Vec::new();

        for document in serde_yaml::Deserializer::from_reader(reader) {
            let manifest =
                Manifest::<DatasetSnapshotWrapper>::deserialize(document).map_err(Error::serde)?;

            assert_eq!(manifest.kind, "DatasetSnapshot");
            assert_eq!(manifest.version, 1);

            ret.push(manifest.content.0);
        }

        Ok(ret)
    }
}

impl DatasetSnapshotDeserializer for YamlDatasetSnapshotDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<DatasetSnapshot, Error> {
        let manifest: Manifest<DatasetSnapshotWrapper> =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        // TODO: Handle conversions?
        assert_eq!(manifest.kind, "DatasetSnapshot");
        assert_eq!(manifest.version, 1);

        Ok(manifest.content.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlEngineProtocol
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlEngineProtocol;

impl EngineProtocolSerializer for YamlEngineProtocol {
    fn write_raw_query_request(&self, inst: &RawQueryRequest) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&RawQueryRequestWrapper(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_raw_query_response(&self, inst: &RawQueryResponse) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&RawQueryResponseWrapper(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_transform_request(&self, inst: &TransformRequest) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&TransformRequestWrapper(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_transform_response(&self, inst: &TransformResponse) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&TransformResponseWrapper(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }
}

impl EngineProtocolDeserializer for YamlEngineProtocol {
    fn read_raw_query_request(&self, data: &[u8]) -> Result<RawQueryRequest, Error> {
        let inst: RawQueryRequestWrapper = serde_yaml::from_slice(data).map_err(Error::serde)?;

        Ok(inst.0)
    }

    fn read_raw_query_response(&self, data: &[u8]) -> Result<RawQueryResponse, Error> {
        let inst: RawQueryResponseWrapper = serde_yaml::from_slice(data).map_err(Error::serde)?;

        Ok(inst.0)
    }

    fn read_transform_request(&self, data: &[u8]) -> Result<TransformRequest, Error> {
        let inst: TransformRequestWrapper = serde_yaml::from_slice(data).map_err(Error::serde)?;

        Ok(inst.0)
    }

    fn read_transform_response(&self, data: &[u8]) -> Result<TransformResponse, Error> {
        let inst: TransformResponseWrapper = serde_yaml::from_slice(data).map_err(Error::serde)?;

        Ok(inst.0)
    }
}
