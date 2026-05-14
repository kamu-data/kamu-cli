// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ::serde::Deserialize;

use crate::dtos;
use crate::serde::yaml::derivations_generated as serde;
use crate::serde::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlMetadataBlockSerializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataBlockSerializer;

impl MetadataBlockSerializer for YamlMetadataBlockSerializer {
    fn write_manifest(&self, block: &dtos::MetadataBlock) -> Result<Buffer<u8>, Error> {
        let manifest = serde::Manifest {
            version: METADATA_BLOCK_CURRENT_VERSION as i32,
            kind: "MetadataBlock".to_owned(),
            content: serde::MetadataBlock::from(block.clone()),
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
    fn read_manifest(&self, data: &[u8]) -> Result<dtos::MetadataBlock, Error> {
        // Read short manifest first, with kind and version only
        let manifest_no_content: serde::Manifest<::serde::de::IgnoredAny> =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        assert_eq!(manifest_no_content.kind, "MetadataBlock");

        let version = MetadataBlockVersion::try_from(manifest_no_content.version)?;
        Self::check_version_compatibility(version)?;

        // TODO: Handle conversions for compatible versions

        // Re-read full manifest with content definition
        let manifest: serde::Manifest<serde::MetadataBlock> =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        Ok(manifest.content.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlMetadataEventSerializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataEventSerializer;

impl YamlMetadataEventSerializer {
    pub fn write_manifest_str(&self, event: &dtos::MetadataEvent) -> Result<String, Error> {
        let manifest = serde::Manifest {
            version: 1,
            kind: "MetadataEvent".to_owned(),
            content: serde::MetadataEvent::from(event.clone()),
        };

        serde_yaml::to_string(&manifest).map_err(Error::serde)
    }

    pub fn write_manifest(&self, event: &dtos::MetadataEvent) -> Result<Buffer<u8>, Error> {
        let buf = self.write_manifest_str(event)?.into_bytes();
        Ok(Buffer::new(0, buf.len(), buf))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlMetadataEventDeserializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlMetadataEventDeserializer;

impl YamlMetadataEventDeserializer {
    pub fn read_manifest(&self, data: &[u8]) -> Result<dtos::MetadataEvent, Error> {
        let manifest: serde::Manifest<serde::MetadataEvent> =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        // TODO: Handle conversions?
        assert_eq!(manifest.kind, "MetadataEvent");
        assert_eq!(manifest.version, 1);

        Ok(manifest.content.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlDatasetSnapshotSerializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlDatasetSnapshotSerializer;

impl YamlDatasetSnapshotSerializer {
    pub fn write_manifest_str(&self, snapshot: &dtos::DatasetSnapshot) -> Result<String, Error> {
        let manifest = serde::Manifest {
            version: 1,
            kind: "DatasetSnapshot".to_owned(),
            content: serde::DatasetSnapshot::from(snapshot.clone()),
        };

        serde_yaml::to_string(&manifest).map_err(Error::serde)
    }
}

impl DatasetSnapshotSerializer for YamlDatasetSnapshotSerializer {
    fn write_manifest(&self, snapshot: &dtos::DatasetSnapshot) -> Result<Buffer<u8>, Error> {
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
    ) -> Result<Vec<dtos::DatasetSnapshot>, Error> {
        let mut ret = Vec::new();

        for document in serde_yaml::Deserializer::from_reader(reader) {
            let manifest = serde::Manifest::<serde::DatasetSnapshot>::deserialize(document)
                .map_err(Error::serde)?;

            assert_eq!(manifest.kind, "DatasetSnapshot");
            assert_eq!(manifest.version, 1);

            ret.push(manifest.content.into());
        }

        Ok(ret)
    }
}

impl DatasetSnapshotDeserializer for YamlDatasetSnapshotDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<dtos::DatasetSnapshot, Error> {
        let manifest: serde::Manifest<serde::DatasetSnapshot> =
            serde_yaml::from_slice(data).map_err(Error::serde)?;

        // TODO: Handle conversions?
        assert_eq!(manifest.kind, "DatasetSnapshot");
        assert_eq!(manifest.version, 1);

        Ok(manifest.content.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// YamlEngineProtocol
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct YamlEngineProtocol;

impl EngineProtocolSerializer for YamlEngineProtocol {
    fn write_raw_query_request(&self, inst: &dtos::RawQueryRequest) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&serde::RawQueryRequest::from(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_raw_query_response(&self, inst: &dtos::RawQueryResponse) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&serde::RawQueryResponse::from(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_transform_request(&self, inst: &dtos::TransformRequest) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&serde::TransformRequest::from(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }

    fn write_transform_response(
        &self,
        inst: &dtos::TransformResponse,
    ) -> Result<Buffer<u8>, Error> {
        let buf = serde_yaml::to_string(&serde::TransformResponse::from(inst.clone()))
            .map_err(Error::serde)?
            .into_bytes();

        Ok(Buffer::new(0, buf.len(), buf))
    }
}

impl EngineProtocolDeserializer for YamlEngineProtocol {
    fn read_raw_query_request(&self, data: &[u8]) -> Result<dtos::RawQueryRequest, Error> {
        let inst: serde::RawQueryRequest = serde_yaml::from_slice(data).map_err(Error::serde)?;
        Ok(inst.into())
    }

    fn read_raw_query_response(&self, data: &[u8]) -> Result<dtos::RawQueryResponse, Error> {
        let inst: serde::RawQueryResponse = serde_yaml::from_slice(data).map_err(Error::serde)?;
        Ok(inst.into())
    }

    fn read_transform_request(&self, data: &[u8]) -> Result<dtos::TransformRequest, Error> {
        let inst: serde::TransformRequest = serde_yaml::from_slice(data).map_err(Error::serde)?;
        Ok(inst.into())
    }

    fn read_transform_response(&self, data: &[u8]) -> Result<dtos::TransformResponse, Error> {
        let inst: serde::TransformResponse = serde_yaml::from_slice(data).map_err(Error::serde)?;
        Ok(inst.into())
    }
}
