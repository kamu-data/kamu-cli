// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ::flatbuffers;

pub use super::convertors_generated::*;
use super::proxies_generated as fbgen;
use crate::dtos::*;
use crate::formats::Multicodec;
pub use crate::serde::*;

///////////////////////////////////////////////////////////////////////////////
// FlatbuffersMetadataBlockSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersMetadataBlockSerializer;

///////////////////////////////////////////////////////////////////////////////

impl FlatbuffersMetadataBlockSerializer {
    const METADATA_BLOCK_SIZE_ESTIMATE: usize = 10 * 1024;

    fn serialize_metadata_block(&self, block: &MetadataBlock) -> Buffer<u8> {
        let mut fb =
            flatbuffers::FlatBufferBuilder::with_capacity(Self::METADATA_BLOCK_SIZE_ESTIMATE);
        let offset = block.serialize(&mut fb);
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        Buffer::new(head, buf.len(), buf)
    }
}

///////////////////////////////////////////////////////////////////////////////

impl MetadataBlockSerializer for FlatbuffersMetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error> {
        // TODO: PERF: Serializing nested flatbuffers turned out to be a pain
        // It's hard to make the inner object length-prefixed in order to then treat it
        // as a [ubyte] array so for now we allocate twice and copy inner object
        // into secondary buffer :(
        //
        // See: https://github.com/google/flatbuffers/issues/7005
        let block_buffer = self.serialize_metadata_block(block);

        let mut fb = flatbuffers::FlatBufferBuilder::with_capacity(block_buffer.len() + 1024);

        let content_offset = fb.create_vector(&block_buffer);

        let mut builder = fbgen::ManifestBuilder::new(&mut fb);
        builder.add_kind(Multicodec::ODFMetadataBlock as i64);
        builder.add_version(METADATA_BLOCK_CURRENT_VERSION as i32);
        builder.add_content(content_offset);
        let offset = builder.finish();

        fb.finish(offset, None);
        let (buf, head) = fb.collapse();

        Ok(Buffer::new(head, buf.len(), buf))
    }
}

///////////////////////////////////////////////////////////////////////////////
// FlatbuffersMetadataBlockDeserializer
///////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersMetadataBlockDeserializer;

///////////////////////////////////////////////////////////////////////////////

impl MetadataBlockDeserializer for FlatbuffersMetadataBlockDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        let manifest_proxy =
            flatbuffers::root::<fbgen::Manifest>(data).map_err(|e| Error::serde(e))?;

        // TODO: Better error handling
        let kind = Multicodec::try_from(manifest_proxy.kind() as u32).unwrap();
        assert_eq!(kind, Multicodec::ODFMetadataBlock);

        // TODO: Handle conversions for compatible versions
        let version = MetadataBlockVersion::try_from(manifest_proxy.version())?;
        Self::check_version_compatibility(version)?;

        let block_proxy =
            flatbuffers::root::<fbgen::MetadataBlock>(manifest_proxy.content().unwrap().bytes())
                .map_err(|e| Error::serde(e))?;

        let block = MetadataBlock::deserialize(block_proxy);
        Ok(block)
    }
}

///////////////////////////////////////////////////////////////////////////////
// FlatbuffersEngineProtocol
///////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersEngineProtocol;

impl EngineProtocolSerializer for FlatbuffersEngineProtocol {
    fn write_execute_query_request(&self, inst: &ExecuteQueryRequest) -> Result<Buffer<u8>, Error> {
        let mut fb = flatbuffers::FlatBufferBuilder::new();
        let offset = inst.serialize(&mut fb);
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        Ok(Buffer::new(head, buf.len(), buf))
    }

    fn write_execute_query_response(
        &self,
        inst: &ExecuteQueryResponse,
    ) -> Result<Buffer<u8>, Error> {
        let mut fb = flatbuffers::FlatBufferBuilder::new();
        let offset = {
            let (typ, offset) = inst.serialize(&mut fb);
            let mut builder = fbgen::ExecuteQueryResponseRootBuilder::new(&mut fb);
            builder.add_value_type(typ);
            builder.add_value(offset);
            builder.finish()
        };
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        Ok(Buffer::new(head, buf.len(), buf))
    }
}

impl EngineProtocolDeserializer for FlatbuffersEngineProtocol {
    fn read_execute_query_request(&self, data: &[u8]) -> Result<ExecuteQueryRequest, Error> {
        let proxy =
            flatbuffers::root::<fbgen::ExecuteQueryRequest>(data).map_err(|e| Error::serde(e))?;

        Ok(ExecuteQueryRequest::deserialize(proxy))
    }

    fn read_execute_query_response(&self, data: &[u8]) -> Result<ExecuteQueryResponse, Error> {
        let proxy = flatbuffers::root::<super::proxies_generated::ExecuteQueryResponseRoot>(data)
            .map_err(|e| Error::serde(e))?;

        Ok(ExecuteQueryResponse::deserialize(
            proxy.value().unwrap(),
            proxy.value_type(),
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////
