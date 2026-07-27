// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use ::flatbuffers;
use multiformats::Multicodec;

pub use super::convertors_generated::*;
use super::proxies_generated as fbgen;
use crate::MetadataBlockBytes;
use crate::dataset::MetadataBlockHeader;
use crate::dtos::dataset::MetadataBlock;
use crate::dtos::engine::*;
use crate::serde::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlatbuffersMetadataBlockSerializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersMetadataBlockSerializer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlatbuffersMetadataBlockSerializer {
    const METADATA_BLOCK_SIZE_ESTIMATE: usize = 10 * 1024;

    fn serialize_metadata_block(&self, block: &MetadataBlock) -> bytes::Bytes {
        let mut fb =
            flatbuffers::FlatBufferBuilder::with_capacity(Self::METADATA_BLOCK_SIZE_ESTIMATE);
        let offset = block.serialize(&mut fb);
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();

        let buf = bytes::Bytes::from(buf);
        buf.slice(head..buf.len())
    }

    pub fn write_manifest_canonical(
        &self,
        block: &MetadataBlock,
    ) -> Result<MetadataBlockBytes, Error> {
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

        let buf = bytes::Bytes::from(buf);
        Ok(MetadataBlockBytes::new(buf.slice(head..buf.len())))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MetadataBlockSerializer for FlatbuffersMetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<bytes::Bytes, Error> {
        self.write_manifest_canonical(block).map(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlatbuffersMetadataBlockDeserializer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersMetadataBlockDeserializer;

impl FlatbuffersMetadataBlockDeserializer {
    fn get_block_proxy<'a>(&self, data: &'a [u8]) -> Result<fbgen::MetadataBlock<'a>, Error> {
        let manifest_proxy = flatbuffers::root::<fbgen::Manifest>(data).map_err(Error::serde)?;

        // TODO: Better error handling
        let manifest_kind = u32::try_from(manifest_proxy.kind()).unwrap();
        let kind = Multicodec::try_from(manifest_kind).unwrap();
        assert_eq!(kind, Multicodec::ODFMetadataBlock);

        // TODO: Handle conversions for compatible versions
        let version = MetadataBlockVersion::try_from(manifest_proxy.version())?;
        Self::check_version_compatibility(version)?;

        let block_proxy =
            flatbuffers::root::<fbgen::MetadataBlock>(manifest_proxy.content().unwrap().bytes())
                .map_err(Error::serde)?;

        Ok(block_proxy)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MetadataBlockDeserializer for FlatbuffersMetadataBlockDeserializer {
    fn read_header(&self, data: &[u8]) -> Result<MetadataBlockHeader, Error> {
        let block_proxy = self.get_block_proxy(data)?;
        Ok(MetadataBlockHeader {
            system_time: super::fb_to_datetime(block_proxy.system_time().unwrap()),
            prev_block_hash: block_proxy
                .prev_block_hash()
                .map(|v| crate::Multihash::from_bytes(v.bytes()).unwrap()),
            sequence_number: block_proxy.sequence_number(),
            event_type: block_proxy.event_type().into(),
        })
    }

    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        let block_proxy = self.get_block_proxy(data)?;
        let block = MetadataBlock::deserialize(block_proxy);
        Ok(block)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlatbuffersEngineProtocol
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersEngineProtocol;

impl EngineProtocolSerializer for FlatbuffersEngineProtocol {
    fn write_raw_query_request(&self, inst: &RawQueryRequest) -> Result<bytes::Bytes, Error> {
        let mut fb = flatbuffers::FlatBufferBuilder::new();
        let offset = inst.serialize(&mut fb);
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        let buf = bytes::Bytes::from(buf);
        Ok(buf.slice(head..buf.len()))
    }

    fn write_raw_query_response(&self, inst: &RawQueryResponse) -> Result<bytes::Bytes, Error> {
        let mut fb = flatbuffers::FlatBufferBuilder::new();
        let offset = {
            let (typ, offset) = inst.serialize(&mut fb);
            let mut builder = fbgen::RawQueryResponseRootBuilder::new(&mut fb);
            builder.add_value_type(typ);
            builder.add_value(offset);
            builder.finish()
        };
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        let buf = bytes::Bytes::from(buf);
        Ok(buf.slice(head..buf.len()))
    }

    fn write_transform_request(&self, inst: &TransformRequest) -> Result<bytes::Bytes, Error> {
        let mut fb = flatbuffers::FlatBufferBuilder::new();
        let offset = inst.serialize(&mut fb);
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        let buf = bytes::Bytes::from(buf);
        Ok(buf.slice(head..buf.len()))
    }

    fn write_transform_response(&self, inst: &TransformResponse) -> Result<bytes::Bytes, Error> {
        let mut fb = flatbuffers::FlatBufferBuilder::new();
        let offset = {
            let (typ, offset) = inst.serialize(&mut fb);
            let mut builder = fbgen::TransformResponseRootBuilder::new(&mut fb);
            builder.add_value_type(typ);
            builder.add_value(offset);
            builder.finish()
        };
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        let buf = bytes::Bytes::from(buf);
        Ok(buf.slice(head..buf.len()))
    }
}

impl EngineProtocolDeserializer for FlatbuffersEngineProtocol {
    fn read_raw_query_request(&self, data: &[u8]) -> Result<RawQueryRequest, Error> {
        let proxy = flatbuffers::root::<fbgen::RawQueryRequest>(data).map_err(Error::serde)?;

        Ok(RawQueryRequest::deserialize(proxy))
    }

    fn read_raw_query_response(&self, data: &[u8]) -> Result<RawQueryResponse, Error> {
        let proxy = flatbuffers::root::<super::proxies_generated::RawQueryResponseRoot>(data)
            .map_err(Error::serde)?;

        Ok(RawQueryResponse::deserialize(
            proxy.value().unwrap(),
            proxy.value_type(),
        ))
    }

    fn read_transform_request(&self, data: &[u8]) -> Result<TransformRequest, Error> {
        let proxy = flatbuffers::root::<fbgen::TransformRequest>(data).map_err(Error::serde)?;

        Ok(TransformRequest::deserialize(proxy))
    }

    fn read_transform_response(&self, data: &[u8]) -> Result<TransformResponse, Error> {
        let proxy = flatbuffers::root::<super::proxies_generated::TransformResponseRoot>(data)
            .map_err(Error::serde)?;

        Ok(TransformResponse::deserialize(
            proxy.value().unwrap(),
            proxy.value_type(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
