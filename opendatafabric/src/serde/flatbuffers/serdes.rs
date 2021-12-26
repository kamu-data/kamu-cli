// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::convertors::*;
use super::odf_generated as fbgen;
pub use crate::serde::{Buffer, Error, MetadataBlockDeserializer, MetadataBlockSerializer};
use crate::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use crate::{ExecuteQueryRequest, ExecuteQueryResponse, MetadataBlock};

///////////////////////////////////////////////////////////////////////////////
// FlatbuffersMetadataBlockSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersMetadataBlockSerializer;

///////////////////////////////////////////////////////////////////////////////

impl FlatbuffersMetadataBlockSerializer {
    const METADATA_BLOCK_SIZE_ESTIMATE: usize = 10 * 1024;

    pub fn serialize_metadata_block(&self, block: &MetadataBlock) -> Buffer<u8> {
        let mut fb =
            flatbuffers::FlatBufferBuilder::with_capacity(Self::METADATA_BLOCK_SIZE_ESTIMATE);
        let offset = block.serialize(&mut fb);
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        Buffer::new(head, buf.len(), buf)
    }

    pub fn prepend_manifest(
        &self,
        mut buffer: Buffer<u8>,
        version: u16,
        kind: &str,
    ) -> Result<Buffer<u8>, Error> {
        use byteorder::{LittleEndian, WriteBytesExt};
        use std::io::Write;

        let kind_bytes = kind.as_bytes();
        let header_size = kind_bytes.len() + 2 + 2;
        buffer.ensure_capacity(header_size, 0);

        let header_start = buffer.head() - header_size;
        let header_end = buffer.head();
        let mut cursor = std::io::Cursor::new(&mut buffer.inner_mut()[header_start..header_end]);
        cursor.write_u16::<LittleEndian>(version)?;
        cursor.write_u16::<LittleEndian>(kind_bytes.len() as u16)?;
        cursor.write(kind_bytes)?;
        buffer.set_head(header_start);
        Ok(buffer)
    }
}

///////////////////////////////////////////////////////////////////////////////

impl MetadataBlockSerializer for FlatbuffersMetadataBlockSerializer {
    fn write_manifest(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error> {
        let buffer = self.serialize_metadata_block(block);
        self.prepend_manifest(buffer, 1, "MetadataBlock")
    }
}

///////////////////////////////////////////////////////////////////////////////
// FlatbuffersMetadataBlockDeserializer
///////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersMetadataBlockDeserializer;

///////////////////////////////////////////////////////////////////////////////

impl FlatbuffersMetadataBlockDeserializer {
    pub fn read_manifest_header<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<(u16, &'a str, &'a [u8]), Error> {
        use byteorder::{LittleEndian, ReadBytesExt};

        let mut cursor = std::io::Cursor::new(data);
        let version = cursor.read_u16::<LittleEndian>()?;
        let kind_len = cursor.read_u16::<LittleEndian>()? as usize;
        let kind = std::str::from_utf8(&data[4..4 + kind_len]).unwrap();
        Ok((version, kind, &data[4 + kind_len..]))
    }
}

///////////////////////////////////////////////////////////////////////////////

impl MetadataBlockDeserializer for FlatbuffersMetadataBlockDeserializer {
    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        let (version, kind, tail) = self.read_manifest_header(data)?;

        // TODO: Handle conversions?
        assert_eq!(version, 1);
        assert_eq!(kind, "MetadataBlock");

        let proxy = flatbuffers::root::<fbgen::MetadataBlock>(tail).map_err(|e| Error::serde(e))?;

        Ok(MetadataBlock::deserialize(proxy))
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
        let proxy = flatbuffers::root::<super::odf_generated::ExecuteQueryResponseRoot>(data)
            .map_err(|e| Error::serde(e))?;

        Ok(ExecuteQueryResponse::deserialize(
            proxy.value().unwrap(),
            proxy.value_type(),
        ))
    }
}
