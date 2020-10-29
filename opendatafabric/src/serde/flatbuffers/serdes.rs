use super::convertors::*;
pub use crate::serde::{Buffer, Error, MetadataBlockDeserializer, MetadataBlockSerializer};
use crate::{MetadataBlock, Sha3_256};
use crypto::digest::Digest;
use crypto::sha3::Sha3;
use std::convert::TryInto;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// FlatbuffersSerializer
///////////////////////////////////////////////////////////////////////////////

pub struct FlatbuffersMetadataBlockSerializer;

///////////////////////////////////////////////////////////////////////////////

impl FlatbuffersMetadataBlockSerializer {
    pub fn serialize_metadata_block(&self, block: &MetadataBlock) -> Buffer<u8> {
        let mut fb = flatbuffers::FlatBufferBuilder::new_with_capacity(0);
        let offset = block.serialize(&mut fb);
        fb.finish(offset, None);
        let (buf, head) = fb.collapse();
        Buffer::new(head, buf.len(), buf)
    }

    pub fn set_metadata_block_hash(&self, data: &mut [u8]) -> Result<Sha3_256, Error> {
        let hash_index = data.len() - Sha3_256::LENGTH;

        FlatbuffersMetadataBlockDeserializer::validate_hash_buffer_position(data)?;

        let mut digest = Sha3::sha3_256();
        digest.input(&data[..hash_index]);
        digest.result(&mut data[hash_index..]);

        Ok(Sha3_256::new(data[hash_index..].try_into().unwrap()))
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
    fn write_manifest(&self, block: &MetadataBlock) -> Result<(Sha3_256, Buffer<u8>), Error> {
        let mut buffer = self.serialize_metadata_block(block);

        let block_hash = if block.block_hash.is_zero() {
            self.set_metadata_block_hash(&mut buffer)?
        } else {
            FlatbuffersMetadataBlockDeserializer::validate_block_hash(&buffer)?;
            block.block_hash
        };

        let buffer = self.prepend_manifest(buffer, 1, "MetadataBlock")?;
        Ok((block_hash, buffer))
    }

    fn write_manifest_unchecked(&self, block: &MetadataBlock) -> Result<Buffer<u8>, Error> {
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

    fn validate_hash_buffer_position(data: &[u8]) -> Result<(), Error> {
        let proxy = flatbuffers::get_root::<super::odf_generated::MetadataBlock>(data);
        let block_hash = proxy.block_hash().unwrap();

        if block_hash.len() != Sha3_256::LENGTH {
            return Err(Error::serde(MalformedFlatbufferError));
        }

        let p_buf_end: *const u8 = data.as_ptr().wrapping_add(data.len() - Sha3_256::LENGTH);
        let p_hash: *const u8 = block_hash.as_ptr();
        if p_buf_end != p_hash {
            return Err(Error::serde(MalformedFlatbufferError));
        }

        Ok(())
    }

    fn validate_block_hash(data: &[u8]) -> Result<(), Error> {
        Self::validate_hash_buffer_position(data)?;

        let hash_index = data.len() - Sha3_256::LENGTH;
        let block_hash = &data[hash_index..];
        let mut true_block_hash = [0u8; 32];

        let mut digest = Sha3::sha3_256();
        digest.input(&data[..hash_index]);
        digest.result(&mut true_block_hash);

        if &true_block_hash != block_hash {
            return Err(Error::invalid_hash(
                Sha3_256::new(block_hash.try_into().unwrap()),
                Sha3_256::new(true_block_hash),
            ));
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

impl MetadataBlockDeserializer for FlatbuffersMetadataBlockDeserializer {
    fn validate_manifest(&self, data: &[u8]) -> Result<(), Error> {
        let (_, _, tail) = self.read_manifest_header(data)?;
        Self::validate_block_hash(tail)
    }

    fn read_manifest(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        let (version, kind, tail) = self.read_manifest_header(data)?;

        // TODO: Handle conversions?
        assert_eq!(version, 1);
        assert_eq!(kind, "MetadataBlock");

        Self::validate_block_hash(tail)?;

        let proxy = flatbuffers::get_root::<super::odf_generated::MetadataBlock>(tail);
        Ok(MetadataBlock::deserialize(proxy))
    }

    fn read_manifest_unchecked(&self, data: &[u8]) -> Result<MetadataBlock, Error> {
        let (version, kind, tail) = self.read_manifest_header(data)?;

        // TODO: Handle conversions?
        assert_eq!(version, 1);
        assert_eq!(kind, "MetadataBlock");

        let proxy = flatbuffers::get_root::<super::odf_generated::MetadataBlock>(tail);
        Ok(MetadataBlock::deserialize(proxy))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Malformet flatbuffers data")]
pub struct MalformedFlatbufferError;
