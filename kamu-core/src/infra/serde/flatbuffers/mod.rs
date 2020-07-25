pub use super::Deserializer;
pub use super::Result;
pub use super::SerdeError;
pub use super::Serializer;

#[allow(dead_code, unused_imports)]
mod metadata_block_generated;
use metadata_block_generated as gen;

/*
pub struct FlatbuffersSerializer;
pub struct FlatbuffersDeserializer;

impl Serializer<MetadataBlock> for FlatbuffersSerializer {
    fn serialize(&self, obj: &MetadataBlock, writer: &mut dyn std::io::Write) -> Result<()> {
        let mut fb = flatbuffers::FlatBufferBuilder::new();
        let block_hash = fb.create_string(&obj.block_hash);
        let prev_block_hash = fb.create_string(&obj.prev_block_hash);

        let b = gen::MetadataBlock::create(
            &mut fb,
            &gen::MetadataBlockArgs {
                block_hash: Some(block_hash),
                prev_block_hash: Some(prev_block_hash),
            },
        );

        fb.finish(b, None);
        let (buf, head) = fb.collapse();
        writer.write(&buf[head..])?;
        Ok(())
    }
}

impl Deserializer<MetadataBlock> for FlatbuffersDeserializer {
    fn deserialize(&self, buf: &[u8]) -> Result<MetadataBlock> {
        let root = gen::get_root_as_metadata_block(buf);
        let obj = MetadataBlock {
            block_hash: root.block_hash().unwrap_or_default().to_string(),
            prev_block_hash: root.prev_block_hash().unwrap_or_default().to_string(),
        };
        Ok(obj)
    }
}
*/
