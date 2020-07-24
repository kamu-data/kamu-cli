/*
use kamu::domain::*;
use kamu::infra::serde::flatbuffers::*;
use std::io;


#[test]
fn to_and_from() -> Result<()> {
    let block = MetadataBlock {
        block_hash: "hash".to_string(),
        prev_block_hash: "prev_hash".to_string(),
    };

    let ser = FlatbuffersSerializer;
    let mut cursor = io::Cursor::new(Vec::new());
    ser.serialize(&block, &mut cursor)?;
    //println!("{}", hex::encode(&buf));

    let de = FlatbuffersDeserializer;
    let block_prime = de.deserialize(cursor.get_ref())?;
    assert_eq!(block, block_prime);

    Ok(())
}
*/
