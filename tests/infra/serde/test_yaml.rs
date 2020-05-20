extern crate kamu;

use indoc::indoc;
use kamu::domain::metadata::*;
use kamu::infra::serde::yaml::*;
use std::io;

#[test]
fn to_and_from() -> Result<()> {
    let block = MetadataBlock {
        block_hash: "hash".to_string(),
        prev_block_hash: "prev_hash".to_string(),
    };

    let ser = YamlSerializer;
    let mut cursor = io::Cursor::new(Vec::new());
    ser.serialize(&block, &mut cursor)?;

    assert_eq!(
        String::from_utf8(cursor.get_ref().clone()).unwrap(),
        indoc!(
            "---
            block_hash: hash
            prev_block_hash: prev_hash"
        )
    );

    let de = YamlDeserializer;
    let block_prime = de.deserialize(cursor.get_ref())?;
    assert_eq!(block, block_prime);
    Ok(())
}

#[test]
fn de_error() {
    let de = YamlDeserializer;

    let yaml = "!!# gibberish #!!";
    let bytes: Vec<u8> = yaml.bytes().collect();

    let res: Result<MetadataBlock> = de.deserialize(&bytes);
    assert!(res.is_err());
}
