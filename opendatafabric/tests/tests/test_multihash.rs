use digest::Digest;
use opendatafabric::*;

#[test]
fn test_multihash() {
    assert_eq!(
        Multihash::new(
            MulticodecCode::Sha3_256,
            &sha3::Sha3_256::digest(b"multihash")
        )
        .to_multibase_string(),
        "zW1a3CNT52HXiJNniLkWMeev3CPRy9QiNRMWGyTrVNg4hY8"
    );

    assert_eq!(
        Multihash::from_multibase_str("zW1a3CNT52HXiJNniLkWMeev3CPRy9QiNRMWGyTrVNg4hY8").unwrap(),
        Multihash::new(
            MulticodecCode::Sha3_256,
            &sha3::Sha3_256::digest(b"multihash")
        ),
    );
}
