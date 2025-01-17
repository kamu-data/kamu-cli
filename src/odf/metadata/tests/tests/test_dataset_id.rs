// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric_metadata::*;

#[test]
fn test_debug() {
    assert_eq!(
        format!("{:?}", DatasetID::new_seeded_ed25519(b"multihash")),
        "DatasetID<Ed25519Pub>(fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc)"
    );

    assert_eq!(
        format!(
            "{:?}",
            DatasetID::new_seeded_ed25519(b"multihash").as_did_str()
        ),
        "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc"
    );

    assert_eq!(
        format!(
            "{:?}",
            DatasetID::new_seeded_ed25519(b"multihash").as_multibase()
        ),
        "fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc"
    );
}

#[test]
fn test_did_string() {
    let value = DatasetID::from_did_str(
        "did:odf:fed012e6fcce36701dc791488e0d0b1745cc1e33a4c1c9fcc41c63bd343dbbe0970e6",
    )
    .unwrap();

    assert_eq!(value.key_type(), Multicodec::Ed25519Pub);

    assert_eq!(
        value.to_string(),
        "did:odf:fed012e6fcce36701dc791488e0d0b1745cc1e33a4c1c9fcc41c63bd343dbbe0970e6",
    );

    assert_eq!(
        value.as_did_key().as_did_str().to_stack_string(),
        "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
    );
}

#[test]
fn test_multibase() {
    assert_eq!(
        DatasetID::new_seeded_ed25519(b"multihash")
            .as_multibase()
            .to_stack_string(),
        "fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc"
    );
}

#[test]
fn test_eq() {
    assert_eq!(
        DatasetID::from_did_str("did:odf:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
            .unwrap(),
        DatasetID::from_did_str("did:odf:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
            .unwrap()
    );
    assert_ne!(
        DatasetID::from_did_str("did:odf:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
            .unwrap(),
        DatasetID::from_did_str("did:odf:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doL")
            .unwrap()
    );
}
