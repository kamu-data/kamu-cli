// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::*;

use std::sync::Arc;
use std::{assert_matches::assert_matches, convert::TryFrom, str::FromStr};
use url::Url;

#[test]
fn test_dataset_refs() {
    assert_eq!(
        DatasetRefAny::from_str("dataset").unwrap(),
        DatasetRefAny::Name(DatasetName::new_unchecked("dataset"))
    );
    assert_eq!(
        DatasetRefAny::from_str("repo/dataset").unwrap(),
        DatasetRefAny::RemoteName(RemoteDatasetName::new(
            RepositoryName::new_unchecked("repo"),
            None,
            DatasetName::new_unchecked("dataset")
        ))
    );
    assert_eq!(
        DatasetRefAny::from_str("repo/account/dataset").unwrap(),
        DatasetRefAny::RemoteName(RemoteDatasetName::new(
            RepositoryName::new_unchecked("repo"),
            Some(AccountName::new_unchecked("account")),
            DatasetName::new_unchecked("dataset")
        ))
    );
    assert_eq!(
        DatasetRefAny::from_str("did:odf:z4k88e8eonGq3xrTzEVyvb4s7Fy3orT7npgW4w3juneJLohqCRs")
            .unwrap(),
        DatasetRefAny::ID(DatasetID::from_pub_key_ed25519(b"key"))
    );
    assert_eq!(
        DatasetRefAny::from_str("https://opendata.ca/odf/census-2016-population/").unwrap(),
        DatasetRefAny::Url(Arc::new(
            Url::from_str("https://opendata.ca/odf/census-2016-population/").unwrap()
        ))
    );
    assert_eq!(
        DatasetRefAny::from_str(
            "ipfs://bafkreie3hfshd4ikinnbio3kewo2hvj6doh5jp3p23iwk2evgo2un5g7km/"
        )
        .unwrap(),
        DatasetRefAny::Url(Arc::new(
            Url::from_str("ipfs://bafkreie3hfshd4ikinnbio3kewo2hvj6doh5jp3p23iwk2evgo2un5g7km/")
                .unwrap()
        ))
    );
    assert_matches!(DatasetRefAny::from_str("foo:bar"), Err(_));
}

#[test]
fn test_dataset_name_newtype() {
    let s = "valid.dataset.id";
    let id = DatasetName::try_from(s).unwrap();

    fn needs_str(_: &str) {}
    fn needs_name(_: &DatasetName) {}

    needs_name(&id);
    needs_str(&id); // implicit converts to str
    assert!(id.starts_with("valid")); // str methods are still accessible
}

#[test]
fn test_dataset_name_fmt() {
    let id = DatasetName::try_from("valid.dataset.id").unwrap();

    assert_eq!(format!("{}", id), "valid.dataset.id");
    assert_eq!(format!("{:?}", id), "DatasetName(\"valid.dataset.id\")");
}

#[test]
fn test_dataset_name_equality() {
    assert_eq!(
        DatasetName::new_unchecked("a"),
        DatasetName::new_unchecked("a")
    );
    assert_ne!(
        DatasetName::new_unchecked("a"),
        DatasetName::new_unchecked("b")
    );
}

#[test]
fn test_dataset_name_validation() {
    assert_matches!(DatasetName::try_from("local.id-only"), Ok(s) if s == "local.id-only");
    assert_matches!(DatasetName::try_from(".invalid"), Err(_));
    assert_matches!(DatasetName::try_from("invalid-"), Err(_));
    assert_matches!(DatasetName::try_from("invalid--id"), Err(_));
    assert_matches!(DatasetName::try_from("invalid..id"), Err(_));
    assert_matches!(DatasetName::try_from("in^valid"), Err(_));
}

#[test]
fn test_remote_dataset_name_validation() {
    assert_matches!(
        RemoteDatasetName::try_from("repo.name/local.id")
            .unwrap()
            .to_string()
            .as_ref(),
        "repo.name/local.id"
    );

    let dr = RemoteDatasetName::try_from("repo.name/local.id").unwrap();
    assert_eq!(dr.dataset(), "local.id");
    assert_eq!(dr.account(), None);
    assert_eq!(dr.repository(), "repo.name");

    assert_matches!(RemoteDatasetName::try_from("repo.name/.invalid"), Err(_));
    assert_matches!(RemoteDatasetName::try_from(".invalid/local.id"), Err(_));

    assert_matches!(
        RemoteDatasetName::try_from("repo.name/user-name/local.id")
            .unwrap()
            .to_string()
            .as_ref(),
        "repo.name/user-name/local.id"
    );

    let dr = RemoteDatasetName::try_from("repo.name/user-name/local.id").unwrap();
    assert_eq!(dr.dataset(), "local.id");
    assert_matches!(dr.account(), Some(id) if id == "user-name");
    assert_eq!(dr.repository(), "repo.name");

    assert_matches!(
        RemoteDatasetName::try_from("repo.name/user-name/.invalid"),
        Err(_)
    );
    assert_matches!(
        RemoteDatasetName::try_from("repo.name/user.name/local.id"),
        Err(_)
    );
    assert_matches!(
        RemoteDatasetName::try_from(".invalid/user-name/local.id"),
        Err(_)
    );
}

#[test]
fn test_dataset_name_with_owner_validation() {
    assert_matches!(
        DatasetNameWithOwner::try_from("local.id")
            .unwrap()
            .to_string()
            .as_ref(),
        "local.id"
    );

    let dr = DatasetNameWithOwner::try_from("local.id").unwrap();
    assert_eq!(dr.dataset(), "local.id");
    assert_eq!(dr.account(), None);

    let dr = DatasetNameWithOwner::try_from("user-name/local.id").unwrap();
    assert_eq!(dr.dataset(), "local.id");
    assert_matches!(dr.account(), Some(id) if id == "user-name");

    assert_matches!(
        DatasetNameWithOwner::try_from("repo.name/user-name/blah"),
        Err(_)
    );
    assert_matches!(DatasetNameWithOwner::try_from("user-name/.invalid"), Err(_));
    assert_matches!(DatasetNameWithOwner::try_from("user.name/local.id"), Err(_));
}

#[test]
fn test_dataset_refs_conversions() {
    fn takes_ref_local<R: Into<DatasetRefLocal>>(_: R) {}
    fn takes_ref_remote<R: Into<DatasetRefRemote>>(_: R) {}
    fn takes_ref_any<R: Into<DatasetRefAny>>(_: R) {}

    takes_ref_local(DatasetID::from_pub_key_ed25519(b"key"));
    takes_ref_local(&DatasetID::from_pub_key_ed25519(b"key"));
    takes_ref_local(DatasetName::new_unchecked("bar"));
    takes_ref_local(&DatasetName::new_unchecked("baz"));
    takes_ref_local(DatasetHandle {
        id: DatasetID::from_pub_key_ed25519(b"key"),
        name: DatasetName::new_unchecked("bar"),
    });
    takes_ref_local(&DatasetHandle {
        id: DatasetID::from_pub_key_ed25519(b"key"),
        name: DatasetName::new_unchecked("bar"),
    });

    takes_ref_remote(DatasetID::from_pub_key_ed25519(b"key"));
    takes_ref_remote(&DatasetID::from_pub_key_ed25519(b"key"));
    takes_ref_remote(RemoteDatasetName::try_from("foo/bar").unwrap());
    takes_ref_remote(&RemoteDatasetName::try_from("foo/bar").unwrap());

    takes_ref_any(DatasetID::from_pub_key_ed25519(b"key"));
    takes_ref_any(&DatasetID::from_pub_key_ed25519(b"key"));
    takes_ref_any(DatasetName::new_unchecked("bar"));
    takes_ref_any(&DatasetName::new_unchecked("baz"));
    takes_ref_any(RemoteDatasetName::try_from("foo/bar").unwrap());
    takes_ref_any(&RemoteDatasetName::try_from("foo/bar").unwrap());
    takes_ref_any(DatasetHandle {
        id: DatasetID::from_pub_key_ed25519(b"key"),
        name: DatasetName::new_unchecked("bar"),
    });
    takes_ref_any(&DatasetHandle {
        id: DatasetID::from_pub_key_ed25519(b"key"),
        name: DatasetName::new_unchecked("bar"),
    });
}
