// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::Arc;

use opendatafabric_metadata::*;
use url::Url;

#[test]
fn test_dataset_refs() {
    assert_eq!(
        DatasetRefAny::from_str("dataset").unwrap(),
        DatasetRefAny::LocalAlias(None, DatasetName::new_unchecked("dataset"))
    );
    assert_eq!(
        DatasetRefAny::from_str("mystery/dataset").unwrap(),
        DatasetRefAny::AmbiguousAlias(Arc::from("mystery"), DatasetName::new_unchecked("dataset"))
    );
    assert_eq!(
        DatasetRefAny::from_str("repo/dataset")
            .unwrap()
            .into_remote_ref(|_| true),
        Ok(DatasetRefRemote::Alias(DatasetAliasRemote::new(
            RepoName::new_unchecked("repo"),
            None,
            DatasetName::new_unchecked("dataset")
        )))
    );
    assert_eq!(
        DatasetRefAny::from_str("acc/dataset")
            .unwrap()
            .into_local_ref(|_| false),
        Ok(DatasetRef::Alias(DatasetAlias::new(
            Some(AccountName::new_unchecked("acc")),
            DatasetName::new_unchecked("dataset")
        )))
    );
    assert_eq!(
        DatasetRefAny::from_str("repo/account/dataset")
            .unwrap()
            .into_remote_ref(|_| true),
        Ok(DatasetRefRemote::Alias(DatasetAliasRemote::new(
            RepoName::new_unchecked("repo"),
            Some(AccountName::new_unchecked("account")),
            DatasetName::new_unchecked("dataset")
        )))
    );
    assert_eq!(
        DatasetRefAny::from_str("did:odf:z6MkmgVreHBu2ABaD59Jq1J2JneXwzpsUWwEWXS4kLhjb4V4")
            .unwrap(),
        DatasetRefAny::ID(None, DatasetID::new_seeded_ed25519(b"key"))
    );
    assert_eq!(
        DatasetRefAny::from_str("repo/did:odf:z6MkmgVreHBu2ABaD59Jq1J2JneXwzpsUWwEWXS4kLhjb4V4")
            .unwrap(),
        DatasetRefAny::ID(
            Some(RepoName::new_unchecked("repo")),
            DatasetID::new_seeded_ed25519(b"key")
        )
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

    assert_eq!(format!("{id}"), "valid.dataset.id");
    assert_eq!(format!("{id:?}"), "DatasetName(\"valid.dataset.id\")");
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
    assert_matches!(DatasetName::try_from("invalid_id"), Err(_));
}

#[test]
fn test_remote_alias_validation() {
    assert_matches!(
        DatasetAliasRemote::try_from("repo.name/local.id")
            .unwrap()
            .to_string()
            .as_ref(),
        "repo.name/local.id"
    );

    let dr = DatasetAliasRemote::try_from("repo.name/local.id").unwrap();
    assert_eq!(dr.dataset_name, "local.id");
    assert_eq!(dr.account_name, None);
    assert_eq!(dr.repo_name, "repo.name");

    assert_matches!(DatasetAliasRemote::try_from("repo.name/.invalid"), Err(_));
    assert_matches!(DatasetAliasRemote::try_from(".invalid/local.id"), Err(_));

    assert_matches!(
        DatasetAliasRemote::try_from("repo.name/user-name/local.id")
            .unwrap()
            .to_string()
            .as_ref(),
        "repo.name/user-name/local.id"
    );

    let dr = DatasetAliasRemote::try_from("repo.name/user-name/local.id").unwrap();
    assert_eq!(dr.dataset_name, "local.id");
    assert_matches!(dr.account_name, Some(id) if id == "user-name");
    assert_eq!(dr.repo_name, "repo.name");

    assert_matches!(
        DatasetAliasRemote::try_from("repo.name/user-name/.invalid"),
        Err(_)
    );
    assert_matches!(
        DatasetAliasRemote::try_from("repo.name/.invalid/local.id"),
        Err(_)
    );
    assert_matches!(
        DatasetAliasRemote::try_from(".invalid/user-name/local.id"),
        Err(_)
    );
}

#[test]
fn test_local_alias_validation() {
    assert_matches!(
        DatasetAlias::try_from("local.id")
            .unwrap()
            .to_string()
            .as_ref(),
        "local.id"
    );

    let dr = DatasetAlias::try_from("local.id").unwrap();
    assert_eq!(dr.dataset_name, "local.id");
    assert_eq!(dr.account_name, None);

    let dr = DatasetAlias::try_from("user-name/local.id").unwrap();
    assert_eq!(dr.dataset_name, "local.id");
    assert_matches!(dr.account_name, Some(id) if id == "user-name");

    assert_matches!(DatasetAlias::try_from("repo.name/user-name/blah"), Err(_));
    assert_matches!(DatasetAlias::try_from("user-name/.invalid"), Err(_));
    assert_matches!(DatasetAlias::try_from(".invalid/local.id"), Err(_));
    assert_matches!(DatasetAlias::try_from("invalid_alias"), Err(_));
}

#[allow(clippy::needless_borrows_for_generic_args)]
#[test]
fn test_dataset_refs_conversions() {
    fn takes_ref_local<R: Into<DatasetRef>>(_: R) {}
    fn takes_ref_remote<R: Into<DatasetRefRemote>>(_: R) {}
    fn takes_ref_any<R: Into<DatasetRefAny>>(_: R) {}

    takes_ref_local(DatasetID::new_seeded_ed25519(b"key"));
    takes_ref_local(DatasetName::new_unchecked("bar"));
    takes_ref_local(&DatasetName::new_unchecked("baz"));
    takes_ref_local(DatasetHandle {
        id: DatasetID::new_seeded_ed25519(b"key"),
        alias: DatasetAlias::try_from("bar").unwrap(),
    });
    takes_ref_local(&DatasetHandle {
        id: DatasetID::new_seeded_ed25519(b"key"),
        alias: DatasetAlias::try_from("bar").unwrap(),
    });

    takes_ref_remote(DatasetID::new_seeded_ed25519(b"key"));
    takes_ref_remote(DatasetAliasRemote::try_from("foo/bar").unwrap());
    takes_ref_remote(&DatasetAliasRemote::try_from("foo/bar").unwrap());

    takes_ref_any(DatasetID::new_seeded_ed25519(b"key"));
    takes_ref_any(DatasetName::new_unchecked("bar"));
    takes_ref_any(&DatasetName::new_unchecked("baz"));
    takes_ref_any(DatasetAliasRemote::try_from("foo/bar").unwrap());
    takes_ref_any(&DatasetAliasRemote::try_from("foo/bar").unwrap());
    takes_ref_any(DatasetHandle {
        id: DatasetID::new_seeded_ed25519(b"key"),
        alias: DatasetAlias::try_from("bar").unwrap(),
    });
    takes_ref_any(&DatasetHandle {
        id: DatasetID::new_seeded_ed25519(b"key"),
        alias: DatasetAlias::try_from("bar").unwrap(),
    });
}

#[test]
fn test_dataset_alias_eq() {
    assert_eq!(
        DatasetAlias::from_str("account/net.example.com").unwrap(),
        DatasetAlias::from_str("aCCouNt/net.ExaMplE.coM").unwrap(),
    );
    assert_eq!(
        DatasetAlias::from_str("account/net.example.com").unwrap(),
        DatasetAlias::from_str("account/net.example.com").unwrap(),
    );
    assert_eq!(
        DatasetAlias::from_str("net.example.com").unwrap(),
        DatasetAlias::from_str("net.ExaMplE.coM").unwrap(),
    );
    assert_ne!(
        DatasetAlias::from_str("account/net.example.com").unwrap(),
        DatasetAlias::from_str("aCCouNt1/net.eXamPle.cOm").unwrap(),
    );
    assert_ne!(
        DatasetAlias::from_str("account1/net.example.com").unwrap(),
        DatasetAlias::from_str("account/net.example.com").unwrap(),
    );
    assert_ne!(
        DatasetAlias::from_str("net.example.com").unwrap(),
        DatasetAlias::from_str("account/net.example.com").unwrap(),
    );
}

#[test]
fn test_dataset_remote_alias_eq() {
    assert_eq!(
        DatasetAliasRemote::from_str("repository/net.example.com").unwrap(),
        DatasetAliasRemote::from_str("repository/net.ExaMplE.coM").unwrap(),
    );
    assert_eq!(
        DatasetAliasRemote::from_str("repository/net.example.com").unwrap(),
        DatasetAliasRemote::from_str("repository/net.example.com").unwrap(),
    );
    assert_eq!(
        DatasetAliasRemote::from_str("repository/account/net.example.com").unwrap(),
        DatasetAliasRemote::from_str("repository/AccOuNt/net.ExaMplE.coM").unwrap(),
    );
    assert_eq!(
        DatasetAliasRemote::from_str("repository/net.example.com").unwrap(),
        DatasetAliasRemote::from_str("rEpoSitOry/net.ExaMplE.coM").unwrap(),
    );
    assert_ne!(
        DatasetAliasRemote::from_str("repository/account/net.example.com").unwrap(),
        DatasetAliasRemote::from_str("repository/net.example.com").unwrap(),
    );
}
