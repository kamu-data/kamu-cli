// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_molecule_domain::{
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryExtraData,
};
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CHANGE_BY: &str = "did:ethr:0x43f3F090af7fF638ad0EfD64c5354B6945fE75BC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_extra_data_with_null_columns() {
    let extra_data = MoleculeVersionedFileEntryExtraData::try_from_fields(
        serde_json::from_value(json!({
            "categories": null,
            "content_text": null,
            "description": null,
            "encryption_metadata": null,
            "molecule_access_level": "public",
            "molecule_change_by": CHANGE_BY,
            "tags": null,
        }))
        .unwrap(),
    )
    .unwrap();

    pretty_assertions::assert_eq!(
        *extra_data.basic_info,
        MoleculeVersionedFileEntryBasicInfo {
            access_level: "public".to_string(),
            change_by: CHANGE_BY.to_string(),
            description: None,
            categories: Vec::new(),
            tags: Vec::new(),
        }
    );
}

#[test]
fn test_extra_data_with_missing_columns() {
    let extra_data = MoleculeVersionedFileEntryExtraData::try_from_fields(
        serde_json::from_value(json!({
            "molecule_access_level": "public",
            "molecule_change_by": CHANGE_BY,
        }))
        .unwrap(),
    )
    .unwrap();

    pretty_assertions::assert_eq!(
        *extra_data.basic_info,
        MoleculeVersionedFileEntryBasicInfo {
            access_level: "public".to_string(),
            change_by: CHANGE_BY.to_string(),
            description: None,
            categories: Vec::new(),
            tags: Vec::new(),
        }
    );
}

#[test]
fn test_extra_data_with_null_access_level_and_change_by() {
    let extra_data = MoleculeVersionedFileEntryExtraData::try_from_fields(
        serde_json::from_value(json!({
            "molecule_access_level": null,
            "molecule_change_by": null,
        }))
        .unwrap(),
    )
    .unwrap();

    pretty_assertions::assert_eq!(
        *extra_data.basic_info,
        MoleculeVersionedFileEntryBasicInfo::default()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
