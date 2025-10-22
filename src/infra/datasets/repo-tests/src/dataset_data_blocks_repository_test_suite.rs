// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_datasets::DatasetDataBlockRepository;

use crate::helpers::{init_dataset_entry, init_test_account};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Temporary test
pub async fn test_no_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-1");
    let dataset_name = odf::DatasetName::new_unchecked("test-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();
    assert!(
        !repo
            .has_blocks(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
