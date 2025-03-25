// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use dill::Catalog;
use kamu_datasets::*;

use crate::helpers::{init_dataset_entry, init_test_account};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_initial_reference(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_ref_repo = catalog.get_one::<dyn DatasetReferenceRepository>().unwrap();

    let ref_hash = odf::Multihash::from_digest_sha3_256(b"a");

    // -- no reference stored initially

    let has_result = dataset_ref_repo.has_any_references().await;
    assert_matches!(has_result, Ok(false));

    let get_res = dataset_ref_repo
        .get_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_res, Err(GetDatasetReferenceError::NotFound(e)) if e.dataset_id == dataset_id && e.block_ref == odf::BlockRef::Head);

    let get_all_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_id)
        .await;
    assert_matches!(get_all_res, Ok(v) if v.is_empty());

    // -- set a reference

    let set_res = dataset_ref_repo
        .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &ref_hash)
        .await;
    assert_matches!(set_res, Ok(()));

    // -- observe the changes

    let has_result = dataset_ref_repo.has_any_references().await;
    assert_matches!(has_result, Ok(true));

    let get_res = dataset_ref_repo
        .get_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_res, Ok(stored_ref_hash) if stored_ref_hash == ref_hash);

    let get_all_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_id)
        .await;
    assert_matches!(get_all_res, Ok(v) if v == vec![(odf::BlockRef::Head, ref_hash)]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_reference_without_cas_violation(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_ref_repo = catalog.get_one::<dyn DatasetReferenceRepository>().unwrap();

    let ref_1_hash = odf::Multihash::from_digest_sha3_256(b"a");
    let ref_2_hash = odf::Multihash::from_digest_sha3_256(b"b");

    // -- set a reference 1 relatively to None

    let set_res = dataset_ref_repo
        .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &ref_1_hash)
        .await;
    assert_matches!(set_res, Ok(()));

    // -- set a reference 2 relatively to reference 1

    let set_res = dataset_ref_repo
        .set_dataset_reference(
            &dataset_id,
            &odf::BlockRef::Head,
            Some(&ref_1_hash),
            &ref_2_hash,
        )
        .await;
    assert_matches!(set_res, Ok(()));

    // -- observe the changes

    let get_res = dataset_ref_repo
        .get_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_res, Ok(stored_ref_hash) if stored_ref_hash == ref_2_hash);

    let get_all_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_id)
        .await;
    assert_matches!(get_all_res, Ok(v) if v == vec![(odf::BlockRef::Head, ref_2_hash)]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_reference_provoke_cas_violation(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_ref_repo = catalog.get_one::<dyn DatasetReferenceRepository>().unwrap();

    let ref_1_hash = odf::Multihash::from_digest_sha3_256(b"a");
    let ref_2_hash = odf::Multihash::from_digest_sha3_256(b"b");

    // -- set a reference 1 relatively to None

    let set_res = dataset_ref_repo
        .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &ref_1_hash)
        .await;
    assert_matches!(set_res, Ok(()));

    // -- set a reference 2 also relatively to None => should provoke CAS error

    let set_res = dataset_ref_repo
        .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &ref_2_hash)
        .await;
    assert_matches!(
        set_res,
        Err(SetDatasetReferenceError::CASFailed(e))
        if e.dataset_id == dataset_id &&
           e.block_ref == odf::BlockRef::Head &&
           e.expected_prev_block_hash.is_none() &&
           e.actual_prev_block_hash == Some(ref_1_hash.clone())
    );

    // -- observe the changes: they should have stucked at reference 1

    let get_res = dataset_ref_repo
        .get_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_res, Ok(stored_ref_hash) if stored_ref_hash == ref_1_hash);

    let get_all_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_id)
        .await;
    assert_matches!(get_all_res, Ok(v) if v == vec![(odf::BlockRef::Head, ref_1_hash)]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_and_remove_reference(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_ref_repo = catalog.get_one::<dyn DatasetReferenceRepository>().unwrap();

    let ref_hash = odf::Multihash::from_digest_sha3_256(b"a");

    // -- try removing ref that doesn't exist yet
    let remove_res = dataset_ref_repo
        .remove_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(remove_res, Err(RemoveDatasetReferenceError::NotFound(e))
        if e.dataset_id == dataset_id && e.block_ref == odf::BlockRef::Head
    );

    // -- set a reference relatively to None

    let set_res = dataset_ref_repo
        .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &ref_hash)
        .await;
    assert_matches!(set_res, Ok(()));

    // -- remove reference -> now it should pass

    let remove_res = dataset_ref_repo
        .remove_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(remove_res, Ok(()));

    // -- can't remove again

    let remove_res = dataset_ref_repo
        .remove_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(remove_res, Err(RemoveDatasetReferenceError::NotFound(e))
        if e.dataset_id == dataset_id && e.block_ref == odf::BlockRef::Head
    );

    // -- should not see reference

    let get_res = dataset_ref_repo
        .get_dataset_reference(&dataset_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_res, Err(GetDatasetReferenceError::NotFound(e)) if e.dataset_id == dataset_id && e.block_ref == odf::BlockRef::Head);

    let get_all_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_id)
        .await;
    assert_matches!(get_all_res, Ok(v) if v.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_multiple_datasets(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_1_id = odf::DatasetID::new_seeded_ed25519(b"some-id-1");
    let dataset_1_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_1_id,
        &dataset_1_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_2_id = odf::DatasetID::new_seeded_ed25519(b"some-id-2");
    let dataset_2_name = odf::DatasetName::new_unchecked("bar");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_2_id,
        &dataset_2_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_ref_repo = catalog.get_one::<dyn DatasetReferenceRepository>().unwrap();

    let ref_1_hash = odf::Multihash::from_digest_sha3_256(b"a");
    let ref_2_hash = odf::Multihash::from_digest_sha3_256(b"b");

    // -- no reference stored initially

    let has_result = dataset_ref_repo.has_any_references().await;
    assert_matches!(has_result, Ok(false));

    let get_1_res = dataset_ref_repo
        .get_dataset_reference(&dataset_1_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_1_res, Err(GetDatasetReferenceError::NotFound(e)) if e.dataset_id == dataset_1_id && e.block_ref == odf::BlockRef::Head);

    let get_2_res = dataset_ref_repo
        .get_dataset_reference(&dataset_2_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_2_res, Err(GetDatasetReferenceError::NotFound(e)) if e.dataset_id == dataset_2_id && e.block_ref == odf::BlockRef::Head);

    let get_all_1_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_1_id)
        .await;
    assert_matches!(get_all_1_res, Ok(v) if v.is_empty());

    let get_all_2_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_2_id)
        .await;
    assert_matches!(get_all_2_res, Ok(v) if v.is_empty());

    // -- set a reference for each dataset

    let set_1_res = dataset_ref_repo
        .set_dataset_reference(&dataset_1_id, &odf::BlockRef::Head, None, &ref_1_hash)
        .await;
    assert_matches!(set_1_res, Ok(()));

    let set_2_res = dataset_ref_repo
        .set_dataset_reference(&dataset_2_id, &odf::BlockRef::Head, None, &ref_2_hash)
        .await;
    assert_matches!(set_2_res, Ok(()));

    // -- observe the changes

    let has_result = dataset_ref_repo.has_any_references().await;
    assert_matches!(has_result, Ok(true));

    let get_1_res = dataset_ref_repo
        .get_dataset_reference(&dataset_1_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_1_res, Ok(stored_ref_hash) if stored_ref_hash == ref_1_hash);

    let get_2_res = dataset_ref_repo
        .get_dataset_reference(&dataset_2_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_2_res, Ok(stored_ref_hash) if stored_ref_hash == ref_2_hash);

    let get_all_1_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_1_id)
        .await;
    assert_matches!(get_all_1_res, Ok(v) if v == vec![(odf::BlockRef::Head, ref_1_hash)]);

    let get_all_2_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_2_id)
        .await;
    assert_matches!(get_all_2_res, Ok(v) if v == vec![(odf::BlockRef::Head, ref_2_hash)]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_reacts_to_dataset_removals(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;

    let dataset_1_id = odf::DatasetID::new_seeded_ed25519(b"some-id-1");
    let dataset_1_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_1_id,
        &dataset_1_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_2_id = odf::DatasetID::new_seeded_ed25519(b"some-id-2");
    let dataset_2_name = odf::DatasetName::new_unchecked("bar");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_2_id,
        &dataset_2_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_ref_repo = catalog.get_one::<dyn DatasetReferenceRepository>().unwrap();

    let ref_1_hash = odf::Multihash::from_digest_sha3_256(b"a");
    let ref_2_hash = odf::Multihash::from_digest_sha3_256(b"b");

    // -- set a reference for each dataset

    let set_1_res = dataset_ref_repo
        .set_dataset_reference(&dataset_1_id, &odf::BlockRef::Head, None, &ref_1_hash)
        .await;
    assert_matches!(set_1_res, Ok(()));

    let set_2_res = dataset_ref_repo
        .set_dataset_reference(&dataset_2_id, &odf::BlockRef::Head, None, &ref_2_hash)
        .await;
    assert_matches!(set_2_res, Ok(()));

    // -- trigger removal for dataset 1

    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
    dataset_entry_repo
        .delete_dataset_entry(&dataset_1_id)
        .await
        .unwrap();

    // -- observe we only see dataset 2 refs now

    let has_result = dataset_ref_repo.has_any_references().await;
    assert_matches!(has_result, Ok(true));

    let get_1_res = dataset_ref_repo
        .get_dataset_reference(&dataset_1_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_1_res, Err(GetDatasetReferenceError::NotFound(e)) if e.dataset_id == dataset_1_id && e.block_ref == odf::BlockRef::Head);

    let get_2_res = dataset_ref_repo
        .get_dataset_reference(&dataset_2_id, &odf::BlockRef::Head)
        .await;
    assert_matches!(get_2_res, Ok(stored_ref_hash) if stored_ref_hash == ref_2_hash);

    let get_all_1_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_1_id)
        .await;
    assert_matches!(get_all_1_res, Ok(v) if v.is_empty());

    let get_all_2_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_2_id)
        .await;
    assert_matches!(get_all_2_res, Ok(v) if v == vec![(odf::BlockRef::Head, ref_2_hash)]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
