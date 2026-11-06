// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use database_common::testing::{
    RacingTransactionOutcome,
    in_committed_transaction,
    race_transactions,
};
use dill::Catalog;
use kamu_datasets::*;

use crate::helpers::{
    init_committed_entries,
    init_dataset_entry,
    init_test_account,
    remove_dataset_entry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_set_initial_reference(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let dataset_ref_repo = catalog.get_one::<dyn DatasetReferenceRepository>().unwrap();

    let ref_1_hash = odf::Multihash::from_digest_sha3_256(b"a");
    let ref_2_hash = odf::Multihash::from_digest_sha3_256(b"b");
    let stale_hash = odf::Multihash::from_digest_sha3_256(b"stale");

    // -- set a reference 2 relatively to a never stored reference 1 => CAS error

    let set_res = dataset_ref_repo
        .set_dataset_reference(
            &dataset_id,
            &odf::BlockRef::Head,
            Some(&ref_1_hash),
            &ref_2_hash,
        )
        .await;
    assert_matches!(
        set_res,
        Err(SetDatasetReferenceError::CASFailed(e))
        if e.dataset_id == dataset_id &&
           e.block_ref == odf::BlockRef::Head &&
           e.expected_prev_block_hash == Some(ref_1_hash.clone()) &&
           e.actual_prev_block_hash.is_none(),
        "missing reference expecting a hash"
    );
    let get_all_res = dataset_ref_repo
        .get_all_dataset_references(&dataset_id)
        .await;
    assert_matches!(
        get_all_res,
        Ok(v) if v.is_empty(),
        "nothing stored after a CAS error on a missing reference"
    );

    // -- set a reference 1 relatively to None

    let set_res = dataset_ref_repo
        .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &ref_1_hash)
        .await;
    assert_matches!(set_res, Ok(()));

    // -- set a reference 2 also relatively to None => CAS error

    let set_res = dataset_ref_repo
        .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &ref_2_hash)
        .await;
    assert_matches!(
        set_res,
        Err(SetDatasetReferenceError::CASFailed(e))
        if e.dataset_id == dataset_id &&
           e.block_ref == odf::BlockRef::Head &&
           e.expected_prev_block_hash.is_none() &&
           e.actual_prev_block_hash == Some(ref_1_hash.clone()),
        "existing reference expecting none"
    );

    // -- set a reference 2 relatively to a hash it does not point at => CAS error

    let set_res = dataset_ref_repo
        .set_dataset_reference(
            &dataset_id,
            &odf::BlockRef::Head,
            Some(&stale_hash),
            &ref_2_hash,
        )
        .await;
    assert_matches!(
        set_res,
        Err(SetDatasetReferenceError::CASFailed(e))
        if e.dataset_id == dataset_id &&
           e.block_ref == odf::BlockRef::Head &&
           e.expected_prev_block_hash == Some(stale_hash.clone()) &&
           e.actual_prev_block_hash == Some(ref_1_hash.clone()),
        "existing reference expecting a stale hash"
    );

    // -- observe the changes: they should have stuck at reference 1

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
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"some-id");
    let dataset_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_1_id = odf::DatasetID::new_seeded_ed25519(b"some-id-1");
    let dataset_1_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
        &test_account_name,
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
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_1_id = odf::DatasetID::new_seeded_ed25519(b"some-id-1");
    let dataset_1_name = odf::DatasetName::new_unchecked("foo");
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
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
        &test_account_name,
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

    remove_dataset_entry(catalog, &dataset_1_id).await;

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

/// Two transactions move `HEAD` from the same block: the one that commits
/// first wins, the other never commits
pub async fn test_update_reference_in_concurrent_transactions(catalog: &Catalog) {
    let hash_a = odf::Multihash::from_digest_sha3_256(b"a");
    let hash_b = odf::Multihash::from_digest_sha3_256(b"b");
    let hash_c = odf::Multihash::from_digest_sha3_256(b"c");

    let dataset_id = init_racing_dataset(catalog, Some(&hash_a)).await;

    let outcome = race_head_updates(catalog, &dataset_id, Some(&hash_a), &hash_b, &hash_c).await;

    assert_second_head_rejected(outcome, &hash_b);
    pretty_assertions::assert_eq!(committed_head(catalog, &dataset_id).await, Some(hash_b));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Two transactions set the first `HEAD` of a dataset: the one that commits
/// first wins, the other never commits
pub async fn test_set_initial_reference_in_concurrent_transactions(catalog: &Catalog) {
    let hash_a = odf::Multihash::from_digest_sha3_256(b"a");
    let hash_b = odf::Multihash::from_digest_sha3_256(b"b");

    let dataset_id = init_racing_dataset(catalog, None).await;

    let outcome = race_head_updates(catalog, &dataset_id, None, &hash_a, &hash_b).await;

    assert_second_head_rejected(outcome, &hash_a);
    pretty_assertions::assert_eq!(committed_head(catalog, &dataset_id).await, Some(hash_a));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset with a committed `HEAD` at `maybe_head`, if given
async fn init_racing_dataset(
    catalog: &Catalog,
    maybe_head: Option<&odf::Multihash>,
) -> odf::DatasetID {
    let (_, entries) = init_committed_entries(catalog, &[("foo", odf::DatasetKind::Root)]).await;
    let dataset_id = entries[0].id.clone();

    if let Some(head) = maybe_head {
        let head_dataset_id = dataset_id.clone();
        in_committed_transaction(catalog, |catalog| async move {
            set_head(&catalog, &head_dataset_id, None, head)
                .await
                .unwrap();
        })
        .await;
    }

    dataset_id
}

/// The second writer never commits; a rejected write reports the first
/// writer's `HEAD` as the actual one
fn assert_second_head_rejected(
    outcome: RacingTransactionOutcome<SetDatasetReferenceError>,
    first_head: &odf::Multihash,
) {
    match outcome {
        RacingTransactionOutcome::FailedAtWrite(SetDatasetReferenceError::CASFailed(e)) => {
            pretty_assertions::assert_eq!(e.actual_prev_block_hash.as_ref(), Some(first_head));
        }
        RacingTransactionOutcome::FailedAtCommit(_) => {}
        RacingTransactionOutcome::FailedAtWrite(SetDatasetReferenceError::Internal(e)) => {
            panic!("Second write failed with an internal error: {e:?}")
        }
        RacingTransactionOutcome::Committed => panic!("Second transaction committed"),
    }
}

async fn race_head_updates(
    catalog: &Catalog,
    dataset_id: &odf::DatasetID,
    expected_head: Option<&odf::Multihash>,
    first_head: &odf::Multihash,
    second_head: &odf::Multihash,
) -> RacingTransactionOutcome<SetDatasetReferenceError> {
    let second_dataset_id = dataset_id.clone();
    let second_expected_head = expected_head.cloned();
    let second_head = second_head.clone();

    race_transactions(
        catalog,
        |catalog| async move {
            set_head(&catalog, dataset_id, expected_head, first_head)
                .await
                .unwrap();
        },
        move |catalog| async move {
            set_head(
                &catalog,
                &second_dataset_id,
                second_expected_head.as_ref(),
                &second_head,
            )
            .await
        },
    )
    .await
}

async fn set_head(
    catalog: &Catalog,
    dataset_id: &odf::DatasetID,
    expected_head: Option<&odf::Multihash>,
    head: &odf::Multihash,
) -> Result<(), SetDatasetReferenceError> {
    catalog
        .get_one::<dyn DatasetReferenceRepository>()
        .unwrap()
        .set_dataset_reference(dataset_id, &odf::BlockRef::Head, expected_head, head)
        .await
}

async fn committed_head(catalog: &Catalog, dataset_id: &odf::DatasetID) -> Option<odf::Multihash> {
    in_committed_transaction(catalog, |catalog| async move {
        match catalog
            .get_one::<dyn DatasetReferenceRepository>()
            .unwrap()
            .get_dataset_reference(dataset_id, &odf::BlockRef::Head)
            .await
        {
            Ok(head) => Some(head),
            Err(GetDatasetReferenceError::NotFound(_)) => None,
            Err(GetDatasetReferenceError::Internal(e)) => panic!("{e}"),
        }
    })
    .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
