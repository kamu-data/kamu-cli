// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_datasets::{UpdateCollectionEntriesResult, UpdateCollectionEntriesUseCaseError};
use odf::metadata::OperationType as Op;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::collection_base_use_case_harness::CollectionBaseUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_commits_diff_in_one_block_in_order() {
    let harness = CollectionBaseUseCaseHarness::new();
    let [foo, bar, baz] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;
    let initial_chain_len = harness.chain_len(&collection).await;

    let res = harness
        .update_success(
            &collection,
            vec![
                harness.add("/a", &foo),
                harness.add("/b", &bar),
                harness.add("/c", &baz),
            ],
        )
        .await;

    assert_eq!(
        res.inserted_records,
        vec![
            (Op::Append, harness.record("/a", &foo)),
            (Op::Append, harness.record("/b", &bar)),
            (Op::Append, harness.record("/c", &baz)),
        ]
    );
    assert_eq!(harness.chain_len(&collection).await, initial_chain_len + 1);

    // Re-adding `/a` retracts the old entry before appending the new one, moving `/c`
    // onto `/b` retracts both paths before appending `/b`
    let res = harness
        .update_success(
            &collection,
            vec![harness.add("/a", &bar), harness.mv("/c", "/b")],
        )
        .await;

    assert_eq!(
        res.inserted_records,
        vec![
            (Op::Retract, harness.record("/a", &foo)),
            (Op::Append, harness.record("/a", &bar)),
            (Op::Retract, harness.record("/b", &bar)),
            (Op::Retract, harness.record("/c", &baz)),
            (Op::Append, harness.record("/b", &baz)),
        ]
    );
    assert_eq!(harness.chain_len(&collection).await, initial_chain_len + 2);

    assert_eq!(
        harness.ledger(&collection).await,
        vec![
            (0, Op::Append, "/a".to_string(), foo.clone()),
            (1, Op::Append, "/b".to_string(), bar.clone()),
            (2, Op::Append, "/c".to_string(), baz.clone()),
            (3, Op::Retract, "/a".to_string(), foo.clone()),
            (4, Op::Append, "/a".to_string(), bar.clone()),
            (5, Op::Retract, "/b".to_string(), bar.clone()),
            (6, Op::Retract, "/c".to_string(), baz.clone()),
            (7, Op::Append, "/b".to_string(), baz.clone()),
        ]
    );

    assert_eq!(
        harness.entries(&collection).await,
        vec![harness.record("/a", &bar), harness.record("/b", &baz)]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_several_changes_to_same_path_in_one_update() {
    let harness = CollectionBaseUseCaseHarness::new();
    let [foo, bar, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add("/a", &foo),
                harness.mv("/a", "/b"),
                harness.add("/a", &bar),
            ],
        )
        .await;

    assert_eq!(
        harness.ledger(&collection).await,
        vec![
            (0, Op::Append, "/a".to_string(), foo.clone()),
            (1, Op::Retract, "/a".to_string(), foo.clone()),
            (2, Op::Append, "/b".to_string(), foo.clone()),
            (3, Op::Append, "/a".to_string(), bar.clone()),
        ]
    );

    assert_eq!(
        harness.entries(&collection).await,
        vec![harness.record("/a", &bar), harness.record("/b", &foo)]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_move_in_place_corrects_extra_data() {
    let harness = CollectionBaseUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness
        .create_collection(vec![odf::schema::DataField::string("tag").optional()])
        .await;

    harness
        .update_success(
            &collection,
            vec![harness.add_with("/a", &foo, json!({"tag": "x"}))],
        )
        .await;

    let res = harness
        .update_success(
            &collection,
            vec![harness.mv_with("/a", "/a", json!({"tag": "y"}))],
        )
        .await;

    assert_eq!(
        res.inserted_records,
        vec![
            (
                Op::CorrectFrom,
                harness.record_with("/a", &foo, json!({"tag": "x"}))
            ),
            (
                Op::CorrectTo,
                harness.record_with("/a", &foo, json!({"tag": "y"}))
            ),
        ]
    );
    assert_eq!(
        harness.entries(&collection).await,
        vec![harness.record_with("/a", &foo, json!({"tag": "y"}))]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_without_changes_is_up_to_date() {
    let harness = CollectionBaseUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(&collection, vec![harness.add("/a", &foo)])
        .await;
    let chain_len = harness.chain_len(&collection).await;

    // Identical entry, a path that does not exist, and no operations at all
    for operations in [
        vec![harness.add("/a", &foo)],
        vec![harness.remove("/missing")],
        vec![],
    ] {
        let res = harness.update(&collection, None, operations, None).await;
        assert_matches!(res, Ok(UpdateCollectionEntriesResult::UpToDate));
    }

    assert_eq!(harness.chain_len(&collection).await, chain_len);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_move_from_missing_path_is_not_found() {
    let harness = CollectionBaseUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;
    let chain_len = harness.chain_len(&collection).await;

    // The valid operation before the failing one is not committed either
    let res = harness
        .update(
            &collection,
            None,
            vec![harness.add("/a", &foo), harness.mv("/missing", "/b")],
            None,
        )
        .await;

    assert_matches!(
        res,
        Ok(UpdateCollectionEntriesResult::NotFound(e)) if e.path.as_str() == "/missing"
    );
    assert_eq!(harness.chain_len(&collection).await, chain_len);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_collection_stale_expected_head_fails() {
    let harness = CollectionBaseUseCaseHarness::new();
    let [foo, bar, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    let res = harness
        .update_success(&collection, vec![harness.add("/a", &foo)])
        .await;
    let stale_head = res.old_head;

    let res = harness
        .update(
            &collection,
            None,
            vec![harness.add("/b", &bar)],
            Some(stale_head),
        )
        .await;

    assert_matches!(
        res,
        Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(_))
    );
    assert_eq!(
        harness.entries(&collection).await,
        vec![harness.record("/a", &foo)]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
