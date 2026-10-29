// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_adapter_http::smart_protocol::messages::{
    DatasetPushObjectsTransferError,
    DatasetPushObjectsTransferNameCollisionError,
    DatasetPushObjectsTransferResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_name_collision_reply_for_a_long_alias_is_read_back() {
    let dataset_alias = odf::DatasetAlias::new(
        Some(odf::AccountName::new_unchecked(
            "an-account-with-a-rather-long-name",
        )),
        odf::DatasetName::new_unchecked(
            "a-dataset-whose-name-alone-is-longer-than-a-close-frame-allows",
        ),
    );

    let json = serde_json::to_string(&DatasetPushObjectsTransferResponse::Err(
        DatasetPushObjectsTransferError::NameCollision(
            DatasetPushObjectsTransferNameCollisionError {
                dataset_alias: dataset_alias.clone(),
            },
        ),
    ))
    .unwrap();

    // Too long for a close frame reason, so the server sends it as a message
    assert!(json.len() > 123, "{json}");
    assert_matches!(
        serde_json::from_str::<DatasetPushObjectsTransferResponse>(&json),
        Ok(Err(DatasetPushObjectsTransferError::NameCollision(e)))
            if e.dataset_alias == dataset_alias
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
