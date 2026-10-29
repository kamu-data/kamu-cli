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
    DatasetPushCompleteConfirmed,
    DatasetPushCompleteError,
    DatasetPushCompleteReply,
    DatasetPushCompleteResponse,
    DatasetPushObjectsUploadProgressRequest,
    DatasetPushUpdatedConcurrentlyError,
};
use pretty_assertions::assert_eq;

// The push complete reply crosses versions in both directions, so these tests
// pin its JSON form

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_confirmation_is_read_by_clients_that_predate_the_typed_reply() {
    let json = serde_json::to_string(&DatasetPushCompleteResponse::Ok(
        DatasetPushCompleteConfirmed {},
    ))
    .unwrap();

    assert_eq!(json, r#"{"Ok":{}}"#);
    assert_matches!(
        serde_json::from_str::<DatasetPushCompleteConfirmed>(&json),
        Ok(DatasetPushCompleteConfirmed {})
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_bare_confirmation_of_servers_that_predate_the_typed_reply_is_accepted() {
    assert_matches!(
        serde_json::from_str::<DatasetPushCompleteReply>("{}"),
        Ok(DatasetPushCompleteReply::Legacy(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_internal_error_of_servers_that_predate_the_typed_reply_is_read_as_internal() {
    // What such a server sends on any failure of the complete request
    let json = r#"{"Err":{"Internal":{"phase":{"Push":"CompleteRequest"},"error_message":"Internal error"}}}"#;

    assert_matches!(
        serde_json::from_str::<DatasetPushCompleteReply>(json),
        Ok(DatasetPushCompleteReply::Response(Err(DatasetPushCompleteError::Internal(e))))
            if e.error_message == "Internal error"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_updated_concurrently_fits_a_close_frame() {
    let json = serde_json::to_string(&DatasetPushCompleteResponse::Err(
        DatasetPushCompleteError::UpdatedConcurrently(DatasetPushUpdatedConcurrentlyError {}),
    ))
    .unwrap();

    assert_eq!(json, r#"{"Err":{"UpdatedConcurrently":{}}}"#);
    assert_matches!(
        serde_json::from_str::<DatasetPushCompleteReply>(&json),
        Ok(DatasetPushCompleteReply::Response(Err(
            DatasetPushCompleteError::UpdatedConcurrently(_)
        )))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unknown_error_is_not_taken_for_a_confirmation() {
    assert_matches!(
        serde_json::from_str::<DatasetPushCompleteReply>(r#"{"Err":{"FutureError":{}}}"#),
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_progress_request_read_with_the_complete_reply_is_not_a_confirmation() {
    let json = serde_json::to_string(&DatasetPushObjectsUploadProgressRequest {
        progress_requested: true,
    })
    .unwrap();

    assert_eq!(json, r#"{"progress_requested":true}"#);
    assert_matches!(
        serde_json::from_str::<DatasetPushCompleteReply>(&json),
        Ok(DatasetPushCompleteReply::StaleProgressRequest(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_progress_request_is_read_by_clients_that_predate_its_field() {
    #[derive(serde::Deserialize, Debug)]
    struct ProgressRequestBeforeTheField {}

    assert_matches!(
        serde_json::from_str::<ProgressRequestBeforeTheField>(r#"{"progress_requested":true}"#),
        Ok(ProgressRequestBeforeTheField {})
    );
    // And the progress request of an older server is read by this one
    assert_eq!(
        serde_json::from_str::<DatasetPushObjectsUploadProgressRequest>("{}").unwrap(),
        DatasetPushObjectsUploadProgressRequest {
            progress_requested: false
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
