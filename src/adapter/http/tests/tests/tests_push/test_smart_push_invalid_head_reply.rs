// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_http::smart_protocol::messages::{
    DatasetPushInvalidHeadError,
    DatasetPushRequestError,
    DatasetPushResponse,
};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_invalid_head_reply_keeps_expected_and_actual_heads() {
    let cas_error = odf::dataset::RefCASError {
        reference: odf::BlockRef::Head,
        expected: Some(odf::Multihash::from_digest_sha3_256(b"planned")),
        actual: Some(odf::Multihash::from_digest_sha3_256(b"concurrent")),
    };

    let json = serde_json::to_string(&DatasetPushResponse::Err(
        DatasetPushRequestError::InvalidHead((&cas_error).into()),
    ))
    .unwrap();
    let DatasetPushResponse::Err(DatasetPushRequestError::InvalidHead(reply)) =
        serde_json::from_str::<DatasetPushResponse>(&json).unwrap()
    else {
        panic!("Unexpected reply: {json}");
    };

    assert_eq!(
        reply,
        DatasetPushInvalidHeadError {
            actual_head: cas_error.actual.clone(),
            expected_head: cas_error.expected.clone(),
        }
    );
    assert_eq!(odf::dataset::RefCASError::from(reply), cas_error);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
