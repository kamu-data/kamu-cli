// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_error_enum! {
    /// The reset cannot be made; its heads come from the flow's configuration,
    /// so a retry fails the same way
    pub enum TaskErrorDatasetReset {
        ResetHeadNotFound,
        OldHeadMismatch(DatasetResetOldHeadMismatch),
    }
    => "ResetDatasetError", recoverable: false
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `HEAD` is not the head the reset expects
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatasetResetOldHeadMismatch {
    pub expected_head: odf::Multihash,
    pub actual_head: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_old_head_mismatch_task_error_is_unrecoverable_and_stable() {
        let expected_head = odf::Multihash::from_digest_sha3_256(b"expected");
        let actual_head = odf::Multihash::from_digest_sha3_256(b"actual");

        let task_error = TaskErrorDatasetReset::OldHeadMismatch(DatasetResetOldHeadMismatch {
            expected_head: expected_head.clone(),
            actual_head: actual_head.clone(),
        })
        .into_task_error();

        assert!(!task_error.recoverable);
        assert_eq!(
            json!({
                "ResetDatasetError": {
                    "OldHeadMismatch": {
                        "expected_head": expected_head.to_string(),
                        "actual_head": actual_head.to_string(),
                    }
                },
                "recoverable": false,
            }),
            serde_json::to_value(&task_error).unwrap(),
        );
    }

    #[test]
    fn test_reset_head_not_found_task_error_is_unrecoverable_and_stable() {
        let task_error = TaskErrorDatasetReset::ResetHeadNotFound.into_task_error();

        assert!(!task_error.recoverable);
        assert_eq!(
            json!({
                "ResetDatasetError": "ResetHeadNotFound",
                "recoverable": false,
            }),
            serde_json::to_value(&task_error).unwrap(),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
