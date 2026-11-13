// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::PullResultUpToDate;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_result_enum! {
    pub enum TaskResultDatasetUpdate {
        /// `HEAD` moved; the increment is the data between the old and the new
        /// head
        Updated {
            old_head: Option<odf::Multihash>,
            new_head: odf::Multihash,
            has_more: bool,
            increment: odf::dataset::MetadataChainIncrementInterval,
        },
        UpToDate(PullResultUpToDate),
    }
    => "UpdateDatasetResult"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_core::PollingIngestResultUpToDate;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_updated_result_json_is_stable() {
        let old_head = odf::Multihash::from_digest_sha3_256(b"old");
        let new_head = odf::Multihash::from_digest_sha3_256(b"new");
        let result = TaskResultDatasetUpdate::Updated {
            old_head: Some(old_head.clone()),
            new_head: new_head.clone(),
            has_more: true,
            increment: odf::dataset::MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 10,
                updated_watermark: None,
            },
        }
        .into_task_result();

        assert_eq!(result.result_type, "UpdateDatasetResult");
        assert_eq!(
            result.payload,
            json!({
                "Updated": {
                    "old_head": old_head.to_string(),
                    "new_head": new_head.to_string(),
                    "has_more": true,
                    "increment": {
                        "num_blocks": 2,
                        "num_records": 10,
                        "updated_watermark": null,
                    },
                }
            })
        );
        assert_eq!(
            TaskResultDatasetUpdate::from_task_result(&result).unwrap(),
            TaskResultDatasetUpdate::Updated {
                old_head: Some(old_head),
                new_head,
                has_more: true,
                increment: odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 2,
                    num_records: 10,
                    updated_watermark: None,
                },
            }
        );
    }

    #[test]
    fn test_up_to_date_result_json_is_stable() {
        let result = TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::PollingIngest(
            PollingIngestResultUpToDate { uncacheable: true },
        ))
        .into_task_result();

        assert_eq!(
            result.payload,
            json!({ "UpToDate": { "PollingIngest": { "uncacheable": true } } })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
