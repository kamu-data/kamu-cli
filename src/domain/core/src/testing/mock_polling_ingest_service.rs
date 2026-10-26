// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ReportedHeads;
use crate::{MockPollingIngestService, PollingIngestResponse, PollingIngestResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockPollingIngestService {
    pub fn make_expect_ingest(mut self, dataset_alias: odf::DatasetAlias) -> Self {
        self.expect_ingest()
            .withf(move |target, _, _, _| target.get_alias() == &dataset_alias)
            .times(1)
            .returning(|_, _, _, _| {
                Ok(PollingIngestResponse {
                    result: PollingIngestResult::UpToDate {
                        no_source_defined: false,
                        uncacheable: false,
                    },
                    metadata_state: None,
                })
            });
        self
    }

    /// Reports `HEAD` moved between the `heads` set by the time of ingest,
    /// without moving any reference.
    pub fn make_expect_ingest_updated(
        mut self,
        dataset_alias: odf::DatasetAlias,
        heads: ReportedHeads,
    ) -> Self {
        self.expect_ingest()
            .withf(move |target, _, _, _| target.get_alias() == &dataset_alias)
            .times(1)
            .returning(move |_, metadata_state, _, _| {
                let (old_head, new_head) = heads.get();
                Ok(PollingIngestResponse {
                    result: PollingIngestResult::Updated {
                        old_head,
                        new_head,
                        has_more: false,
                        uncacheable: false,
                    },
                    metadata_state: Some(metadata_state),
                })
            });
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
