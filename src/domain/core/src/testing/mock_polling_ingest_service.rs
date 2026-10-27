// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ReportedHeads;
use crate::{
    MockPollingIngestService,
    PollingIngestError,
    PollingIngestResponse,
    PollingIngestResult,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockPollingIngestService {
    pub fn make_expect_ingest(mut self, dataset_alias: odf::DatasetAlias) -> Self {
        self.expect_ingest()
            .withf(move |target, _, _, _, _, _| target.get_alias() == &dataset_alias)
            .times(1)
            .returning(|_, _, _, _, _, _| {
                Ok(PollingIngestResponse {
                    result: PollingIngestResult::UpToDate { uncacheable: false },
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
            .withf(move |target, _, _, _, _, _| target.get_alias() == &dataset_alias)
            .times(1)
            .returning(move |_, plan, _, _, _, _| {
                let (old_head, new_head) = heads.get();
                Ok(PollingIngestResponse {
                    result: PollingIngestResult::Updated {
                        old_head,
                        new_head,
                        has_more: false,
                        uncacheable: false,
                        increment: odf::dataset::MetadataChainIncrementInterval {
                            num_blocks: 1,
                            num_records: 0,
                            updated_watermark: None,
                        },
                        new_source_state: None,
                    },
                    metadata_state: Some(plan.into_metadata_state()),
                })
            });
        self
    }
    /// Reports `iterations` in order, one per call
    pub fn make_expect_ingest_iterations(
        mut self,
        dataset_alias: &odf::DatasetAlias,
        iterations: Vec<MockIngestIteration>,
    ) -> Self {
        let mut seq = mockall::Sequence::new();
        for iteration in iterations {
            let dataset_alias = dataset_alias.clone();
            self.expect_ingest()
                .withf(move |target, _, _, _, _, _| target.get_alias() == &dataset_alias)
                .times(1)
                .in_sequence(&mut seq)
                .returning(move |_, plan, _, _, _, _| match &iteration {
                    MockIngestIteration::Updated {
                        heads,
                        has_more,
                        num_records,
                    } => {
                        let (old_head, new_head) = heads.get();
                        Ok(PollingIngestResponse {
                            result: PollingIngestResult::Updated {
                                old_head,
                                new_head,
                                has_more: *has_more,
                                uncacheable: false,
                                increment: odf::dataset::MetadataChainIncrementInterval {
                                    num_blocks: 1,
                                    num_records: *num_records,
                                    updated_watermark: None,
                                },
                                new_source_state: None,
                            },
                            metadata_state: Some(plan.into_metadata_state()),
                        })
                    }
                    MockIngestIteration::Unreachable => {
                        Err(PollingIngestError::unreachable("http://example.com", None))
                    }
                });
        }
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One ingest iteration that [`MockPollingIngestService`] reports
pub enum MockIngestIteration {
    Updated {
        heads: ReportedHeads,
        has_more: bool,
        num_records: u64,
    },
    Unreachable,
}

impl MockIngestIteration {
    pub fn updated(heads: &ReportedHeads, has_more: bool, num_records: u64) -> Self {
        Self::Updated {
            heads: heads.clone(),
            has_more,
            num_records,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
