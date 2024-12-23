// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::{
    DataWriterMetadataState,
    PollingIngestError,
    PollingIngestListener,
    PollingIngestOptions,
    PollingIngestResult,
    PollingIngestService,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub PollingIngestService {}

    #[async_trait::async_trait]
    impl PollingIngestService for PollingIngestService {
        async fn ingest(
            &self,
            target: ResolvedDataset,
            metadata_state: Box<DataWriterMetadataState>,
            options: PollingIngestOptions,
            listener: Option<Arc<dyn PollingIngestListener>>,
        ) -> Result<PollingIngestResult, PollingIngestError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockPollingIngestService {
    pub fn make_expect_ingest(mut self, dataset_alias: odf::DatasetAlias) -> Self {
        self.expect_ingest()
            .withf(move |target, _, _, _| target.get_alias() == &dataset_alias)
            .times(1)
            .returning(|_, _, _, _| {
                Ok(PollingIngestResult::UpToDate {
                    no_source_defined: false,
                    uncacheable: false,
                })
            });
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
