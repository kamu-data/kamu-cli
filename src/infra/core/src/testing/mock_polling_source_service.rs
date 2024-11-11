// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use kamu_core::{
    GetDatasetError,
    PollingIngestError,
    PollingIngestListener,
    PollingIngestOptions,
    PollingIngestResult,
    PollingIngestService,
    ResolvedDataset,
};
use opendatafabric::{
    FetchStep,
    FetchStepUrl,
    MergeStrategy,
    MergeStrategyAppend,
    MetadataBlockTyped,
    Multihash,
    ReadStep,
    ReadStepJson,
    SetPollingSource,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub PollingIngestService {}
    #[async_trait::async_trait]
    impl PollingIngestService for PollingIngestService {
        async fn get_active_polling_source(
            &self,
            target: ResolvedDataset,
        ) -> Result<Option<(Multihash, MetadataBlockTyped<SetPollingSource>)>, GetDatasetError>;

        async fn ingest(
            &self,
            target: ResolvedDataset,
            options: PollingIngestOptions,
            listener: Option<Arc<dyn PollingIngestListener>>,
        ) -> Result<PollingIngestResult, PollingIngestError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockPollingIngestService {
    pub fn without_active_polling_source() -> Self {
        let mut dependency_graph_repo_mock = MockPollingIngestService::default();
        dependency_graph_repo_mock
            .expect_get_active_polling_source()
            .returning(|_| Ok(None));
        dependency_graph_repo_mock
    }

    pub fn with_active_polling_source() -> Self {
        let mut dependency_graph_repo_mock = MockPollingIngestService::default();
        dependency_graph_repo_mock
            .expect_get_active_polling_source()
            .returning(|_| {
                Ok(Some((
                    Multihash::from_digest_sha3_256(b"a"),
                    MetadataBlockTyped {
                        system_time: Utc::now(),
                        prev_block_hash: None,
                        event: SetPollingSource {
                            fetch: FetchStep::Url(FetchStepUrl {
                                url: "http://foo".to_string(),
                                event_time: None,
                                cache: None,
                                headers: None,
                            }),
                            prepare: None,
                            read: ReadStep::Json(ReadStepJson {
                                sub_path: None,
                                schema: None,
                                date_format: None,
                                encoding: None,
                                timestamp_format: None,
                            }),
                            preprocess: None,
                            merge: MergeStrategy::Append(MergeStrategyAppend {}),
                        },
                        sequence_number: 0,
                    },
                )))
            });
        dependency_graph_repo_mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
