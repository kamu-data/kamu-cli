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
    TransformError,
    TransformListener,
    TransformMultiListener,
    TransformOptions,
    TransformResult,
    TransformService,
    VerificationError,
    VerificationListener,
};
use opendatafabric::{
    DatasetRef,
    MetadataBlockTyped,
    Multihash,
    SetTransform,
    Transform,
    TransformSql,
};

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TransformService {}
    #[async_trait::async_trait]
    impl TransformService for TransformService {
      async fn get_active_transform(
          &self,
          dataset_ref: &DatasetRef,
      ) -> Result<Option<(Multihash, MetadataBlockTyped<SetTransform>)>, GetDatasetError>;

      async fn transform(
          &self,
          dataset_ref: &DatasetRef,
          options: TransformOptions,
          listener: Option<Arc<dyn TransformListener>>,
      ) -> Result<TransformResult, TransformError>;

      async fn transform_multi(
          &self,
          dataset_refs: Vec<DatasetRef>,
          options: TransformOptions,
          listener: Option<Arc<dyn TransformMultiListener>>,
      ) -> Vec<(DatasetRef, Result<TransformResult, TransformError>)>;

      async fn verify_transform(
          &self,
          dataset_ref: &DatasetRef,
          block_range: (Option<Multihash>, Option<Multihash>),
          listener: Option<Arc<dyn VerificationListener>>,
      ) -> Result<(), VerificationError>;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl MockTransformService {
    pub fn without_set_transform() -> Self {
        let mut dependency_graph_repo_mock = MockTransformService::default();
        dependency_graph_repo_mock
            .expect_get_active_transform()
            .return_once(|_| Ok(None));
        dependency_graph_repo_mock
    }

    pub fn with_set_transform() -> Self {
        let mut dependency_graph_repo_mock = MockTransformService::default();
        dependency_graph_repo_mock
            .expect_get_active_transform()
            .return_once(|_| {
                Ok(Some((
                    Multihash::from_digest_sha3_256(b"a"),
                    MetadataBlockTyped {
                        system_time: Utc::now(),
                        prev_block_hash: None,
                        event: SetTransform {
                            inputs: vec![],
                            transform: Transform::Sql(TransformSql {
                                engine: "spark".to_string(),
                                version: None,
                                query: None,
                                queries: None,
                                temporal_tables: None,
                            }),
                        },
                        sequence_number: 0,
                    },
                )))
            });
        dependency_graph_repo_mock
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
