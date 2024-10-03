// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use internal_error::InternalError;
use kamu_core::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TransformRequestPlanner {}
    #[async_trait::async_trait]
    impl TransformRequestPlanner for TransformRequestPlanner {
        async fn get_active_transform(
            &self,
            target: ResolvedDataset,
        ) -> Result<Option<(Multihash, MetadataBlockTyped<SetTransform>)>, InternalError>;

        async fn build_transform_plan(
            &self,
            target: ResolvedDataset,
            options: &TransformOptions,
        ) -> Result<TransformPlan, TransformPlanError>;

        async fn build_transform_verification_plan(
            &self,
            target: ResolvedDataset,
            block_range: (Option<Multihash>, Option<Multihash>),
        ) -> Result<VerifyTransformOperation, VerifyTransformPlanError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockTransformRequestPlanner {
    pub fn without_set_transform() -> Self {
        let mut mock = Self::default();
        mock.expect_get_active_transform().return_once(|_| Ok(None));
        mock
    }

    pub fn with_set_transform() -> Self {
        let mut mock = Self::default();
        mock.expect_get_active_transform().return_once(|_| {
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
        mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
