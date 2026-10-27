// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_datasets::ResolvedDatasetsMap;

use crate::engine::TransformRequestExt;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockTransformPlanner {
    /// Elaborates a transform without inputs. The request's head is a fixed
    /// placeholder: the mocked executors report the heads they commit.
    pub fn make_expect_elaborate_transform(mut self, target_alias: odf::DatasetAlias) -> Self {
        self.expect_elaborate_transform()
            .withf(move |target| target.get_alias() == &target_alias)
            .times(1)
            .returning(|target| {
                Ok(TransformElaboration::Elaborated(TransformExecutionPlan {
                    request: TransformRequestExt {
                        dataset_handle: target.get_handle().clone(),
                        block_ref: odf::BlockRef::Head,
                        head: odf::Multihash::from_digest_sha3_256(b"elaborated-head"),
                        transform: odf::metadata::Transform::Sql(odf::metadata::TransformSql {
                            engine: "mock".to_string(),
                            version: None,
                            query: None,
                            queries: Some(Vec::new()),
                            temporal_tables: None,
                        }),
                        transform_block: odf::Multihash::from_digest_sha3_256(b"set-transform"),
                        system_time: Utc::now(),
                        schema: None,
                        prev_offset: None,
                        vocab: odf::metadata::DatasetVocabulary::default(),
                        inputs: vec![],
                        prev_checkpoint: None,
                    },
                    datasets_map: ResolvedDatasetsMap::default(),
                }))
            });
        self
    }

    pub fn make_expect_elaborate_transform_up_to_date(
        mut self,
        target_alias: odf::DatasetAlias,
    ) -> Self {
        self.expect_elaborate_transform()
            .withf(move |target| target.get_alias() == &target_alias)
            .times(1)
            .returning(|_| Ok(TransformElaboration::UpToDate));
        self
    }

    /// Reports an input whose history was rewritten
    pub fn make_expect_elaborate_transform_diverged(self, target_alias: odf::DatasetAlias) -> Self {
        self.make_expect_elaborate_transform_error(target_alias, || {
            InvalidInputIntervalError {
                input_dataset: odf::DatasetHandle::new(
                    odf::DatasetID::new_seeded_ed25519(b"input"),
                    odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("input")),
                    odf::DatasetKind::Root,
                ),
                head: odf::Multihash::from_digest_sha3_256(b"head"),
                tail: odf::Multihash::from_digest_sha3_256(b"tail"),
            }
            .into()
        })
    }

    pub fn make_expect_elaborate_transform_error(
        mut self,
        target_alias: odf::DatasetAlias,
        make_error: impl Fn() -> TransformElaborateError + Send + 'static,
    ) -> Self {
        self.expect_elaborate_transform()
            .withf(move |target| target.get_alias() == &target_alias)
            .times(1)
            .returning(move |_| Err(make_error()));
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
