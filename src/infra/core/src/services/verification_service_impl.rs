// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use odf::dataset::{MetadataChainImpl, MetadataChainReferenceRepositoryImpl};
use odf::storage::inmem::{NamedObjectRepositoryInMemory, ObjectRepositoryInMemory};
use odf::storage::{MetadataBlockRepositoryImpl, ReferenceRepositoryImpl};

use crate::utils::cached_object::CachedObject;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VerificationServiceImpl {
    transform_request_planner: Arc<dyn TransformRequestPlanner>,
    transform_executor: Arc<dyn TransformExecutor>,
}

#[component(pub)]
#[interface(dyn VerificationService)]
impl VerificationServiceImpl {
    pub fn new(
        transform_request_planner: Arc<dyn TransformRequestPlanner>,
        transform_executor: Arc<dyn TransformExecutor>,
    ) -> Self {
        Self {
            transform_request_planner,
            transform_executor,
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn check_data_integrity(
        &self,
        resolved_dataset: &ResolvedDataset,
        dataset_kind: odf::DatasetKind,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
        check_logical_hashes: bool,
        listener: Arc<dyn VerificationListener>,
    ) -> Result<(), VerificationError> {
        let chain = resolved_dataset.as_metadata_chain();

        let head = match block_range.1 {
            None => chain.resolve_ref(&odf::BlockRef::Head).await?,
            Some(hash) => hash,
        };
        let tail = block_range.0;

        // TODO: Avoid collecting and stream instead, perhaps use nonce for `num_blocks`
        // estimate
        use futures::TryStreamExt;
        use odf::dataset::MetadataChainExt;
        let plan: Vec<_> = chain
            .iter_blocks_interval(&head, tail.as_ref(), false)
            .filter_data_stream_blocks()
            .try_collect()
            .await?;

        let num_blocks = plan.len();

        listener.begin_phase(VerificationPhase::DataIntegrity);

        for (block_index, (block_hash, block)) in plan.into_iter().enumerate() {
            listener.begin_block(
                &block_hash,
                block_index,
                num_blocks,
                VerificationPhase::DataIntegrity,
            );

            if let Some(output_slice) = &block.event.new_data {
                // Check size first
                let size_actual = resolved_dataset
                    .as_data_repo()
                    .get_size(&output_slice.physical_hash)
                    .await
                    .int_err()?;

                if size_actual != output_slice.size {
                    return Err(VerificationError::DataDoesNotMatchMetadata(
                        DataDoesNotMatchMetadata {
                            block_hash,
                            error: DataVerificationError::SizeMismatch {
                                expected: output_slice.size,
                                actual: size_actual,
                            },
                        },
                    ));
                }

                let data_hashing_helper = CachedObject::from(
                    &output_slice.physical_hash,
                    resolved_dataset.as_data_repo(),
                )
                .await?;

                // Do a fast pass using physical hash
                let physical_hash_actual = data_hashing_helper.physical_hash().await.int_err()?;
                if physical_hash_actual != output_slice.physical_hash {
                    // Root data files are non-reproducible by definition, so
                    // if physical hashes don't match - we can give up right away.
                    if dataset_kind == odf::DatasetKind::Root {
                        return Err(VerificationError::DataDoesNotMatchMetadata(
                            DataDoesNotMatchMetadata {
                                block_hash,
                                error: DataVerificationError::PhysicalHashMismatch {
                                    expected: output_slice.physical_hash.clone(),
                                    actual: physical_hash_actual,
                                },
                            },
                        ));
                    } else if check_logical_hashes {
                        // Derivative data may be replayed and produce different binary file
                        // but data must have same logical hash to be valid.
                        let logical_hash_actual =
                            data_hashing_helper.logical_hash().await.int_err()?;

                        if logical_hash_actual != output_slice.logical_hash {
                            return Err(VerificationError::DataDoesNotMatchMetadata(
                                DataDoesNotMatchMetadata {
                                    block_hash,
                                    error: DataVerificationError::LogicalHashMismatch {
                                        expected: output_slice.logical_hash.clone(),
                                        actual: logical_hash_actual,
                                    },
                                },
                            ));
                        }
                    }
                }

                if let Some(checkpoint) = block.event.new_checkpoint {
                    // Check size
                    let size_actual = resolved_dataset
                        .as_checkpoint_repo()
                        .get_size(&checkpoint.physical_hash)
                        .await
                        .int_err()?;

                    if size_actual != checkpoint.size {
                        return Err(VerificationError::CheckpointDoesNotMatchMetadata(
                            CheckpointDoesNotMatchMetadata {
                                block_hash,
                                error: CheckpointVerificationError::SizeMismatch {
                                    expected: checkpoint.size,
                                    actual: size_actual,
                                },
                            },
                        ));
                    }

                    // Check physical hash
                    let checkpoint_hashing_helper = CachedObject::from(
                        &checkpoint.physical_hash,
                        resolved_dataset.as_checkpoint_repo(),
                    )
                    .await?;

                    let physical_hash_actual =
                        checkpoint_hashing_helper.physical_hash().await.int_err()?;

                    if physical_hash_actual != checkpoint.physical_hash {
                        return Err(VerificationError::CheckpointDoesNotMatchMetadata(
                            CheckpointDoesNotMatchMetadata {
                                block_hash,
                                error: CheckpointVerificationError::PhysicalHashMismatch {
                                    expected: checkpoint.physical_hash,
                                    actual: physical_hash_actual,
                                },
                            },
                        ));
                    }
                }
            }

            listener.end_block(
                &block_hash,
                block_index,
                num_blocks,
                VerificationPhase::DataIntegrity,
            );
        }

        listener.end_phase(VerificationPhase::DataIntegrity);

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn check_metadata_integrity(
        &self,
        resolved_dataset: &ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
        listener: Arc<dyn VerificationListener>,
    ) -> Result<(), VerificationError> {
        let chain = resolved_dataset.as_metadata_chain();

        let head = match block_range.1 {
            None => chain.resolve_ref(&odf::BlockRef::Head).await?,
            Some(hash) => hash,
        };
        let tail = block_range.0;

        listener.begin_phase(VerificationPhase::MetadataIntegrity);

        use odf::dataset::MetadataChainExt;
        let blocks: Vec<_> = resolved_dataset
            .as_metadata_chain()
            .iter_blocks_interval(&head, tail.as_ref(), false)
            .try_collect()
            .await?;

        // To verify sequence integrity, let's build a temporary chain from the same
        // blocks in memory. Here we reuse validations implemented in append
        // rules when adding blocks to new chain.
        let in_memory_chain = MetadataChainImpl::new(
            MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new()),
            MetadataChainReferenceRepositoryImpl::new(ReferenceRepositoryImpl::new(
                NamedObjectRepositoryInMemory::new(),
            )),
        );

        for (block_hash, block) in blocks.into_iter().rev() {
            use odf::MetadataChain;
            match in_memory_chain
                .append(
                    block,
                    odf::dataset::AppendOpts {
                        precomputed_hash: Some(&block_hash),
                        ..odf::dataset::AppendOpts::default()
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(odf::dataset::AppendError::RefNotFound(e)) => {
                    Err(VerificationError::RefNotFound(e))
                }
                Err(e) => Err(VerificationError::Internal(e.int_err())),
            }?;
        }

        listener.end_phase(VerificationPhase::MetadataIntegrity);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl VerificationService for VerificationServiceImpl {
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            target_alias=%request.target.get_alias(),
            block_range=?request.block_range
        )
    )]
    async fn verify(
        &self,
        request: VerificationRequest<ResolvedDataset>,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult {
        let dataset_kind = request.target.get_kind();

        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));
        listener.begin();

        let outcome = try {
            if request.options.check_integrity {
                self.check_metadata_integrity(
                    &request.target,
                    request.block_range.clone(),
                    listener.clone(),
                )
                .await?;

                self.check_data_integrity(
                    &request.target,
                    dataset_kind,
                    request.block_range.clone(),
                    request.options.check_logical_hashes,
                    listener.clone(),
                )
                .await?;
            }

            if dataset_kind == odf::DatasetKind::Derivative
                && request.options.replay_transformations
            {
                let plan = self
                    .transform_request_planner
                    .build_transform_verification_plan(
                        request.target.clone(),
                        request.block_range.clone(),
                    )
                    .await
                    .map_err(|e| {
                        VerificationError::VerifyTransform(VerifyTransformError::Plan(e))
                    })?;

                self.transform_executor
                    .execute_verify_transform(request.target.clone(), plan, Some(listener.clone()))
                    .await
                    .map_err(|e| {
                        VerificationError::VerifyTransform(VerifyTransformError::Execute(e))
                    })?;
            }
        };

        let result = VerificationResult {
            dataset_handle: Some(request.target.take_handle()),
            outcome,
        };

        tracing::debug!(result = ?result, "Dataset verification finished");

        match &result.outcome {
            Ok(_) => listener.success(&result),
            Err(error) => listener.error(error),
        }

        result
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn verify_multi(
        &self,
        requests: Vec<VerificationRequest<ResolvedDataset>>,
        maybe_multi_listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Vec<VerificationResult> {
        let multi_listener =
            maybe_multi_listener.unwrap_or(Arc::new(NullVerificationMultiListener {}));

        let mut results = Vec::new();
        for request in requests {
            let dataset_handle = request.target.get_handle();
            let listener = multi_listener.begin_verify(dataset_handle);
            results.push(self.verify(request, listener).await);
        }

        results
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
