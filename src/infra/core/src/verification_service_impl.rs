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
use kamu_core::*;
use opendatafabric::*;

use crate::utils::cached_object::CachedObject;
use crate::*;

pub struct VerificationServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
    transform_service: Arc<dyn TransformService>,
}

#[component(pub)]
#[interface(dyn VerificationService)]
impl VerificationServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
        transform_service: Arc<dyn TransformService>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_authorizer,
            transform_service,
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn check_data_integrity<'a>(
        &'a self,
        dataset_handle: &'a DatasetHandle,
        dataset_kind: DatasetKind,
        block_range: (Option<Multihash>, Option<Multihash>),
        check_logical_hashes: bool,
        listener: Arc<dyn VerificationListener>,
    ) -> Result<(), VerificationError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let chain = dataset.as_metadata_chain();

        let head = match block_range.1 {
            None => chain.get_ref(&BlockRef::Head).await?,
            Some(hash) => hash,
        };
        let tail = block_range.0;

        // TODO: Avoid collecting and stream instead, perhaps use nonce for `num_blocks`
        // estimate
        use futures::TryStreamExt;
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
                let size_actual = dataset
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

                let data_hashing_helper =
                    CachedObject::from(&output_slice.physical_hash, dataset.as_data_repo()).await?;

                // Do a fast pass using physical hash
                let physical_hash_actual = data_hashing_helper.physical_hash().await.int_err()?;
                if physical_hash_actual != output_slice.physical_hash {
                    // Root data files are non-reproducible by definition, so
                    // if physical hashes don't match - we can give up right away.
                    if dataset_kind == DatasetKind::Root {
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
                    let size_actual = dataset
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
                    let checkpoint_hashing_helper =
                        CachedObject::from(&checkpoint.physical_hash, dataset.as_checkpoint_repo())
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
    async fn check_metadata_integrity<'a>(
        &'a self,
        dataset_handle: &'a DatasetHandle,
        block_range: (Option<Multihash>, Option<Multihash>),
        listener: Arc<dyn VerificationListener>,
    ) -> Result<(), VerificationError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let chain = dataset.as_metadata_chain();

        let head = match block_range.1 {
            None => chain.get_ref(&BlockRef::Head).await?,
            Some(hash) => hash,
        };
        let tail = block_range.0;

        listener.begin_phase(VerificationPhase::MetadataIntegrity);

        let blocks: Vec<_> = dataset
            .as_metadata_chain()
            .iter_blocks_interval(&head, tail.as_ref(), false)
            .try_collect()
            .await?;

        // To verify sequence integrity, let's build a temporary chain from the same
        // blocks in memory. Here we reuse validations implemented in append
        // rules when adding blocks to new chain.
        let in_memory_chain = MetadataChainImpl::new(
            MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new()),
            ReferenceRepositoryImpl::new(NamedObjectRepositoryInMemory::new()),
        );

        for (block_hash, block) in blocks.into_iter().rev() {
            match in_memory_chain
                .append(
                    block,
                    AppendOpts {
                        precomputed_hash: Some(&block_hash),
                        ..AppendOpts::default()
                    },
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(AppendError::RefNotFound(e)) => Err(VerificationError::RefNotFound(e)),
                Err(e) => Err(VerificationError::Internal(e.int_err())),
            }?;
        }

        listener.end_phase(VerificationPhase::MetadataIntegrity);

        Ok(())
    }
}

#[async_trait::async_trait]
impl VerificationService for VerificationServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref, ?block_range))]
    async fn verify(
        &self,
        dataset_ref: &DatasetRef,
        block_range: (Option<Multihash>, Option<Multihash>),
        options: VerificationOptions,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult {
        let dataset_handle = match self.dataset_repo.resolve_dataset_ref(dataset_ref).await {
            Ok(v) => v,
            Err(e) => return VerificationResult::err_no_handle(e),
        };

        match self
            .dataset_authorizer
            .check_action_allowed(&dataset_handle, domain::auth::DatasetAction::Read)
            .await
        {
            Ok(_) => {}
            Err(e) => return VerificationResult::err(dataset_handle, e),
        };

        let dataset = match self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
        {
            Ok(v) => v,
            Err(e) => return VerificationResult::err(dataset_handle, e),
        };

        let dataset_kind = match dataset.get_summary(GetSummaryOpts::default()).await {
            Ok(summary) => summary.kind,
            Err(e) => return VerificationResult::err(dataset_handle, e.int_err()),
        };

        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));
        listener.begin();

        let outcome = try {
            if options.check_integrity {
                self.check_metadata_integrity(
                    &dataset_handle,
                    block_range.clone(),
                    listener.clone(),
                )
                .await?;

                self.check_data_integrity(
                    &dataset_handle,
                    dataset_kind,
                    block_range.clone(),
                    options.check_logical_hashes,
                    listener.clone(),
                )
                .await?;
            }

            if dataset_kind == DatasetKind::Derivative && options.replay_transformations {
                self.transform_service
                    .verify_transform(
                        &dataset_handle.as_local_ref(),
                        block_range.clone(),
                        Some(listener.clone()),
                    )
                    .await?;
            }
        };

        let result = VerificationResult {
            dataset_handle: Some(dataset_handle),
            outcome,
        };

        match &result.outcome {
            Ok(_) => listener.success(&result),
            Err(error) => listener.error(error),
        }

        result
    }

    async fn verify_multi(
        &self,
        requests: Vec<VerificationRequest>,
        options: VerificationOptions,
        maybe_listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Vec<VerificationResult> {
        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationMultiListener {}));

        let mut results = Vec::new();
        for request in requests {
            let res = match self
                .dataset_repo
                .resolve_dataset_ref(&request.dataset_ref)
                .await
            {
                Ok(dataset_handle) => {
                    self.verify(
                        &request.dataset_ref,
                        request.block_range,
                        options.clone(),
                        listener.begin_verify(&dataset_handle),
                    )
                    .await
                }
                Err(e) => VerificationResult::err_no_handle(e),
            };

            results.push(res);
        }

        results
    }
}
