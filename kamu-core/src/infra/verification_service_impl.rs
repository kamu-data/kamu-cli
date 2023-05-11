// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::*;
use opendatafabric::*;

use dill::*;
use futures::TryStreamExt;
use std::sync::Arc;

pub struct VerificationServiceImpl {
    local_repo: Arc<dyn DatasetRepository>,
    transform_service: Arc<dyn TransformService>,
    workspace_layout: Arc<WorkspaceLayout>,
}

#[component(pub)]
impl VerificationServiceImpl {
    pub fn new(
        local_repo: Arc<dyn DatasetRepository>,
        transform_service: Arc<dyn TransformService>,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Self {
        Self {
            local_repo,
            transform_service,
            workspace_layout,
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn check_data_integrity<'a>(
        &'a self,
        dataset_handle: &'a DatasetHandle,
        dataset_kind: DatasetKind,
        block_range: (Option<Multihash>, Option<Multihash>),
        listener: Arc<dyn VerificationListener>,
    ) -> Result<VerificationResult, VerificationError> {
        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let chain = dataset.as_metadata_chain();

        let head = match block_range.1 {
            None => chain.get_ref(&BlockRef::Head).await?,
            Some(hash) => hash,
        };
        let tail = block_range.0;

        // TODO: Avoid collecting and stream instead, perhaps use nonce for `num_blocks` estimate
        use futures::TryStreamExt;
        let plan: Vec<_> = chain
            .iter_blocks_interval(&head, tail.as_ref(), false)
            .filter_data_stream_blocks()
            .try_collect()
            .await?;

        let dataset_layout = self.workspace_layout.dataset_layout(&dataset_handle.alias);
        let num_blocks = plan.len();

        listener.begin_phase(VerificationPhase::DataIntegrity);

        for (block_index, (block_hash, block)) in plan.into_iter().enumerate() {
            listener.begin_block(
                &block_hash,
                block_index,
                num_blocks,
                VerificationPhase::DataIntegrity,
            );

            if let Some(output_slice) = &block.event.output_data {
                let data_path = dataset_layout.data_slice_path(&output_slice);

                // Check size first
                let size_actual = std::fs::metadata(&data_path).int_err()?.len();

                if size_actual != (output_slice.size as u64) {
                    return Err(VerificationError::DataDoesNotMatchMetadata(
                        DataDoesNotMatchMetadata {
                            block_hash,
                            error: DataVerificationError::SizeMismatch {
                                expected: output_slice.size as u64,
                                actual: size_actual,
                            },
                        },
                    ));
                }

                // Do a fast pass using physical hash
                let physical_hash_actual =
                    crate::infra::utils::data_utils::get_file_physical_hash(&data_path)
                        .int_err()?;

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
                    } else {
                        // Derivative data may be replayed and produce different binary file
                        // but data must have same logical hash to be valid.
                        let logical_hash_actual =
                            crate::infra::utils::data_utils::get_parquet_logical_hash(&data_path)
                                .int_err()?;

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

                if let Some(checkpoint) = block.event.output_checkpoint {
                    let checkpoint_path = dataset_layout.checkpoint_path(&checkpoint.physical_hash);

                    // Check size
                    let size_actual = std::fs::metadata(&checkpoint_path).int_err()?.len();

                    if size_actual != (checkpoint.size as u64) {
                        return Err(VerificationError::CheckpointDoesNotMatchMetadata(
                            CheckpointDoesNotMatchMetadata {
                                block_hash,
                                error: CheckpointVerificationError::SizeMismatch {
                                    expected: checkpoint.size as u64,
                                    actual: size_actual,
                                },
                            },
                        ));
                    }

                    // Check physical hash
                    let physical_hash_actual =
                        crate::infra::utils::data_utils::get_file_physical_hash(&checkpoint_path)
                            .int_err()?;

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

        Ok(VerificationResult::Valid)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn check_metadata_integrity<'a>(
        &'a self,
        dataset_handle: &'a DatasetHandle,
        block_range: (Option<Multihash>, Option<Multihash>),
        listener: Arc<dyn VerificationListener>,
    ) -> Result<VerificationResult, VerificationError> {
        let dataset = self
            .local_repo
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

        // To verify sequence integrity, let's build a temporary chain from the same blocks in memory.
        // Here we reuse validations implemented in append rules when adding blocks to new chain.
        let in_memory_chain = MetadataChainImpl::new(
            ObjectRepositoryInMemory::new(),
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

        Ok(VerificationResult::Valid)
    }
}

#[async_trait::async_trait(?Send)]
impl VerificationService for VerificationServiceImpl {
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref, ?block_range))]
    async fn verify(
        &self,
        dataset_ref: &DatasetRef,
        block_range: (Option<Multihash>, Option<Multihash>),
        options: VerificationOptions,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        let dataset_handle = self.local_repo.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let dataset_kind = dataset
            .get_summary(GetSummaryOpts::default())
            .await
            .int_err()?
            .kind;

        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));
        listener.begin();

        let res = try {
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
                    listener.clone(),
                )
                .await?;
            }

            if dataset_kind == DatasetKind::Derivative && options.replay_transformations {
                self.transform_service
                    .verify_transform(
                        &dataset_handle.as_local_ref(),
                        block_range.clone(),
                        options,
                        Some(listener.clone()),
                    )
                    .await?;
            }

            VerificationResult::Valid
        };

        match &res {
            Ok(result) => listener.success(result),
            Err(error) => listener.error(error),
        }

        res
    }

    async fn verify_multi(
        &self,
        _requests: &mut dyn Iterator<Item = VerificationRequest>,
        _options: VerificationOptions,
        _listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}
