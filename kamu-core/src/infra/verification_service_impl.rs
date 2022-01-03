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
use std::sync::Arc;
use tracing::info_span;

pub struct VerificationServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    transform_service: Arc<dyn TransformService>,
    volume_layout: Arc<VolumeLayout>,
}

#[component(pub)]
impl VerificationServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        transform_service: Arc<dyn TransformService>,
        volume_layout: Arc<VolumeLayout>,
    ) -> Self {
        Self {
            metadata_repo,
            transform_service,
            volume_layout,
        }
    }

    fn get_integrity_check_plan(
        &self,
        dataset_handle: &DatasetHandle,
        block_range: (Option<Multihash>, Option<Multihash>),
    ) -> Result<Vec<(Multihash, DataSlice)>, VerificationError> {
        let metadata_chain = self
            .metadata_repo
            .get_metadata_chain(&dataset_handle.as_local_ref())?;

        let start_block = block_range.0;
        let end_block = block_range
            .1
            .unwrap_or_else(|| metadata_chain.read_ref(&BlockRef::Head).unwrap());

        let plan: Vec<_> = metadata_chain
            .iter_blocks_starting(&end_block)
            .ok_or(VerificationError::NoSuchBlock(end_block))?
            .take_while(|(hash, _)| Some(hash) != start_block.as_ref())
            .filter_map(|(hash, block)| match block.event {
                MetadataEvent::AddData(e) => Some((hash, e.output_data)),
                MetadataEvent::ExecuteQuery(e) => e.output_data.map(|v| (hash, v)),
                _ => None,
            })
            .collect();

        if let Some(start_block) = start_block {
            if start_block != plan[plan.len() - 1].0 {
                return Err(VerificationError::NoSuchBlock(start_block));
            }
        }

        Ok(plan)
    }

    fn check_data_integrity(
        &self,
        dataset_handle: &DatasetHandle,
        dataset_kind: DatasetKind,
        block_range: (Option<Multihash>, Option<Multihash>),
        listener: Arc<dyn VerificationListener>,
    ) -> Result<VerificationResult, VerificationError> {
        let span = info_span!("Verifying data integrity");
        let _span_guard = span.enter();

        let dataset_layout = DatasetLayout::new(&self.volume_layout, &dataset_handle.name);
        let plan = self.get_integrity_check_plan(dataset_handle, block_range)?;

        let num_blocks = plan.len();

        listener.begin_phase(VerificationPhase::DataIntegrity, num_blocks);

        for (block_index, (block_hash, output_slice)) in plan.into_iter().enumerate() {
            listener.begin_block(
                &block_hash,
                block_index,
                num_blocks,
                VerificationPhase::DataIntegrity,
            );

            let data_path = dataset_layout.data_dir.join(block_hash.to_string());

            // Do a fast pass using physical hash
            let physical_hash_actual =
                crate::infra::utils::data_utils::get_parquet_physical_hash(&data_path)
                    .map_err(|e| DomainError::InfraError(e.into()))?;

            if physical_hash_actual != output_slice.physical_hash {
                // Root data files are non-reproducible by definition, so
                // if physical hashes don't match - we can give up right away.
                if dataset_kind == DatasetKind::Root {
                    return Err(VerificationError::DataDoesNotMatchMetadata(
                        DataDoesNotMatchMetadata {
                            block_hash,
                            logical_hash: None,
                            physical_hash: Some(HashMismatch {
                                expected: output_slice.physical_hash.clone(),
                                actual: physical_hash_actual,
                            }),
                        },
                    ));
                } else {
                    // Derivative data may be replayed and produce different binary file
                    // but data must have same logical hash to be valid.
                    let logical_hash_actual =
                        crate::infra::utils::data_utils::get_parquet_logical_hash(&data_path)
                            .map_err(|e| DomainError::InfraError(e.into()))?;

                    if logical_hash_actual != output_slice.logical_hash {
                        return Err(VerificationError::DataDoesNotMatchMetadata(
                            DataDoesNotMatchMetadata {
                                block_hash,
                                logical_hash: Some(HashMismatch {
                                    expected: output_slice.logical_hash.clone(),
                                    actual: logical_hash_actual,
                                }),
                                physical_hash: None,
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

        listener.end_phase(VerificationPhase::DataIntegrity, num_blocks);

        Ok(VerificationResult::Valid)
    }
}

impl VerificationService for VerificationServiceImpl {
    fn verify(
        &self,
        dataset_ref: &DatasetRefLocal,
        block_range: (Option<Multihash>, Option<Multihash>),
        options: VerificationOptions,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        let dataset_handle = self.metadata_repo.resolve_dataset_ref(dataset_ref)?;

        let span = info_span!("Verifying dataset", %dataset_handle, ?block_range);
        let _span_guard = span.enter();

        let dataset_kind = self
            .metadata_repo
            .get_summary(&dataset_handle.as_local_ref())?
            .kind;

        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));
        listener.begin();

        let res = try {
            if options.check_integrity {
                self.check_data_integrity(
                    &dataset_handle,
                    dataset_kind,
                    block_range.clone(),
                    listener.clone(),
                )?;
            }

            if dataset_kind == DatasetKind::Derivative && options.replay_transformations {
                self.transform_service.verify_transform(
                    &dataset_handle.as_local_ref(),
                    block_range.clone(),
                    options,
                    Some(listener.clone()),
                )?;
            }

            VerificationResult::Valid
        };

        match &res {
            Ok(result) => listener.success(result),
            Err(error) => listener.error(error),
        }

        res
    }

    fn verify_multi(
        &self,
        _requests: &mut dyn Iterator<Item = VerificationRequest>,
        _options: VerificationOptions,
        _listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}
