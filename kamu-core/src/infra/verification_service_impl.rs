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
}

#[component(pub)]
impl VerificationServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        transform_service: Arc<dyn TransformService>,
    ) -> Self {
        Self {
            metadata_repo,
            transform_service,
        }
    }

    fn get_integrity_check_plan(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
    ) -> Result<Vec<MetadataBlock>, VerificationError> {
        let span = info_span!(
            "Preparing data integrity verification plan",
            output_dataset = dataset_id.as_str()
        );
        let _span_guard = span.enter();

        let metadata_chain = self.metadata_repo.get_metadata_chain(dataset_id)?;

        let start_block = block_range.0;
        let end_block = block_range
            .1
            .unwrap_or_else(|| metadata_chain.read_ref(&BlockRef::Head).unwrap());

        let plan: Vec<_> = metadata_chain
            .iter_blocks_starting(&end_block)
            .ok_or(VerificationError::NoSuchBlock(end_block))?
            .filter_map(|block| {
                if block.output_slice.is_some() {
                    Some(block)
                } else {
                    None
                }
            })
            .take_while(|block| Some(block.block_hash) != start_block)
            .collect();

        if let Some(start_block) = start_block {
            if start_block != plan[plan.len() - 1].block_hash {
                return Err(VerificationError::NoSuchBlock(start_block));
            }
        }

        Ok(plan)
    }

    fn check_data_integrity(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        listener: Arc<dyn VerificationListener>,
    ) -> Result<VerificationResult, VerificationError> {
        let plan = self.get_integrity_check_plan(dataset_id, block_range)?;

        let num_blocks = plan.len();

        listener.begin_phase(VerificationPhase::DataIntegrity, num_blocks);

        for (block_index, block) in plan.into_iter().enumerate() {
            listener.begin_block(
                &block.block_hash,
                block_index,
                num_blocks,
                VerificationPhase::DataIntegrity,
            );

            ////////////////////////////////////////////////////////////
            // TODO: VERIFY DATA HASH
            // Hashing design is still pending in the ODF protocol spec,
            // so we do a no-op now.
            ////////////////////////////////////////////////////////////

            listener.end_block(
                &block.block_hash,
                block_index,
                num_blocks,
                VerificationPhase::DataIntegrity,
            );
        }

        listener.end_phase(VerificationPhase::DataIntegrity, num_blocks);

        Ok(VerificationResult::Valid {
            blocks_verified: num_blocks,
        })
    }
}

impl VerificationService for VerificationServiceImpl {
    fn verify(
        &self,
        dataset_id: &DatasetID,
        block_range: (Option<Sha3_256>, Option<Sha3_256>),
        options: VerificationOptions,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        let span = info_span!("Verifying dataset", dataset_id = dataset_id.as_str(), block_range = ?block_range);
        let _span_guard = span.enter();

        let listener = maybe_listener.unwrap_or(Arc::new(NullVerificationListener {}));
        listener.begin();

        let res: Result<VerificationResult, VerificationError> = try {
            let res = self.check_data_integrity(dataset_id, block_range, listener.clone())?;

            match self.metadata_repo.get_summary(dataset_id)?.kind {
                DatasetKind::Root => res,
                DatasetKind::Derivative => self.transform_service.verify_transform(
                    dataset_id,
                    block_range,
                    options,
                    Some(listener.clone()),
                )?,
            }
        };

        match &res {
            Ok(result) => listener.success(result),
            Err(error) => listener.error(error),
        }

        res
    }

    fn verify_multi(
        &self,
        _datasets: &mut dyn Iterator<Item = VerificationRequest>,
        _options: VerificationOptions,
        _listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Result<VerificationResult, VerificationError> {
        unimplemented!()
    }
}
