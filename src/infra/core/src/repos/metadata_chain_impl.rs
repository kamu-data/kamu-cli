// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use kamu_core::*;
use opendatafabric::*;

use crate::{
    ValidateLogicalStructureVisitor,
    ValidateOffsetsAreSequentialVisitor,
    ValidatePrevBlockExistsVisitor,
    ValidateSeedBlockOrderVisitor,
    ValidateSequenceNumbersIntegrityVisitor,
    ValidateSystemTimeIsMonotonicVisitor,
    ValidateWatermarkIsMonotonicVisitor,
};

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! invalid_event {
    ($e:expr, $msg:expr $(,)?) => {
        return Err(kamu_core::AppendValidationError::InvalidEvent(
            kamu_core::InvalidEventError::new($e, $msg),
        )
        .into());
    };
}

pub(crate) use invalid_event;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainImpl<MetaBlockRepo, RefRepo> {
    meta_block_repo: MetaBlockRepo,
    ref_repo: RefRepo,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<MetaBlockRepo, RefRepo> MetadataChainImpl<MetaBlockRepo, RefRepo>
where
    MetaBlockRepo: MetadataBlockRepository + Sync + Send,
    RefRepo: ReferenceRepository + Sync + Send,
{
    pub fn new(meta_block_repo: MetaBlockRepo, ref_repo: RefRepo) -> Self {
        Self {
            meta_block_repo,
            ref_repo,
        }
    }

    async fn accept_append_validators<'a>(
        &'a self,
        decisions: &'a mut [MetadataVisitorDecision],
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = AppendError>],
        prev_append_block_hash: Option<&'a Multihash>,
    ) -> Result<(), AppendError> {
        let have_already_stopped = decisions
            .iter()
            .all(|decision| *decision == MetadataVisitorDecision::Stop);

        if have_already_stopped {
            return Ok(());
        }

        let Some(prev_block_hash) = prev_append_block_hash else {
            return Ok(());
        };

        self.accept_by_interval_with_decisions(decisions, visitors, prev_block_hash, None)
            .await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<MetaBlockRepo, RefRepo> MetadataChain for MetadataChainImpl<MetaBlockRepo, RefRepo>
where
    MetaBlockRepo: MetadataBlockRepository + Sync + Send,
    RefRepo: ReferenceRepository + Sync + Send,
{
    async fn resolve_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        self.ref_repo.get(r).await
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        self.meta_block_repo
            .get_block(hash)
            .await
            .map_err(Into::into)
    }

    fn iter_blocks_interval<'a>(
        &'a self,
        head_hash: &'a Multihash,
        tail_hash: Option<&'a Multihash>,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'a> {
        Box::pin(async_stream::try_stream! {
            let mut current = Some(head_hash.clone());

            while current.is_some() && current.as_ref() != tail_hash {
                let block = self.get_block(current.as_ref().unwrap()).await?;
                let next = block.prev_block_hash.clone();
                yield (current.take().unwrap(), block);
                current = next;
            }

            if !ignore_missing_tail && current.is_none() && tail_hash.is_some() {
                Err(IterBlocksError::InvalidInterval(InvalidIntervalError {
                    head: head_hash.clone(),
                    tail: tail_hash.cloned().unwrap(),
                }))?;
            }
        })
    }

    fn iter_blocks_interval_inclusive<'a>(
        &'a self,
        head_hash: &'a Multihash,
        tail_hash: &'a Multihash,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'a> {
        Box::pin(async_stream::try_stream! {
            let mut current = head_hash.clone();

            loop {
                let block = self.get_block(&current).await?;
                let next = block.prev_block_hash.clone();
                let done = current == *tail_hash;
                yield (current.clone(), block);
                if done || next.is_none() {
                    break;
                }
                current = next.unwrap();
            }

            if !ignore_missing_tail && current != *tail_hash {
                Err(IterBlocksError::InvalidInterval(InvalidIntervalError {
                    head: head_hash.clone(),
                    tail: tail_hash.clone(),
                }))?;
            }
        })
    }

    fn iter_blocks_interval_ref<'a>(
        &'a self,
        head: &'a BlockRef,
        tail: Option<&'a BlockRef>,
    ) -> DynMetadataStream<'a> {
        Box::pin(async_stream::try_stream! {
            let head_hash = self.resolve_ref(head).await?;
            let tail_hash = match tail {
                None => None,
                Some(r) => Some(self.resolve_ref(r).await?),
            };

            let mut current = Some(head_hash.clone());

            while current.is_some() && current != tail_hash {
                let block = self.get_block(current.as_ref().unwrap()).await?;
                let next = block.prev_block_hash.clone();
                yield (current.take().unwrap(), block);
                current = next;
            }

            if current.is_none() && tail_hash.is_some() {
                Err(IterBlocksError::InvalidInterval(InvalidIntervalError {
                    head: head_hash,
                    tail: tail_hash.unwrap()
                }))?;
            }
        })
    }

    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        opts: SetRefOpts<'a>,
    ) -> Result<(), SetRefError> {
        if opts.validate_block_present {
            match self.meta_block_repo.contains_block(hash).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(SetRefError::BlockNotFound(BlockNotFoundError {
                    hash: hash.clone(),
                })),
                Err(ContainsBlockError::Access(e)) => Err(SetRefError::Access(e)),
                Err(ContainsBlockError::Internal(e)) => Err(SetRefError::Internal(e)),
            }?;
        }

        // TODO: CONCURRENCY: Implement true CAS
        if let Some(prev_expected) = opts.check_ref_is {
            let prev_actual = match self.ref_repo.get(r).await {
                Ok(r) => Ok(Some(r)),
                Err(GetRefError::NotFound(_)) => Ok(None),
                Err(GetRefError::Access(e)) => Err(SetRefError::Access(e)),
                Err(GetRefError::Internal(e)) => Err(SetRefError::Internal(e)),
            }?;
            if prev_expected != prev_actual.as_ref() {
                return Err(RefCASError {
                    reference: r.clone(),
                    expected: prev_expected.cloned(),
                    actual: prev_actual,
                }
                .into());
            }
        }

        self.ref_repo.set(r, hash).await?;

        Ok(())
    }

    async fn append<'a>(
        &'a self,
        block: MetadataBlock,
        opts: AppendOpts<'a>,
    ) -> Result<Multihash, AppendError> {
        if opts.validation == AppendValidation::Full {
            let (d1, mut v1) = ValidateSeedBlockOrderVisitor::new(&block)?;
            let (d2, mut v2) = ValidatePrevBlockExistsVisitor::new(&block)?;
            let (d3, mut v3) = ValidateSequenceNumbersIntegrityVisitor::new(&block)?;
            let (d4, mut v4) = ValidateSystemTimeIsMonotonicVisitor::new(&block)?;
            let (d5, mut v5) = ValidateWatermarkIsMonotonicVisitor::new(&block)?;
            let (d6, mut v6) = ValidateOffsetsAreSequentialVisitor::new(&block)?;
            let (d7, mut v7) = ValidateLogicalStructureVisitor::new(&block)?;

            let mut decisions = [d1, d2, d3, d4, d5, d6, d7];
            let mut validators = [
                &mut v1 as &mut dyn MetadataChainVisitor<Error = _>,
                &mut v2,
                &mut v3,
                &mut v4,
                &mut v5,
                &mut v6,
                &mut v7,
            ];

            self.accept_append_validators(
                &mut decisions,
                &mut validators,
                block.prev_block_hash.as_ref(),
            )
            .await?;

            v7.post_visit()?;
        }

        if opts.update_ref.is_some()
            && (opts.check_ref_is.is_some() || opts.check_ref_is_prev_block)
        {
            let r = opts.update_ref.unwrap();
            let expected = opts.check_ref_is.unwrap_or(block.prev_block_hash.as_ref());

            let actual = match self.ref_repo.get(r).await {
                Ok(h) => Ok(Some(h)),
                Err(GetRefError::NotFound(_)) => Ok(None),
                Err(GetRefError::Access(e)) => Err(AppendError::Access(e)),
                Err(GetRefError::Internal(e)) => Err(AppendError::Internal(e)),
            }?;

            if expected != actual.as_ref() {
                return Err(AppendError::RefCASFailed(RefCASError {
                    reference: r.clone(),
                    expected: expected.cloned(),
                    actual,
                }));
            }
        }

        let res = self
            .meta_block_repo
            .insert_block(
                &block,
                InsertOpts {
                    precomputed_hash: opts.precomputed_hash,
                    expected_hash: opts.expected_hash,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| match e {
                InsertBlockError::HashMismatch(e) => AppendValidationError::HashMismatch(e).into(),
                InsertBlockError::Access(e) => AppendError::Access(e),
                InsertBlockError::Internal(e) => AppendError::Internal(e),
            })?;

        if let Some(r) = opts.update_ref {
            self.ref_repo.set(r, &res.hash).await?;
        }

        Ok(res.hash)
    }

    fn as_reference_repo(&self) -> &dyn ReferenceRepository {
        &self.ref_repo
    }

    fn as_metadata_block_repository(&self) -> &dyn MetadataBlockRepository {
        &self.meta_block_repo
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
