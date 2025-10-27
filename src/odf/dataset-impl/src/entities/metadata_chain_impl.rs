// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::ErrorIntoInternal;
use odf_dataset::*;
use odf_metadata::*;
use odf_storage::*;

use super::MetadataChainReferenceRepository;
use super::metadata_chain_validators::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! invalid_event {
    ($e:expr, $msg:expr $(,)?) => {
        return Err(AppendValidationError::InvalidEvent(InvalidEventError::new(
            $e, $msg,
        )))
    };
}

pub(crate) use invalid_event;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainImpl<MetaBlockRepo, MetaRefRepo> {
    meta_block_repo: MetaBlockRepo,
    meta_ref_repo: MetaRefRepo,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<MetaBlockRepo, MetaRefRepo> MetadataChainImpl<MetaBlockRepo, MetaRefRepo>
where
    MetaBlockRepo: MetadataBlockRepository + Sync + Send,
    MetaRefRepo: MetadataChainReferenceRepository + Sync + Send,
{
    pub fn new(meta_block_repo: MetaBlockRepo, meta_ref_repo: MetaRefRepo) -> Self {
        Self {
            meta_block_repo,
            meta_ref_repo,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<MetaBlockRepo, MetaRefRepo> MetadataChain for MetadataChainImpl<MetaBlockRepo, MetaRefRepo>
where
    MetaBlockRepo: MetadataBlockRepository + Sync + Send,
    MetaRefRepo: MetadataChainReferenceRepository + Sync + Send,
{
    fn detach_from_transaction(&self) {
        self.meta_ref_repo.detach_from_transaction();
    }

    fn as_raw_version(&self) -> &dyn MetadataChain {
        self
    }

    async fn contains_block(&self, hash: &Multihash) -> Result<bool, ContainsBlockError> {
        self.meta_block_repo.contains_block(hash).await
    }

    async fn get_block_size(&self, hash: &Multihash) -> Result<u64, GetBlockDataError> {
        self.meta_block_repo.get_block_size(hash).await
    }

    async fn get_block_bytes(&self, hash: &Multihash) -> Result<bytes::Bytes, GetBlockDataError> {
        self.meta_block_repo.get_block_bytes(hash).await
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        self.meta_block_repo.get_block(hash).await
    }

    async fn get_preceding_block_with_hint(
        &self,
        head_block: &MetadataBlock,
        tail_sequence_number: Option<u64>,
        hint: MetadataVisitorDecision,
    ) -> Result<Option<(Multihash, MetadataBlock)>, GetBlockError> {
        // Guard against stopped hint
        assert!(hint != MetadataVisitorDecision::Stop);

        // Have we reached the tail? (if specified the boundary, otherwise Seed=0)
        if tail_sequence_number.unwrap_or_default() >= head_block.sequence_number {
            // We are at the tail, no need to go further
            return Ok(None);
        }

        // No hints are supported in default chain implementation
        // Simply take the previous block, unless we reached the seed
        if let Some(prev_block_hash) = &head_block.prev_block_hash {
            // Got previous block
            let prev_block = self.get_block(prev_block_hash).await?;
            Ok(Some((prev_block_hash.clone(), prev_block)))
        } else {
            // Reached the seed block
            Ok(None)
        }
    }

    fn iter_blocks_interval<'a>(
        &'a self,
        head_boundary: MetadataChainIterBoundary<'a>,
        tail_boundary: Option<MetadataChainIterBoundary<'a>>,
        ignore_missing_tail: bool,
    ) -> DynMetadataStream<'a> {
        Box::pin(async_stream::try_stream! {
            let head_hash = match head_boundary {
                MetadataChainIterBoundary::Hash(h) => h.clone(),
                MetadataChainIterBoundary::Ref(r) => self.resolve_ref(r).await?,
            };

            let tail_hash = match tail_boundary {
                None => None,
                Some(MetadataChainIterBoundary::Hash(h)) => Some(h.clone()),
                Some(MetadataChainIterBoundary::Ref(r)) => Some(self.resolve_ref(r).await?),
            };

            let mut current = Some(head_hash.clone());

            while current.is_some() && current != tail_hash {
                let block = self.get_block(current.as_ref().unwrap()).await?;
                let next = block.prev_block_hash.clone();
                yield (current.take().unwrap(), block);
                current = next;
            }

            if !ignore_missing_tail && current.is_none() && let Some(tail_hash) = tail_hash {
                Err(IterBlocksError::InvalidInterval(InvalidIntervalError {
                    head: head_hash,
                    tail: tail_hash,
                }))?;
            }
        })
    }

    async fn append<'a>(
        &'a self,
        block: MetadataBlock,
        opts: AppendOpts<'a>,
    ) -> Result<Multihash, AppendError> {
        tracing::trace!(?block, "Trying to append block");

        if opts.validation == AppendValidation::Full {
            let mut validators = [
                &mut ValidateAddDataVisitor::new(&block)
                    as &mut dyn MetadataChainVisitor<Error = _>,
                &mut ValidateExecuteTransformVisitor::new(&block),
                &mut ValidateUnimplementedEventsVisitor::new(&block),
                &mut ValidateSeedBlockOrderVisitor::new(&block)?,
                &mut ValidateSequenceNumbersIntegrityVisitor::new(&block)?,
                &mut ValidateSystemTimeIsMonotonicVisitor::new(&block),
                &mut ValidateWatermarkIsMonotonicVisitor::new(&block),
                &mut ValidateEventIsNotEmptyVisitor::new(&block)?,
                &mut ValidateOffsetsAreSequentialVisitor::new(&block)?,
                &mut ValidateAddPushSourceVisitor::new(&block)?,
                &mut ValidateSetPollingSourceVisitor::new(&block)?,
                &mut ValidateSetTransformVisitor::new(&block)?,
                &mut ValidateSetDataSchemaVisitor::new(&block)?,
            ];

            match self
                .accept_by_interval(&mut validators, block.prev_block_hash.as_ref(), None)
                .await
            {
                Ok(()) => Ok(()),
                Err(AcceptVisitorError::Visitor(err)) => Err(AppendError::InvalidBlock(err)),
                // Detect non-existing prev block situation
                Err(AcceptVisitorError::Traversal(IterBlocksError::BlockNotFound(err)))
                    if Some(&err.hash) == block.prev_block_hash.as_ref() =>
                {
                    Err(AppendValidationError::PrevBlockNotFound(err).into())
                }
                Err(AcceptVisitorError::Traversal(err)) => {
                    Err(AppendError::Internal(err.int_err()))
                }
            }?;
        }

        let check_ref_is = if opts.update_ref.is_some()
            && (opts.check_ref_is.is_some() || opts.check_ref_is_prev_block)
        {
            opts.check_ref_is.or(Some(block.prev_block_hash.as_ref()))
        } else {
            None
        };

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

        tracing::debug!(?block, "Successfully appended block");

        if let Some(r) = opts.update_ref {
            self.set_ref(
                r,
                &res.hash,
                SetRefOpts {
                    validate_block_present: false,
                    check_ref_is,
                },
            )
            .await
            .map_err(|e| match e {
                SetChainRefError::BlockNotFound(_) => {
                    unreachable!("We've just created this block")
                }
                SetChainRefError::CASFailed(e) => AppendError::RefCASFailed(e),
                SetChainRefError::Access(e) => AppendError::Access(e),
                SetChainRefError::Internal(e) => AppendError::Internal(e),
            })?;
        }

        Ok(res.hash)
    }

    async fn resolve_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        self.meta_ref_repo.get_ref(r).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(r, hash, ?opts))]
    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        opts: SetRefOpts<'a>,
    ) -> Result<(), SetChainRefError> {
        if opts.validate_block_present {
            match self.meta_block_repo.contains_block(hash).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(SetChainRefError::BlockNotFound(BlockNotFoundError {
                    hash: hash.clone(),
                })),
                Err(ContainsBlockError::Access(e)) => Err(SetChainRefError::Access(e)),
                Err(ContainsBlockError::Internal(e)) => Err(SetChainRefError::Internal(e)),
            }?;
        }

        self.meta_ref_repo
            .set_ref(r, hash, opts.check_ref_is)
            .await?;

        Ok(())
    }

    fn as_uncached_ref_repo(&self) -> &dyn ReferenceRepository {
        self.meta_ref_repo.as_uncached_ref_repo()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
