// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::serde::flatbuffers::*;
use opendatafabric::*;

use async_trait::async_trait;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChain2Impl<ObjRepo, RefRepo> {
    obj_repo: ObjRepo,
    ref_repo: RefRepo,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<ObjRepo, RefRepo> MetadataChain2Impl<ObjRepo, RefRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
    RefRepo: ReferenceRepository + Sync + Send,
{
    pub fn new(obj_repo: ObjRepo, ref_repo: RefRepo) -> Self {
        Self { obj_repo, ref_repo }
    }

    async fn validate_append_prev_block_exists(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut Vec<MetadataBlock>,
    ) -> Result<(), AppendError> {
        if let Some(prev) = &new_block.prev_block_hash {
            match self.obj_repo.contains(prev).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(
                    AppendValidationError::PrevBlockNotFound(BlockNotFoundError {
                        hash: prev.clone(),
                    })
                    .into(),
                ),
                Err(ContainsError::Access(e)) => Err(AppendError::Access(e)),
                Err(ContainsError::Internal(e)) => Err(AppendError::Internal(e)),
            }?;
        }
        Ok(())
    }

    async fn validate_append_seed_block_order(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut Vec<MetadataBlock>,
    ) -> Result<(), AppendError> {
        match &new_block.event {
            MetadataEvent::Seed(_) if new_block.prev_block_hash.is_none() => Ok(()),
            MetadataEvent::Seed(_) if new_block.prev_block_hash.is_some() => {
                Err(AppendValidationError::AppendingSeedBlockToNonEmptyChain.into())
            }
            _ if new_block.prev_block_hash.is_none() => {
                Err(AppendValidationError::FirstBlockMustBeSeed.into())
            }
            _ => Ok(()),
        }
    }

    async fn validate_append_system_time_is_monotonic(
        &self,
        new_block: &MetadataBlock,
        block_cache: &mut Vec<MetadataBlock>,
    ) -> Result<(), AppendError> {
        let maybe_prev_block = if !block_cache.is_empty() {
            Ok(block_cache.first())
        } else if let Some(prev_hash) = &new_block.prev_block_hash {
            match self.get_block(prev_hash).await {
                Ok(b) => {
                    block_cache.push(b);
                    Ok(block_cache.first())
                }
                Err(GetBlockError::NotFound(e)) => Err(AppendError::InvalidBlock(
                    AppendValidationError::PrevBlockNotFound(e),
                )),
                Err(GetBlockError::Access(e)) => Err(AppendError::Access(e)),
                Err(GetBlockError::Internal(e)) => Err(AppendError::Internal(e)),
            }
        } else {
            Ok(None)
        }?;

        if let Some(prev_block) = maybe_prev_block {
            if new_block.system_time < prev_block.system_time {
                return Err(AppendValidationError::SystemTimeIsNotMonotonic.into());
            }
        }
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo, RefRepo> MetadataChain2 for MetadataChain2Impl<ObjRepo, RefRepo>
where
    ObjRepo: ObjectRepository + Sync + Send,
    RefRepo: ReferenceRepository + Sync + Send,
{
    async fn get_ref(&self, r: &BlockRef) -> Result<Multihash, GetRefError> {
        self.ref_repo.get(r).await
    }

    async fn get_block(&self, hash: &Multihash) -> Result<MetadataBlock, GetBlockError> {
        let data = match self.obj_repo.get_bytes(hash).await {
            Ok(data) => Ok(data),
            Err(GetError::NotFound(e)) => {
                Err(GetBlockError::NotFound(BlockNotFoundError { hash: e.hash }))
            }
            Err(GetError::Access(e)) => Err(GetBlockError::Access(e)),
            Err(GetError::Internal(e)) => Err(GetBlockError::Internal(e)),
        }?;

        let block = FlatbuffersMetadataBlockDeserializer
            .read_manifest(&data)
            .map_err(|e| BlockMalformedError {
                hash: hash.clone(),
                source: e.into(),
            })
            .int_err()?;

        Ok(block)
    }

    fn iter_blocks_interval<'a, 'b>(
        &'a self,
        head_ref: &'b Multihash,
        tail: Option<&'b Multihash>,
    ) -> BlockStream<'a> {
        use async_stream::stream;

        let interval_head = head_ref.clone();
        let tail = tail.cloned();

        let s = stream! {
            let mut head = Some(interval_head.clone());

            while head.is_some() && head != tail {
                head = match self.get_block(head.as_ref().unwrap()).await {
                    Ok(block) => {
                        let new_head = block.prev_block_hash.clone();
                        yield Ok((head.clone().unwrap(), block));
                        new_head
                    }
                    Err(GetBlockError::NotFound(e)) => {
                        yield Err(IterBlocksError::BlockNotFound(e));
                        break;
                    }
                    Err(GetBlockError::Access(e)) => {
                        yield Err(IterBlocksError::Access(e));
                        break;
                    }
                    Err(GetBlockError::Internal(e)) => {
                        yield Err(IterBlocksError::Internal(e));
                        break;
                    }
                };
            }

            if head.is_none() && tail.is_some() {
                yield Err(IterBlocksError::InvalidInterval(InvalidIntervalError{ head: interval_head, tail: tail.unwrap() }));
            }
        };

        Box::pin(s)
    }

    async fn iter_blocks<'a>(&'a self) -> Result<BlockStream<'a>, GetRefError> {
        let head = self.get_ref(&BlockRef::Head).await?;
        Ok(self.iter_blocks_interval(&head, None))
    }

    async fn iter_blocks_ref<'a>(&'a self, r: &BlockRef) -> Result<BlockStream<'a>, GetRefError> {
        let head = self.get_ref(r).await?;
        Ok(self.iter_blocks_interval(&head, None))
    }

    async fn set_ref<'a>(
        &'a self,
        r: &BlockRef,
        hash: &Multihash,
        opts: SetRefOpts<'a>,
    ) -> Result<(), SetRefError> {
        if opts.validate_block_present {
            match self.obj_repo.contains(hash).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(SetRefError::BlockNotFound(BlockNotFoundError {
                    hash: hash.clone(),
                })),
                Err(ContainsError::Access(e)) => Err(SetRefError::Access(e)),
                Err(ContainsError::Internal(e)) => Err(SetRefError::Internal(e)),
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
        // TODO: PERF: Add caching layer that persists between multiple append calls
        let mut block_cache = Vec::new();

        if opts.validation == AppendValidation::Full {
            self.validate_append_prev_block_exists(&block, &mut block_cache)
                .await?;
            self.validate_append_seed_block_order(&block, &mut block_cache)
                .await?;
            self.validate_append_system_time_is_monotonic(&block, &mut block_cache)
                .await?;
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

        let data = FlatbuffersMetadataBlockSerializer
            .write_manifest(&block)
            .int_err()?;

        let res = self
            .obj_repo
            .insert_bytes(
                &data,
                InsertOpts {
                    precomputed_hash: opts.precomputed_hash,
                    expected_hash: opts.expected_hash,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| match e {
                InsertError::HashMismatch(e) => {
                    AppendError::InvalidBlock(AppendValidationError::HashMismatch(e))
                }
                InsertError::Access(e) => AppendError::Access(e),
                InsertError::Internal(e) => AppendError::Internal(e),
            })?;

        if let Some(r) = opts.update_ref {
            self.ref_repo.set(r, &res.hash).await?;
        }

        Ok(res.hash)
    }

    fn as_object_repo(&self) -> &dyn ObjectRepository {
        &self.obj_repo
    }

    fn as_reference_repo(&self) -> &dyn ReferenceRepository {
        &self.ref_repo
    }
}
