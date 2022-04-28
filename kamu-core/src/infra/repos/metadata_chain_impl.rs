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
            if !self
                .obj_repo
                .contains(prev)
                .await
                .map_err(|e| AppendError::Internal(e))?
            {
                return Err(
                    AppendValidationError::PrevBlockNotFound(BlockNotFoundError {
                        hash: prev.clone(),
                    })
                    .into(),
                );
            }
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
            block_cache.first()
        } else if let Some(prev_hash) = &new_block.prev_block_hash {
            match self.get_block(prev_hash).await {
                Ok(b) => {
                    block_cache.push(b);
                    block_cache.first()
                }
                Err(GetBlockError::NotFound(e)) => {
                    return Err(AppendValidationError::PrevBlockNotFound(e).into())
                }
                Err(GetBlockError::Internal(e)) => return Err(AppendError::Internal(e)),
            }
        } else {
            None
        };

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
            Err(GetError::Internal(e)) => Err(GetBlockError::Internal(e)),
        }?;

        let block = FlatbuffersMetadataBlockDeserializer
            .read_manifest(&data)
            .map_err(|e| {
                GetBlockError::Internal(
                    BlockMalformedError {
                        hash: hash.clone(),
                        source: e.into(),
                    }
                    .into(),
                )
            })?;

        Ok(block)
    }

    fn iter_blocks_interval<'a, 'b>(
        &'a self,
        head: &'b Multihash,
        tail: Option<&'b Multihash>,
    ) -> BlockStream<'a> {
        use async_stream::stream;

        let head = head.clone();
        let tail = tail.cloned();

        let s = stream! {
            let mut head = Some(head);

            while head.is_some() && head != tail {
                head = match self.get_block(head.as_ref().unwrap()).await {
                    Ok(block) => {
                        let new_head = block.prev_block_hash.clone();
                        yield Ok((head.clone().unwrap(), block));
                        new_head
                    }
                    _ => {
                        yield Err(IterBlocksError::BlockNotFound(BlockNotFoundError { hash: head.clone().unwrap() }));
                        None
                    }
                };
            }

            if head.is_none() && tail.is_some() {
                yield Err(IterBlocksError::InvalidInterval(InvalidIntervalError{ tail: tail.unwrap() }));
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

    async fn set_ref(&self, r: &BlockRef, hash: &Multihash) -> Result<(), SetRefError> {
        if !self
            .obj_repo
            .contains(hash)
            .await
            .map_err(|e| SetRefError::Internal(e))?
        {
            return Err(SetRefError::BlockNotFound(BlockNotFoundError {
                hash: hash.clone(),
            }));
        }

        self.ref_repo
            .set(r, hash)
            .await
            .map_err(|e| SetRefError::Internal(e))?;

        Ok(())
    }

    async fn append<'a>(
        &'a self,
        block: MetadataBlock,
        opts: AppendOpts<'a>,
    ) -> Result<Multihash, AppendError> {
        // TODO: Smarter caching
        let mut block_cache = Vec::new();

        if opts.validations == AppendValidations::Full {
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
                Ok(h) => Some(h),
                Err(GetRefError::NotFound(_)) => None,
                Err(GetRefError::Internal(e)) => return Err(AppendError::Internal(e)),
            };

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
            .map_err(|e| AppendError::Internal(e.into()))?;

        let res = self
            .obj_repo
            .insert_bytes(&data, InsertOpts::default())
            .await
            .map_err(|e| match e {
                InsertError::HashMismatch(_) => unreachable!(),
                InsertError::Internal(e) => AppendError::Internal(e),
            })?;

        if let Some(r) = opts.update_ref {
            self.ref_repo
                .set(r, &res.hash)
                .await
                .map_err(|e| AppendError::Internal(e))?;
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
