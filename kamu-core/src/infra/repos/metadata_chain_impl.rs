// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use futures::TryStreamExt;
use opendatafabric::serde::flatbuffers::*;
use opendatafabric::*;

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainImpl<ObjRepo, RefRepo> {
    obj_repo: ObjRepo,
    ref_repo: RefRepo,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<ObjRepo, RefRepo> MetadataChainImpl<ObjRepo, RefRepo>
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

    async fn validate_append_sequence_numbers_integrity(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut Vec<MetadataBlock>,
    ) -> Result<(), AppendError> {
        if !new_block.prev_block_hash.is_some() {
            if new_block.sequence_number != 0 {
                return Err(
                    AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                        prev_block_hash: None,
                        prev_block_sequence_number: None,
                        next_block_sequence_number: new_block.sequence_number,
                    })
                    .into(),
                );
            }
        } else {
            let block_hash = new_block.prev_block_hash.as_ref().unwrap();
            let block = match self.get_block(block_hash).await {
                Ok(block) => Ok(block),
                Err(GetBlockError::NotFound(e)) => Err(AppendError::InvalidBlock(
                    AppendValidationError::PrevBlockNotFound(e),
                )),
                Err(GetBlockError::BlockVersion(e)) => Err(AppendError::Internal(e.int_err())),
                Err(GetBlockError::BlockMalformed(e)) => Err(AppendError::Internal(e.int_err())),
                Err(GetBlockError::Access(e)) => Err(AppendError::Access(e)),
                Err(GetBlockError::Internal(e)) => Err(AppendError::Internal(e)),
            }?;
            if block.sequence_number != (new_block.sequence_number - 1) {
                return Err(
                    AppendValidationError::SequenceIntegrity(SequenceIntegrityError {
                        prev_block_hash: Some(block_hash.clone()),
                        prev_block_sequence_number: Some(block.sequence_number),
                        next_block_sequence_number: new_block.sequence_number,
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
                Err(GetBlockError::BlockVersion(e)) => Err(AppendError::Internal(e.int_err())),
                Err(GetBlockError::BlockMalformed(e)) => Err(AppendError::Internal(e.int_err())),
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

    async fn validate_append_event_logical_structure(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut Vec<MetadataBlock>,
    ) -> Result<(), AppendError> {
        match &new_block.event {
            // TODO: ensure only used on Root datasets
            MetadataEvent::AddData(e) => {
                // Validate event is not empty
                if e.output_data.is_none()
                    && e.output_checkpoint.is_none()
                    && e.output_watermark.is_none()
                    && e.source_state.is_none()
                {
                    return Err(AppendValidationError::NoOpEvent(NoOpEventError::new(
                        e.clone(),
                        "Event is empty",
                    ))
                    .into());
                }

                let mut prev_checkpoint = None;
                let mut prev_watermark = None;
                let mut prev_source_state = None;

                // TODO: Generalize this logic
                // TODO: PERF: Use block cache
                let mut blocks = self.iter_blocks_interval(
                    new_block.prev_block_hash.as_ref().unwrap(),
                    None,
                    false,
                );
                while let Some((_, block)) = blocks.try_next().await.int_err()? {
                    match block.event {
                        MetadataEvent::AddData(e) => {
                            prev_checkpoint = Some(e.output_checkpoint);
                            prev_source_state = Some(e.source_state);
                            if prev_watermark.is_none() {
                                prev_watermark = Some(e.output_watermark);
                            }
                        }
                        MetadataEvent::SetWatermark(e) => {
                            prev_watermark = Some(Some(e.output_watermark));
                        }
                        _ => (),
                    }
                    if prev_checkpoint.is_some()
                        && prev_watermark.is_some()
                        && prev_source_state.is_some()
                    {
                        break;
                    }
                }

                let prev_checkpoint = prev_checkpoint.unwrap_or_default();
                let prev_watermark = prev_watermark.unwrap_or_default();
                let prev_source_state = prev_source_state.unwrap_or_default();

                // Validate input/output checkpoint sequencing
                if e.input_checkpoint.as_ref() != prev_checkpoint.as_ref().map(|c| &c.physical_hash)
                {
                    return Err(AppendValidationError::InvalidEvent(InvalidEventError::new(
                        e.clone(),
                        "Input checkpoint does not correspond to the previous checkpoint in the \
                         chain",
                    ))
                    .into());
                }

                // Validate event advances some state
                if e.output_data.is_none()
                    && e.output_checkpoint.as_ref().map(|v| &v.physical_hash)
                        == prev_checkpoint.as_ref().map(|v| &v.physical_hash)
                    && e.output_watermark == prev_watermark
                    && e.source_state == prev_source_state
                {
                    return Err(AppendValidationError::NoOpEvent(NoOpEventError::new(
                        e.clone(),
                        "Event neither has data nor it advances checkpoint, watermark, or source \
                         state",
                    ))
                    .into());
                }

                Ok(())
            }
            // TODO: ensure only used on Derivative datasets
            MetadataEvent::ExecuteQuery(e) => {
                // Validate event is not empty
                if e.output_data.is_none()
                    && e.output_checkpoint.is_none()
                    && e.output_watermark.is_none()
                {
                    return Err(AppendValidationError::NoOpEvent(NoOpEventError::new(
                        e.clone(),
                        "Event is empty",
                    ))
                    .into());
                }

                // TODO: PERF: Use block cache
                let prev_query = self
                    .iter_blocks_interval(new_block.prev_block_hash.as_ref().unwrap(), None, false)
                    .filter_map_ok(|(_, b)| match b.event {
                        MetadataEvent::ExecuteQuery(e) => Some(e),
                        _ => None,
                    })
                    .try_first()
                    .await
                    .int_err()?;

                let prev_checkpoint = prev_query
                    .as_ref()
                    .and_then(|e| e.output_checkpoint.as_ref())
                    .map(|c| &c.physical_hash);

                let prev_watermark = prev_query
                    .as_ref()
                    .and_then(|e| e.output_watermark.as_ref());

                // Validate input/output checkpoint sequencing
                if e.input_checkpoint.as_ref() != prev_checkpoint {
                    return Err(AppendValidationError::InvalidEvent(InvalidEventError::new(
                        e.clone(),
                        "Input checkpoint does not correspond to the previous checkpoint in the \
                         chain",
                    ))
                    .into());
                }

                // Validate event advances some state
                if e.output_data.is_none()
                    && e.output_checkpoint.as_ref().map(|v| &v.physical_hash) == prev_checkpoint
                    && e.output_watermark.as_ref() == prev_watermark
                {
                    return Err(AppendValidationError::NoOpEvent(NoOpEventError::new(
                        e.clone(),
                        "Event neither has data nor it advances checkpoint or watermark",
                    ))
                    .into());
                }

                Ok(())
            }
            MetadataEvent::Seed(_) => Ok(()),
            MetadataEvent::SetPollingSource(_) => Ok(()),
            MetadataEvent::SetTransform(_) => Ok(()),
            MetadataEvent::SetVocab(_) => Ok(()),
            // TODO: Ensure is not called on Derivative datasets (in a performant way)
            MetadataEvent::SetWatermark(_) => Ok(()),
            MetadataEvent::SetAttachments(_) => Ok(()),
            MetadataEvent::SetInfo(_) => Ok(()),
            MetadataEvent::SetLicense(_) => Ok(()),
        }
    }

    async fn validate_append_watermark_is_monotonic(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut Vec<MetadataBlock>,
    ) -> Result<(), AppendError> {
        if let Some(new_block) = new_block.as_data_stream_block() {
            // TODO: PERF: Use block cache
            let prev_wm = self
                .iter_blocks_interval(new_block.prev_block_hash.unwrap(), None, false)
                .filter_data_stream_blocks()
                .map_ok(|(_, b)| b.event.output_watermark)
                .try_first()
                .await
                .int_err()?;

            if let Some(prev_wm) = &prev_wm {
                match (prev_wm, new_block.event.output_watermark) {
                    (Some(_), None) => {
                        return Err(AppendValidationError::WatermarkIsNotMonotonic.into())
                    }
                    (Some(prev_wm), Some(new_wm)) if prev_wm > new_wm => {
                        return Err(AppendValidationError::WatermarkIsNotMonotonic.into())
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }

    async fn validate_append_offsets_are_sequential(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut Vec<MetadataBlock>,
    ) -> Result<(), AppendError> {
        if let Some(new_block) = new_block.as_data_stream_block() {
            if let Some(new_data) = new_block.event.output_data {
                // TODO: PERF: Use block cache
                let prev_data = self
                    .iter_blocks_interval(new_block.prev_block_hash.unwrap(), None, false)
                    .filter_data_stream_blocks()
                    .filter_map_ok(|(_, b)| b.event.output_data)
                    .try_first()
                    .await
                    .int_err()?;

                let last_offset = prev_data.map(|v| v.interval.end).unwrap_or(-1);

                if new_data.interval.start != last_offset + 1 {
                    tracing::warn!(
                        "Expected offset interval to start at {} but got {:?}",
                        last_offset + 1,
                        new_data.interval
                    );
                    return Err(AppendValidationError::OffsetsAreNotSequential.into());
                }

                if new_data.interval.end < new_data.interval.start {
                    tracing::debug!(
                        "Expected valid offset interval but got {:?}",
                        new_data.interval
                    );
                    return Err(AppendValidationError::OffsetsAreNotSequential.into());
                }
            }
        }
        Ok(())
    }
}

impl<ObjRepo, RefRepo> MetadataChainImpl<ObjRepo, RefRepo> {
    pub fn deserialize_block(
        hash: &Multihash,
        block_bytes: &[u8],
    ) -> Result<MetadataBlock, GetBlockError> {
        match FlatbuffersMetadataBlockDeserializer.read_manifest(&block_bytes) {
            Ok(block) => Ok(block),
            Err(e) => match e {
                Error::UnsupportedVersion { .. } => {
                    Err(GetBlockError::BlockVersion(BlockVersionError {
                        hash: hash.clone(),
                        source: e.into(),
                    }))
                }
                _ => Err(GetBlockError::BlockMalformed(BlockMalformedError {
                    hash: hash.clone(),
                    source: e.into(),
                })),
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<ObjRepo, RefRepo> MetadataChain for MetadataChainImpl<ObjRepo, RefRepo>
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

        Self::deserialize_block(hash, &data)
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

    fn iter_blocks_interval_ref<'a>(
        &'a self,
        head: &'a BlockRef,
        tail: Option<&'a BlockRef>,
    ) -> DynMetadataStream<'a> {
        Box::pin(async_stream::try_stream! {
            let head_hash = self.get_ref(head).await?;
            let tail_hash = match tail {
                None => None,
                Some(r) => Some(self.get_ref(r).await?),
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
            self.validate_append_sequence_numbers_integrity(&block, &mut block_cache)
                .await?;
            self.validate_append_seed_block_order(&block, &mut block_cache)
                .await?;
            self.validate_append_system_time_is_monotonic(&block, &mut block_cache)
                .await?;
            self.validate_append_watermark_is_monotonic(&block, &mut block_cache)
                .await?;
            self.validate_append_offsets_are_sequential(&block, &mut block_cache)
                .await?;
            self.validate_append_event_logical_structure(&block, &mut block_cache)
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
