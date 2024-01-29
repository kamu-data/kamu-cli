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
use kamu_core::*;
use opendatafabric::serde::flatbuffers::*;
use opendatafabric::serde::*;
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! invalid_event {
    ($e:expr, $msg:expr $(,)?) => {
        return Err(AppendValidationError::InvalidEvent(InvalidEventError::new($e, $msg)).into())
    };
}

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
        _block_cache: &mut [MetadataBlock],
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
        _block_cache: &mut [MetadataBlock],
    ) -> Result<(), AppendError> {
        if new_block.prev_block_hash.is_none() {
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

    fn validate_append_seed_block_order(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut [MetadataBlock],
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
        _block_cache: &mut [MetadataBlock],
    ) -> Result<(), AppendError> {
        match &new_block.event {
            MetadataEvent::SetDataSchema(_) => {
                // TODO: Consider schema evolution rules
                // TODO: Consider what happens with previously defined sources
                Ok(())
            }
            MetadataEvent::AddData(e) => {
                // TODO: ensure only used on Root datasets

                // Validate event is not empty
                if e.new_data.is_none()
                    && e.new_checkpoint.is_none()
                    && e.new_watermark.is_none()
                    && e.new_source_state.is_none()
                {
                    return Err(AppendValidationError::NoOpEvent(NoOpEventError::new(
                        e.clone(),
                        "Event is empty",
                    ))
                    .into());
                }

                let mut prev_schema = None;
                let mut prev_add_data = None;

                // TODO: Generalize this logic
                // TODO: PERF: Use block cache
                let mut blocks = self.iter_blocks_interval(
                    new_block.prev_block_hash.as_ref().unwrap(),
                    None,
                    false,
                );
                while let Some((_, block)) = blocks.try_next().await.int_err()? {
                    match block.event {
                        MetadataEvent::SetDataSchema(e) if prev_schema.is_none() => {
                            prev_schema = Some(e);
                        }
                        MetadataEvent::AddData(e) => {
                            if prev_add_data.is_none() {
                                prev_add_data = Some(e);
                            }
                        }
                        _ => (),
                    }
                    if prev_schema.is_some() && prev_add_data.is_some() {
                        break;
                    }
                }

                // Validate schema was defined before adding any data
                if prev_schema.is_none() && e.new_data.is_some() {
                    invalid_event!(
                        e.clone(),
                        "SetDataSchema event must be present before adding data",
                    );
                }

                let expected_prev_checkpoint = prev_add_data
                    .as_ref()
                    .and_then(|v| v.new_checkpoint.as_ref())
                    .map(|c| &c.physical_hash);
                let prev_watermark = prev_add_data
                    .as_ref()
                    .and_then(|v| v.new_watermark.as_ref());
                let prev_source_state = prev_add_data
                    .as_ref()
                    .and_then(|v| v.new_source_state.as_ref());

                // Validate input/output checkpoint sequencing
                if e.prev_checkpoint.as_ref() != expected_prev_checkpoint {
                    invalid_event!(
                        e.clone(),
                        "Input checkpoint does not correspond to the last checkpoint in the chain",
                    );
                }

                // Validate event advances some state
                if e.new_data.is_none()
                    && e.new_checkpoint.as_ref().map(|v| &v.physical_hash)
                        == e.prev_checkpoint.as_ref()
                    && e.new_watermark.as_ref() == prev_watermark
                    && e.new_source_state.as_ref() == prev_source_state
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
            MetadataEvent::ExecuteTransform(e) => {
                // Validate event is not empty
                if e.new_data.is_none() && e.new_checkpoint.is_none() && e.new_watermark.is_none() {
                    return Err(AppendValidationError::NoOpEvent(NoOpEventError::new(
                        e.clone(),
                        "Event is empty",
                    ))
                    .into());
                }

                let mut prev_transform = None;
                let mut prev_schema = None;
                let mut prev_query = None;

                // TODO: Generalize this logic
                // TODO: PERF: Use block cache
                let mut blocks = self.iter_blocks_interval(
                    new_block.prev_block_hash.as_ref().unwrap(),
                    None,
                    false,
                );
                while let Some((_, block)) = blocks.try_next().await.int_err()? {
                    match block.event {
                        MetadataEvent::SetDataSchema(e) if prev_schema.is_none() => {
                            prev_schema = Some(e);
                        }
                        MetadataEvent::SetTransform(e) if prev_transform.is_none() => {
                            prev_transform = Some(e);
                        }
                        MetadataEvent::ExecuteTransform(e) if prev_query.is_none() => {
                            prev_query = Some(e);
                        }
                        _ => (),
                    }
                    // Note: `prev_transform` is optional
                    if prev_schema.is_some() && prev_query.is_some() {
                        break;
                    }
                }

                // Validate schema was defined if we're adding data
                if prev_schema.is_none() && e.new_data.is_some() {
                    invalid_event!(
                        e.clone(),
                        "SetDataSchema event must be present before adding data",
                    );
                }

                // Validate inputs are listed in the same exact order as in SetTransform (or
                // through recursion, in previous ExecuteTransform)
                let actual_inputs = e.query_inputs.iter().map(|i| &i.dataset_id);
                if let Some(prev_transform) = &prev_transform {
                    if actual_inputs.ne(prev_transform
                        .inputs
                        .iter()
                        .map(|i| i.dataset_ref.id().unwrap()))
                    {
                        invalid_event!(
                            e.clone(),
                            "Inputs must be listed in same order as initially declared in \
                             SetTransform event",
                        );
                    }
                } else if let Some(prev_query) = &prev_query {
                    if actual_inputs.ne(prev_query.query_inputs.iter().map(|i| &i.dataset_id)) {
                        invalid_event!(
                            e.clone(),
                            "Inputs must be listed in same order as initially declared in \
                             SetTransform event",
                        );
                    }
                } else {
                    invalid_event!(
                        e.clone(),
                        "ExecuteTransform must be preceded by SetTransform event",
                    );
                }

                // Validate input offset and block sequencing
                if let Some(prev_query) = &prev_query {
                    for (prev, new) in prev_query.query_inputs.iter().zip(&e.query_inputs) {
                        if new.new_block_hash.is_some() && new.new_block_hash == new.prev_block_hash
                        {
                            invalid_event!(e.clone(), "Invalid input block interval");
                        }

                        if new.new_offset.is_some() && new.new_offset == new.prev_offset {
                            invalid_event!(e.clone(), "Invalid input offset interval");
                        }

                        if new.prev_block_hash.as_ref() != prev.last_block_hash() {
                            invalid_event!(
                                e.clone(),
                                "Input prevBlockHash does not correspond to the last block \
                                 included in the previous query",
                            );
                        }

                        if new.prev_offset != prev.last_offset() {
                            invalid_event!(
                                e.clone(),
                                "Input prevOffset hash does not correspond to the last offset \
                                 included in the previous query",
                            );
                        }

                        if new.new_offset.is_some() && new.new_block_hash.is_none() {
                            invalid_event!(
                                e.clone(),
                                "Input specifies a non-empty offset interval, but its block \
                                 interval is empty",
                            );
                        }
                    }
                }

                let expected_prev_checkpoint = prev_query
                    .as_ref()
                    .and_then(|v| v.new_checkpoint.as_ref())
                    .map(|c| &c.physical_hash);
                let prev_watermark = prev_query.as_ref().and_then(|v| v.new_watermark.as_ref());

                // Validate input/output checkpoint sequencing
                if e.prev_checkpoint.as_ref() != expected_prev_checkpoint {
                    invalid_event!(
                        e.clone(),
                        "Input checkpoint does not correspond to the last checkpoint in the chain",
                    );
                }

                // Validate event advances some state
                if e.new_data.is_none()
                    && e.new_checkpoint.as_ref().map(|v| &v.physical_hash)
                        == e.prev_checkpoint.as_ref()
                    && e.new_watermark.as_ref() == prev_watermark
                {
                    return Err(AppendValidationError::NoOpEvent(NoOpEventError::new(
                        e.clone(),
                        "Event neither has data nor it advances checkpoint or watermark",
                    ))
                    .into());
                }

                Ok(())
            }
            MetadataEvent::SetPollingSource(e) => {
                // Queries must be normalized
                if let Some(transform) = &e.preprocess {
                    Self::validate_transform(&new_block.event, transform)?;
                }

                // Ensure no active push sources
                let mut blocks = self.iter_blocks_interval(
                    new_block.prev_block_hash.as_ref().unwrap(),
                    None,
                    false,
                );
                while let Some((_, block)) = blocks.try_next().await.int_err()? {
                    if let MetadataEvent::AddPushSource(e) = block.event {
                        invalid_event!(
                            e.clone(),
                            "Cannot add a polling source while some push sources are still active",
                        );
                    }
                }
                Ok(())
            }
            MetadataEvent::DisablePollingSource(_) => {
                // TODO: Ensure has previously active polling source
                unimplemented!("Disabling sources is not yet fully supported")
            }
            MetadataEvent::AddPushSource(e) => {
                // Ensure specifies the schema
                if e.read.schema().is_none() {
                    invalid_event!(
                        e.clone(),
                        "Push sources must specify the read schema explicitly",
                    );
                }

                // Queries must be normalized
                if let Some(transform) = &e.preprocess {
                    Self::validate_transform(&new_block.event, transform)?;
                }

                // Ensure no active polling source
                let mut blocks = self.iter_blocks_interval(
                    new_block.prev_block_hash.as_ref().unwrap(),
                    None,
                    false,
                );
                while let Some((_, block)) = blocks.try_next().await.int_err()? {
                    if let MetadataEvent::SetPollingSource(e) = block.event {
                        invalid_event!(
                            e.clone(),
                            "Cannot add a push source while polling source is still active",
                        );
                    }
                }

                Ok(())
            }
            MetadataEvent::DisablePushSource(_) => {
                // TODO: Ensure has previous push source with matching name
                unimplemented!("Disabling sources is not yet fully supported")
            }
            MetadataEvent::SetTransform(e) => {
                // Ensure has inputs
                if e.inputs.is_empty() {
                    invalid_event!(e.clone(), "Transform must have at least one input");
                }

                // Ensure inputs are resolved to IDs and aliases are specified
                for i in &e.inputs {
                    if i.dataset_ref.id().is_none() || i.alias.is_none() {
                        invalid_event!(
                            e.clone(),
                            "Transform inputs must be resolved to dataset IDs and specify aliases"
                        );
                    }
                }

                // Queries must be normalized
                Self::validate_transform(&new_block.event, &e.transform)?;

                Ok(())
            }
            MetadataEvent::Seed(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_) => Ok(()),
        }
    }

    fn validate_transform(e: &MetadataEvent, transform: &Transform) -> Result<(), AppendError> {
        let Transform::Sql(transform) = transform;
        if transform.query.is_some() {
            invalid_event!(e.clone(), "Transform queries must be normalized");
        }

        if transform.queries.is_none() || transform.queries.as_ref().unwrap().is_empty() {
            invalid_event!(e.clone(), "Transform must have at least one query");
        }

        let queries = transform.queries.as_ref().unwrap();

        if queries[queries.len() - 1].alias.is_some() {
            invalid_event!(
                e.clone(),
                "Last query in a transform must have no alias an will be treated as an output"
            );
        }

        for q in &queries[..queries.len() - 1] {
            if q.alias.is_none() {
                invalid_event!(
                    e.clone(),
                    "In a transform all queries except the last one must have aliases"
                );
            }
        }

        Ok(())
    }

    async fn validate_append_watermark_is_monotonic(
        &self,
        new_block: &MetadataBlock,
        _block_cache: &mut [MetadataBlock],
    ) -> Result<(), AppendError> {
        if let Some(new_block) = new_block.as_data_stream_block() {
            // TODO: PERF: Use block cache
            let prev_wm = self
                .iter_blocks_interval(new_block.prev_block_hash.unwrap(), None, false)
                .filter_data_stream_blocks()
                .map_ok(|(_, b)| b.event.new_watermark)
                .try_first()
                .await
                .int_err()?;

            if let Some(prev_wm) = &prev_wm {
                match (prev_wm, new_block.event.new_watermark) {
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
        _block_cache: &mut [MetadataBlock],
    ) -> Result<(), AppendError> {
        // Only check AddData and ExecuteTransform.
        // SetWatermark is also considered a data stream event but does not carry the
        // offsets.
        if let Some(e) = match new_block.event {
            MetadataEvent::AddData(_) | MetadataEvent::ExecuteTransform(_) => {
                Some(new_block.event.as_data_stream_event().unwrap())
            }
            _ => None,
        } {
            // Validate input/output offset sequencing
            // TODO: PERF: Use block cache
            let expected_prev_offset = self
                .iter_blocks_interval(new_block.prev_block_hash.as_ref().unwrap(), None, false)
                .filter_data_stream_blocks()
                .try_next()
                .await
                .int_err()?
                .and_then(|(_, b)| b.event.last_offset());

            if e.prev_offset != expected_prev_offset {
                invalid_event!(
                    new_block.event.clone(),
                    "Carried prev offset does not correspond to the last offset in the chain",
                );
            }

            // Validate internal offset consistency
            if let Some(new_data) = e.new_data {
                let expected_start_offset = e.prev_offset.map_or(0, |v| v + 1);
                if new_data.offset_interval.start != expected_start_offset {
                    return Err(AppendValidationError::OffsetsAreNotSequential(
                        OffsetsNotSequentialError::new(
                            expected_start_offset,
                            new_data.offset_interval.start,
                        ),
                    )
                    .into());
                }

                if new_data.offset_interval.end < new_data.offset_interval.start {
                    invalid_event!(new_block.event.clone(), "Invalid offset interval",);
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
        match FlatbuffersMetadataBlockDeserializer.read_manifest(block_bytes) {
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
            self.validate_append_seed_block_order(&block, &mut block_cache)?;
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
