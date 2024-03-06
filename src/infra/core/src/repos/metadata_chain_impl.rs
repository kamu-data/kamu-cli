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
use opendatafabric::*;

use crate::{
    ValidateOffsetsAreSequentialVisitor,
    ValidatePrevBlockExistsVisitor,
    ValidateSeedBlockOrderVisitor,
    ValidateSequenceNumbersIntegrityVisitor,
    ValidateSystemTimeIsMonotonicVisitor,
    ValidateWatermarkIsMonotonicVisitor,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! invalid_event {
    ($e:expr, $msg:expr $(,)?) => {
        use $crate::domain::{AppendValidationError, InvalidEventError};

        return Err(AppendValidationError::InvalidEvent(InvalidEventError::new($e, $msg)).into());
    };
}

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

    async fn validate_append_event_logical_structure(
        &self,
        new_block: &MetadataBlock,
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
                    return Err(
                        AppendValidationError::no_op_event(e.clone(), "Event is empty").into(),
                    );
                }

                let mut prev_schema = None;
                let mut prev_add_data = None;

                // TODO: Generalize this logic
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
                    return Err(AppendValidationError::no_op_event(
                        e.clone(),
                        "Event neither has data nor it advances checkpoint, watermark, or source \
                         state",
                    )
                    .into());
                }

                Ok(())
            }
            // TODO: ensure only used on Derivative datasets
            MetadataEvent::ExecuteTransform(e) => {
                // Validate event is not empty
                if e.new_data.is_none() && e.new_checkpoint.is_none() && e.new_watermark.is_none() {
                    return Err(
                        AppendValidationError::no_op_event(e.clone(), "Event is empty").into(),
                    );
                }

                let mut prev_transform = None;
                let mut prev_schema = None;
                let mut prev_query = None;

                // TODO: Generalize this logic
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
                    return Err(AppendValidationError::no_op_event(
                        e.clone(),
                        "Event neither has data nor it advances checkpoint or watermark",
                    )
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

    async fn accept_append_validators<'a>(
        &'a self,
        decisions: DecisionsMutRef<'a>,
        visitors: VisitorsMutRef<'a, 'a, AppendError>,
        prev_append_block_hash: Option<&'a Multihash>,
    ) -> Result<(), AppendError> {
        if MetadataChainVisitorFacade::<AppendError>::finished(decisions) {
            return Ok(());
        }

        let Some(prev_block_hash) = prev_append_block_hash else {
            return Ok(());
        };

        self.accept_interval(visitors, prev_block_hash, None, false)
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

            let mut decisions = [d1, d2, d3, d4, d5, d6];
            let validators: VisitorsMutRef<'_, '_, AppendError> =
                &mut [&mut v1, &mut v2, &mut v3, &mut v4, &mut v5, &mut v6];

            self.accept_append_validators(
                &mut decisions,
                validators,
                block.prev_block_hash.as_ref(),
            )
            .await?;

            self.validate_append_event_logical_structure(&block).await?;
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
