// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::debug;

use crate::domain::repos::named_object_repository::GetError;
use crate::domain::*;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetImpl<MetaChain, DataRepo, CheckpointRepo, CacheRepo, InfoRepo> {
    metadata_chain: MetaChain,
    data_repo: DataRepo,
    checkpoint_repo: CheckpointRepo,
    cache_repo: CacheRepo,
    info_repo: InfoRepo,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<MetaChain, DataRepo, CheckpointRepo, CacheRepo, InfoRepo>
    DatasetImpl<MetaChain, DataRepo, CheckpointRepo, CacheRepo, InfoRepo>
where
    MetaChain: MetadataChain + Sync + Send,
    DataRepo: ObjectRepository + Sync + Send,
    CheckpointRepo: ObjectRepository + Sync + Send,
    CacheRepo: NamedObjectRepository + Sync + Send,
    InfoRepo: NamedObjectRepository + Sync + Send,
{
    pub fn new(
        metadata_chain: MetaChain,
        data_repo: DataRepo,
        checkpoint_repo: CheckpointRepo,
        cache_repo: CacheRepo,
        info_repo: InfoRepo,
    ) -> Self {
        Self {
            metadata_chain,
            data_repo,
            checkpoint_repo,
            cache_repo,
            info_repo,
        }
    }

    async fn read_summary(&self) -> Result<Option<DatasetSummary>, GetSummaryError> {
        let data = match self.info_repo.get("summary").await {
            Ok(data) => data,
            Err(GetError::NotFound(_)) => return Ok(None),
            Err(GetError::Access(e)) => return Err(GetSummaryError::Access(e)),
            Err(GetError::Internal(e)) => return Err(GetSummaryError::Internal(e)),
        };

        let manifest: Manifest<DatasetSummary> = serde_yaml::from_slice(&data[..]).int_err()?;

        if manifest.kind != "DatasetSummary" {
            return Err(InvalidObjectKind {
                expected: "DatasetSummary".to_owned(),
                actual: manifest.kind,
            }
            .int_err()
            .into());
        }

        Ok(Some(manifest.content))
    }

    async fn write_summary(&self, summary: &DatasetSummary) -> Result<(), GetSummaryError> {
        let manifest = Manifest {
            kind: "DatasetSummary".to_owned(),
            version: 1,
            content: summary.clone(),
        };

        let data = serde_yaml::to_vec(&manifest).int_err()?;

        match self.info_repo.set("summary", &data).await {
            Ok(()) => Ok(()),
            Err(SetError::Access(e)) => Err(GetSummaryError::Access(e)),
            Err(SetError::Internal(e)) => Err(GetSummaryError::Internal(e)),
        }?;

        Ok(())
    }

    async fn update_summary(
        &self,
        prev: Option<DatasetSummary>,
    ) -> Result<Option<DatasetSummary>, GetSummaryError> {
        let current_head = match self.metadata_chain.get_ref(&BlockRef::Head).await {
            Ok(h) => h,
            Err(GetRefError::NotFound(_)) => return Ok(prev),
            Err(GetRefError::Access(e)) => return Err(GetSummaryError::Access(e)),
            Err(GetRefError::Internal(e)) => return Err(GetSummaryError::Internal(e)),
        };

        let last_seen = prev.as_ref().map(|s| &s.last_block_hash);
        if last_seen == Some(&current_head) {
            return Ok(prev);
        }

        debug!(?current_head, ?last_seen, "Updating dataset summary");

        let increment = self
            .compute_summary_increment(&current_head, last_seen)
            .await?;

        let blank_summary: DatasetSummary = Self::prepare_blank_summary_for_update(&current_head);
        let base_summary = if increment.seen_empty_prev_block_hash {
            &blank_summary
        } else {
            prev.as_ref().unwrap_or(&blank_summary)
        };

        let summary =
            UpdateSummaryIncrement::apply_increment_to(increment, &base_summary, &current_head);

        self.write_summary(&summary).await?;

        Ok(Some(summary))
    }

    async fn compute_summary_increment(
        &self,
        current_head: &Multihash,
        last_seen: Option<&Multihash>,
    ) -> Result<UpdateSummaryIncrement, GetSummaryError> {
        let mut block_stream =
            self.metadata_chain
                .iter_blocks_interval(&current_head, last_seen, true);

        use tokio_stream::StreamExt;
        let mut increment: UpdateSummaryIncrement = UpdateSummaryIncrement::new();

        while let Some((_, block)) = block_stream.try_next().await.int_err()? {
            increment.seen_empty_prev_block_hash = block.prev_block_hash.is_none();

            match block.event {
                MetadataEvent::Seed(seed) => {
                    increment.seen_id = increment.seen_id.or_else(|| Some(seed.dataset_id));
                    increment.seen_kind = increment.seen_kind.or_else(|| Some(seed.dataset_kind));
                }
                MetadataEvent::SetTransform(set_transform) => {
                    increment.seen_dependencies = increment
                        .seen_dependencies
                        .or_else(|| Some(set_transform.inputs));
                }
                MetadataEvent::AddData(add_data) => {
                    increment.seen_last_pulled = increment
                        .seen_last_pulled
                        .or_else(|| Some(block.system_time));

                    let iv = add_data.output_data.interval;
                    increment.seen_num_records += (iv.end - iv.start + 1) as u64;

                    increment.seen_data_size += add_data.output_data.size as u64;

                    if let Some(checkpoint) = add_data.output_checkpoint {
                        increment.seen_checkpoints_size += checkpoint.size as u64;
                    }
                }
                MetadataEvent::ExecuteQuery(execute_query) => {
                    increment.seen_last_pulled = increment
                        .seen_last_pulled
                        .or_else(|| Some(block.system_time));

                    if let Some(output_data) = execute_query.output_data {
                        let iv = output_data.interval;
                        increment.seen_num_records += (iv.end - iv.start + 1) as u64;

                        increment.seen_data_size += output_data.size as u64;
                    }

                    if let Some(checkpoint) = execute_query.output_checkpoint {
                        increment.seen_checkpoints_size += checkpoint.size as u64;
                    }
                }
                MetadataEvent::SetAttachments(_)
                | MetadataEvent::SetInfo(_)
                | MetadataEvent::SetLicense(_)
                | MetadataEvent::SetWatermark(_)
                | MetadataEvent::SetVocab(_)
                | MetadataEvent::SetPollingSource(_) => (),
            }
        }

        Ok(increment)
    }

    fn prepare_blank_summary_for_update(last_block_hash: &Multihash) -> DatasetSummary {
        DatasetSummary {
            id: DatasetID::from_pub_key_ed25519(b""), // Will be replaced
            kind: DatasetKind::Root,
            last_block_hash: last_block_hash.clone(),
            dependencies: Vec::new(),
            last_pulled: None,
            num_records: 0,
            data_size: 0,
            checkpoints_size: 0,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct UpdateSummaryIncrement {
    seen_id: Option<DatasetID>,
    seen_kind: Option<DatasetKind>,
    seen_dependencies: Option<Vec<TransformInput>>,
    seen_last_pulled: Option<DateTime<Utc>>,
    seen_num_records: u64,
    seen_data_size: u64,
    seen_checkpoints_size: u64,
    seen_empty_prev_block_hash: bool,
}

impl UpdateSummaryIncrement {
    fn new() -> Self {
        Self {
            seen_id: None,
            seen_kind: None,
            seen_dependencies: None,
            seen_last_pulled: None,
            seen_num_records: 0,
            seen_data_size: 0,
            seen_checkpoints_size: 0,
            seen_empty_prev_block_hash: false,
        }
    }

    fn apply_increment_to(
        increment: UpdateSummaryIncrement,
        base_summary: &DatasetSummary,
        head: &Multihash,
    ) -> DatasetSummary {
        DatasetSummary {
            id: increment.seen_id.unwrap_or_else(|| base_summary.id.clone()),
            kind: increment.seen_kind.unwrap_or(base_summary.kind),
            last_block_hash: head.clone(),
            dependencies: increment
                .seen_dependencies
                .unwrap_or_else(|| base_summary.dependencies.clone()),
            last_pulled: increment.seen_last_pulled.or(base_summary.last_pulled),
            num_records: base_summary.num_records + increment.seen_num_records,
            data_size: base_summary.data_size + increment.seen_data_size,
            checkpoints_size: base_summary.checkpoints_size + increment.seen_checkpoints_size,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<MetaChain, DataRepo, CheckpointRepo, CacheRepo, InfoRepo> Dataset
    for DatasetImpl<MetaChain, DataRepo, CheckpointRepo, CacheRepo, InfoRepo>
where
    MetaChain: MetadataChain + Sync + Send,
    DataRepo: ObjectRepository + Sync + Send,
    CheckpointRepo: ObjectRepository + Sync + Send,
    CacheRepo: NamedObjectRepository + Sync + Send,
    InfoRepo: NamedObjectRepository + Sync + Send,
{
    async fn get_summary(&self, opts: SummaryOptions) -> Result<DatasetSummary, GetSummaryError> {
        let summary = self.read_summary().await?;

        let summary = if opts.update_if_stale {
            self.update_summary(summary).await?
        } else {
            summary
        };

        summary.ok_or_else(|| GetSummaryError::EmptyDataset)
    }

    fn as_metadata_chain(&self) -> &dyn MetadataChain {
        &self.metadata_chain
    }

    fn as_data_repo(&self) -> &dyn ObjectRepository {
        &self.data_repo
    }

    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository {
        &self.checkpoint_repo
    }

    fn as_cache_repo(&self) -> &dyn NamedObjectRepository {
        &self.cache_repo
    }
}
