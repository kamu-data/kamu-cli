// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    FLOW_SCOPE_TYPE_DATASET,
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_RESET_TO_METADATA,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_adapter_flow_webhook::{
    FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION,
    FLOW_TYPE_WEBHOOK_DELIVER,
    FlowScopeSubscription,
};
use kamu_adapter_task_dataset::{
    TaskResultDatasetHardCompact,
    TaskResultDatasetReset,
    TaskResultDatasetResetToMetadata,
    TaskResultDatasetUpdate,
};
use kamu_core::{CompactionResult, PullResultUpToDate};
use kamu_datasets::{DatasetIncrementQueryService, GetIncrementError};
use kamu_flow_system::{FLOW_SCOPE_TYPE_SYSTEM, FLOW_TYPE_SYSTEM_GC};
use {kamu_flow_system as fs, kamu_task_system as ts};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowDescription {
    #[graphql(flatten)]
    Dataset(FlowDescriptionDataset),
    #[graphql(flatten)]
    System(FlowDescriptionSystem),
    #[graphql(flatten)]
    Webhook(FlowDescriptionWebhook),
}

#[derive(Union)]
pub(crate) enum FlowDescriptionSystem {
    GC(FlowDescriptionSystemGC),
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionSystemGC {
    dummy: bool,
}

#[derive(Union)]
pub(crate) enum FlowDescriptionWebhook {
    Deliver(FlowDescriptionWebhookDeliver),
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionWebhookDeliver {
    target_url: url::Url,
    label: String,
    event_type: String,
}

#[derive(Union)]
pub(crate) enum FlowDescriptionDataset {
    Unknown(FlowDescriptionUnknown),
    PollingIngest(FlowDescriptionDatasetPollingIngest),
    PushIngest(FlowDescriptionDatasetPushIngest),
    ExecuteTransform(FlowDescriptionDatasetExecuteTransform),
    HardCompaction(FlowDescriptionDatasetHardCompaction),
    Reset(FlowDescriptionDatasetReset),
    ResetToMetadata(FlowDescriptionDatasetResetToMetadata),
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionUnknown {
    message: String,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetPollingIngest {
    ingest_result: Option<FlowDescriptionUpdateResult>,
    polling_source: SetPollingSource,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetPushIngest {
    source_name: Option<String>,
    ingest_result: Option<FlowDescriptionUpdateResult>,
    message: String,
    // TODO: SetPushSource
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetExecuteTransform {
    transform_result: Option<FlowDescriptionUpdateResult>,
    transform: SetTransform,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetHardCompaction {
    compaction_result: Option<FlowDescriptionDatasetReorganizationResult>,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetReset {
    reset_result: Option<FlowDescriptionResetResult>,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetResetToMetadata {
    reset_to_metadata_result: Option<FlowDescriptionDatasetReorganizationResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowDescriptionUpdateResult {
    UpToDate(FlowDescriptionUpdateResultUpToDate),
    Success(FlowDescriptionUpdateResultSuccess),
    Unknown(FlowDescriptionUpdateResultUnknown),
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionUpdateResultUnknown {
    message: String,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionUpdateResultUpToDate {
    /// The value indicates whether the api cache was used
    uncacheable: bool,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionUpdateResultSuccess {
    num_blocks: u64,
    num_records: u64,
    updated_watermark: Option<DateTime<Utc>>,
    has_more: bool,
}

impl FlowDescriptionUpdateResult {
    async fn from_maybe_flow_outcome(
        maybe_outcome: Option<&fs::FlowOutcome>,
        dataset_id: &odf::DatasetID,
        increment_query_service: &dyn DatasetIncrementQueryService,
    ) -> Result<Option<Self>, InternalError> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result.result_type.as_str() {
                    ts::TaskResult::TASK_RESULT_EMPTY
                    | TaskResultDatasetHardCompact::TYPE_ID
                    | TaskResultDatasetResetToMetadata::TYPE_ID
                    | TaskResultDatasetReset::TYPE_ID => Ok(None),

                    TaskResultDatasetUpdate::TYPE_ID => {
                        // TODO: consider caching the increment in the task result itself
                        let update = TaskResultDatasetUpdate::from_task_result(result)?;
                        if let Some((old_head, new_head)) = update.try_as_increment() {
                            match increment_query_service
                                .get_increment_between(dataset_id, old_head, new_head)
                                .await
                            {
                                Ok(increment) => {
                                    Ok(Some(Self::Success(FlowDescriptionUpdateResultSuccess {
                                        num_blocks: increment.num_blocks,
                                        num_records: increment.num_records,
                                        updated_watermark: increment.updated_watermark,
                                        has_more: update.has_more(),
                                    })))
                                }
                                Err(err) => {
                                    let unknown_message = match err {
                                        GetIncrementError::BlockNotFound(e) => format!(
                                            "Unable to fetch increment. Block is missing: {}",
                                            e.hash
                                        ),
                                        _ => "Unable to fetch increment".to_string(),
                                    };
                                    Ok(Some(Self::Unknown(FlowDescriptionUpdateResultUnknown {
                                        message: unknown_message,
                                    })))
                                }
                            }
                        } else if let Some(up_to_date_result) = update.try_as_up_to_date() {
                            match up_to_date_result {
                                PullResultUpToDate::PollingIngest(pi) => {
                                    Ok(Some(Self::UpToDate(FlowDescriptionUpdateResultUpToDate {
                                        uncacheable: pi.uncacheable,
                                    })))
                                }
                                PullResultUpToDate::PushIngest(pi) => {
                                    Ok(Some(Self::UpToDate(FlowDescriptionUpdateResultUpToDate {
                                        uncacheable: pi.uncacheable,
                                    })))
                                }
                                PullResultUpToDate::Sync | PullResultUpToDate::Transform => {
                                    Ok(Some(Self::UpToDate(FlowDescriptionUpdateResultUpToDate {
                                        uncacheable: false,
                                    })))
                                }
                            }
                        } else {
                            unreachable!()
                        }
                    }

                    _ => {
                        tracing::error!(
                            "Unexpected task result type: {} for flow outcome: {:?}",
                            result.result_type,
                            outcome
                        );
                        Ok(None)
                    }
                },
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug)]
enum FlowDescriptionDatasetReorganizationResult {
    NothingToDo(FlowDescriptionReorganizationNothingToDo),
    Success(FlowDescriptionReorganizationSuccess),
}

#[derive(SimpleObject, Debug)]
struct FlowDescriptionReorganizationSuccess {
    original_blocks_count: u64,
    resulting_blocks_count: u64,
    new_head: Multihash<'static>,
}

#[derive(SimpleObject, Debug, Default)]
#[graphql(complex)]
pub struct FlowDescriptionReorganizationNothingToDo {
    _dummy: Option<String>,
}

#[ComplexObject]
impl FlowDescriptionReorganizationNothingToDo {
    async fn message(&self) -> String {
        "Nothing to do".to_string()
    }
}

impl FlowDescriptionDatasetReorganizationResult {
    fn from_maybe_flow_outcome(maybe_outcome: Option<&fs::FlowOutcome>) -> Result<Option<Self>> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result.result_type.as_str() {
                    TaskResultDatasetReset::TYPE_ID | TaskResultDatasetUpdate::TYPE_ID => Ok(None),

                    ts::TaskResult::TASK_RESULT_EMPTY => Ok(Some(Self::NothingToDo(
                        FlowDescriptionReorganizationNothingToDo::default(),
                    ))),

                    TaskResultDatasetHardCompact::TYPE_ID => {
                        let r = TaskResultDatasetHardCompact::from_task_result(result)?;
                        match r.compaction_result {
                            CompactionResult::NothingToDo => Ok(Some(Self::NothingToDo(
                                FlowDescriptionReorganizationNothingToDo::default(),
                            ))),
                            CompactionResult::Success {
                                old_head: _,
                                ref new_head,
                                old_num_blocks,
                                new_num_blocks,
                            } => Ok(Some(Self::Success(FlowDescriptionReorganizationSuccess {
                                original_blocks_count: old_num_blocks as u64,
                                resulting_blocks_count: new_num_blocks as u64,
                                new_head: new_head.clone().into(),
                            }))),
                        }
                    }

                    TaskResultDatasetResetToMetadata::TYPE_ID => {
                        let r = TaskResultDatasetResetToMetadata::from_task_result(result)?;
                        match r.compaction_metadata_only_result {
                            CompactionResult::NothingToDo => Ok(Some(Self::NothingToDo(
                                FlowDescriptionReorganizationNothingToDo::default(),
                            ))),
                            CompactionResult::Success {
                                old_head: _,
                                ref new_head,
                                old_num_blocks,
                                new_num_blocks,
                            } => Ok(Some(Self::Success(FlowDescriptionReorganizationSuccess {
                                original_blocks_count: old_num_blocks as u64,
                                resulting_blocks_count: new_num_blocks as u64,
                                new_head: new_head.clone().into(),
                            }))),
                        }
                    }

                    _ => {
                        tracing::error!(
                            "Unexpected task result type: {} for flow outcome: {:?}",
                            result.result_type,
                            outcome
                        );
                        Ok(None)
                    }
                },

                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
struct FlowDescriptionResetResult {
    new_head: Multihash<'static>,
}

impl FlowDescriptionResetResult {
    fn from_maybe_flow_outcome(maybe_outcome: Option<&fs::FlowOutcome>) -> Result<Option<Self>> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result.result_type.as_str() {
                    ts::TaskResult::TASK_RESULT_EMPTY
                    | TaskResultDatasetHardCompact::TYPE_ID
                    | TaskResultDatasetResetToMetadata::TYPE_ID
                    | TaskResultDatasetUpdate::TYPE_ID => Ok(None),

                    TaskResultDatasetReset::TYPE_ID => {
                        let r = TaskResultDatasetReset::from_task_result(result)?;
                        Ok(Some(Self {
                            new_head: r.reset_result.new_head.clone().into(),
                        }))
                    }

                    _ => {
                        tracing::error!(
                            "Unexpected task result type: {} for flow outcome: {:?}",
                            result.result_type,
                            outcome
                        );
                        Ok(None)
                    }
                },
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowDescriptionBuilder {
    polling_sources_by_dataset_id: HashMap<odf::DatasetID, odf::metadata::SetPollingSource>,
    transforms_by_dataset_id: HashMap<odf::DatasetID, odf::metadata::SetTransform>,
}

impl FlowDescriptionBuilder {
    pub async fn prepare(
        ctx: &Context<'_>,
        flow_states: &[fs::FlowState],
    ) -> Result<Self, InternalError> {
        let unique_dataset_ids = FlowDescriptionBuilder::collect_unique_dataset_ids(flow_states);

        Ok(Self {
            polling_sources_by_dataset_id: HashMap::from_iter(
                FlowDescriptionBuilder::detect_datasets_with_polling_sources(
                    ctx,
                    &unique_dataset_ids,
                )
                .await?,
            ),
            transforms_by_dataset_id: HashMap::from_iter(
                FlowDescriptionBuilder::detect_datasets_with_transforms(ctx, &unique_dataset_ids)
                    .await?,
            ),
        })
    }

    fn collect_unique_dataset_ids(flow_states: &[fs::FlowState]) -> Vec<odf::DatasetID> {
        flow_states
            .iter()
            .filter_map(|flow_state| {
                FlowScopeDataset::maybe_dataset_id_in_scope(&flow_state.flow_binding.scope)
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    }

    async fn detect_datasets_with_polling_sources(
        ctx: &Context<'_>,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<Vec<(odf::DatasetID, odf::metadata::SetPollingSource)>, InternalError> {
        // Locate datasets with polling sources
        let key_blocks_repository =
            from_catalog_n!(ctx, dyn kamu_datasets::DatasetKeyBlockRepository);

        // TODO: to be more precise, we should query the key block within the sequence
        // number range of the head at the flow launch moment,
        // as metadata might have already evolved by now
        let matches = key_blocks_repository
            .match_datasets_having_key_blocks(
                dataset_ids,
                &odf::BlockRef::Head,
                kamu_datasets::MetadataEventType::SetPollingSource,
            )
            .await?;

        let mut results = Vec::new();
        for (dataset_id, key_block) in matches {
            let metadata_block = odf::storage::deserialize_metadata_block(
                &key_block.block_hash,
                &key_block.block_payload,
            )
            .int_err()?;

            if let odf::MetadataEvent::SetPollingSource(set_polling_source) = metadata_block.event {
                results.push((dataset_id, set_polling_source));
            } else {
                panic!(
                    "Expected SetPollingSource event for dataset: {}, but found: {:?}",
                    dataset_id, metadata_block.event
                );
            }
        }

        Ok(results)
    }

    async fn detect_datasets_with_transforms(
        ctx: &Context<'_>,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<Vec<(odf::DatasetID, odf::metadata::SetTransform)>, InternalError> {
        // Locate datasets with transforms
        let key_blocks_repository =
            from_catalog_n!(ctx, dyn kamu_datasets::DatasetKeyBlockRepository);

        // TODO: to be more precise, we should query the key block within the sequence
        // number range of the head at the flow launch moment,
        // as metadata might have already evolved by now
        let matches = key_blocks_repository
            .match_datasets_having_key_blocks(
                dataset_ids,
                &odf::BlockRef::Head,
                kamu_datasets::MetadataEventType::SetTransform,
            )
            .await?;

        let mut results = Vec::new();
        for (dataset_id, key_block) in matches {
            let metadata_block = odf::storage::deserialize_metadata_block(
                &key_block.block_hash,
                &key_block.block_payload,
            )
            .int_err()?;

            if let odf::MetadataEvent::SetTransform(set_transform) = metadata_block.event {
                results.push((dataset_id, set_transform));
            } else {
                panic!(
                    "Expected SetTransform event for dataset: {}, but found: {:?}",
                    dataset_id, metadata_block.event
                );
            }
        }

        Ok(results)
    }

    pub async fn build(
        &mut self,
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
    ) -> Result<FlowDescription> {
        let flow_type = flow_state.flow_binding.flow_type.as_str();
        Ok(match flow_state.flow_binding.scope.scope_type() {
            FLOW_SCOPE_TYPE_DATASET => {
                let dataset_id = FlowScopeDataset::new(&flow_state.flow_binding.scope).dataset_id();
                FlowDescription::Dataset(
                    self.dataset_flow_description(ctx, flow_state, flow_type, &dataset_id)
                        .await?,
                )
            }

            FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION => {
                let subscription_id =
                    FlowScopeSubscription::new(&flow_state.flow_binding.scope).subscription_id();
                self.webhook_flow_description(ctx, flow_state, flow_type, subscription_id)
                    .await?
            }

            FLOW_SCOPE_TYPE_SYSTEM => {
                FlowDescription::System(self.system_flow_description(flow_type)?)
            }

            _ => {
                tracing::error!(
                    "Unexpected flow scope type: {}",
                    flow_state.flow_binding.scope.scope_type()
                );
                return Err(GqlError::Internal(InternalError::new(format!(
                    "Unexpected flow scope type: {}",
                    flow_state.flow_binding.scope.scope_type()
                ))));
            }
        })
    }

    async fn webhook_flow_description(
        &self,
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
        flow_type: &str,
        subscription_id: kamu_webhooks::WebhookSubscriptionID,
    ) -> Result<FlowDescription> {
        match flow_type {
            FLOW_TYPE_WEBHOOK_DELIVER => {
                let webhook_subscription_query_svc =
                    from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionQueryService);

                let subscription = webhook_subscription_query_svc
                    .find_webhook_subscription(
                        subscription_id,
                        kamu_webhooks::WebhookSubscriptionQueryMode::IncludingRemoved,
                    )
                    .await
                    .int_err()?
                    .ok_or_else(|| {
                        GqlError::Internal(InternalError::new(format!(
                            "Webhook subscription not found: {subscription_id}",
                        )))
                    })?;

                let subscription_scope = FlowScopeSubscription::new(&flow_state.flow_binding.scope);
                let event_type = subscription_scope.event_type();

                Ok(FlowDescription::Webhook(FlowDescriptionWebhook::Deliver(
                    FlowDescriptionWebhookDeliver {
                        target_url: subscription.target_url().clone(),
                        label: subscription.label().to_string(),
                        event_type: event_type.to_string(),
                    },
                )))
            }
            _ => {
                tracing::error!("Unexpected webhook flow type: {}", flow_type);
                Err(GqlError::Internal(InternalError::new(format!(
                    "Unexpected webhook flow type: {flow_type}",
                ))))
            }
        }
    }

    fn system_flow_description(&self, flow_type: &str) -> Result<FlowDescriptionSystem> {
        match flow_type {
            FLOW_TYPE_SYSTEM_GC => Ok(FlowDescriptionSystem::GC(FlowDescriptionSystemGC {
                dummy: true,
            })),

            _ => {
                tracing::error!("Unexpected system flow type: {}", flow_type,);
                Err(GqlError::Internal(InternalError::new(format!(
                    "Unexpected system flow type: {flow_type}",
                ))))
            }
        }
    }

    async fn dataset_flow_description(
        &self,
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
        flow_type: &str,
        dataset_id: &odf::DatasetID,
    ) -> Result<FlowDescriptionDataset> {
        Ok(match flow_type {
            FLOW_TYPE_DATASET_INGEST => {
                let increment_query_service =
                    from_catalog_n!(ctx, dyn DatasetIncrementQueryService);
                let ingest_result = FlowDescriptionUpdateResult::from_maybe_flow_outcome(
                    flow_state.outcome.as_ref(),
                    dataset_id,
                    increment_query_service.as_ref(),
                )
                .await
                .int_err()?;

                if let Some(polling_source) = self.polling_sources_by_dataset_id.get(dataset_id) {
                    FlowDescriptionDataset::PollingIngest(FlowDescriptionDatasetPollingIngest {
                        ingest_result,
                        polling_source: polling_source.clone().into(),
                    })
                } else {
                    let (maybe_push_source_name, activation_descrtiption) =
                        match flow_state.primary_activation_cause() {
                            fs::FlowActivationCause::Manual(_) => {
                                (None, "Flow activated manually".to_string())
                            }
                            fs::FlowActivationCause::AutoPolling(_)
                            | fs::FlowActivationCause::IterationFinished(_) => {
                                (None, "Flow activated automatically".to_string())
                            }
                            fs::FlowActivationCause::ResourceUpdate(update) => {
                                if update.resource_type == DATASET_RESOURCE_TYPE {
                                    let dataset_update_details: DatasetResourceUpdateDetails =
                                        serde_json::from_value(update.details.clone()).int_err()?;
                                    (
                                        dataset_update_details.push_source_name(),
                                        dataset_update_details.flow_description(),
                                    )
                                } else {
                                    panic!("Unexpected resource type: {}", update.resource_type);
                                }
                            }
                        };

                    FlowDescriptionDataset::PushIngest(FlowDescriptionDatasetPushIngest {
                        source_name: maybe_push_source_name,
                        ingest_result,
                        message: activation_descrtiption,
                    })
                }
            }

            FLOW_TYPE_DATASET_TRANSFORM => {
                let increment_query_service =
                    from_catalog_n!(ctx, dyn DatasetIncrementQueryService);

                if let Some(transform) = self.transforms_by_dataset_id.get(dataset_id) {
                    let transform_result = FlowDescriptionUpdateResult::from_maybe_flow_outcome(
                        flow_state.outcome.as_ref(),
                        dataset_id,
                        increment_query_service.as_ref(),
                    )
                    .await
                    .int_err()?;

                    FlowDescriptionDataset::ExecuteTransform(
                        FlowDescriptionDatasetExecuteTransform {
                            transform_result,
                            transform: transform.clone().into(),
                        },
                    )
                } else {
                    tracing::warn!(
                        "Flow {} of type {} has no SetTransformEvent for dataset {}",
                        flow_state.flow_id,
                        flow_type,
                        dataset_id
                    );
                    FlowDescriptionDataset::Unknown(FlowDescriptionUnknown {
                        message: format!("No SetTransformEvent for dataset {dataset_id}",),
                    })
                }
            }

            FLOW_TYPE_DATASET_COMPACT => {
                FlowDescriptionDataset::HardCompaction(FlowDescriptionDatasetHardCompaction {
                    compaction_result:
                        FlowDescriptionDatasetReorganizationResult::from_maybe_flow_outcome(
                            flow_state.outcome.as_ref(),
                        )?,
                })
            }

            FLOW_TYPE_DATASET_RESET => FlowDescriptionDataset::Reset(FlowDescriptionDatasetReset {
                reset_result: FlowDescriptionResetResult::from_maybe_flow_outcome(
                    flow_state.outcome.as_ref(),
                )?,
            }),

            FLOW_TYPE_DATASET_RESET_TO_METADATA => {
                FlowDescriptionDataset::ResetToMetadata(FlowDescriptionDatasetResetToMetadata {
                    reset_to_metadata_result:
                        FlowDescriptionDatasetReorganizationResult::from_maybe_flow_outcome(
                            flow_state.outcome.as_ref(),
                        )?,
                })
            }

            _ => {
                tracing::error!("Unexpected flow type: {flow_type} for flow state: {flow_state:?}",);
                return Err(GqlError::Internal(InternalError::new(format!(
                    "Unexpected flow type: {flow_type} for flow state: {flow_state:?}",
                ))));
            }
        })
    }
}
