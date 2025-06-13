// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_variants::*;
use serde::{Deserialize, Serialize};

use crate::{
    TASK_TYPE_DATASET_UPDATE,
    TASK_TYPE_DELIVER_WEBHOOK,
    TASK_TYPE_HARD_COMPACT_DATASET,
    TASK_TYPE_PROBE,
    TASK_TYPE_RESET_DATASET,
    TaskOutcome,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents logical steps needed to carry out a task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalPlan {
    /// Perform an update on a dataset like update from polling source or a
    /// derivative transform
    UpdateDataset(LogicalPlanUpdateDataset),
    /// A task that can be used for testing the scheduling system
    Probe(LogicalPlanProbe),
    /// Perform a dataset hard compaction
    HardCompactDataset(LogicalPlanHardCompactDataset),
    /// Perform a dataset resetting
    ResetDataset(LogicalPlanResetDataset),
    /// Deliver a webhook
    DeliverWebhook(LogicalPlanDeliverWebhook),
}

impl LogicalPlan {
    /// Returns the dataset ID this plan operates on if any
    pub fn dataset_id(&self) -> Option<&odf::DatasetID> {
        match self {
            LogicalPlan::UpdateDataset(upd) => Some(&upd.dataset_id),
            LogicalPlan::Probe(p) => p.dataset_id.as_ref(),
            LogicalPlan::HardCompactDataset(hard_compaction) => Some(&hard_compaction.dataset_id),
            LogicalPlan::ResetDataset(reset) => Some(&reset.dataset_id),
            LogicalPlan::DeliverWebhook(_) => None,
        }
    }

    pub fn task_type(&self) -> &'static str {
        match self {
            LogicalPlan::UpdateDataset(_) => TASK_TYPE_DATASET_UPDATE,
            LogicalPlan::Probe(_) => TASK_TYPE_PROBE,
            LogicalPlan::HardCompactDataset(_) => TASK_TYPE_HARD_COMPACT_DATASET,
            LogicalPlan::ResetDataset(_) => TASK_TYPE_RESET_DATASET,
            LogicalPlan::DeliverWebhook(_) => TASK_TYPE_DELIVER_WEBHOOK,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Perform an update on a dataset like update from polling source or a
/// derivative transform
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalPlanUpdateDataset {
    /// ID of the dataset to update
    pub dataset_id: odf::DatasetID,
    pub fetch_uncacheable: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task that can be used for testing the scheduling system
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogicalPlanProbe {
    /// ID of the dataset this task should be associated with
    pub dataset_id: Option<odf::DatasetID>,
    pub busy_time: Option<std::time::Duration>,
    pub end_with_outcome: Option<TaskOutcome>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task to perform a hard compaction of dataset
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalPlanHardCompactDataset {
    pub dataset_id: odf::DatasetID,
    pub max_slice_size: Option<u64>,
    pub max_slice_records: Option<u64>,
    pub keep_metadata_only: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task to perform the resetting of a dataset
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalPlanResetDataset {
    pub dataset_id: odf::DatasetID,
    pub new_head_hash: Option<odf::Multihash>,
    pub old_head_hash: Option<odf::Multihash>,
    pub recursive: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task that can be used for testing the scheduling system
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogicalPlanDeliverWebhook {
    pub webhook_subscription_id: uuid::Uuid,
    pub webhook_event_id: uuid::Uuid,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with derive macro
impl_enum_with_variants!(LogicalPlan);
impl_enum_variant!(LogicalPlan::UpdateDataset(LogicalPlanUpdateDataset));
impl_enum_variant!(LogicalPlan::Probe(LogicalPlanProbe));
impl_enum_variant!(LogicalPlan::ResetDataset(LogicalPlanResetDataset));
impl_enum_variant!(LogicalPlan::HardCompactDataset(
    LogicalPlanHardCompactDataset
));
impl_enum_variant!(LogicalPlan::DeliverWebhook(LogicalPlanDeliverWebhook));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
