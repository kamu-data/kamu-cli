// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset as afs;
use kamu_flow_system as fs;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings that define how a flow runs
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfiguration {
    /// Settings specific to the flow type
    pub rule: FlowConfigRule,
    /// Retry policy for failed runs, null to use the flow type's default
    pub retry_policy: Option<FlowRetryPolicy>,
}

impl From<kamu_flow_system::FlowConfigurationState> for FlowConfiguration {
    fn from(value: kamu_flow_system::FlowConfigurationState) -> Self {
        Self {
            rule: FlowConfigRule::from(value.rule),
            retry_policy: value.retry_policy.map(FlowRetryPolicy::from),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings specific to the flow type
#[derive(Union, Eq, PartialEq)]
pub enum FlowConfigRule {
    Ingest(FlowConfigRuleIngest),
    Compaction(FlowConfigRuleCompaction),
    Reset(FlowConfigRuleReset),
}

impl From<fs::FlowConfigurationRule> for FlowConfigRule {
    fn from(value: fs::FlowConfigurationRule) -> Self {
        match value.rule_type.as_str() {
            afs::FlowConfigRuleIngest::TYPE_ID => {
                let ingest_rule = afs::FlowConfigRuleIngest::from_flow_config(&value).unwrap();
                Self::Ingest(ingest_rule.into())
            }

            afs::FlowConfigRuleReset::TYPE_ID => {
                let reset_rule = afs::FlowConfigRuleReset::from_flow_config(&value).unwrap();
                Self::Reset(reset_rule.into())
            }

            afs::FlowConfigRuleCompact::TYPE_ID => {
                let compaction_rule = afs::FlowConfigRuleCompact::from_flow_config(&value).unwrap();
                Self::Compaction(compaction_rule.into())
            }

            _ => panic!(
                "Unsupported flow configuration rule type: {}",
                value.rule_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings of an ingest flow
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigRuleIngest {
    /// Whether to ignore the cache for API calls during ingest
    pub fetch_uncacheable: bool,
    /// Whether to run the next ingest right away when the source has more
    /// data
    pub fetch_next_iteration: bool,
}

impl From<afs::FlowConfigRuleIngest> for FlowConfigRuleIngest {
    fn from(value: afs::FlowConfigRuleIngest) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
            fetch_next_iteration: value.fetch_next_iteration,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings of a compaction flow
#[derive(SimpleObject, Eq, PartialEq)]
pub struct FlowConfigRuleCompaction {
    /// Maximum size of a compacted data slice, in bytes
    pub max_slice_size: u64,
    /// Maximum number of records in a compacted data slice
    pub max_slice_records: u64,
}

impl From<afs::FlowConfigRuleCompact> for FlowConfigRuleCompaction {
    fn from(value: afs::FlowConfigRuleCompact) -> Self {
        Self {
            max_slice_records: value.max_slice_records(),
            max_slice_size: value.max_slice_size(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings of a reset flow
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigRuleReset {
    /// Block the dataset head is moved to
    pub mode: FlowConfigResetPropagationMode,
    /// Expected current head; the reset fails if the head differs, null to
    /// skip the check
    pub old_head_hash: Option<Multihash<'static>>,
}

/// Block a reset moves the dataset head to
#[derive(Union, PartialEq, Eq)]
pub enum FlowConfigResetPropagationMode {
    Custom(FlowConfigResetPropagationModeCustom),
    ToSeed(FlowConfigResetPropagationModeToSeed),
}

/// Reset the dataset head to a given block
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigResetPropagationModeCustom {
    /// Hash of the block that becomes the new head
    pub new_head_hash: Multihash<'static>,
}

/// Reset the dataset head to its seed block
#[derive(SimpleObject, PartialEq, Eq, Default)]
pub struct FlowConfigResetPropagationModeToSeed {
    /// Placeholder without meaning, always null
    _dummy: Option<String>,
}

impl From<afs::FlowConfigRuleReset> for FlowConfigRuleReset {
    fn from(value: afs::FlowConfigRuleReset) -> Self {
        let propagation_mode = if let Some(new_head_hash) = value.new_head_hash {
            FlowConfigResetPropagationMode::Custom(FlowConfigResetPropagationModeCustom {
                new_head_hash: new_head_hash.into(),
            })
        } else {
            FlowConfigResetPropagationMode::ToSeed(FlowConfigResetPropagationModeToSeed::default())
        };
        Self {
            mode: propagation_mode,
            old_head_hash: value.old_head_hash.map(Into::into),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings of a reset flow
#[derive(InputObject)]
pub struct FlowConfigResetInput {
    /// Block the dataset head is moved to
    pub mode: FlowConfigInputResetPropagationMode,
    /// Expected current head; the reset fails if the head differs, null to
    /// skip the check
    pub old_head_hash: Option<Multihash<'static>>,
}

/// Block a reset moves the dataset head to
#[derive(OneofObject)]
pub enum FlowConfigInputResetPropagationMode {
    /// Reset to a given block
    Custom(FlowConfigInputResetPropagationModeCustom),
    /// Reset to the seed block
    ToSeed(FlowConfigInputResetPropagationModeToSeed),
}

/// Reset the dataset head to a given block
#[derive(InputObject)]
pub struct FlowConfigInputResetPropagationModeCustom {
    /// Hash of the block that becomes the new head
    pub new_head_hash: Multihash<'static>,
}

/// Reset the dataset head to its seed block
#[derive(InputObject)]
pub struct FlowConfigInputResetPropagationModeToSeed {
    /// Placeholder without meaning, the value is ignored
    _dummy: Option<String>,
}

impl FlowConfigResetInput {
    pub fn new_head_hash(&self) -> Option<Multihash<'_>> {
        match &self.mode {
            FlowConfigInputResetPropagationMode::Custom(custom_args) => {
                Some(custom_args.new_head_hash.clone())
            }
            FlowConfigInputResetPropagationMode::ToSeed(_) => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings of a compaction flow
#[derive(InputObject, Copy, Clone)]
pub struct FlowConfigCompactionInput {
    /// Maximum size of a compacted data slice, in bytes
    pub max_slice_size: u64,
    /// Maximum number of records in a compacted data slice
    pub max_slice_records: u64,
}

impl TryFrom<FlowConfigCompactionInput> for afs::FlowConfigRuleCompact {
    type Error = String;

    fn try_from(value: FlowConfigCompactionInput) -> Result<Self, Self::Error> {
        afs::FlowConfigRuleCompact::try_new(value.max_slice_size, value.max_slice_records)
            .map_err(|err| err.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Settings of an ingest flow
#[derive(InputObject, Copy, Clone)]
pub struct FlowConfigIngestInput {
    /// Flag indicates to ignore cache during ingest step for API calls
    pub fetch_uncacheable: bool,
    /// Flag indicates to trigger next flow iteration right if `has_more` is
    /// true
    pub fetch_next_iteration: bool,
}

impl From<FlowConfigIngestInput> for afs::FlowConfigRuleIngest {
    fn from(value: FlowConfigIngestInput) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
            fetch_next_iteration: value.fetch_next_iteration,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
