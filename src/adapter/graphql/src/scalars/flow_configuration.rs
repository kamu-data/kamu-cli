// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfiguration {
    pub rule: FlowConfigRule,
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
                Self::Compaction(match compaction_rule {
                    afs::FlowConfigRuleCompact::Full(full_rule) => FlowConfigRuleCompaction {
                        compaction_mode: FlowConfigCompactionMode::Full(full_rule.into()),
                    },
                    afs::FlowConfigRuleCompact::MetadataOnly { recursive } => {
                        FlowConfigRuleCompaction {
                            compaction_mode: FlowConfigCompactionMode::MetadataOnly(
                                FlowConfigCompactionModeMetadataOnly { recursive },
                            ),
                        }
                    }
                })
            }

            _ => panic!(
                "Unsupported flow configuration rule type: {}",
                value.rule_type
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigRuleIngest {
    pub fetch_uncacheable: bool,
}

impl From<afs::FlowConfigRuleIngest> for FlowConfigRuleIngest {
    fn from(value: afs::FlowConfigRuleIngest) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Eq, PartialEq)]
pub struct FlowConfigRuleCompaction {
    compaction_mode: FlowConfigCompactionMode,
}

#[derive(Union, PartialEq, Eq)]
pub enum FlowConfigCompactionMode {
    Full(FlowConfigCompactionModeFull),
    MetadataOnly(FlowConfigCompactionModeMetadataOnly),
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigCompactionModeFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

impl From<afs::FlowConfigRuleCompactFull> for FlowConfigCompactionModeFull {
    fn from(value: afs::FlowConfigRuleCompactFull) -> Self {
        Self {
            max_slice_records: value.max_slice_records(),
            max_slice_size: value.max_slice_size(),
            recursive: value.recursive(),
        }
    }
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigCompactionModeMetadataOnly {
    pub recursive: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigRuleReset {
    pub mode: FlowConfigResetPropagationMode,
    pub old_head_hash: Option<Multihash<'static>>,
    pub recursive: bool,
}

#[derive(Union, PartialEq, Eq)]
pub enum FlowConfigResetPropagationMode {
    Custom(FlowConfigResetPropagationModeCustom),
    ToSeed(FlowConfigResetPropagationModeToSeed),
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigResetPropagationModeCustom {
    pub new_head_hash: Multihash<'static>,
}

#[derive(SimpleObject, PartialEq, Eq, Default)]
pub struct FlowConfigResetPropagationModeToSeed {
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
            recursive: value.recursive,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct FlowConfigResetInput {
    pub mode: FlowConfigInputResetPropagationMode,
    pub old_head_hash: Option<Multihash<'static>>,
    pub recursive: bool,
}

#[derive(OneofObject)]
pub enum FlowConfigInputResetPropagationMode {
    Custom(FlowConfigInputResetPropagationModeCustom),
    ToSeed(FlowConfigInputResetPropagationModeToSeed),
}

#[derive(InputObject)]
pub struct FlowConfigInputResetPropagationModeCustom {
    pub new_head_hash: Multihash<'static>,
}

#[derive(InputObject)]
pub struct FlowConfigInputResetPropagationModeToSeed {
    _dummy: Option<String>,
}

impl FlowConfigResetInput {
    pub fn new_head_hash(&self) -> Option<Multihash> {
        match &self.mode {
            FlowConfigInputResetPropagationMode::Custom(custom_args) => {
                Some(custom_args.new_head_hash.clone())
            }
            FlowConfigInputResetPropagationMode::ToSeed(_) => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Copy, Clone)]
pub enum FlowConfigCompactionInput {
    Full(FlowConfigInputCompactionFull),
    MetadataOnly(FlowConfigInputCompactionMetadataOnly),
}

#[derive(InputObject, Copy, Clone)]
pub struct FlowConfigInputCompactionFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

#[derive(InputObject, Copy, Clone)]
pub struct FlowConfigInputCompactionMetadataOnly {
    pub recursive: bool,
}

impl TryFrom<FlowConfigCompactionInput> for afs::FlowConfigRuleCompact {
    type Error = String;

    fn try_from(value: FlowConfigCompactionInput) -> Result<Self, Self::Error> {
        Ok(match value {
            FlowConfigCompactionInput::Full(full_input) => afs::FlowConfigRuleCompact::Full(
                afs::FlowConfigRuleCompactFull::new_checked(
                    full_input.max_slice_size,
                    full_input.max_slice_records,
                    full_input.recursive,
                )
                .map_err(|err| err.to_string())?,
            ),
            FlowConfigCompactionInput::MetadataOnly(metadata_only_input) => {
                afs::FlowConfigRuleCompact::MetadataOnly {
                    recursive: metadata_only_input.recursive,
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Copy, Clone)]
pub struct FlowConfigIngestInput {
    /// Flag indicates to ignore cache during ingest step for API calls
    pub fetch_uncacheable: bool,
}

impl From<FlowConfigIngestInput> for afs::FlowConfigRuleIngest {
    fn from(value: FlowConfigIngestInput) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
