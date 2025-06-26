// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf::dataset::MetadataChainExt as _;
use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use crate::mutations::{FlowInvalidRunConfigurations, FlowTypeIsNotSupported};
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

#[derive(OneofObject, Copy, Clone)]
pub enum FlowConfigInput {
    Ingest(FlowConfigInputIngest),
    Compaction(FlowConfigInputCompaction),
}

impl From<FlowConfigInput> for FlowRunConfigInput {
    fn from(value: FlowConfigInput) -> Self {
        match value {
            FlowConfigInput::Ingest(ingest_input) => Self::Ingest(ingest_input),
            FlowConfigInput::Compaction(compaction_input) => Self::Compaction(compaction_input),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject)]
pub enum FlowRunConfigInput {
    Compaction(FlowConfigInputCompaction),
    Ingest(FlowConfigInputIngest),
    Reset(FlowConfigInputReset),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct FlowConfigInputReset {
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

impl FlowConfigInputReset {
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
pub enum FlowConfigInputCompaction {
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

impl From<FlowConfigInputCompaction> for FlowRunConfigInput {
    fn from(value: FlowConfigInputCompaction) -> Self {
        Self::Compaction(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Copy, Clone)]
pub struct FlowConfigInputIngest {
    /// Flag indicates to ignore cache during ingest step for API calls
    pub fetch_uncacheable: bool,
}

impl From<FlowConfigInputIngest> for afs::FlowConfigRuleIngest {
    fn from(value: FlowConfigInputIngest) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowRunConfigInput {
    pub async fn try_into_snapshot(
        ctx: &Context<'_>,
        dataset_flow_type: &DatasetFlowType,
        dataset_handle: &odf::DatasetHandle,
        flow_run_configuration_maybe: Option<&FlowRunConfigInput>,
    ) -> Result<Option<fs::FlowConfigurationRule>, FlowInvalidRunConfigurations> {
        match dataset_flow_type {
            DatasetFlowType::Ingest => {
                if let Some(flow_run_configuration) = flow_run_configuration_maybe {
                    if let Self::Ingest(ingest_input) = flow_run_configuration {
                        return Ok(Some(
                            afs::FlowConfigRuleIngest {
                                fetch_uncacheable: ingest_input.fetch_uncacheable,
                            }
                            .into_flow_config(),
                        ));
                    }
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                }
            }
            DatasetFlowType::ExecuteTransform => return Ok(None),
            DatasetFlowType::HardCompaction => {
                if let Some(flow_run_configuration) = flow_run_configuration_maybe {
                    if let Self::Compaction(compaction_input) = flow_run_configuration {
                        return Ok(Some(
                            match compaction_input {
                                FlowConfigInputCompaction::Full(compaction_input) => {
                                    afs::FlowConfigRuleCompact::Full(
                                        afs::FlowConfigRuleCompactFull::new_checked(
                                            compaction_input.max_slice_size,
                                            compaction_input.max_slice_records,
                                            compaction_input.recursive,
                                        )
                                        .map_err(|_| {
                                            FlowInvalidRunConfigurations {
                                                error: "Invalid compaction flow run configuration"
                                                    .to_string(),
                                            }
                                        })?,
                                    )
                                }
                                FlowConfigInputCompaction::MetadataOnly(compaction_input) => {
                                    afs::FlowConfigRuleCompact::MetadataOnly {
                                        recursive: compaction_input.recursive,
                                    }
                                }
                            }
                            .into_flow_config(),
                        ));
                    }
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                }
            }
            DatasetFlowType::Reset => {
                let dataset_registry = from_catalog_n!(ctx, dyn kamu_core::DatasetRegistry);
                let resolved_dataset = dataset_registry.get_dataset_by_handle(dataset_handle).await;

                // Assume unwrap safe such as we have checked this existence during
                // validation step
                let current_head_hash = resolved_dataset
                    .as_metadata_chain()
                    .try_get_ref(&odf::BlockRef::Head)
                    .await
                    .map_err(|_| FlowInvalidRunConfigurations {
                        error: "Cannot fetch default value".to_string(),
                    })?;
                if let Some(flow_run_configuration) = flow_run_configuration_maybe {
                    if let Self::Reset(reset_input) = flow_run_configuration {
                        let old_head_hash = if reset_input.old_head_hash.is_some() {
                            reset_input.old_head_hash.clone().map(Into::into)
                        } else {
                            current_head_hash
                        };
                        return Ok(Some(
                            afs::FlowConfigRuleReset {
                                new_head_hash: reset_input.new_head_hash().map(Into::into),
                                old_head_hash,
                                recursive: reset_input.recursive,
                            }
                            .into_flow_config(),
                        ));
                    }
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                }
                return Ok(Some(
                    afs::FlowConfigRuleReset {
                        new_head_hash: None,
                        old_head_hash: current_head_hash,
                        recursive: false,
                    }
                    .into_flow_config(),
                ));
            }
        }
        Ok(None)
    }

    pub fn check_type_compatible(
        &self,
        flow_type: DatasetFlowType,
    ) -> Result<(), FlowTypeIsNotSupported> {
        match self {
            Self::Ingest(_) => {
                if flow_type == DatasetFlowType::Ingest {
                    return Ok(());
                }
            }
            Self::Compaction(_) => {
                if flow_type == DatasetFlowType::HardCompaction {
                    return Ok(());
                }
            }
            Self::Reset(_) => {
                if flow_type == DatasetFlowType::Reset {
                    return Ok(());
                }
            }
        }
        Err(FlowTypeIsNotSupported)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowRetryPolicy {
    pub max_attempts: u32,
    pub min_delay: TimeDelta,
    pub backoff_type: FlowRetryBackoffType,
}

impl From<kamu_flow_system::RetryPolicy> for FlowRetryPolicy {
    fn from(value: kamu_flow_system::RetryPolicy) -> Self {
        let min_delay_duration = chrono::Duration::seconds(i64::from(value.min_delay_seconds));

        Self {
            max_attempts: value.max_attempts,
            min_delay: min_delay_duration.into(),
            backoff_type: value.backoff_type.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
#[graphql(remote = "kamu_flow_system::RetryBackoffType")]
pub enum FlowRetryBackoffType {
    Fixed,
    Linear,
    Exponential,
    ExponentialWithJitter,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
