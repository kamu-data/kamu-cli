// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::{
    FlowConfigRuleCompact,
    FlowConfigRuleCompactFull,
    FlowConfigRuleIngest,
    FlowConfigRuleReset,
};
use kamu_flow_system::FlowConfigurationRule;
use odf::dataset::MetadataChainExt as _;

use crate::mutations::{FlowInvalidRunConfigurations, FlowTypeIsNotSupported};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfiguration {
    pub compaction: Option<FlowConfigurationCompaction>,
    pub ingest: Option<FlowConfigurationIngest>,
    pub reset: Option<FlowConfigurationReset>,
}

impl From<kamu_flow_system::FlowConfigurationState> for FlowConfiguration {
    fn from(value: kamu_flow_system::FlowConfigurationState) -> Self {
        let (compaction, ingest, reset) = match value.rule.rule_type.as_str() {
            FlowConfigRuleCompact::TYPE_ID => {
                let compaction_rule = FlowConfigRuleCompact::from_flow_config(&value.rule).unwrap();
                match compaction_rule {
                    FlowConfigRuleCompact::Full(full_rule) => (
                        Some(FlowConfigurationCompaction::Full(full_rule.into())),
                        None,
                        None,
                    ),
                    FlowConfigRuleCompact::MetadataOnly { recursive } => (
                        Some(FlowConfigurationCompaction::MetadataOnly(
                            CompactionMetadataOnly { recursive },
                        )),
                        None,
                        None,
                    ),
                }
            }

            FlowConfigRuleIngest::TYPE_ID => {
                let ingest_rule = FlowConfigRuleIngest::from_flow_config(&value.rule).unwrap();
                (None, Some(ingest_rule.into()), None)
            }

            FlowConfigRuleReset::TYPE_ID => {
                let reset_rule = FlowConfigRuleReset::from_flow_config(&value.rule).unwrap();
                (None, None, Some(reset_rule.into()))
            }

            _ => panic!(
                "Unsupported flow configuration rule type: {}",
                value.rule.rule_type
            ),
        };

        Self {
            compaction,
            ingest,
            reset,
        }
    }
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigurationIngest {
    pub fetch_uncacheable: bool,
}

impl From<FlowConfigRuleIngest> for FlowConfigurationIngest {
    fn from(value: FlowConfigRuleIngest) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowConfigurationReset {
    pub mode: SnapshotPropagationMode,
    pub old_head_hash: Option<Multihash<'static>>,
    pub recursive: bool,
}

#[derive(Union, PartialEq, Eq)]
pub enum SnapshotPropagationMode {
    Custom(SnapshotConfigurationResetCustom),
    ToSeed(SnapshotConfigurationResetToSeedDummy),
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct SnapshotConfigurationResetCustom {
    pub new_head_hash: Multihash<'static>,
}

#[derive(SimpleObject, PartialEq, Eq, Default)]
pub struct SnapshotConfigurationResetToSeedDummy {
    _dummy: Option<String>,
}

impl From<FlowConfigRuleReset> for FlowConfigurationReset {
    fn from(value: FlowConfigRuleReset) -> Self {
        let propagation_mode = if let Some(new_head_hash) = value.new_head_hash {
            SnapshotPropagationMode::Custom(SnapshotConfigurationResetCustom {
                new_head_hash: new_head_hash.into(),
            })
        } else {
            SnapshotPropagationMode::ToSeed(SnapshotConfigurationResetToSeedDummy::default())
        };
        Self {
            mode: propagation_mode,
            old_head_hash: value.old_head_hash.map(Into::into),
            recursive: value.recursive,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, PartialEq, Eq)]
pub enum FlowConfigurationCompaction {
    Full(CompactionFull),
    MetadataOnly(CompactionMetadataOnly),
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct CompactionFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

impl From<FlowConfigRuleCompactFull> for CompactionFull {
    fn from(value: FlowConfigRuleCompactFull) -> Self {
        Self {
            max_slice_records: value.max_slice_records(),
            max_slice_size: value.max_slice_size(),
            recursive: value.recursive(),
        }
    }
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct CompactionMetadataOnly {
    pub recursive: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject)]
pub enum FlowRunConfiguration {
    Compaction(CompactionConditionInput),
    Ingest(IngestConditionInput),
    Reset(ResetConditionInput),
}

#[derive(OneofObject)]
pub enum PropagationMode {
    Custom(FlowConfigurationResetCustom),
    ToSeed(FlowConfigurationResetToSeedDummy),
}

#[derive(InputObject)]
pub struct FlowConfigurationResetCustom {
    pub new_head_hash: Multihash<'static>,
}

#[derive(InputObject)]
pub struct FlowConfigurationResetToSeedDummy {
    _dummy: Option<String>,
}

#[derive(InputObject)]
pub struct ResetConditionInput {
    pub mode: PropagationMode,
    pub old_head_hash: Option<Multihash<'static>>,
    pub recursive: bool,
}

impl ResetConditionInput {
    pub fn new_head_hash(&self) -> Option<Multihash> {
        match &self.mode {
            PropagationMode::Custom(custom_args) => Some(custom_args.new_head_hash.clone()),
            PropagationMode::ToSeed(_) => None,
        }
    }
}

impl From<ResetConditionInput> for FlowRunConfiguration {
    fn from(value: ResetConditionInput) -> Self {
        Self::Reset(value)
    }
}

#[derive(OneofObject, Copy, Clone)]
pub enum CompactionConditionInput {
    Full(CompactionConditionFull),
    MetadataOnly(CompactionConditionMetadataOnly),
}

#[derive(InputObject, Copy, Clone)]
pub struct CompactionConditionFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

#[derive(InputObject, Copy, Clone)]
pub struct CompactionConditionMetadataOnly {
    pub recursive: bool,
}

impl From<CompactionConditionInput> for FlowRunConfiguration {
    fn from(value: CompactionConditionInput) -> Self {
        Self::Compaction(value)
    }
}

#[derive(InputObject, Copy, Clone)]
pub struct IngestConditionInput {
    /// Flag indicates to ignore cache during ingest step for API calls
    pub fetch_uncacheable: bool,
}

impl From<IngestConditionInput> for FlowConfigRuleIngest {
    fn from(value: IngestConditionInput) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
        }
    }
}

impl From<IngestConditionInput> for FlowRunConfiguration {
    fn from(value: IngestConditionInput) -> Self {
        Self::Ingest(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Copy, Clone)]
pub enum FlowConfigurationInput {
    Ingest(IngestConditionInput),
    Compaction(CompactionConditionInput),
}

impl From<FlowConfigurationInput> for FlowRunConfiguration {
    fn from(value: FlowConfigurationInput) -> Self {
        match value {
            FlowConfigurationInput::Ingest(ingest_input) => Self::Ingest(ingest_input),
            FlowConfigurationInput::Compaction(compaction_input) => {
                Self::Compaction(compaction_input)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowRunConfiguration {
    pub async fn try_into_snapshot(
        ctx: &Context<'_>,
        dataset_flow_type: &DatasetFlowType,
        dataset_handle: &odf::DatasetHandle,
        flow_run_configuration_maybe: Option<&FlowRunConfiguration>,
    ) -> Result<Option<FlowConfigurationRule>, FlowInvalidRunConfigurations> {
        match dataset_flow_type {
            DatasetFlowType::Ingest => {
                if let Some(flow_run_configuration) = flow_run_configuration_maybe {
                    if let Self::Ingest(ingest_input) = flow_run_configuration {
                        return Ok(Some(
                            FlowConfigRuleIngest {
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
                                CompactionConditionInput::Full(compaction_input) => {
                                    FlowConfigRuleCompact::Full(
                                        FlowConfigRuleCompactFull::new_checked(
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
                                CompactionConditionInput::MetadataOnly(compaction_input) => {
                                    FlowConfigRuleCompact::MetadataOnly {
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
                            FlowConfigRuleReset {
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
                    FlowConfigRuleReset {
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
