// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{
    CompactionRule,
    CompactionRuleFull,
    CompactionRuleMetadataOnly,
    FlowConfigurationRule,
    IngestRule,
    ResetRule,
};
use odf::dataset::MetadataChainExt as _;

use crate::mutations::{FlowInvalidRunConfigurations, FlowTypeIsNotSupported};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfiguration {
    pub ingest: Option<FlowConfigurationIngest>,
    pub compaction: Option<FlowConfigurationCompaction>,
    pub reset: Option<FlowConfigurationReset>,
}

impl From<kamu_flow_system::FlowConfigurationState> for FlowConfiguration {
    fn from(value: kamu_flow_system::FlowConfigurationState) -> Self {
        Self {
            ingest: if let FlowConfigurationRule::IngestRule(ingest_rule) = &value.rule {
                Some(ingest_rule.clone().into())
            } else {
                None
            },
            reset: if let FlowConfigurationRule::ResetRule(condition) = &value.rule {
                Some(condition.clone().into())
            } else {
                None
            },
            compaction: if let FlowConfigurationRule::CompactionRule(compaction_args) = &value.rule
            {
                match compaction_args {
                    CompactionRule::Full(compaction_rule) => {
                        Some(FlowConfigurationCompaction::Full((*compaction_rule).into()))
                    }
                    CompactionRule::MetadataOnly(compaction_rule) => Some(
                        FlowConfigurationCompaction::MetadataOnly((*compaction_rule).into()),
                    ),
                }
            } else {
                None
            },
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfigurationIngest {
    pub fetch_uncacheable: bool,
}

impl From<IngestRule> for FlowConfigurationIngest {
    fn from(value: IngestRule) -> Self {
        Self {
            fetch_uncacheable: value.fetch_uncacheable,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfigurationReset {
    pub mode: SnapshotPropagationMode,
    pub old_head_hash: Option<Multihash>,
    pub recursive: bool,
}

#[derive(Union, Clone, PartialEq, Eq)]
pub enum SnapshotPropagationMode {
    Custom(SnapshotConfigurationResetCustom),
    ToSeed(SnapshotConfigurationResetToSeedDummy),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct SnapshotConfigurationResetCustom {
    pub new_head_hash: Multihash,
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct SnapshotConfigurationResetToSeedDummy {
    pub dummy: String,
}

impl From<ResetRule> for FlowConfigurationReset {
    fn from(value: ResetRule) -> Self {
        let propagation_mode = if let Some(new_head_hash) = &value.new_head_hash {
            SnapshotPropagationMode::Custom(SnapshotConfigurationResetCustom {
                new_head_hash: new_head_hash.clone().into(),
            })
        } else {
            SnapshotPropagationMode::ToSeed(SnapshotConfigurationResetToSeedDummy {
                dummy: String::new(),
            })
        };
        Self {
            mode: propagation_mode,
            old_head_hash: value.old_head_hash.map(Into::into),
            recursive: value.recursive,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone, PartialEq, Eq)]
pub enum FlowConfigurationCompaction {
    Full(CompactionFull),
    MetadataOnly(CompactionMetadataOnly),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct CompactionFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

impl From<CompactionRuleFull> for CompactionFull {
    fn from(value: CompactionRuleFull) -> Self {
        Self {
            max_slice_records: value.max_slice_records(),
            max_slice_size: value.max_slice_size(),
            recursive: value.recursive(),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct CompactionMetadataOnly {
    pub recursive: bool,
}

impl From<CompactionRuleMetadataOnly> for CompactionMetadataOnly {
    fn from(value: CompactionRuleMetadataOnly) -> Self {
        Self {
            recursive: value.recursive,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject)]
pub enum FlowRunConfiguration {
    Compaction(CompactionConditionInput),
    Ingest(IngestConditionInput),
    Reset(ResetConditionInput),
}

#[derive(OneofObject, Clone)]
pub enum PropagationMode {
    Custom(FlowConfigurationResetCustom),
    ToSeed(FlowConfigurationResetToSeedDummy),
}

#[derive(InputObject, Clone)]
pub struct FlowConfigurationResetCustom {
    pub new_head_hash: Multihash,
}

#[derive(InputObject, Clone)]
pub struct FlowConfigurationResetToSeedDummy {
    dummy: String,
}

#[derive(InputObject, Clone)]
pub struct ResetConditionInput {
    pub mode: PropagationMode,
    pub old_head_hash: Option<Multihash>,
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

#[derive(OneofObject, Clone)]
pub enum CompactionConditionInput {
    Full(CompactionConditionFull),
    MetadataOnly(CompactionConditionMetadataOnly),
}

#[derive(InputObject, Clone)]
pub struct CompactionConditionFull {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
    pub recursive: bool,
}

#[derive(InputObject, Clone)]
pub struct CompactionConditionMetadataOnly {
    pub recursive: bool,
}

impl From<CompactionConditionInput> for FlowRunConfiguration {
    fn from(value: CompactionConditionInput) -> Self {
        Self::Compaction(value)
    }
}

#[derive(InputObject, Clone)]
pub struct IngestConditionInput {
    /// Flag indicates to ignore cache during ingest step for API calls
    pub fetch_uncacheable: bool,
}

impl From<IngestConditionInput> for IngestRule {
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

#[derive(OneofObject, Clone)]
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
                        return Ok(Some(FlowConfigurationRule::IngestRule(IngestRule {
                            fetch_uncacheable: ingest_input.fetch_uncacheable,
                        })));
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
                        return Ok(Some(FlowConfigurationRule::CompactionRule(
                            match compaction_input {
                                CompactionConditionInput::Full(compaction_input) => {
                                    CompactionRule::Full(
                                        CompactionRuleFull::new_checked(
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
                                    CompactionRule::MetadataOnly(CompactionRuleMetadataOnly {
                                        recursive: compaction_input.recursive,
                                    })
                                }
                            },
                        )));
                    }
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                }
            }
            DatasetFlowType::Reset => {
                let dataset_registry =
                    crate::utils::unsafe_from_catalog_n!(ctx, dyn kamu_core::DatasetRegistry);
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
                        return Ok(Some(FlowConfigurationRule::ResetRule(ResetRule {
                            new_head_hash: reset_input.new_head_hash().map(Into::into),
                            old_head_hash,
                            recursive: reset_input.recursive,
                        })));
                    }
                    return Err(FlowInvalidRunConfigurations {
                        error: "Incompatible flow run configuration and dataset flow type"
                            .to_string(),
                    });
                }
                return Ok(Some(FlowConfigurationRule::ResetRule(ResetRule {
                    new_head_hash: None,
                    old_head_hash: current_head_hash,
                    recursive: false,
                })));
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
