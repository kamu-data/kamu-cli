// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use serde::{Deserialize, Serialize};

use super::CompactionRuleFull;
use crate::{CompactionRuleMetadataOnly, IngestRule, ResetRule, Schedule, TransformRule};

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "dataset_flow_type", rename_all = "snake_case")]
pub enum DatasetFlowType {
    Ingest,
    ExecuteTransform,
    HardCompaction,
    Reset,
}

impl DatasetFlowType {
    pub fn all() -> &'static [DatasetFlowType] {
        &[
            Self::Ingest,
            Self::ExecuteTransform,
            Self::HardCompaction,
            Self::Reset,
        ]
    }

    pub fn dataset_kind_restriction(
        &self,
        flow_cofiguration_rule_type_maybe: Option<&str>,
    ) -> Option<opendatafabric::DatasetKind> {
        match self {
            DatasetFlowType::Ingest => Some(opendatafabric::DatasetKind::Root),
            DatasetFlowType::ExecuteTransform => Some(opendatafabric::DatasetKind::Derivative),
            DatasetFlowType::Reset => None,
            DatasetFlowType::HardCompaction => {
                if let Some(flow_cofiguration_rule_type) = flow_cofiguration_rule_type_maybe
                    && flow_cofiguration_rule_type == std::any::type_name::<CompactionRuleFull>()
                {
                    return Some(opendatafabric::DatasetKind::Root);
                }
                None
            }
        }
    }

    pub fn config_restriction(&self, flow_configuration_type: &'static str) -> bool {
        match self {
            DatasetFlowType::Ingest => {
                flow_configuration_type == std::any::type_name::<Schedule>()
                    || flow_configuration_type == std::any::type_name::<IngestRule>()
            }
            DatasetFlowType::ExecuteTransform => {
                flow_configuration_type == std::any::type_name::<TransformRule>()
            }
            DatasetFlowType::HardCompaction => {
                flow_configuration_type == std::any::type_name::<CompactionRuleMetadataOnly>()
                    || flow_configuration_type == std::any::type_name::<CompactionRuleFull>()
            }
            DatasetFlowType::Reset => flow_configuration_type == std::any::type_name::<ResetRule>(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "system_flow_type", rename_all = "snake_case")]
pub enum SystemFlowType {
    GC,
}

impl SystemFlowType {
    pub fn all() -> &'static [SystemFlowType] {
        &[Self::GC]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AnyFlowType {
    Dataset(DatasetFlowType),
    System(SystemFlowType),
}

impl AnyFlowType {
    /// What should be the reaction on flow success
    pub fn success_followup_method(&self) -> FlowSuccessFollowupMethod {
        match self {
            AnyFlowType::Dataset(
                DatasetFlowType::Ingest
                | DatasetFlowType::ExecuteTransform
                | DatasetFlowType::HardCompaction
                | DatasetFlowType::Reset,
            ) => FlowSuccessFollowupMethod::TriggerDependent,
            _ => FlowSuccessFollowupMethod::Ignore,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum FlowSuccessFollowupMethod {
    /// Nothing should happen if flow succeeds
    Ignore,

    /// If flow succeeds, it's dependent flows should trigger
    TriggerDependent,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
