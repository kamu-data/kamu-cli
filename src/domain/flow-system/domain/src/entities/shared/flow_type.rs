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

    pub fn dataset_kind_restriction(&self) -> Option<odf::DatasetKind> {
        match self {
            DatasetFlowType::Ingest | DatasetFlowType::HardCompaction => {
                Some(odf::DatasetKind::Root)
            }
            DatasetFlowType::ExecuteTransform => Some(odf::DatasetKind::Derivative),
            DatasetFlowType::Reset => None,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
