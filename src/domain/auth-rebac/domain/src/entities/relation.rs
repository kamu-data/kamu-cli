// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const GROUP_SEPARATOR: &str = "/";
const RELATION_GROUP_ACCOUNT_TO_DATASET: &str = "account->dataset";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Relation {
    AccountToDataset(AccountToDatasetRelation),
}

impl Relation {
    pub fn account_is_a_dataset_reader() -> Self {
        Self::AccountToDataset(AccountToDatasetRelation::Reader)
    }

    pub fn account_is_a_dataset_editor() -> Self {
        Self::AccountToDataset(AccountToDatasetRelation::Editor)
    }
}

impl std::fmt::Display for Relation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AccountToDataset(relation) => {
                write!(
                    f,
                    "{RELATION_GROUP_ACCOUNT_TO_DATASET}{GROUP_SEPARATOR}{relation}"
                )
            }
        }
    }
}

impl FromStr for Relation {
    type Err = InternalError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let split: Vec<&str> = value.split(GROUP_SEPARATOR).collect();
        let [relation_group, relation_name] = split[..] else {
            return InternalError::bail(format!("Invalid format for value: '{value}'"));
        };

        let res = match relation_group {
            RELATION_GROUP_ACCOUNT_TO_DATASET => {
                let relation = relation_name
                    .parse::<AccountToDatasetRelation>()
                    .int_err()?;

                Self::AccountToDataset(relation)
            }
            unexpected_property_group => {
                return InternalError::bail(format!(
                    "Unexpected relation group: '{unexpected_property_group}'"
                ))
            }
        };

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, strum::EnumString, strum::Display,
)]
pub enum AccountToDatasetRelation {
    Reader,
    Editor,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct RelationRowModel {
    pub relationship: String,
}

#[cfg(feature = "sqlx")]
impl TryFrom<RelationRowModel> for Relation {
    type Error = InternalError;

    fn try_from(row_model: RelationRowModel) -> Result<Self, Self::Error> {
        let relationship = row_model.relationship.parse()?;

        Ok(relationship)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
