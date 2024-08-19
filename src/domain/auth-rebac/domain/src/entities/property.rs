// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::str::FromStr;

use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const PROPERTY_GROUP_SEPARATOR: &str = "/";
const PROPERTY_GROUP_DATASET: &str = "dataset";
const PROPERTY_GROUP_ACCOUNT: &str = "account";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type PropertyValue<'a> = Cow<'a, str>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PropertyName {
    Dataset(DatasetPropertyName),
    Account(AccountPropertyName),
}

impl PropertyName {
    pub fn dataset_allows_anonymous_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let value = if allows { "true" } else { "false" };

        (
            Self::Dataset(DatasetPropertyName::AllowsAnonymousRead),
            value.into(),
        )
    }

    pub fn dataset_allows_public_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let value = if allows { "true" } else { "false" };

        (
            Self::Dataset(DatasetPropertyName::AllowsPublicRead),
            value.into(),
        )
    }

    pub fn property_group(&self) -> &'static str {
        match self {
            PropertyName::Dataset(_) => PROPERTY_GROUP_DATASET,
            PropertyName::Account(_) => PROPERTY_GROUP_ACCOUNT,
        }
    }
}

impl std::fmt::Display for PropertyName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dataset(dataset_property) => {
                write!(
                    f,
                    "{PROPERTY_GROUP_DATASET}{PROPERTY_GROUP_SEPARATOR}{dataset_property}"
                )
            }
            Self::Account(account_property) => {
                write!(
                    f,
                    "{PROPERTY_GROUP_ACCOUNT}{PROPERTY_GROUP_SEPARATOR}{account_property}"
                )
            }
        }
    }
}

impl FromStr for PropertyName {
    type Err = InternalError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let split: Vec<&str> = value.split(PROPERTY_GROUP_SEPARATOR).collect();
        let [property_group, property_name] = split[..] else {
            return InternalError::bail(format!("Invalid format for value: '{value}'"));
        };

        let res = match property_group {
            group @ PROPERTY_GROUP_DATASET => {
                let dataset_property = property_name
                    .parse::<DatasetPropertyName>()
                    .context_int_err(format!("group '{group}', property_name '{property_name}'"))?;

                Self::Dataset(dataset_property)
            }
            group @ PROPERTY_GROUP_ACCOUNT => {
                let account_property = property_name
                    .parse::<AccountPropertyName>()
                    .context_int_err(format!("group '{group}', property_name '{property_name}'"))?;

                Self::Account(account_property)
            }
            unexpected_property_group => {
                return InternalError::bail(format!(
                    "Unexpected property group: '{unexpected_property_group}'"
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
#[strum(serialize_all = "snake_case")]
pub enum DatasetPropertyName {
    AllowsAnonymousRead,
    AllowsPublicRead,
}

impl From<DatasetPropertyName> for PropertyName {
    fn from(value: DatasetPropertyName) -> PropertyName {
        PropertyName::Dataset(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, strum::EnumString, strum::Display,
)]
#[strum(serialize_all = "snake_case")]
pub enum AccountPropertyName {
    // TODO: ReBAC: absorb the `is_admin` attribute from the Accounts domain
    //       https://github.com/kamu-data/kamu-cli/issues/766
    IsAnAdmin,
}

impl From<AccountPropertyName> for PropertyName {
    fn from(value: AccountPropertyName) -> PropertyName {
        PropertyName::Account(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct PropertyRowModel {
    pub property_name: String,
    pub property_value: String,
}

#[cfg(feature = "sqlx")]
impl TryFrom<PropertyRowModel> for (PropertyName, PropertyValue<'static>) {
    type Error = InternalError;

    fn try_from(row_model: PropertyRowModel) -> Result<Self, Self::Error> {
        let property_name = row_model.property_name.parse()?;
        let property_value = row_model.property_value.into();

        Ok((property_name, property_value))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
