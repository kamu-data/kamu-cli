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

pub const PROPERTY_VALUE_BOOLEAN_TRUE: &str = "true";
pub const PROPERTY_VALUE_BOOLEAN_FALSE: &str = "false";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type PropertyValue<'a> = Cow<'a, str>;

pub trait PropertyValueExt {
    fn is_true(&self) -> bool;
}

impl PropertyValueExt for PropertyValue<'_> {
    fn is_true(&self) -> bool {
        self == PROPERTY_VALUE_BOOLEAN_TRUE
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PropertyName {
    Dataset(DatasetPropertyName),
    Account(AccountPropertyName),
}

impl PropertyName {
    pub fn dataset_allows_anonymous_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let (name, value) = DatasetPropertyName::allows_anonymous_read(allows);

        (Self::Dataset(name), value)
    }

    pub fn dataset_allows_public_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let (name, value) = DatasetPropertyName::allows_public_read(allows);

        (Self::Dataset(name), value)
    }

    pub fn account_is_admin<'a>(yes: bool) -> (Self, PropertyValue<'a>) {
        let (name, value) = AccountPropertyName::is_admin(yes);

        (Self::Account(name), value)
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

impl DatasetPropertyName {
    pub fn allows_anonymous_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let value = boolean_property_value(allows);

        (DatasetPropertyName::AllowsAnonymousRead, value.into())
    }

    pub fn allows_public_read<'a>(allows: bool) -> (Self, PropertyValue<'a>) {
        let value = boolean_property_value(allows);

        (DatasetPropertyName::AllowsPublicRead, value.into())
    }
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
    IsAdmin,
}

impl AccountPropertyName {
    pub fn is_admin<'a>(yes: bool) -> (Self, PropertyValue<'a>) {
        let value = boolean_property_value(yes);

        (AccountPropertyName::IsAdmin, value.into())
    }
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

fn boolean_property_value(value: bool) -> &'static str {
    if value {
        PROPERTY_VALUE_BOOLEAN_TRUE
    } else {
        PROPERTY_VALUE_BOOLEAN_FALSE
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
