// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DatasetPropertyName {
    AllowsAnonymousRead,
    AllowsPublicRead,
}

impl From<DatasetPropertyName> for PropertyName {
    fn from(value: DatasetPropertyName) -> PropertyName {
        PropertyName::Dataset(value)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccountPropertyName {
    // TBA
}

impl From<AccountPropertyName> for PropertyName {
    fn from(value: AccountPropertyName) -> PropertyName {
        PropertyName::Account(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
