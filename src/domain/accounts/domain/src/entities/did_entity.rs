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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "did_entity_type", rename_all = "lowercase")
)]
pub enum DidEntityType {
    Dataset,
    Account,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DidEntityId<'a> = Cow<'a, str>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DidEntity<'a> {
    pub entity_type: DidEntityType,
    pub entity_id: DidEntityId<'a>,
}

impl<'a> DidEntity<'a> {
    pub fn new(entity_type: DidEntityType, entity_id: impl Into<DidEntityId<'a>>) -> Self {
        Self {
            entity_type,
            entity_id: entity_id.into(),
        }
    }

    pub fn new_account(entity_id: impl Into<DidEntityId<'a>>) -> Self {
        Self::new(DidEntityType::Account, entity_id)
    }

    pub fn new_dataset(entity_id: impl Into<DidEntityId<'a>>) -> Self {
        Self::new(DidEntityType::Dataset, entity_id)
    }

    pub fn into_owned(self) -> DidEntity<'static> {
        DidEntity {
            entity_type: self.entity_type,
            entity_id: Cow::Owned(self.entity_id.into_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
