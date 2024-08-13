// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccountToDatasetRelation {
    Reader,
    Editor,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
