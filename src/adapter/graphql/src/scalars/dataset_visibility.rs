// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetVisibility {
    Private,
    Public,
}

impl From<domain::DatasetVisibility> for DatasetVisibility {
    fn from(value: domain::DatasetVisibility) -> Self {
        match value {
            domain::DatasetVisibility::Private => Self::Private,
            domain::DatasetVisibility::Public => Self::Public,
        }
    }
}

impl From<DatasetVisibility> for domain::DatasetVisibility {
    fn from(value: DatasetVisibility) -> Self {
        match value {
            DatasetVisibility::Private => domain::DatasetVisibility::Private,
            DatasetVisibility::Public => domain::DatasetVisibility::Public,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
