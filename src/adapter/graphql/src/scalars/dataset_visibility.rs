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
pub enum DatasetPublicity {
    Private,
    Public,
}

impl From<domain::DatasetPublicity> for DatasetPublicity {
    fn from(value: domain::DatasetPublicity) -> Self {
        match value {
            domain::DatasetPublicity::Private => Self::Private,
            domain::DatasetPublicity::Public => Self::Public,
        }
    }
}

impl From<DatasetPublicity> for domain::DatasetPublicity {
    fn from(value: DatasetPublicity) -> Self {
        match value {
            DatasetPublicity::Private => domain::DatasetPublicity::Private,
            DatasetPublicity::Public => domain::DatasetPublicity::Public,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
