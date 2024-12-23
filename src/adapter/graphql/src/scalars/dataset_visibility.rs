// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetVisibility {
    Private,
    Public,
}

impl From<odf::DatasetVisibility> for DatasetVisibility {
    fn from(value: odf::DatasetVisibility) -> Self {
        match value {
            odf::DatasetVisibility::Private => Self::Private,
            odf::DatasetVisibility::Public => Self::Public,
        }
    }
}

impl From<DatasetVisibility> for odf::DatasetVisibility {
    fn from(value: DatasetVisibility) -> Self {
        match value {
            DatasetVisibility::Private => odf::DatasetVisibility::Private,
            DatasetVisibility::Public => odf::DatasetVisibility::Public,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
