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
pub enum DatasetArchetype {
    Collection,
    VersionedFile,
}

impl From<odf::schema::ext::DatasetArchetype> for DatasetArchetype {
    fn from(value: odf::schema::ext::DatasetArchetype) -> Self {
        match value {
            odf::schema::ext::DatasetArchetype::Collection => Self::Collection,
            odf::schema::ext::DatasetArchetype::VersionedFile => Self::VersionedFile,
        }
    }
}

impl From<DatasetArchetype> for odf::schema::ext::DatasetArchetype {
    fn from(value: DatasetArchetype) -> Self {
        match value {
            DatasetArchetype::Collection => Self::Collection,
            DatasetArchetype::VersionedFile => Self::VersionedFile,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
