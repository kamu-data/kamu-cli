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

/// Well-known kind of a dataset that defines how its data is interpreted
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "odf::schema::ext::DatasetArchetype")]
pub enum DatasetArchetype {
    /// Set of entries linking paths to other datasets
    Collection,
    /// File whose every version is stored as a record
    VersionedFile,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
