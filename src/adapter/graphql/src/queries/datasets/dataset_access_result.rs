// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset the caller may read, or only its ID when the dataset is missing
/// or not readable by the caller
#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DatasetAccessResult<'a> {
    Accessible(DatasetAccessResultAccessible),
    NotAccessible(DatasetAccessResultNotAccessible<'a>),
}

impl DatasetAccessResult<'_> {
    pub fn accessible(dataset: Dataset) -> Self {
        Self::Accessible(DatasetAccessResultAccessible { dataset })
    }

    pub fn not_accessible(dataset_id: odf::DatasetID) -> Self {
        Self::NotAccessible(DatasetAccessResultNotAccessible {
            id: dataset_id.into(),
        })
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DatasetAccessResultAccessible {
    pub dataset: Dataset,
}

#[ComplexObject]
impl DatasetAccessResultAccessible {
    async fn message(&self) -> String {
        "Found".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DatasetAccessResultNotAccessible<'a> {
    pub id: DatasetID<'a>,
}

#[ComplexObject]
impl DatasetAccessResultNotAccessible<'_> {
    async fn message(&self) -> String {
        "Not Accessible".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
