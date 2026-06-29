// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources::ResourceID;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ReplaceDatasetBindingsError {
    #[error(transparent)]
    Duplicate(#[from] DatasetResourceBindingDuplicateError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Duplicate dataset binding for dataset '{dataset_id}' and resource '{resource_id}'")]
pub struct DatasetResourceBindingDuplicateError {
    pub dataset_id: odf::DatasetID,
    pub resource_id: ResourceID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
