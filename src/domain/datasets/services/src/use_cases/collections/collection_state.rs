// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Projects the changelog of a collection into its current entries, one per
/// path
pub(crate) fn project_collection_state(
    changelog: odf::utils::data::DataFrameExt,
) -> Result<odf::utils::data::DataFrameExt, InternalError> {
    odf::utils::data::changelog::project(
        changelog,
        &["path".to_string()],
        &odf::metadata::DatasetVocabulary::default(),
    )
    .int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
