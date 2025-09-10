// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::DatasetEntryService;
use kamu_flow_system::{self as fs};

use crate::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_RESET_TO_METADATA,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn ingest_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_INGEST,
        FlowScopeDataset::make_scope(dataset_id),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn transform_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_TRANSFORM,
        FlowScopeDataset::make_scope(dataset_id),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn compaction_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_COMPACT,
        FlowScopeDataset::make_scope(dataset_id),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn reset_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_RESET,
        FlowScopeDataset::make_scope(dataset_id),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn reset_to_metadata_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_RESET_TO_METADATA,
        FlowScopeDataset::make_scope(dataset_id),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn make_dataset_flow_sort_key(
    dataset_entry_service: &dyn DatasetEntryService,
    dataset_id: &odf::DatasetID,
) -> Result<String, InternalError> {
    let dataset_entry = dataset_entry_service
        .get_entry(dataset_id)
        .await
        .int_err()?;

    Ok(dataset_entry.alias().to_string())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
