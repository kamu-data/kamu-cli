// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Check if the seed block has the same `dataset_id` and `kind` as the existing
/// dataset.
pub(crate) fn ensure_seed_not_in_conflict(
    first_incoming_block_maybe: Option<&odf::dataset::HashedMetadataBlockBytesDecoded>,
    existing_dataset_handle: &odf::DatasetHandle,
) -> bool {
    if let Some((_, first_incoming_block, _)) = first_incoming_block_maybe
        && let odf::MetadataEvent::Seed(seed_event) = &first_incoming_block.event
        && (seed_event.dataset_id != existing_dataset_handle.id
            || seed_event.dataset_kind != existing_dataset_handle.kind)
    {
        return false;
    }
    true
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
