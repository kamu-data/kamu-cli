// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use internal_error::{InternalError, ResultIntoInternal};
use odf::dataset::AcceptByIntervalOptions;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) enum DependencyChange {
    Unchanged,
    Dropped,
    Changed(HashSet<odf::DatasetID>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn extract_modified_dependencies_in_interval(
    metadata_chain: &dyn odf::MetadataChain,
    head: &odf::Multihash,
    maybe_tail: Option<&odf::Multihash>,
) -> Result<DependencyChange, InternalError> {
    let mut new_upstream_ids: HashSet<odf::DatasetID> = HashSet::new();

    let mut set_transform_visitor = odf::dataset::SearchSetTransformVisitor::new();
    let mut seed_visitor = odf::dataset::SearchSeedVisitor::new();

    use odf::dataset::MetadataChainExt;
    metadata_chain
        .accept_by_interval_ext(
            &mut [&mut set_transform_visitor, &mut seed_visitor],
            Some(head),
            maybe_tail,
            AcceptByIntervalOptions {
                inclusive_tail: true,
                ignore_missing_tail: true,
            },
        )
        .await
        .int_err()?;

    if let Some(event) = set_transform_visitor.into_event() {
        for new_input in &event.inputs {
            if let Some(id) = new_input.dataset_ref.id() {
                new_upstream_ids.insert(id.clone());
            }
        }
    }

    // 3 cases:
    //  - we see `SetTransform` event that is relevant now (changed)
    //  - we don't see it and stop where asked (unchanged)
    //  - we don't see it and reach seed (dropped)
    if !new_upstream_ids.is_empty() {
        Ok(DependencyChange::Changed(new_upstream_ids))
    } else if seed_visitor.into_event().is_some() {
        Ok(DependencyChange::Dropped)
    } else {
        Ok(DependencyChange::Unchanged)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
