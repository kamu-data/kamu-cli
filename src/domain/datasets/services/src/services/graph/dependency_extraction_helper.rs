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
use odf::dataset::{AcceptByIntervalOptions, MetadataChainVisitor};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) enum DependencyChange {
    Unchanged,
    Dropped,
    Changed(HashSet<odf::DatasetID>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum SeedPresence {
    KnownByHint(bool), // yes/no (w/o iteration)
    SearchRequired(odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::Seed>),
}

impl SeedPresence {
    fn into_bool(self) -> bool {
        match self {
            SeedPresence::KnownByHint(present) => present,
            SeedPresence::SearchRequired(seed_visitor) => seed_visitor.into_event().is_some(),
        }
    }
}

pub(crate) async fn extract_modified_dependencies_in_interval(
    metadata_chain: &dyn odf::MetadataChain,
    head: &odf::Multihash,
    maybe_tail: Option<&odf::Multihash>,
    maybe_hint_flags: Option<odf::metadata::MetadataEventTypeFlags>,
) -> Result<DependencyChange, InternalError> {
    use odf::metadata::MetadataEventTypeFlags as Flag;

    // With hints available, we can immediately assess the necessity of iteration.
    // If there are no necessary events in the interval, exit immediately.
    if let Some(hint_flags) = maybe_hint_flags
        && !hint_flags.contains(Flag::SET_TRANSFORM)
    {
        let has_seed = hint_flags.contains(Flag::SEED);
        // 2 cases:
        //  - no SetTransform but Seed is present, need to reset dependencies (dropped)
        //  - no SetTransform w/o Seed means no dependency changes (unchanged)
        return if has_seed {
            Ok(DependencyChange::Dropped)
        } else {
            Ok(DependencyChange::Unchanged)
        };
    }

    let mut new_upstream_ids: HashSet<odf::DatasetID> = HashSet::new();

    // Prepare visitors:
    // - We need to search for SetTransform in any case, as we need data from the
    //   event itself,
    // - For Seed we only need to know its presence: we search only if we don't
    //   initially know if it exists.
    let mut set_transform_visitor = odf::dataset::SearchSetTransformVisitor::new();
    let mut seed_presence = match maybe_hint_flags {
        Some(flags) => {
            let present = flags.contains(Flag::SEED);
            SeedPresence::KnownByHint(present)
        }
        None => SeedPresence::SearchRequired(odf::dataset::SearchSeedVisitor::new()),
    };

    let mut visitors = Vec::<&mut dyn MetadataChainVisitor<Error = _>>::with_capacity(2);
    visitors.push(&mut set_transform_visitor);

    if let SeedPresence::SearchRequired(seed_visitor) = &mut seed_presence {
        visitors.push(seed_visitor);
    }

    use odf::dataset::MetadataChainExt;
    metadata_chain
        .accept_by_interval_ext(
            &mut visitors,
            Some(head),
            maybe_tail,
            AcceptByIntervalOptions {
                inclusive_tail: true,
                ..Default::default()
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
    } else if seed_presence.into_bool() {
        Ok(DependencyChange::Dropped)
    } else {
        Ok(DependencyChange::Unchanged)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
