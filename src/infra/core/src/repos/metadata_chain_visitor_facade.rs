// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{AppendError, HashedMetadataBlockRef};

use crate::{Decision, MetadataBlockTypeFlags, MetadataChainVisitor};

///////////////////////////////////////////////////////////////////////////////

pub type StackVisitorsWithDecisionsMutRef<'a> =
    &'a mut [(Decision, &'a mut dyn MetadataChainVisitor)];

///////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainVisitorFacade<'a, 'b> {
    visitors: &'b mut StackVisitorsWithDecisionsMutRef<'a>,
}

impl<'a, 'b> MetadataChainVisitorFacade<'a, 'b> {
    pub fn new(
        visitors: &'b mut StackVisitorsWithDecisionsMutRef<'a>,
    ) -> MetadataChainVisitorFacade<'a, 'b> {
        Self { visitors }
    }

    pub fn visit(&mut self) -> Result<bool, AppendError> {
        for (decision, visitor) in self.visitors.iter_mut() {
            *decision = visitor.visit()?;
        }

        let all_visitors_finished = self
            .visitors
            .iter()
            .all(|(decision, _)| matches!(*decision, Decision::Stop));

        Ok(all_visitors_finished)
    }

    pub fn visit_with_block(
        &mut self,
        hashed_block_ref: HashedMetadataBlockRef,
    ) -> Result<bool, AppendError> {
        let (hash, block) = hashed_block_ref;
        let mut stopped_visitors = 0;

        for (decision, visitor) in self.visitors.iter_mut() {
            match decision {
                Decision::Stop => {
                    stopped_visitors += 1;
                }
                Decision::NextWithHash(requested_hash) => {
                    if hash == requested_hash {
                        *decision = visitor.visit_with_block(hashed_block_ref)?;
                    }
                }
                Decision::NextOfType(requested_flags) => {
                    let block_flag = MetadataBlockTypeFlags::from(block);

                    if requested_flags.contains(block_flag) {
                        *decision = visitor.visit_with_block(hashed_block_ref)?;
                    }
                }
            }
        }

        let all_visitors_finished = self.visitors.len() == stopped_visitors;

        Ok(all_visitors_finished)
    }
}

///////////////////////////////////////////////////////////////////////////////
