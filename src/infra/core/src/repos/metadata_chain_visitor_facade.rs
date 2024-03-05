// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use kamu_core::{
    Decision,
    HashedMetadataBlockRef,
    MetadataBlockTypeFlags,
    MetadataChainVisitor,
    VisitorsMutRef,
};

///////////////////////////////////////////////////////////////////////////////

pub type DecisionsMutRef<'a> = &'a mut [Decision];

///////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainVisitorFacade<'a, 'b, E> {
    decisions: DecisionsMutRef<'a>,
    visitors: VisitorsMutRef<'a, 'b, E>,
}

impl<'a, 'b, E> MetadataChainVisitorFacade<'a, 'b, E>
where
    E: Error,
{
    pub fn new(visitors: VisitorsMutRef<'a, 'b, E>) -> MetadataChainVisitorFacade<'a, 'b, E> {
        Self {
            decisions: &mut *Vec::with_capacity(visitors.len()),
            visitors,
        }
    }

    pub fn with_decisions(
        decisions: DecisionsMutRef<'a>,
        visitors: VisitorsMutRef<'a, 'b, E>,
    ) -> MetadataChainVisitorFacade<'a, 'b, E> {
        assert_eq!(decisions.len(), visitors.len());

        Self {
            decisions,
            visitors,
        }
    }

    pub fn finished(&mut self) -> bool {
        self.decisions
            .iter()
            .all(|decision| matches!(*decision, Decision::Stop))
    }

    pub fn visit(&mut self, hashed_block_ref: HashedMetadataBlockRef) -> Result<bool, E> {
        let (hash, block) = hashed_block_ref;
        let mut stopped_visitors = 0;

        for (decision, visitor) in self.decisions.iter_mut().zip(self.visitors.iter_mut()) {
            match decision {
                Decision::Stop => {
                    stopped_visitors += 1;
                }
                Decision::Next => {
                    *decision = visitor.visit(hashed_block_ref)?;
                }
                Decision::NextWithHash(requested_hash) => {
                    if hash == requested_hash {
                        *decision = visitor.visit(hashed_block_ref)?;
                    }
                }
                Decision::NextOfType(requested_flags) => {
                    let block_flag = MetadataBlockTypeFlags::from(block);

                    if requested_flags.contains(block_flag) {
                        *decision = visitor.visit(hashed_block_ref)?;
                    }
                }
            }
        }

        let all_visitors_finished = self.visitors.len() == stopped_visitors;

        Ok(all_visitors_finished)
    }
}

///////////////////////////////////////////////////////////////////////////////
