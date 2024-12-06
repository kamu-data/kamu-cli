// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;

use futures::Stream;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct PaginationOpts {
    pub limit: usize,
    pub offset: usize,
}

impl PaginationOpts {
    pub fn safe_limit(&self, total: usize) -> usize {
        let rest = total.saturating_sub(self.offset);

        self.limit.min(rest)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EntityPageListing<Entity> {
    pub list: Vec<Entity>,
    pub total_count: usize,
}

pub type EntityPageStream<'a, Entity> =
    Pin<Box<dyn Stream<Item = Result<Entity, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EntityPageStreamer {
    start_offset: usize,
    page_limit: usize,
}

impl Default for EntityPageStreamer {
    fn default() -> Self {
        Self {
            start_offset: 0,
            page_limit: 100,
        }
    }
}

impl EntityPageStreamer {
    pub fn new(start_offset: usize, page_limit: usize) -> Self {
        Self {
            start_offset,
            page_limit,
        }
    }

    pub fn into_stream<'a, Entity, Args, HInitArgs, HInitArgsFut, HListing, HListingFut>(
        self,
        get_args_callback: HInitArgs,
        next_entities_callback: HListing,
    ) -> EntityPageStream<'a, Entity>
    where
        Entity: Send + 'a,

        Args: Clone + Send + 'a,

        HInitArgs: FnOnce() -> HInitArgsFut + Send + 'a,
        HInitArgsFut: Future<Output = Result<Args, InternalError>> + Send + 'a,

        HListing: Fn(Args, PaginationOpts) -> HListingFut + Send + 'a,
        HListingFut: Future<Output = Result<EntityPageListing<Entity>, InternalError>> + Send + 'a,
    {
        let init_offset = self.start_offset;
        let init_limit = self.page_limit;

        Box::pin(async_stream::try_stream! {
            // Init arguments
            let args = get_args_callback().await?;

            // Tracking pagination progress
            let mut offset = init_offset;
            let limit = init_limit;

            loop {
                // Load a page of dataset entities
                let entities_page =
                    next_entities_callback(args.clone(), PaginationOpts { limit, offset })
                        .await?;

                // Actually read entities
                let loaded_entries_count = entities_page.list.len();

                // Stream the entities
                for entity in entities_page.list {
                    yield entity;
                }

                // Next page
                offset += loaded_entries_count;
                if offset >= entities_page.total_count {
                    break;
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
