// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use futures::Stream;
use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct PaginationOpts {
    pub limit: usize,
    pub offset: usize,
}

impl PaginationOpts {
    pub fn all() -> Self {
        PaginationOpts {
            // i64 type is used because sometimes repositories use a conversion to it
            limit: usize::try_from(i64::MAX).unwrap(),
            offset: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EntityStreamer<Entity> {
    start_offset: usize,
    page_limit: usize,
    _phantom: PhantomData<Entity>,
}

impl<Entity> Default for EntityStreamer<Entity> {
    fn default() -> Self {
        Self {
            start_offset: 0,
            page_limit: 100,
            _phantom: PhantomData,
        }
    }
}

pub struct EntityListing<Entity> {
    pub list: Vec<Entity>,
    pub total_count: usize,
}

impl<Entity> EntityStreamer<Entity>
where
    Entity: Send,
{
    pub type EntityStream<'a> =
        Pin<Box<dyn Stream<Item = Result<Entity, InternalError>> + Send + 'a>>;

    pub fn new(start_offset: usize, page_limit: usize) -> Self {
        Self {
            start_offset,
            page_limit,
            _phantom: PhantomData,
        }
    }

    pub fn stream_entities<'a, Args, HInitArgs, HInitArgsFut, HListing, HListingFut>(
        &'a self,
        get_args_callback: HInitArgs,
        next_entities_callback: HListing,
    ) -> Self::EntityStream<'_>
    where
        Args: Clone + Send + 'a,

        HInitArgs: FnOnce() -> HInitArgsFut + Send + 'a,
        HInitArgsFut: Future<Output = Result<Args, InternalError>> + Send + 'a,

        HListing: Fn(Args, PaginationOpts) -> HListingFut + Send + 'a,
        HListingFut: Future<Output = Result<EntityListing<Entity>, InternalError>> + Send + 'a,
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
                        .await
                        .int_err()?;

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
