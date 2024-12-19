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

/// `EntityPageStreamer` is a helper hiding the pagination logic.
/// The default page size is `100` rows, but this value can be changed if
/// desired.
///
/// The primary method is [`EntityPageStreamer::into_stream()`].
///
/// # Examples
/// ```
/// // Usage without arguments for page callback:
/// let dataset_handles_stream = EntityPageStreamer::default().into_stream(
///     || async { Ok(()) },
///     |_, pagination| self.list_all_dataset_handles(pagination),
/// );
///
/// // An example use case with passing some value to each page callback:
/// let dataset_handles_by_owner_name_stream = EntityPageStreamer::default().into_stream(
///     move || async move {
///         let owner_id = self
///             .resolve_account_id_by_maybe_name(Some(&owner_name))
///             .await?;
///         Ok(Arc::new(owner_id))
///     },
///     move |owner_id, pagination| async move {
///         self.list_all_dataset_handles_by_owner_name(&owner_id, pagination)
///             .await
///     },
/// )
///
/// // More examples can be found in unit tests.
/// ```
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

    /// # Arguments
    /// * `get_args_callback` - a function to generating arguments for
    ///   `next_entities_callback`. Note, it is only called once.
    /// * `next_entities_callback` - a function that will be called for each
    ///   page.
    ///
    /// # Examples
    /// You can find examples of use in [`EntityPageStreamer`].
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
